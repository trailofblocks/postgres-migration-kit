package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type phaseReport struct {
	Name          string   `json:"name"`
	V2Percent     int      `json:"v2_weight_percent"`
	DurationMS    int64    `json:"duration_ms"`
	Requests      int      `json:"requests"`
	RequestsPerS  float64  `json:"requests_per_second"`
	V1Requests    int      `json:"v1_requests"`
	V2Requests    int      `json:"v2_requests"`
	Failed        int      `json:"failed"`
	P95MS         float64  `json:"p95_ms"`
	P99MS         float64  `json:"p99_ms"`
	FailureSample []string `json:"failure_sample,omitempty"`
}

type phase struct {
	result   phaseReport
	started  time.Time
	ended    time.Time
	timings  []time.Duration
	next     atomic.Uint64
	mu       sync.Mutex
	dispatch sync.RWMutex
}

func (p *phase) target() int {
	if (p.next.Add(1)*37)%100 < uint64(p.result.V2Percent) {
		return 1
	}
	return 0
}

func (p *phase) record(target int, elapsed time.Duration, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.result.Requests++
	if target == 0 {
		p.result.V1Requests++
	} else {
		p.result.V2Requests++
	}
	p.timings = append(p.timings, elapsed)
	if err != nil {
		p.result.Failed++
		if len(p.result.FailureSample) < 3 {
			p.result.FailureSample = append(p.result.FailureSample, err.Error())
		}
	}
}

func (p *phase) report() phaseReport {
	r := p.result
	r.DurationMS = p.ended.Sub(p.started).Milliseconds()
	if r.DurationMS > 0 {
		r.RequestsPerS = float64(r.Requests) * 1000 / float64(r.DurationMS)
	}
	if r.Requests > 0 {
		sort.Slice(p.timings, func(i, j int) bool { return p.timings[i] < p.timings[j] })
		r.P95MS = percentile(p.timings, 95)
		r.P99MS = percentile(p.timings, 99)
	}
	return r
}

func percentile(sorted []time.Duration, percent int) float64 {
	rank := (len(sorted)*percent + 99) / 100
	return float64(sorted[rank-1].Microseconds()) / 1_000
}

type traffic struct {
	ticker   *time.Ticker
	cancel   context.CancelFunc
	workers  sync.WaitGroup
	current  atomic.Pointer[phase]
	history  []*phase
	nextID   atomic.Uint64
	client   *http.Client
	backends [2]string
	expected map[string]payment
	mu       sync.Mutex
}

func startTraffic(ctx context.Context, cfg benchmarkConfig, backends [2]string) *traffic {
	ctx, cancel := context.WithCancel(ctx)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = cfg.Clients
	w := &traffic{
		ticker: time.NewTicker(time.Second / time.Duration(cfg.Rate)), cancel: cancel,
		client: &http.Client{Timeout: 4 * time.Second, Transport: transport}, backends: backends,
		expected: make(map[string]payment),
	}
	w.switchPhase("baseline_v1", 0)
	for range cfg.Clients {
		w.workers.Go(func() { w.drive(ctx) })
	}
	return w
}

func (w *traffic) switchPhase(name string, v2 int) *phase {
	now := time.Now()
	p := &phase{started: now, result: phaseReport{Name: name, V2Percent: v2}}
	old := w.current.Swap(p)
	if old != nil {
		old.ended = now
	}
	w.history = append(w.history, p)
	return old
}

func (w *traffic) stop() {
	w.cancel()
	w.ticker.Stop()
	w.current.Load().ended = time.Now()
	w.workers.Wait()
	w.client.CloseIdleConnections()
}

func (w *traffic) wait(ctx context.Context) (*phase, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case <-w.ticker.C:
		return w.current.Load(), ctx.Err() == nil
	}
}

func (w *traffic) drive(ctx context.Context) {
	for {
		p, running := w.wait(ctx)
		if !running {
			return
		}
		id := w.nextID.Add(1)
		value := payment{
			ID: fmt.Sprintf("live-%d", id), Amount: 1000 + int64(id%100_000), Currency: "USD", State: "pending",
		}
		if err := w.dispatch(p, value, true); err != nil {
			continue
		}
		p, running = w.wait(ctx)
		if !running {
			return
		}
		value.State = "captured"
		_ = w.dispatch(p, value, false)
	}
}

func (w *traffic) dispatch(p *phase, value payment, create bool) error {
	for {
		p.dispatch.RLock()
		if current := w.current.Load(); current != p {
			p.dispatch.RUnlock()
			p = current
			continue
		}
		err := w.write(p, p.target(), value, create)
		p.dispatch.RUnlock()
		return err
	}
}

func (w *traffic) write(p *phase, target int, value payment, create bool) error {
	method, path, expectedCode := http.MethodPut, "/payments/"+value.ID+"/state", http.StatusOK
	var payload any = map[string]string{"state": value.State}
	if create {
		method, path, expectedCode = http.MethodPost, "/payments", http.StatusCreated
		payload = map[string]any{"id": value.ID, "amount_minor": value.Amount, "currency": value.Currency}
	}
	start := time.Now()
	err := w.request(target, method, path, payload, expectedCode, value)
	p.record(target, time.Since(start), err)
	if err == nil {
		w.mu.Lock()
		w.expected[value.ID] = value
		w.mu.Unlock()
	}
	return err
}

func (w *traffic) request(target int, method string, path string, payload any, code int, expected payment) error {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, w.backends[target]+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	version := fmt.Sprintf("v%d", target+1)
	if resp.StatusCode != code || resp.Header.Get("X-Payments-API") != version {
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("%s %s on %s returned HTTP %d", method, path, version, resp.StatusCode)
	}
	var got payment
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf("%s %s on %s returned different payment data", method, path, version)
	}
	return nil
}

func loopbackAPI(a paymentAPI) (string, func(), error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	server := newServer(a.handler())
	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()
	closeAPI := func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
		<-done
	}
	return "http://" + listener.Addr().String(), closeAPI, nil
}
