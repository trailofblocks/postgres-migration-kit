package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type benchmarkConfig struct {
	SeedRows int           `json:"seed_rows"`
	Batch    int           `json:"batch_size"`
	Rate     int           `json:"target_write_requests_per_second"`
	Clients  int           `json:"concurrent_clients"`
	Hold     time.Duration `json:"-"`
}

type interruption struct {
	WorkerKilled bool     `json:"worker_process_killed"`
	BeforeKill   progress `json:"before_kill"`
	RestartFrom  progress `json:"restart_from"`
	AfterResume  progress `json:"after_resume"`
}

type consistencyReport struct {
	Expected   int      `json:"expected_rows"`
	Found      int      `json:"found_rows"`
	Missing    int      `json:"missing_rows"`
	Unexpected int      `json:"unexpected_rows"`
	Mismatches int      `json:"mismatched_rows"`
	Duplicates int      `json:"duplicate_ids"`
	Examples   []string `json:"examples,omitempty"`
}

type benchmarkReport struct {
	GeneratedAt       time.Time         `json:"generated_at"`
	Configuration     benchmarkConfig   `json:"configuration"`
	HoldMS            int64             `json:"minimum_phase_duration_ms"`
	Host              string            `json:"runner"`
	LogicalCPUs       int               `json:"logical_cpus"`
	Postgres          string            `json:"postgres_version"`
	Phases            []phaseReport     `json:"phases"`
	Interruption      interruption      `json:"interruption"`
	CompatMismatches  int               `json:"compatibility_column_mismatches"`
	RollbackConfirmed bool              `json:"v1_read_payment_written_by_v2_after_rollback"`
	RollbackMS        float64           `json:"route_switch_to_verified_v1_read_ms"`
	Consistency       consistencyReport `json:"final_consistency"`
	FinalStage        string            `json:"final_stage"`
	TotalRequests     int               `json:"total_write_requests"`
	FailedRequests    int               `json:"failed_write_requests"`
	Passed            bool              `json:"passed"`
}

func sandbox(ctx context.Context, dsn string) (store, string, func() error, error) {
	name := "migration_" + strings.ToLower(rand.Text())
	admin, err := openStore(ctx, dsn, "public")
	if err != nil {
		return store{}, "", nil, err
	}
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.pool.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.pool.Close()
		return store{}, "", nil, err
	}
	s, err := openStore(ctx, dsn, name)
	cleanup := func() error {
		if s.pool != nil {
			s.pool.Close()
		}
		defer admin.pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.pool.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE")
		return err
	}
	if err != nil {
		return store{}, "", nil, errors.Join(err, cleanup())
	}
	return s, name, cleanup, nil
}

func workerCommand(ctx context.Context, dsn, schema string, size int, pause time.Duration) (*exec.Cmd, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, exe, "backfill", "--batch-size", strconv.Itoa(size), "--pause", pause.String())
	cmd.Env = append(os.Environ(), "DATABASE_URL="+dsn, "MIGRATION_SCHEMA="+schema)
	return cmd, nil
}

func interruptWorker(ctx context.Context, s store, dsn, schema string, batch int) (interruption, error) {
	var result interruption
	cmd, err := workerCommand(ctx, dsn, schema, batch, 35*time.Millisecond)
	if err != nil {
		return result, err
	}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		return result, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(2 * time.Minute)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-timeout.C:
			return result, errors.New("backfill did not reach the interruption checkpoint within two minutes")
		case err := <-done:
			waited = true
			return result, fmt.Errorf("first worker exited before interruption (%v): %s", err, output.String())
		case <-ticker.C:
			p, err := readProgress(ctx, s.pool, false)
			if err != nil {
				return result, err
			}
			if p.Checkpoint < (p.Cutoff+1)/2 {
				continue
			}
			if p.Checkpoint >= p.Cutoff {
				return result, errors.New("first worker finished before interruption")
			}
			result.BeforeKill = p
			if err := cmd.Process.Kill(); err != nil {
				return result, err
			}
			err = <-done
			waited = true
			var exited *exec.ExitError
			if !errors.As(err, &exited) {
				return result, fmt.Errorf("expected the first worker to be terminated, got %v", err)
			}
			result.WorkerKilled = true
			result.RestartFrom, err = readProgress(ctx, s.pool, false)
			if err == nil && (result.RestartFrom.Checkpoint < p.Checkpoint || result.RestartFrom.Checkpoint >= p.Cutoff) {
				err = errors.New("the interruption did not leave a partially committed backfill")
			}
			return result, err
		}
	}
}

func runBenchmark(ctx context.Context, dsn string, cfg benchmarkConfig) (_ *benchmarkReport, resultErr error) {
	if cfg.Batch < 1 || cfg.Batch > 5000 || cfg.SeedRows < 6*cfg.Batch || cfg.SeedRows > 2_000_000 ||
		cfg.SeedRows > 1000*cfg.Batch ||
		cfg.Rate < 20 || cfg.Rate > 2500 || cfg.Clients < 1 || cfg.Clients > 40 ||
		cfg.Hold < 400*time.Millisecond || cfg.Hold > 20*time.Second {
		return nil, errors.New("use 6–1000 batches of seed rows (up to 2 million), batch size 1–5000, rate 20–2500, clients 1–40 and hold 400ms–20s")
	}
	s, schema, cleanup, err := sandbox(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, cleanup()) }()
	if err := s.init(ctx, cfg.SeedRows); err != nil {
		return nil, err
	}
	r := &benchmarkReport{
		GeneratedAt: time.Now().UTC(), Configuration: cfg, HoldMS: cfg.Hold.Milliseconds(),
		Host: runtime.GOOS + "/" + runtime.GOARCH + " " + runtime.Version(), LogicalCPUs: runtime.NumCPU(),
	}
	if err := s.pool.QueryRow(ctx, "SHOW server_version").Scan(&r.Postgres); err != nil {
		return nil, err
	}
	v1Store, err := openStore(ctx, dsn, schema)
	if err != nil {
		return nil, err
	}
	defer v1Store.pool.Close()
	v1, closeV1, err := loopbackAPI(paymentAPI{store: v1Store, version: "v1"})
	if err != nil {
		return nil, err
	}
	defer closeV1()
	v2Store, err := openStore(ctx, dsn, schema)
	if err != nil {
		return nil, err
	}
	defer v2Store.pool.Close()
	v2, closeV2, err := loopbackAPI(paymentAPI{store: v2Store, version: "v2"})
	if err != nil {
		return nil, err
	}
	defer closeV2()
	work := startTraffic(ctx, cfg, [2]string{v1, v2})
	active := true
	defer func() {
		if active {
			work.stop()
		}
	}()
	if err := exerciseMigration(ctx, s, work, r, dsn, schema, cfg); err != nil {
		return nil, err
	}
	work.stop()
	active = false
	r.Consistency, err = compareRows(ctx, s, cfg.SeedRows, work.expected)
	if err != nil {
		return nil, err
	}
	p, err := readProgress(ctx, s.pool, false)
	if err != nil {
		return nil, err
	}
	r.FinalStage = p.Stage
	r.Passed = r.CompatMismatches == 0 && r.RollbackConfirmed && p.Stage == stageContracted &&
		r.Consistency.Missing+r.Consistency.Mismatches+r.Consistency.Unexpected+r.Consistency.Duplicates == 0
	for _, p := range work.history {
		report := p.report()
		r.Phases = append(r.Phases, report)
		r.TotalRequests += report.Requests
		r.FailedRequests += report.Failed
		if report.Failed > 0 || report.Requests == 0 ||
			(report.V2Percent == 25 && (report.V1Requests == 0 || report.V2Requests == 0)) {
			r.Passed = false
		}
	}
	if !r.Passed {
		return r, errors.New("migration benchmark failed one or more verification checks")
	}
	return r, nil
}

func exerciseMigration(ctx context.Context, s store, w *traffic, r *benchmarkReport, dsn, schema string, cfg benchmarkConfig) error {
	hold := func() error { return sleepContext(ctx, cfg.Hold) }
	if err := hold(); err != nil {
		return err
	}
	w.switchPhase("expand", 0)
	if err := s.transition(ctx, "expand"); err != nil {
		return err
	}
	if err := hold(); err != nil {
		return err
	}

	w.switchPhase("backfill_interrupted", 0)
	var err error
	r.Interruption, err = interruptWorker(ctx, s, dsn, schema, cfg.Batch)
	if err != nil {
		return err
	}
	if err := hold(); err != nil {
		return err
	}
	w.switchPhase("backfill_resumed", 0)
	cmd, err := workerCommand(ctx, dsn, schema, cfg.Batch, 0)
	if err != nil {
		return err
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("replacement worker failed (%v): %s", err, string(output))
	}
	p, err := readProgress(ctx, s.pool, false)
	if err != nil {
		return err
	}
	if p.Checkpoint != p.Cutoff || p.Batches <= r.Interruption.RestartFrom.Batches {
		return errors.New("replacement worker did not complete the remaining batches")
	}
	r.Interruption.AfterResume = p
	if err := hold(); err != nil {
		return err
	}

	w.switchPhase("validate", 0)
	if err := s.transition(ctx, "validate"); err != nil {
		return err
	}
	if err := hold(); err != nil {
		return err
	}
	w.switchPhase("canary_25", 25)
	if err := hold(); err != nil {
		return err
	}
	w.switchPhase("cutover_v2", 100)
	witness := payment{ID: "rollback-witness", Amount: 8725, Currency: "USD", State: "pending"}
	if err := w.write(w.current.Load(), 1, witness, true); err != nil {
		return fmt.Errorf("create rollback witness on v2: %w", err)
	}
	witness.State = "captured"
	if err := w.write(w.current.Load(), 1, witness, false); err != nil {
		return fmt.Errorf("capture rollback witness on v2: %w", err)
	}
	if err := hold(); err != nil {
		return err
	}
	rolledAt := time.Now()
	w.switchPhase("rollback_v1", 0)
	if err := w.request(0, http.MethodGet, "/payments/"+witness.ID, nil, http.StatusOK, witness); err != nil {
		return fmt.Errorf("v1 could not read acknowledged v2 data after rollback: %w", err)
	}
	r.RollbackMS = float64(time.Since(rolledAt)) / float64(time.Millisecond)
	r.RollbackConfirmed = true
	if err := hold(); err != nil {
		return err
	}
	if err := s.pool.QueryRow(ctx,
		"SELECT count(*) FROM payments WHERE state IS DISTINCT FROM lower(status)").Scan(&r.CompatMismatches); err != nil {
		return err
	}
	if r.CompatMismatches != 0 {
		return fmt.Errorf("contract blocked by %d mismatched compatibility rows", r.CompatMismatches)
	}
	w.switchPhase("contract_v2", 100)
	for _, prior := range w.history[:len(w.history)-1] {
		prior.dispatch.Lock()
		prior.dispatch.Unlock()
	}
	if err := s.transition(ctx, "contract"); err != nil {
		return err
	}
	return hold()
}

func compareRows(ctx context.Context, s store, seed int, live map[string]payment) (consistencyReport, error) {
	r := consistencyReport{Expected: seed + len(live)}
	seen := make(map[string]struct{}, r.Expected)
	rows, err := s.pool.Query(ctx, "SELECT external_id,amount_minor,currency,state FROM payments")
	if err != nil {
		return r, err
	}
	defer rows.Close()
	states := []string{"pending", "captured", "refunded"}
	for rows.Next() {
		got, err := scanPayment(rows)
		if err != nil {
			return r, err
		}
		r.Found++
		if _, exists := seen[got.ID]; exists {
			r.Duplicates++
			continue
		}
		seen[got.ID] = struct{}{}
		expected, known := live[got.ID]
		if seq, found := strings.CutPrefix(got.ID, "seed-"); found {
			n, err := strconv.Atoi(seq)
			if err == nil && n > 0 && n <= seed && strconv.Itoa(n) == seq {
				expected = payment{ID: got.ID, Amount: int64(1000 + n%100000), Currency: "USD", State: states[n%3]}
				known = true
			}
		}
		if !known {
			r.Unexpected++
		} else if got != expected {
			r.Mismatches++
		} else {
			continue
		}
		if len(r.Examples) < 5 {
			r.Examples = append(r.Examples, got.ID)
		}
	}
	r.Missing = r.Expected - (len(seen) - r.Unexpected)
	return r, rows.Err()
}
