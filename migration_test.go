package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestCanaryRoutesAndPercentiles(t *testing.T) {
	p := &phase{result: phaseReport{V2Percent: 25}}
	v2 := 0
	for range 100 {
		v2 += p.target()
	}
	if v2 != 25 {
		t.Fatalf("routed %d of 100 requests to v2", v2)
	}
	times := make([]time.Duration, 100)
	for i := range times {
		times[i] = time.Duration(i+1) * time.Millisecond
	}
	if got := percentile(times, 95); got != 95 {
		t.Fatalf("p95: %.3f ms", got)
	}
}

func TestMalformedCreateDoesNotReachStorage(t *testing.T) {
	a := paymentAPI{version: "v1"}
	for _, body := range []string{
		`{"id":"one","amount_minor":0,"currency":"USD"}`,
		`{"id":"one","amount_minor":100,"currency":"usd"}`,
		`{"id":"one","amount_minor":100,"currency":"USD","state":"captured"}`,
		`{"id":"one","amount_minor":100,"currency":"USD"} {}`,
	} {
		recorder := httptest.NewRecorder()
		a.handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body)))
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("invalid input returned %d: %s", recorder.Code, body)
		}
	}
}

func testStore(t *testing.T, seed int) store {
	t.Helper()
	dsn := os.Getenv("MIGRATION_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MIGRATION_TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	s, _, cleanup, err := sandbox(t.Context(), dsn)
	must(t, err)
	t.Cleanup(func() { must(t, cleanup()) })
	must(t, s.init(t.Context(), seed))
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBackfillRestartsAfterUncommittedBatch(t *testing.T) {
	s := testStore(t, 40)
	ctx := t.Context()
	must(t, s.transition(ctx, "expand"))
	if err := s.transition(ctx, "validate"); err == nil {
		t.Fatal("validation succeeded before the backfill")
	}
	first, err := s.copyBatch(ctx, 10)
	must(t, err)
	if first.Checkpoint != 10 || first.Copied != 10 {
		t.Fatalf("unexpected checkpoint: %+v", first)
	}
	locked, err := s.pool.Begin(ctx)
	must(t, err)
	defer rollback(locked)
	_, err = locked.Exec(ctx, "UPDATE payments SET status='REFUNDED' WHERE id=15")
	must(t, err)
	_, err = s.copyBatch(ctx, 10)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("expected a row lock timeout, got %v", err)
	}
	afterBlocked, err := readProgress(ctx, s.pool, false)
	must(t, err)
	if afterBlocked != first {
		t.Fatalf("failed transaction advanced the checkpoint: %+v", afterBlocked)
	}
	var copiedInFailedBatch int
	must(t, s.pool.QueryRow(ctx, "SELECT count(*) FROM payments WHERE id BETWEEN 11 AND 20 AND state IS NOT NULL").Scan(&copiedInFailedBatch))
	if copiedInFailedBatch != 0 {
		t.Fatalf("failed batch exposed %d uncommitted copies", copiedInFailedBatch)
	}
	must(t, locked.Rollback(ctx))
	complete, err := s.backfill(ctx, 10, 0)
	must(t, err)
	if complete.Checkpoint != 40 || complete.Copied != 40 || complete.Batches != 4 {
		t.Fatalf("resumed checkpoint is wrong: %+v", complete)
	}
	must(t, s.transition(ctx, "validate"))
}

func TestVersionsShareDataUntilContract(t *testing.T) {
	s := testStore(t, 2)
	ctx := t.Context()
	v1 := paymentAPI{store: s, version: "v1"}
	v2 := paymentAPI{store: s, version: "v2"}
	must(t, s.transition(ctx, "expand"))
	_, err := v1.change(ctx, "seed-1", "refunded")
	must(t, err)
	copied, err := s.backfill(ctx, 2, 0)
	must(t, err)
	if copied.Copied != 1 {
		t.Fatalf("backfill should skip the seed row populated by v1: %+v", copied)
	}
	must(t, s.transition(ctx, "validate"))
	historical, err := v2.find(ctx, s.pool, "seed-1")
	must(t, err)
	if historical.State != "refunded" {
		t.Fatalf("backfill lost the v1 write: %+v", historical)
	}
	input := payment{ID: "concurrent", Amount: 99, Currency: "USD"}
	type attempt struct {
		p       payment
		created bool
		err     error
	}
	attempts := make(chan attempt, 8)
	var workers sync.WaitGroup
	for i := range cap(attempts) {
		api := []paymentAPI{v1, v2}[i%2]
		workers.Go(func() {
			p, created, err := api.create(ctx, input)
			attempts <- attempt{p, created, err}
		})
	}
	workers.Wait()
	close(attempts)
	created := 0
	for a := range attempts {
		must(t, a.err)
		if a.created {
			created++
		}
		if a.p.State != "pending" || a.p.Amount != input.Amount {
			t.Fatalf("wrong creation result: %+v", a.p)
		}
	}
	if created != 1 {
		t.Fatalf("created %d payments for the same ID", created)
	}
	conflict := input
	conflict.Amount++
	if _, _, err := v2.create(ctx, conflict); !errors.Is(err, errAlreadyUsed) {
		t.Fatalf("expected amount conflict, got %v", err)
	}
	_, err = v1.change(ctx, input.ID, "captured")
	must(t, err)
	got, err := v2.find(ctx, s.pool, input.ID)
	must(t, err)
	if got.State != "captured" {
		t.Fatalf("v2 missed v1 update: %+v", got)
	}
	_, err = v2.change(ctx, input.ID, "refunded")
	must(t, err)
	rolledBack, err := v1.find(ctx, s.pool, input.ID)
	must(t, err)
	if rolledBack.State != "refunded" {
		t.Fatalf("v1 missed v2 update: %+v", rolledBack)
	}
	_, err = s.pool.Exec(ctx, "UPDATE payments SET status='CAPTURED',state='pending' WHERE external_id=$1", input.ID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("inconsistent dual-write should be rejected, got %v", err)
	}
	must(t, s.transition(ctx, "contract"))
	must(t, s.transition(ctx, "contract"))
	p, err := readProgress(ctx, s.pool, false)
	must(t, err)
	if v1.ready(p.Stage) || !v2.ready(p.Stage) {
		t.Fatalf("wrong readiness after contract: %+v", p)
	}
	contracted, err := v2.find(ctx, s.pool, input.ID)
	must(t, err)
	if contracted != rolledBack {
		t.Fatalf("contract altered the payment: %+v", contracted)
	}
}
