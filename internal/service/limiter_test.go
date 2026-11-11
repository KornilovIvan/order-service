package service

import (
	"context"
	"database/sql"
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"order-service/internal/erp"
	"order-service/internal/limiter"
	"order-service/internal/model"
	"order-service/internal/repository"
)

type stubLimiter struct {
	before func()
	resp   limiter.ClaimResponse
	err    error
	n      int
}

func (s *stubLimiter) Claim(context.Context) (limiter.ClaimResponse, error) {
	s.n++
	if s.before != nil {
		s.before()
	}
	return s.resp, s.err
}

// postponeQueuedJobs отодвигает чужие queued, чтобы ClaimJob взял только что подтверждённый заказ.
func postponeQueuedJobs(t *testing.T, db *sqlx.DB) {
	t.Helper()
	if _, err := db.Exec(`UPDATE integration_jobs SET run_at = now() + interval '1 hour' WHERE status = 'queued'`); err != nil {
		t.Fatalf("postpone queued: %v", err)
	}
}

func jobLease(t *testing.T, db *sqlx.DB, orderID int64) (status string, locked, token sql.NullString, runAt time.Time) {
	t.Helper()
	var lockedAt sql.NullTime
	err := db.QueryRow(
		`SELECT status, locked_at, lease_token::text, run_at
		   FROM integration_jobs WHERE order_id = $1`, orderID,
	).Scan(&status, &lockedAt, &token, &runAt)
	if err != nil {
		t.Fatalf("job: %v", err)
	}
	if lockedAt.Valid {
		locked = sql.NullString{String: "set", Valid: true}
	}
	return status, locked, token, runAt
}

func TestLimiterDenySetsRunAtFromPTTL(t *testing.T) {
	db := testDB(t)
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	lim := limiter.New(rdb, 1, 2*time.Second)
	if _, err := lim.Claim(context.Background()); err != nil {
		t.Fatal(err)
	}

	erpHits := 0
	erpSrv := startERP(t, func(http.ResponseWriter, *http.Request) { erpHits++ })
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second)).UseLimiter(lim)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	before := time.Now()
	ok, err := svc.ProcessNextJob(context.Background())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !ok {
		t.Fatal("expected claimed job")
	}
	if erpHits != 0 {
		t.Fatal("ERP must not be called without a slot")
	}

	status, locked, token, runAt := jobLease(t, db, id)
	if status != model.JobQueued {
		t.Fatalf("job status = %s, want queued", status)
	}
	if locked.Valid || token.Valid {
		t.Fatalf("lease_token/locked_at must be NULL, locked=%v token=%v", locked, token)
	}
	pttl := srv.TTL("erp:window")
	if pttl <= 0 {
		t.Fatal("window ttl missing")
	}
	delta := runAt.Sub(before)
	if delta < pttl-200*time.Millisecond || delta > pttl+time.Second {
		t.Fatalf("run_at in %s, pttl %s", delta, pttl)
	}
}

func TestLimiterPersistExpireOnDeny(t *testing.T) {
	db := testDB(t)
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	lim := limiter.New(rdb, 1, 2*time.Second)
	if _, err := lim.Claim(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Persist(context.Background(), "erp:window").Err(); err != nil {
		t.Fatal(err)
	}

	erpSrv := startERP(t, func(http.ResponseWriter, *http.Request) {
		t.Error("ERP must not be called")
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second)).UseLimiter(lim)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	before := time.Now()
	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if !srv.Exists("erp:window") {
		t.Fatal("DEL must not remove erp:window")
	}
	if srv.TTL("erp:window") <= 0 {
		t.Fatal("deny must EXPIRE the persisted key")
	}
	status, locked, token, runAt := jobLease(t, db, id)
	if status != model.JobQueued || locked.Valid || token.Valid {
		t.Fatalf("job = %s locked=%v token=%v", status, locked, token)
	}
	if runAt.Before(before) {
		t.Fatalf("run_at %s not from new PTTL", runAt)
	}
}

func TestLimiterMissingKeyPausesThirtySeconds(t *testing.T) {
	db := testDB(t)
	lim := &stubLimiter{resp: limiter.ClaimResponse{RetryAfter: 30 * time.Second}}
	erpSrv := startERP(t, func(http.ResponseWriter, *http.Request) {
		t.Error("ERP must not be called")
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second)).UseLimiter(lim)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	before := time.Now()
	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if lim.n != 1 {
		t.Fatalf("limiter calls = %d, want 1 (no second INCR)", lim.n)
	}
	_, _, _, runAt := jobLease(t, db, id)
	wait := runAt.Sub(before)
	if wait < 29*time.Second || wait > 31*time.Second {
		t.Fatalf("run_at in %s, want ~30s", wait)
	}
}

func TestLimiterDenyOnCancelledOrder(t *testing.T) {
	db := testDB(t)
	erpSrv := startERP(t, func(http.ResponseWriter, *http.Request) {
		t.Error("ERP must not be called")
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second))
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	var runAtBefore time.Time
	lim := &stubLimiter{
		resp: limiter.ClaimResponse{RetryAfter: 5 * time.Second},
		before: func() {
			_, _, _, runAtBefore = jobLease(t, db, id)
			if _, err := svc.Cancel(context.Background(), id); err != nil {
				t.Errorf("cancel: %v", err)
			}
		},
	}
	svc.UseLimiter(lim)

	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	st := mustStored(t, db, id)
	if st.Status != model.StatusCancelled {
		t.Fatalf("order = %s, want cancelled", st.Status)
	}
	status, locked, token, runAt := jobLease(t, db, id)
	if status != model.JobCancelled {
		t.Fatalf("job = %s, want cancelled", status)
	}
	if locked.Valid || token.Valid {
		t.Fatal("lease_token and locked_at must be NULL")
	}
	if !runAt.Equal(runAtBefore) {
		t.Fatalf("run_at changed: %s → %s", runAtBefore, runAt)
	}
}

func TestRequestTimeoutIsERPTemporaryNotLimiter(t *testing.T) {
	db := testDB(t)
	lim := &stubLimiter{resp: limiter.ClaimResponse{Allowed: true}}
	erpHits := 0
	erpSrv := startERP(t, func(w http.ResponseWriter, _ *http.Request) {
		erpHits++
		w.WriteHeader(http.StatusRequestTimeout)
		_, _ = w.Write([]byte(`timeout`))
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second)).UseLimiter(lim)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	before := time.Now()
	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if lim.n != 1 || !lim.resp.Allowed {
		t.Fatalf("limiter must allow the slot, calls=%d allowed=%v", lim.n, lim.resp.Allowed)
	}
	if erpHits != 1 {
		t.Fatalf("ERP hits = %d, want 1 (408 is Submit, not limiter)", erpHits)
	}
	status, locked, token, runAt := jobLease(t, db, id)
	if status != model.JobQueued {
		t.Fatalf("job = %s, want queued via ApplyJobTemporary", status)
	}
	if locked.Valid || token.Valid {
		t.Fatal("lease must be cleared by result tx")
	}
	wait := runAt.Sub(before)
	if wait < 29*time.Second || wait > 31*time.Second {
		t.Fatalf("temporary retry in %s, want 30s", wait)
	}
}
