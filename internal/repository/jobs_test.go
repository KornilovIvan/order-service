package repository

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"order-service/internal/model"
)

// Без Postgres этот пакет падает (не Skip): таблица переходов в service/transition.go
// от БД не зависит и остаётся зелёной.
func testDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://orders:orders@localhost:5432/orders?sslmode=disable"
	}
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		t.Fatalf("postgres unavailable: %v", err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func postponeQueuedJobs(t *testing.T, db *sqlx.DB) {
	t.Helper()
	if _, err := db.Exec(`UPDATE integration_jobs SET run_at = now() + interval '1 hour' WHERE status = 'queued'`); err != nil {
		t.Fatalf("postpone queued: %v", err)
	}
}

func insertCreatedOrder(t *testing.T, db *sqlx.DB) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO orders (customer_id) VALUES (1) RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("insert order: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO order_items (order_id, product_id, qty, price_at_order) VALUES ($1, '1', 1, 100.00)`, id,
	); err != nil {
		t.Fatalf("insert item: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM integration_jobs WHERE order_id = $1`, id)
		_, _ = db.Exec(`DELETE FROM orders WHERE id = $1`, id)
	})
	return id
}

func TestTwoConfirmsOneJobOneKey(t *testing.T) {
	db := testDB(t)
	repo := NewOrders(db)
	id := insertCreatedOrder(t, db)
	ctx := context.Background()

	keyA, keyB := uuid.NewString(), uuid.NewString()
	ok, err := repo.Confirm(ctx, id, keyA, uuid.NewString())
	if err != nil || !ok {
		t.Fatalf("first confirm: ok=%v err=%v", ok, err)
	}
	ok, err = repo.Confirm(ctx, id, keyB, uuid.NewString())
	if err != nil {
		t.Fatalf("second confirm: %v", err)
	}
	if ok {
		t.Fatal("second confirm must not insert another job")
	}

	var n int
	var key string
	if err := db.QueryRow(
		`SELECT count(*), min(idempotency_key::text) FROM integration_jobs WHERE order_id = $1`, id,
	).Scan(&n, &key); err != nil {
		t.Fatalf("jobs: %v", err)
	}
	if n != 1 {
		t.Fatalf("jobs = %d, want 1", n)
	}
	if key != keyA {
		t.Fatalf("idempotency_key = %s, want %s", key, keyA)
	}
}

func TestParallelConfirmOneJob(t *testing.T) {
	db := testDB(t)
	repo := NewOrders(db)
	id := insertCreatedOrder(t, db)
	ctx := context.Background()

	var (
		mu      sync.Mutex
		applied int
		wg      sync.WaitGroup
	)
	wg.Add(2)
	for range 2 {
		key := uuid.NewString()
		go func(key string) {
			defer wg.Done()
			ok, err := repo.Confirm(ctx, id, key, uuid.NewString())
			if err != nil {
				t.Errorf("confirm: %v", err)
				return
			}
			if ok {
				mu.Lock()
				applied++
				mu.Unlock()
			}
		}(key)
	}
	wg.Wait()
	if applied != 1 {
		t.Fatalf("successful confirms = %d, want 1", applied)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM integration_jobs WHERE order_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("jobs = %d, want 1", n)
	}
}

func TestApplyJobSuccessOnCancelledStoresNumber(t *testing.T) {
	db := testDB(t)
	repo := NewOrders(db)
	id := insertCreatedOrder(t, db)
	ctx := context.Background()
	postponeQueuedJobs(t, db)
	if ok, err := repo.Confirm(ctx, id, uuid.NewString(), uuid.NewString()); err != nil || !ok {
		t.Fatalf("confirm: ok=%v err=%v", ok, err)
	}
	job, ok, err := repo.ClaimJob(ctx)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if _, err := repo.Cancel(ctx, id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := repo.ApplyJobSuccess(ctx, job, "ERP-LATE"); err != nil {
		t.Fatalf("apply success: %v", err)
	}

	var status, jobStatus string
	var erpID string
	if err := db.QueryRow(`SELECT status, coalesce(erp_order_id, '') FROM orders WHERE id = $1`, id).Scan(&status, &erpID); err != nil {
		t.Fatalf("order: %v", err)
	}
	if err := db.QueryRow(`SELECT status FROM integration_jobs WHERE order_id = $1`, id).Scan(&jobStatus); err != nil {
		t.Fatalf("job: %v", err)
	}
	if status != model.StatusCancelled {
		t.Fatalf("order = %s, want cancelled", status)
	}
	if erpID != "ERP-LATE" {
		t.Fatalf("erp_order_id = %q", erpID)
	}
	if jobStatus != model.JobDone {
		t.Fatalf("job = %s, want done", jobStatus)
	}
}

func TestApplyJobPermanentOnCancelledDoesNotFailOrder(t *testing.T) {
	db := testDB(t)
	repo := NewOrders(db)
	id := insertCreatedOrder(t, db)
	ctx := context.Background()
	postponeQueuedJobs(t, db)
	if ok, err := repo.Confirm(ctx, id, uuid.NewString(), uuid.NewString()); err != nil || !ok {
		t.Fatalf("confirm: ok=%v err=%v", ok, err)
	}
	job, ok, err := repo.ClaimJob(ctx)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if _, err := repo.Cancel(ctx, id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := repo.ApplyJobPermanent(ctx, job); err != nil {
		t.Fatalf("apply permanent: %v", err)
	}

	var status, jobStatus string
	if err := db.QueryRow(`SELECT status FROM orders WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("order: %v", err)
	}
	if err := db.QueryRow(`SELECT status FROM integration_jobs WHERE order_id = $1`, id).Scan(&jobStatus); err != nil {
		t.Fatalf("job: %v", err)
	}
	if status != model.StatusCancelled {
		t.Fatalf("order = %s, want cancelled (not failed)", status)
	}
	if jobStatus != model.JobFailed {
		t.Fatalf("job = %s, want failed", jobStatus)
	}
}
