package service

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"order-service/internal/model"
	"order-service/internal/repository"
)

func cleanupOrder(t *testing.T, db interface {
	Exec(string, ...any) (sql.Result, error)
}, id int64) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM integration_jobs WHERE order_id = $1`, id)
		_, _ = db.Exec(`DELETE FROM orders WHERE id = $1`, id)
	})
}

type storedJob struct {
	Status         string
	IdempotencyKey string
	PublishedAt    sql.NullTime
}

func jobsOf(t *testing.T, db interface {
	Query(string, ...any) (*sql.Rows, error)
}, orderID int64) []storedJob {
	t.Helper()
	rows, err := db.Query(
		`SELECT status, idempotency_key::text, published_at
		   FROM integration_jobs WHERE order_id = $1 ORDER BY id`, orderID)
	if err != nil {
		t.Fatalf("select jobs: %v", err)
	}
	defer rows.Close()
	var out []storedJob
	for rows.Next() {
		var j storedJob
		if err := rows.Scan(&j.Status, &j.IdempotencyKey, &j.PublishedAt); err != nil {
			t.Fatalf("scan job: %v", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("jobs rows: %v", err)
	}
	return out
}

func TestConfirmEnqueuesQueuedJob(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	order, err := svc.Confirm(context.Background(), id)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if order.Status != model.StatusConfirmed {
		t.Fatalf("status = %s, want confirmed", order.Status)
	}

	got := jobsOf(t, db, id)
	if len(got) != 1 {
		t.Fatalf("jobs = %d, want 1", len(got))
	}
	if got[0].Status != model.JobQueued {
		t.Fatalf("job status = %s, want queued", got[0].Status)
	}
	if got[0].IdempotencyKey == "" {
		t.Fatal("job idempotency_key is empty")
	}
	if got[0].PublishedAt.Valid {
		t.Fatal("confirm job published_at must be NULL")
	}
}

func TestSecondConfirmDoesNotInsertJob(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	first := jobsOf(t, db, id)
	if _, err := svc.Confirm(context.Background(), id); !errors.Is(err, ErrCannotConfirm) {
		t.Fatalf("second confirm error = %v, want cannot confirm", err)
	}
	got := jobsOf(t, db, id)
	if len(got) != 1 {
		t.Fatalf("jobs = %d, want 1", len(got))
	}
	if got[0].IdempotencyKey != first[0].IdempotencyKey {
		t.Fatalf("key changed: %s → %s", first[0].IdempotencyKey, got[0].IdempotencyKey)
	}
}

func TestParallelConfirmEnqueuesOneJob(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i := range 2 {
		go func() {
			defer wg.Done()
			_, errs[i] = svc.Confirm(context.Background(), id)
		}()
	}
	wg.Wait()

	var ok, denied int
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrCannotConfirm):
			denied++
		default:
			t.Fatalf("confirm: %v", err)
		}
	}
	if ok != 1 || denied != 1 {
		t.Fatalf("success=%d cannot-confirm=%d", ok, denied)
	}
	got := jobsOf(t, db, id)
	if len(got) != 1 {
		t.Fatalf("jobs = %d, want 1 row", len(got))
	}
	if got[0].IdempotencyKey == "" {
		t.Fatal("idempotency_key is empty")
	}
}

func TestCancelQueuedJobCancelsOrderAndJob(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	order, err := svc.Cancel(context.Background(), id)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if order.Status != model.StatusCancelled {
		t.Fatalf("order status = %s, want cancelled", order.Status)
	}
	got := jobsOf(t, db, id)
	if len(got) != 1 || got[0].Status != model.JobCancelled {
		t.Fatalf("job = %+v, want cancelled", got)
	}
}

func TestRetryAfterFailedInsertsNewJob(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := db.Exec(`UPDATE integration_jobs SET status = 'failed' WHERE order_id = $1`, id); err != nil {
		t.Fatalf("fail job: %v", err)
	}
	if _, err := db.Exec(`UPDATE orders SET status = 'failed' WHERE id = $1`, id); err != nil {
		t.Fatalf("fail order: %v", err)
	}

	if _, err := svc.Retry(context.Background(), id); err != nil {
		t.Fatalf("retry: %v", err)
	}

	got := jobsOf(t, db, id)
	if len(got) != 2 {
		t.Fatalf("jobs = %d, want 2", len(got))
	}
	if got[0].Status != model.JobFailed {
		t.Fatalf("old job status = %s, want failed", got[0].Status)
	}
	if got[1].Status != model.JobQueued {
		t.Fatalf("new job status = %s, want queued", got[1].Status)
	}
	if got[0].IdempotencyKey == got[1].IdempotencyKey {
		t.Fatal("retry reused idempotency_key")
	}
	if !got[1].PublishedAt.Valid {
		t.Fatal("retry job published_at is NULL")
	}
	st := mustStored(t, db, id)
	if st.Status != model.StatusConfirmed {
		t.Fatalf("order status = %s, want confirmed", st.Status)
	}
	if st.ERPOrderID.Valid {
		t.Fatalf("erp_order_id = %q, want empty", st.ERPOrderID.String)
	}

	if _, err := svc.Retry(context.Background(), id); !errors.Is(err, ErrCannotRetry) {
		t.Fatalf("second retry error = %v, want cannot retry", err)
	}
}

func TestCancelFromSubmittedIsRejected(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := db.Exec(`UPDATE orders SET status = 'submitted_to_erp' WHERE id = $1`, id); err != nil {
		t.Fatalf("set submitted: %v", err)
	}
	if _, err := svc.Cancel(context.Background(), id); !errors.Is(err, ErrCancelNotAllowed) {
		t.Fatalf("cancel error = %v, want cancel not allowed", err)
	}
}
