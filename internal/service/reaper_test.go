package service

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"order-service/internal/model"
	"order-service/internal/repository"
)

func TestValidateReaperAfterRejectsOneSecond(t *testing.T) {
	if err := ValidateReaperAfter(time.Second, 15*time.Second); err == nil {
		t.Fatal("REAPER_AFTER=1s must fail when ERP_TIMEOUT=15s")
	}
	if err := ValidateReaperAfter(30*time.Second, 15*time.Second); err != nil {
		t.Fatalf("REAPER_AFTER=30s: %v", err)
	}
}

func insertStuckJob(t *testing.T, db *sqlx.DB, orderID int64, lockedAgo time.Duration) (jobID int64, runAt time.Time) {
	t.Helper()
	err := db.QueryRow(
		`INSERT INTO integration_jobs
		    (order_id, idempotency_key, status, run_at, locked_at, lease_token, correlation_id)
		 VALUES ($1, $2, 'in_progress', now(), now() - ($3 * interval '1 millisecond'), $4, $5)
		 RETURNING id, run_at`,
		orderID, uuid.NewString(), lockedAgo.Milliseconds(), uuid.NewString(), uuid.NewString(),
	).Scan(&jobID, &runAt)
	if err != nil {
		t.Fatalf("insert stuck job: %v", err)
	}
	return jobID, runAt
}

func jobRow(t *testing.T, db *sqlx.DB, jobID int64) (status string, locked, token sql.NullString, runAt time.Time) {
	t.Helper()
	var lockedAt sql.NullTime
	err := db.QueryRow(
		`SELECT status, locked_at, lease_token::text, run_at FROM integration_jobs WHERE id = $1`, jobID,
	).Scan(&status, &lockedAt, &token, &runAt)
	if err != nil {
		t.Fatalf("job %d: %v", jobID, err)
	}
	if lockedAt.Valid {
		locked = sql.NullString{String: "set", Valid: true}
	}
	return status, locked, token, runAt
}

func TestReaperRequeuesStuckConfirmedJob(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	if _, err := db.Exec(`UPDATE orders SET status = 'confirmed' WHERE id = $1`, id); err != nil {
		t.Fatalf("confirm order: %v", err)
	}
	jobID, _ := insertStuckJob(t, db, id, 2*time.Minute)

	if err := svc.ReapOnce(context.Background(), 30*time.Second); err != nil {
		t.Fatalf("reap: %v", err)
	}
	status, locked, token, _ := jobRow(t, db, jobID)
	if status != model.JobQueued {
		t.Fatalf("status = %s, want queued", status)
	}
	if locked.Valid {
		t.Fatal("locked_at must be NULL")
	}
	if token.Valid {
		t.Fatal("lease_token must be NULL")
	}
}

func TestReaperCancelsStuckJobWhenOrderCancelled(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	if _, err := db.Exec(`UPDATE orders SET status = 'cancelled' WHERE id = $1`, id); err != nil {
		t.Fatalf("cancel order: %v", err)
	}
	jobID, runAt := insertStuckJob(t, db, id, 2*time.Minute)

	if err := svc.ReapOnce(context.Background(), 30*time.Second); err != nil {
		t.Fatalf("reap: %v", err)
	}
	status, locked, token, after := jobRow(t, db, jobID)
	if status != model.JobCancelled {
		t.Fatalf("status = %s, want cancelled", status)
	}
	if locked.Valid || token.Valid {
		t.Fatal("locked_at and lease_token must be NULL")
	}
	if !after.Equal(runAt) && after.Sub(runAt).Abs() > time.Second {
		t.Fatalf("run_at changed: %s → %s", runAt, after)
	}
}

func TestReaperSkipsRowLockedByOtherTransaction(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id1 := insertCreatedOrder(t, db)
	id2 := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id1)
	cleanupOrder(t, db, id2)
	_, _ = db.Exec(`UPDATE orders SET status = 'confirmed' WHERE id IN ($1, $2)`, id1, id2)
	job1, _ := insertStuckJob(t, db, id1, 2*time.Minute)
	job2, _ := insertStuckJob(t, db, id2, 2*time.Minute)

	tx, err := db.Beginx()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SELECT id FROM integration_jobs WHERE id = $1 FOR UPDATE`, job1); err != nil {
		t.Fatalf("lock first: %v", err)
	}

	if err := svc.ReapOnce(context.Background(), 30*time.Second); err != nil {
		t.Fatalf("reap: %v", err)
	}

	st1, _, _, _ := jobRow(t, db, job1)
	st2, locked2, token2, _ := jobRow(t, db, job2)
	if st1 != model.JobInProgress {
		t.Fatalf("locked job status = %s, want in_progress", st1)
	}
	if st2 != model.JobQueued {
		t.Fatalf("free job status = %s, want queued", st2)
	}
	if locked2.Valid || token2.Valid {
		t.Fatal("second job must have NULL lock and token")
	}
}
