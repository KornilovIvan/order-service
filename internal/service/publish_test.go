package service

import (
	"context"
	"testing"
	"time"

	"order-service/internal/repository"
)

func TestClaimPublishSkipsCancelledQueued(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := svc.Cancel(context.Background(), id); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	pending, err := repository.NewOrders(db).ClaimPendingPublish(context.Background(), 5000)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, job := range pending {
		if job.OrderID == id {
			t.Fatalf("cancelled job %d was claimed", job.ID)
		}
	}
}

func TestClaimPublishIncludesConfirmJob(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	repo := repository.NewOrders(db)
	pending, err := repo.ClaimPendingPublish(context.Background(), 5000)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	var jobID int64
	for _, job := range pending {
		if job.OrderID == id {
			jobID = job.ID
			if job.CorrelationID == "" {
				t.Fatal("correlation_id empty")
			}
		}
	}
	if jobID == 0 {
		t.Fatal("confirm job was not claimed")
	}
	ok, err := repo.MarkPublished(context.Background(), jobID, time.Now())
	if err != nil {
		t.Fatalf("mark: %v", err)
	}
	if !ok {
		t.Fatal("mark published = false")
	}
	ok, err = repo.MarkPublished(context.Background(), jobID, time.Now())
	if err != nil {
		t.Fatalf("second mark: %v", err)
	}
	if ok {
		t.Fatal("second mark should be 0 rows")
	}
}

func TestRetryJobWithPublishedAtIsNotClaimed(t *testing.T) {
	db := testDB(t)
	svc := NewOrders(repository.NewOrders(db), nil, nil)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := db.Exec(`UPDATE integration_jobs SET status = 'failed', published_at = now() WHERE order_id = $1`, id); err != nil {
		t.Fatalf("fail job: %v", err)
	}
	if _, err := db.Exec(`UPDATE orders SET status = 'failed' WHERE id = $1`, id); err != nil {
		t.Fatalf("fail order: %v", err)
	}
	if _, err := svc.Retry(context.Background(), id); err != nil {
		t.Fatalf("retry: %v", err)
	}

	pending, err := repository.NewOrders(db).ClaimPendingPublish(context.Background(), 5000)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, job := range pending {
		if job.OrderID == id {
			t.Fatalf("retry job %d was claimed for publish", job.ID)
		}
	}
}
