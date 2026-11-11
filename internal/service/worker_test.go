package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"order-service/internal/erp"
	"order-service/internal/model"
	"order-service/internal/repository"
)

func startERP(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func TestWorkerSubmitsConfirmedOrder(t *testing.T) {
	db := testDB(t)
	var payload []byte
	srv := startERP(t, func(w http.ResponseWriter, r *http.Request) {
		payload, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"erp_order_id":"ERP-1"}`))
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(srv.URL, time.Second))
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	ok, err := svc.ProcessNextJob(context.Background())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !ok {
		t.Fatal("expected a job")
	}

	st := mustStored(t, db, id)
	if st.Status != model.StatusSubmittedToERP {
		t.Fatalf("status = %s, want submitted_to_erp", st.Status)
	}
	if st.ERPOrderID.String != "ERP-1" {
		t.Fatalf("erp_order_id = %q", st.ERPOrderID.String)
	}
	jobs := jobsOf(t, db, id)
	if len(jobs) != 1 || jobs[0].Status != model.JobDone {
		t.Fatalf("jobs = %+v", jobs)
	}

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if body["order_type"] != "regular" || body["delivery_date"] != nil {
		t.Fatalf("payload = %s", payload)
	}
}

func TestWorkerPollsWithinThreeSeconds(t *testing.T) {
	db := testDB(t)
	srv := startERP(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"erp_order_id":"ERP-9"}`))
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(srv.URL, time.Second))
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunJobs(ctx, 100*time.Millisecond)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := mustStored(t, db, id)
		if st.Status == model.StatusSubmittedToERP && st.ERPOrderID.Valid {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	st := mustStored(t, db, id)
	t.Fatalf("after 3s status=%s erp=%q", st.Status, st.ERPOrderID.String)
}

func TestWorkerConflictIsSuccess(t *testing.T) {
	db := testDB(t)
	srv := startERP(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"erp_order_id":"ERP-1"}`))
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(srv.URL, time.Second))
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if st := mustStored(t, db, id); st.Status != model.StatusSubmittedToERP {
		t.Fatalf("status = %s", st.Status)
	}
}

func TestWorkerPermanentFailsConfirmed(t *testing.T) {
	db := testDB(t)
	srv := startERP(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"rejected"}`))
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(srv.URL, time.Second))
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	st := mustStored(t, db, id)
	if st.Status != model.StatusFailed {
		t.Fatalf("status = %s, want failed", st.Status)
	}
	jobs := jobsOf(t, db, id)
	if len(jobs) != 1 || jobs[0].Status != model.JobFailed {
		t.Fatalf("jobs = %+v", jobs)
	}
}

func TestWorkerCancelledKeepsOrderOnReject(t *testing.T) {
	db := testDB(t)
	started := make(chan struct{})
	release := make(chan struct{})
	srv := startERP(t, func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`no`))
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(srv.URL, 5*time.Second))
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := svc.ProcessNextJob(context.Background())
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("submit did not start")
	}
	if _, err := svc.Cancel(context.Background(), id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("process: %v", err)
	}
	st := mustStored(t, db, id)
	if st.Status != model.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", st.Status)
	}
	jobs := jobsOf(t, db, id)
	if len(jobs) != 1 || jobs[0].Status != model.JobFailed {
		t.Fatalf("jobs = %+v, want failed", jobs)
	}
}
