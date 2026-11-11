package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"order-service/internal/erp"
	"order-service/internal/metrics"
	"order-service/internal/model"
	"order-service/internal/repository"
)

func TestFormatDeliveryDateIsDateOnly(t *testing.T) {
	day := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	req := buildSubmit(model.Order{
		ID:           1,
		OrderType:    model.OrderTypePreorder,
		DeliveryDate: &day,
	})
	if req.DeliveryDate == nil || *req.DeliveryDate != "2026-05-01" {
		t.Fatalf("delivery_date = %v, want 2026-05-01", req.DeliveryDate)
	}
	if err := validateERPPayload(req); err != nil {
		t.Fatalf("valid date: %v", err)
	}
}

func TestValidateDeliveryDateRejectsT(t *testing.T) {
	rfc := "2026-05-01T00:00:00Z"
	if err := validateERPPayload(erp.SubmitRequest{
		OrderType:    model.OrderTypePreorder,
		DeliveryDate: &rfc,
	}); err == nil {
		t.Fatal("RFC3339 with T must be rejected")
	}
}

func TestPreorderSubmitUsesDateWithoutT(t *testing.T) {
	db := testDB(t)
	var payload []byte
	erpSrv := startERP(t, func(w http.ResponseWriter, r *http.Request) {
		payload, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"erp_order_id":"ERP-P"}`))
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second))
	id := insertPreorder(t, db, "2026-05-01")
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	body := string(payload)
	if !strings.Contains(body, `"delivery_date":"2026-05-01"`) {
		t.Fatalf("payload = %s", payload)
	}
	if strings.Contains(body, "T") {
		t.Fatalf("payload has T: %s", payload)
	}
}

func TestPayloadWithTSkipsSubmitAndFailsConfirmed(t *testing.T) {
	db := testDB(t)
	erpHits := 0
	erpSrv := startERP(t, func(http.ResponseWriter, *http.Request) { erpHits++ })
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second))
	svc.payload = func(order model.Order) erp.SubmitRequest {
		d := "2026-05-01T00:00:00Z"
		req := buildSubmit(order)
		req.DeliveryDate = &d
		return req
	}
	id := insertPreorder(t, db, "2026-05-01")
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	before := counterValue(t, metrics.PayloadPrepareErrors, "preorder", metrics.ErrorInvalidDeliveryDate)
	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if erpHits != 0 {
		t.Fatalf("Submit called %d times, want 0", erpHits)
	}
	after := counterValue(t, metrics.PayloadPrepareErrors, "preorder", metrics.ErrorInvalidDeliveryDate)
	if after != before+1 {
		t.Fatalf("prepare errors %v → %v, want +1", before, after)
	}
	if st := mustStored(t, db, id); st.Status != model.StatusFailed {
		t.Fatalf("order = %s, want failed", st.Status)
	}
	jobs := jobsOf(t, db, id)
	if len(jobs) != 1 || jobs[0].Status != model.JobFailed {
		t.Fatalf("jobs = %+v, want failed", jobs)
	}
}

func TestPayloadWithTDoesNotFailCancelledOrder(t *testing.T) {
	db := testDB(t)
	erpSrv := startERP(t, func(http.ResponseWriter, *http.Request) {
		t.Error("Submit must not be called")
	})
	svc := NewOrders(repository.NewOrders(db), nil, erp.New(erpSrv.URL, time.Second))
	id := insertPreorder(t, db, "2026-05-01")
	cleanupOrder(t, db, id)
	postponeQueuedJobs(t, db)
	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	svc.payload = func(order model.Order) erp.SubmitRequest {
		if _, err := svc.Cancel(context.Background(), order.ID); err != nil {
			t.Errorf("cancel: %v", err)
		}
		d := "2026-05-01T00:00:00Z"
		req := buildSubmit(order)
		req.DeliveryDate = &d
		return req
	}

	if _, err := svc.ProcessNextJob(context.Background()); err != nil {
		t.Fatalf("process: %v", err)
	}
	if st := mustStored(t, db, id); st.Status != model.StatusCancelled {
		t.Fatalf("order = %s, want cancelled (failed only from confirmed)", st.Status)
	}
	jobs := jobsOf(t, db, id)
	if len(jobs) != 1 || jobs[0].Status != model.JobFailed {
		t.Fatalf("jobs = %+v, want failed", jobs)
	}
}

func insertPreorder(t *testing.T, db *sqlx.DB, date string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO orders (customer_id, order_type, delivery_date) VALUES (1, 'preorder', $1) RETURNING id`,
		date,
	).Scan(&id); err != nil {
		t.Fatalf("insert preorder: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO order_items (order_id, product_id, qty, price_at_order) VALUES ($1, '1', 1, 100.00)`, id,
	); err != nil {
		t.Fatalf("insert item: %v", err)
	}
	return id
}

func counterValue(t *testing.T, cv *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	m, err := cv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatal(err)
	}
	var pb dto.Metric
	if err := m.Write(&pb); err != nil {
		t.Fatal(err)
	}
	return pb.GetCounter().GetValue()
}
