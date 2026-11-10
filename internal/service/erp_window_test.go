package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.uber.org/mock/gomock"

	"order-service/internal/erp"
	"order-service/internal/model"
	"order-service/internal/repository"
)

func testDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://orders:orders@localhost:5432/orders?sslmode=disable"
	}
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	db.SetMaxOpenConns(10)
	t.Cleanup(func() { _ = db.Close() })
	return db
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
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM orders WHERE id = $1`, id) })
	return id
}

type storedOrder struct {
	Status         string
	ERPOrderID     sql.NullString
	IdempotencyKey sql.NullString
}

func mustStored(t *testing.T, db *sqlx.DB, id int64) storedOrder {
	t.Helper()
	var st storedOrder
	err := db.QueryRow(
		`SELECT status, erp_order_id, idempotency_key::text FROM orders WHERE id = $1`, id,
	).Scan(&st.Status, &st.ERPOrderID, &st.IdempotencyKey)
	if err != nil {
		t.Fatalf("select order %d: %v", id, err)
	}
	return st
}

func newERPTest(t *testing.T, db *sqlx.DB) (*Orders, *MockERPSubmitter) {
	t.Helper()
	mock := NewMockERPSubmitter(gomock.NewController(t))
	return NewOrders(repository.NewOrders(db), nil, mock), mock
}

func TestParallelConfirmSubmitsOnce(t *testing.T) {
	db := testDB(t)
	svc, mock := newERPTest(t, db)
	id := insertCreatedOrder(t, db)

	var calls atomic.Int32
	mock.EXPECT().
		Submit(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, erp.SubmitRequest) (string, error) {
			calls.Add(1)
			return "ERP-1", nil
		}).
		Times(1)

	var wg sync.WaitGroup
	errs := make([]error, 2)
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
		t.Fatalf("confirm results: success=%d cannot-confirm=%d", ok, denied)
	}
	if calls.Load() != 1 {
		t.Fatalf("Submit calls = %d, want 1", calls.Load())
	}
	if st := mustStored(t, db, id); st.Status != model.StatusSubmittedToERP {
		t.Fatalf("status = %s, want submitted_to_erp", st.Status)
	}
}

func TestERPConflictMovesConfirmedToSubmitted(t *testing.T) {
	db := testDB(t)
	svc, mock := newERPTest(t, db)
	id := insertCreatedOrder(t, db)

	// Клиент каталога ERP на 409 {"erp_order_id":"ERP-1"} возвращает номер без ошибки.
	mock.EXPECT().
		Submit(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, erp.SubmitRequest) (string, error) {
			if st := mustStored(t, db, id); st.Status != model.StatusConfirmed {
				t.Errorf("status during Submit = %s, want confirmed", st.Status)
			}
			return "ERP-1", nil
		})

	order, err := svc.Confirm(context.Background(), id)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if order.Status != model.StatusSubmittedToERP {
		t.Fatalf("status = %s, want submitted_to_erp", order.Status)
	}
	st := mustStored(t, db, id)
	if st.ERPOrderID.String != "ERP-1" {
		t.Fatalf("erp_order_id = %q, want ERP-1", st.ERPOrderID.String)
	}
}

func TestPermanentRejectRetryUsesNewKey(t *testing.T) {
	db := testDB(t)
	svc, mock := newERPTest(t, db)
	id := insertCreatedOrder(t, db)

	var keys []string
	gomock.InOrder(
		mock.EXPECT().
			Submit(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, key string, _ erp.SubmitRequest) (string, error) {
				keys = append(keys, key)
				return "", fmt.Errorf("%w: status 422", erp.ErrPermanent)
			}),
		mock.EXPECT().
			Submit(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, key string, _ erp.SubmitRequest) (string, error) {
				keys = append(keys, key)
				return "ERP-2", nil
			}),
	)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if st := mustStored(t, db, id); st.Status != model.StatusFailed {
		t.Fatalf("status after 422 = %s, want failed", st.Status)
	}

	if _, err := svc.Retry(context.Background(), id); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("idempotency keys = %v, want two different keys", keys)
	}
	st := mustStored(t, db, id)
	if st.IdempotencyKey.String != keys[1] {
		t.Fatalf("stored key = %s, want retry key %s", st.IdempotencyKey.String, keys[1])
	}

	if _, err := svc.Retry(context.Background(), id); !errors.Is(err, ErrCannotRetry) {
		t.Fatalf("second retry error = %v, want cannot retry", err)
	}
}

func TestCancelFromSubmittedIsRejected(t *testing.T) {
	db := testDB(t)
	svc, mock := newERPTest(t, db)
	id := insertCreatedOrder(t, db)

	mock.EXPECT().Submit(gomock.Any(), gomock.Any(), gomock.Any()).Return("ERP-1", nil)

	if _, err := svc.Confirm(context.Background(), id); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if _, err := svc.Cancel(context.Background(), id); !errors.Is(err, ErrCancelNotAllowed) {
		t.Fatalf("cancel error = %v, want cancel not allowed", err)
	}
	if st := mustStored(t, db, id); st.Status != model.StatusSubmittedToERP {
		t.Fatalf("status = %s, want submitted_to_erp", st.Status)
	}
}

func TestCancelDuringSubmitKeepsCancelledOnSuccess(t *testing.T) {
	db := testDB(t)
	svc, mock := newERPTest(t, db)
	id := insertCreatedOrder(t, db)

	started := make(chan struct{})
	release := make(chan struct{})
	mock.EXPECT().
		Submit(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, erp.SubmitRequest) (string, error) {
			close(started)
			<-release
			return "ERP-1", nil
		})

	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Confirm(context.Background(), id)
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Submit did not start")
	}

	order, err := svc.Cancel(context.Background(), id)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if order.Status != model.StatusCancelled {
		t.Fatalf("status after cancel = %s, want cancelled", order.Status)
	}

	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("confirm after ERP 200: %v", err)
	}

	st := mustStored(t, db, id)
	if st.Status != model.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", st.Status)
	}
	if st.ERPOrderID.String != "ERP-1" {
		t.Fatalf("erp_order_id = %q, want ERP-1", st.ERPOrderID.String)
	}
}

func TestCancelDuringSubmitKeepsCancelledOnReject(t *testing.T) {
	db := testDB(t)
	svc, mock := newERPTest(t, db)
	id := insertCreatedOrder(t, db)

	started := make(chan struct{})
	release := make(chan struct{})
	mock.EXPECT().
		Submit(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, erp.SubmitRequest) (string, error) {
			close(started)
			<-release
			return "", fmt.Errorf("%w: status 422", erp.ErrPermanent)
		})

	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Confirm(context.Background(), id)
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Submit did not start")
	}

	if _, err := svc.Cancel(context.Background(), id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("confirm after ERP 422: %v", err)
	}

	st := mustStored(t, db, id)
	if st.Status != model.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", st.Status)
	}
	if st.ERPOrderID.Valid {
		t.Fatalf("erp_order_id = %q, want empty", st.ERPOrderID.String)
	}
}
