package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"order-service/internal/repository"
	"order-service/internal/service"
)

func testHandler(t *testing.T) (*sqlx.DB, http.Handler) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://orders:orders@localhost:5432/orders?sslmode=disable"
	}
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, NewOrders(service.NewOrders(repository.NewOrders(db), nil, nil)).Router()
}

func insertOrder(t *testing.T, db *sqlx.DB) int64 {
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

func TestConfirmReturns202(t *testing.T) {
	db, h := testHandler(t)
	id := insertOrder(t, db)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/orders/%d/confirm", id), nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "confirmed" {
		t.Fatalf("body = %v, want status=confirmed", body)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/orders/%d/confirm", id), nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("second confirm status = %d, want 409", rec.Code)
	}
}

func TestResendIsGone(t *testing.T) {
	h := NewOrders(nil).Router()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/orders/1/resend", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestRetryReturns202(t *testing.T) {
	db, h := testHandler(t)
	id := insertOrder(t, db)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/orders/%d/confirm", id), nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("confirm status = %d", rec.Code)
	}
	if _, err := db.Exec(`UPDATE integration_jobs SET status = 'failed' WHERE order_id = $1`, id); err != nil {
		t.Fatalf("fail job: %v", err)
	}
	if _, err := db.Exec(`UPDATE orders SET status = 'failed' WHERE id = $1`, id); err != nil {
		t.Fatalf("fail order: %v", err)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/orders/%d/retry", id), nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d, body = %s", rec.Code, rec.Body.String())
	}
}
