package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"order-service/internal/repository"
	"order-service/internal/service"
)

func TestListRequiresBothQueries(t *testing.T) {
	h := NewOrders(nil)
	cases := []string{
		"/api/v1/orders",
		"/api/v1/orders?customer_id=1",
		"/api/v1/orders?status=created",
		"/api/v1/orders?customer_id=&status=created",
	}
	for _, path := range cases {
		rec := httptest.NewRecorder()
		h.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", path, rec.Code)
		}
	}
}

func TestListReturnsOrders(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://orders:orders@localhost:5432/orders?sslmode=disable"
	}
	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var id int64
	if err := db.QueryRow(`INSERT INTO orders (customer_id, status) VALUES (1, 'created') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("insert order: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM orders WHERE id = $1`, id) })

	h := NewOrders(service.NewOrders(repository.NewOrders(db), nil, nil))
	rec := httptest.NewRecorder()
	h.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orders?customer_id=1&status=created", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body) == 0 || len(body) > 50 {
		t.Fatalf("len = %d, want 1..50", len(body))
	}
}
