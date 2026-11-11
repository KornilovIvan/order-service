package service

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

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
	return id
}

type storedOrder struct {
	Status     string
	ERPOrderID sql.NullString
}

func mustStored(t *testing.T, db *sqlx.DB, id int64) storedOrder {
	t.Helper()
	var st storedOrder
	err := db.QueryRow(
		`SELECT status, erp_order_id FROM orders WHERE id = $1`, id,
	).Scan(&st.Status, &st.ERPOrderID)
	if err != nil {
		t.Fatalf("select order %d: %v", id, err)
	}
	return st
}

func TestSetStatusDoesNotReviveCancelled(t *testing.T) {
	db := testDB(t)
	id := insertCreatedOrder(t, db)
	cleanupOrder(t, db, id)

	if _, err := db.Exec(`UPDATE orders SET status = 'cancelled' WHERE id = $1`, id); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	repo := repository.NewOrders(db)
	if err := repo.SetStatus(context.Background(), id, model.StatusSubmittedToERP, "ERP-1"); err != nil {
		t.Fatalf("set status: %v", err)
	}
	st := mustStored(t, db, id)
	if st.Status != model.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", st.Status)
	}
	if st.ERPOrderID.String != "ERP-1" {
		t.Fatalf("erp_order_id = %q, want ERP-1", st.ERPOrderID.String)
	}
}
