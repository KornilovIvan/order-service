package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"order-service/internal/model"
)

var ErrNotFound = errors.New("repository: order not found")

type Orders struct {
	db *sqlx.DB
}

func NewOrders(db *sqlx.DB) *Orders {
	return &Orders{db: db}
}

// WithTx выполняет fn в транзакции: ошибка → ROLLBACK, успех → COMMIT.
func (r *Orders) WithTx(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	// Откат после успешного COMMIT возвращает sql.ErrTxDone — это ожидаемо.
	defer func() { _ = tx.Rollback() }()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// CustomerExists проверяет наличие клиента в рамках транзакции.
func (r *Orders) CustomerExists(ctx context.Context, tx *sqlx.Tx, id int64) (bool, error) {
	var exists bool
	err := tx.GetContext(ctx, &exists, `SELECT EXISTS (SELECT 1 FROM customers WHERE id = $1)`, id)
	if err != nil {
		return false, fmt.Errorf("customer exists: %w", err)
	}
	return exists, nil
}

// InsertOrder создаёт заказ со статусом по умолчанию ('created').
func (r *Orders) InsertOrder(ctx context.Context, tx *sqlx.Tx, customerID int64) (int64, error) {
	var id int64
	err := tx.GetContext(ctx, &id, `INSERT INTO orders (customer_id) VALUES ($1) RETURNING id`, customerID)
	if err != nil {
		return 0, fmt.Errorf("insert order: %w", err)
	}
	return id, nil
}

// InsertItem добавляет позицию заказа.
func (r *Orders) InsertItem(ctx context.Context, tx *sqlx.Tx, orderID int64, productID string, qty int) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO order_items (order_id, product_id, qty) VALUES ($1, $2, $3)`,
		orderID, productID, qty,
	)
	if err != nil {
		return fmt.Errorf("insert item %q: %w", productID, err)
	}
	return nil
}

// GetOrder читает заказ вместе с позициями. ErrNotFound — заказа нет.
func (r *Orders) GetOrder(ctx context.Context, id int64) (model.Order, error) {
	var order model.Order
	err := r.db.GetContext(ctx, &order,
		`SELECT id, customer_id, status, created_at FROM orders WHERE id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Order{}, ErrNotFound
	}
	if err != nil {
		return model.Order{}, fmt.Errorf("select order: %w", err)
	}

	if err := r.db.SelectContext(ctx, &order.Items,
		`SELECT id, order_id, product_id, qty FROM order_items WHERE order_id = $1 ORDER BY id`, id); err != nil {
		return model.Order{}, fmt.Errorf("select items: %w", err)
	}
	return order, nil
}
