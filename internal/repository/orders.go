package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
func (r *Orders) InsertOrder(ctx context.Context, tx *sqlx.Tx, customerID int64, orderType string, deliveryDate *time.Time) (int64, error) {
	var id int64
	err := tx.GetContext(ctx, &id,
		`INSERT INTO orders (customer_id, order_type, delivery_date) VALUES ($1, $2, $3) RETURNING id`,
		customerID, orderType, deliveryDate,
	)
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
		`SELECT id, customer_id, status, order_type, delivery_date, created_at FROM orders WHERE id = $1`, id)
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

// Confirm переводит заказ created → confirmed, фиксируя ключ идемпотентности
// и correlation_id. false — заказ не в статусе created (или его нет).
func (r *Orders) Confirm(ctx context.Context, id int64, idempotencyKey, correlationID string) (bool, error) {
	var updated bool
	err := r.WithTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE orders
			    SET status = 'confirmed', idempotency_key = $2, correlation_id = $3
			  WHERE id = $1 AND status = 'created'`,
			id, idempotencyKey, correlationID,
		)
		if err != nil {
			return fmt.Errorf("confirm order: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("confirm order rows: %w", err)
		}
		updated = n == 1
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

// Cancel переводит заказ из created/confirmed в cancelled.
// false — заказ в статусе, из которого отмена невозможна.
func (r *Orders) Cancel(ctx context.Context, id int64) (bool, error) {
	var updated bool
	err := r.WithTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE orders SET status = 'cancelled'
			  WHERE id = $1 AND status IN ('created', 'confirmed')`, id)
		if err != nil {
			return fmt.Errorf("cancel order: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("cancel order rows: %w", err)
		}
		updated = n == 1
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

// SubmitState — состояние заказа, нужное для отправки в ERP.
type SubmitState struct {
	Status         string         `db:"status"`
	IdempotencyKey sql.NullString `db:"idempotency_key"`
	CorrelationID  sql.NullString `db:"correlation_id"`
	ERPOrderID     sql.NullString `db:"erp_order_id"`
}

// SubmitState читает статус и сохранённые идентификаторы отправки.
func (r *Orders) SubmitState(ctx context.Context, id int64) (SubmitState, error) {
	var st SubmitState
	err := r.db.GetContext(ctx, &st,
		`SELECT status, idempotency_key, correlation_id, erp_order_id FROM orders WHERE id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return SubmitState{}, ErrNotFound
	}
	if err != nil {
		return SubmitState{}, fmt.Errorf("select submit state: %w", err)
	}
	return st, nil
}

// SetStatus меняет статус и (если задан) номер заказа в ERP.
// Пустой erpOrderID сохраняет прежнее значение.
func (r *Orders) SetStatus(ctx context.Context, id int64, status, erpOrderID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE orders
		    SET status = $2,
		        erp_order_id = COALESCE(NULLIF($3, ''), erp_order_id)
		  WHERE id = $1`,
		id, status, erpOrderID,
	)
	if err != nil {
		return fmt.Errorf("set status %q: %w", status, err)
	}
	return nil
}

// PendingPublish возвращает заказы, по которым ещё не опубликовано событие.
func (r *Orders) PendingPublish(ctx context.Context, limit int) ([]model.PendingPublish, error) {
	var rows []struct {
		ID            int64          `db:"id"`
		CorrelationID sql.NullString `db:"correlation_id"`
	}

	err := r.db.SelectContext(ctx, &rows,
		`SELECT id, correlation_id
		   FROM orders
		  WHERE published_at IS NULL
		    AND status NOT IN ('created', 'cancelled')
		    AND idempotency_key IS NOT NULL
		  ORDER BY id
		  LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending publish: %w", err)
	}

	out := make([]model.PendingPublish, 0, len(rows))
	for _, row := range rows {
		out = append(out, model.PendingPublish{ID: row.ID, CorrelationID: row.CorrelationID.String})
	}
	return out, nil
}

// MarkPublished проставляет published_at опубликованным заказам.
func (r *Orders) MarkPublished(ctx context.Context, ids []int64, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}

	query, args, err := sqlx.In(
		`UPDATE orders SET published_at = ? WHERE id IN (?) AND published_at IS NULL`, at, ids)
	if err != nil {
		return fmt.Errorf("build mark published: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, r.db.Rebind(query), args...); err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	return nil
}
