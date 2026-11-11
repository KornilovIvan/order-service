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

// InsertItem добавляет позицию заказа вместе с ценой на момент создания.
func (r *Orders) InsertItem(ctx context.Context, tx *sqlx.Tx, orderID int64, productID string, qty int, priceAtOrder string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO order_items (order_id, product_id, qty, price_at_order) VALUES ($1, $2, $3, $4)`,
		orderID, productID, qty, priceAtOrder,
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
		`SELECT id, order_id, product_id, qty, price_at_order::text AS price_at_order
		   FROM order_items WHERE order_id = $1 ORDER BY id`, id); err != nil {
		return model.Order{}, fmt.Errorf("select items: %w", err)
	}
	return order, nil
}

// ListByCustomerStatus возвращает до limit заказов клиента в заданном статусе,
// сначала самые новые. Порядок совпадает с orders_customer_status_created_idx.
func (r *Orders) ListByCustomerStatus(ctx context.Context, customerID int64, status string, limit int) ([]model.Order, error) {
	var orders []model.Order
	err := r.db.SelectContext(ctx, &orders,
		`SELECT id, customer_id, status, order_type, delivery_date, created_at
		   FROM orders
		  WHERE customer_id = $1 AND status = $2
		  ORDER BY created_at DESC
		  LIMIT $3`, customerID, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	if len(orders) == 0 {
		return []model.Order{}, nil
	}

	ids := make([]int64, len(orders))
	byID := make(map[int64]*model.Order, len(orders))
	for i := range orders {
		ids[i] = orders[i].ID
		orders[i].Items = []model.OrderItem{}
		byID[orders[i].ID] = &orders[i]
	}

	query, args, err := sqlx.In(
		`SELECT id, order_id, product_id, qty, price_at_order::text AS price_at_order
		   FROM order_items
		  WHERE order_id IN (?)
		  ORDER BY id`, ids)
	if err != nil {
		return nil, fmt.Errorf("list order items: %w", err)
	}
	var items []model.OrderItem
	if err := r.db.SelectContext(ctx, &items, r.db.Rebind(query), args...); err != nil {
		return nil, fmt.Errorf("list order items: %w", err)
	}
	for _, item := range items {
		byID[item.OrderID].Items = append(byID[item.OrderID].Items, item)
	}
	return orders, nil
}

// Confirm переводит created → confirmed и вставляет queued-задание.
// Ключ идемпотентности пишется в integration_jobs, не в orders.
// false — заказ не в статусе created (или его нет).
func (r *Orders) Confirm(ctx context.Context, id int64, idempotencyKey, correlationID string) (bool, error) {
	return r.enqueue(ctx, id, model.StatusCreated, idempotencyKey, correlationID, false)
}

// Retry переводит failed → confirmed и ставит новое задание.
// published_at = now(), чтобы повторная отправка в ERP не публиковала событие ещё раз.
// false — заказ не в статусе failed.
func (r *Orders) Retry(ctx context.Context, id int64, idempotencyKey, correlationID string) (bool, error) {
	return r.enqueue(ctx, id, model.StatusFailed, idempotencyKey, correlationID, true)
}

func (r *Orders) enqueue(ctx context.Context, id int64, fromStatus, idempotencyKey, correlationID string, alreadyPublished bool) (bool, error) {
	var updated bool
	err := r.WithTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE orders SET status = 'confirmed' WHERE id = $1 AND status = $2`,
			id, fromStatus,
		)
		if err != nil {
			return fmt.Errorf("set confirmed from %s: %w", fromStatus, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("set confirmed from %s rows: %w", fromStatus, err)
		}
		if n != 1 {
			return nil
		}
		if err := insertJob(ctx, tx, id, idempotencyKey, correlationID, alreadyPublished); err != nil {
			return err
		}
		updated = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

func insertJob(ctx context.Context, tx *sqlx.Tx, orderID int64, idempotencyKey, correlationID string, alreadyPublished bool) error {
	query := `INSERT INTO integration_jobs
		(order_id, idempotency_key, status, run_at, correlation_id)
		VALUES ($1, $2, 'queued', now(), $3)`
	args := []any{orderID, idempotencyKey, correlationID}
	if alreadyPublished {
		query = `INSERT INTO integration_jobs
			(order_id, idempotency_key, status, run_at, published_at, correlation_id)
			VALUES ($1, $2, 'queued', now(), now(), $3)`
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// Cancel переводит заказ из created/confirmed в cancelled.
// false — статус уже сменился (или заказа нет).
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
		if n != 1 {
			return nil
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE integration_jobs SET status = 'cancelled'
			  WHERE order_id = $1 AND status = 'queued'`, id); err != nil {
			return fmt.Errorf("cancel queued jobs: %w", err)
		}
		updated = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

// CancelFailed переводит failed → cancelled и не меняет ключ идемпотентности.
// false — заказ уже не в статусе failed.
func (r *Orders) CancelFailed(ctx context.Context, id int64) (bool, error) {
	var updated bool
	err := r.WithTx(ctx, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE orders SET status = 'cancelled'
			  WHERE id = $1 AND status = 'failed'`, id)
		if err != nil {
			return fmt.Errorf("cancel failed order: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("cancel failed order rows: %w", err)
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
// Уже отменённый заказ не возвращается в другой статус: пишется только erp_order_id.
func (r *Orders) SetStatus(ctx context.Context, id int64, status, erpOrderID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE orders
		    SET status = CASE WHEN status = 'cancelled' THEN status ELSE $2 END,
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
