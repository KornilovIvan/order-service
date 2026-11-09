package model

import "time"

// Статусы заказа. Совпадают с CHECK-ограничением orders_status_check.
const (
	StatusCreated        = "created"
	StatusConfirmed      = "confirmed"
	StatusSubmittedToERP = "submitted_to_erp"
	StatusCompleted      = "completed"
	StatusCancelled      = "cancelled"
	StatusFailed         = "failed"
)

type Order struct {
	ID         int64       `db:"id"`
	CustomerID int64       `db:"customer_id"`
	Status     string      `db:"status"`
	CreatedAt  time.Time   `db:"created_at"`
	Items      []OrderItem `db:"-"`
}

// OrderItem — позиция заказа.
// Цена намеренно не хранится: она запрашивается у каталога и не копируется.
type OrderItem struct {
	ID        int64  `db:"id"`
	OrderID   int64  `db:"order_id"`
	ProductID string `db:"product_id"`
	Qty       int    `db:"qty"`
}
