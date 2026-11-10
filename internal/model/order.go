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

// Типы заказа. Совпадают с CHECK-ограничением orders_type_check.
const (
	OrderTypeRegular  = "regular"
	OrderTypePreorder = "preorder"
)

type Order struct {
	ID           int64       `db:"id"`
	CustomerID   int64       `db:"customer_id"`
	Status       string      `db:"status"`
	OrderType    string      `db:"order_type"`
	DeliveryDate *time.Time  `db:"delivery_date"`
	CreatedAt    time.Time   `db:"created_at"`
	Items        []OrderItem `db:"-"`
}

// PendingPublish — заказ, ожидающий публикации события.
type PendingPublish struct {
	ID            int64
	CorrelationID string
}

// OrderItem — позиция заказа.
// PriceAtOrder — цена из каталога на момент создания заказа.
// Дальнейшие изменения цены в каталоге на эту строку не влияют.
type OrderItem struct {
	ID           int64  `db:"id"`
	OrderID      int64  `db:"order_id"`
	ProductID    string `db:"product_id"`
	Qty          int    `db:"qty"`
	PriceAtOrder string `db:"price_at_order"`
}
