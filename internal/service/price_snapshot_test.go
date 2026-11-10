package service

import (
	"context"
	"testing"

	"order-service/internal/repository"
)

// mutablePrices — каталог, цену которого тест меняет после создания заказа.
type mutablePrices struct {
	price string
}

func (p *mutablePrices) GetPrice(context.Context, string) (string, error) {
	return p.price, nil
}

func TestPriceAtOrderStaysAfterCatalogChange(t *testing.T) {
	db := testDB(t)
	prices := &mutablePrices{price: "100.00"}
	svc := NewOrders(repository.NewOrders(db), prices, nil)

	created, err := svc.Create(context.Background(), CreateInput{
		CustomerID: 1,
		Items:      []CreateItem{{ProductID: "1", Qty: 1}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM orders WHERE id = $1`, created.ID) })

	if got := created.Items[0].PriceAtOrder; got != "100.00" {
		t.Fatalf("price at create = %q, want 100.00", got)
	}

	prices.price = "120.00"
	current, err := prices.GetPrice(context.Background(), "1")
	if err != nil {
		t.Fatalf("get price: %v", err)
	}
	if current != "120.00" {
		t.Fatalf("catalog price = %q, want 120.00", current)
	}

	got, err := svc.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(got.Items))
	}
	if got.Items[0].PriceAtOrder != "100.00" {
		t.Fatalf("price_at_order = %q, want 100.00", got.Items[0].PriceAtOrder)
	}
}
