package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"

	"order-service/internal/catalog"
	"order-service/internal/model"
	"order-service/internal/repository"
)

// Ошибки бизнес-валидации. Все они превращаются в HTTP 400.
var (
	ErrNoItems          = errors.New("items must not be empty")
	ErrInvalidQty       = errors.New("qty must be >= 1")
	ErrCustomerNotFound = errors.New("customer not found")
	ErrProductNotFound  = errors.New("product not found")
)

// PriceProvider — источник цен. Реализуется клиентом каталога.
type PriceProvider interface {
	GetPrice(ctx context.Context, productID string) (string, error)
}

type Orders struct {
	repo    *repository.Orders
	catalog PriceProvider
}

func NewOrders(repo *repository.Orders, prices PriceProvider) *Orders {
	return &Orders{repo: repo, catalog: prices}
}

// CreateInput — входные данные создания заказа.
type CreateInput struct {
	CustomerID int64
	Items      []CreateItem
}

// CreateItem — позиция заказа на входе.
type CreateItem struct {
	ProductID string
	Qty       int
}

// Create создаёт заказ вместе с позициями в одной транзакции.
// Любая ошибка (в том числе NotFound из каталога) приводит к ROLLBACK.
func (s *Orders) Create(ctx context.Context, in CreateInput) (model.Order, error) {
	if len(in.Items) == 0 {
		return model.Order{}, ErrNoItems
	}
	for _, item := range in.Items {
		if item.Qty < 1 {
			return model.Order{}, ErrInvalidQty
		}
	}

	var orderID int64
	err := s.repo.WithTx(ctx, func(tx *sqlx.Tx) error {
		exists, err := s.repo.CustomerExists(ctx, tx, in.CustomerID)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCustomerNotFound
		}

		id, err := s.repo.InsertOrder(ctx, tx, in.CustomerID)
		if err != nil {
			return err
		}
		orderID = id

		for _, item := range in.Items {
			// Цена запрашивается у каталога только для проверки товара
			// и намеренно НЕ копируется в заказ.
			if _, err := s.catalog.GetPrice(ctx, item.ProductID); err != nil {
				if errors.Is(err, catalog.ErrNotFound) {
					return ErrProductNotFound
				}
				return fmt.Errorf("get price for %q: %w", item.ProductID, err)
			}
			if err := s.repo.InsertItem(ctx, tx, id, item.ProductID, item.Qty); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return model.Order{}, err
	}

	// Дочитываем заказ, чтобы отдать created_at и позиции из БД.
	created, err := s.repo.GetOrder(ctx, orderID)
	if err != nil {
		return model.Order{}, err
	}
	return created, nil
}

// Get возвращает заказ по идентификатору.
func (s *Orders) Get(ctx context.Context, id int64) (model.Order, error) {
	order, err := s.repo.GetOrder(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return model.Order{}, nil
	}
	if err != nil {
		return model.Order{}, err
	}
	return order, nil
}
