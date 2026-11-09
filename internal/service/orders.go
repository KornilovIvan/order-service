package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"order-service/internal/catalog"
	"order-service/internal/erp"
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

// Ошибки переходов состояний и интеграции с ERP.
var (
	ErrCannotConfirm  = errors.New("cannot confirm")
	ErrCannotResend   = errors.New("cannot resend")
	ErrCannotCancel   = errors.New("cannot cancel")
	ErrERPUnavailable = errors.New("erp unavailable")
)

// PriceProvider — источник цен. Реализуется клиентом каталога.
type PriceProvider interface {
	GetPrice(ctx context.Context, productID string) (string, error)
}

// ERPSubmitter — отправка заказа в ERP.
type ERPSubmitter interface {
	Submit(ctx context.Context, idempotencyKey string, req erp.SubmitRequest) (string, error)
}

type Orders struct {
	repo    *repository.Orders
	catalog PriceProvider
	erp     ERPSubmitter
}

func NewOrders(repo *repository.Orders, prices PriceProvider, erpClient ERPSubmitter) *Orders {
	return &Orders{repo: repo, catalog: prices, erp: erpClient}
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

// Confirm переводит заказ created → confirmed и отправляет его в ERP.
//
// Ключ идемпотентности и correlation_id генерируются один раз и сохраняются
// в той же транзакции, что и смена статуса: повторный вызов или Resend
// используют ровно тот же ключ.
func (s *Orders) Confirm(ctx context.Context, orderID int64) (model.Order, error) {
	key := uuid.NewString()
	correlationID := uuid.NewString()

	updated, err := s.repo.Confirm(ctx, orderID, key, correlationID)
	if err != nil {
		return model.Order{}, err
	}
	if !updated {
		return model.Order{}, ErrCannotConfirm
	}

	order, err := s.repo.GetOrder(ctx, orderID)
	if err != nil {
		return model.Order{}, err
	}

	if err := s.submit(ctx, order, key); err != nil {
		return model.Order{}, err
	}
	return s.repo.GetOrder(ctx, orderID)
}

// Resend повторяет отправку в ERP для заказа в статусе confirmed,
// используя ранее сохранённый Idempotency-Key.
func (s *Orders) Resend(ctx context.Context, orderID int64) (model.Order, error) {
	state, err := s.repo.SubmitState(ctx, orderID)
	if err != nil {
		return model.Order{}, err
	}
	if state.Status != model.StatusConfirmed {
		return model.Order{}, ErrCannotResend
	}
	if !state.IdempotencyKey.Valid || state.IdempotencyKey.String == "" {
		return model.Order{}, fmt.Errorf("resend order %d: idempotency key is missing", orderID)
	}

	order, err := s.repo.GetOrder(ctx, orderID)
	if err != nil {
		return model.Order{}, err
	}

	if err := s.submit(ctx, order, state.IdempotencyKey.String); err != nil {
		return model.Order{}, err
	}
	return s.repo.GetOrder(ctx, orderID)
}

// Cancel отменяет заказ из статусов created/confirmed.
func (s *Orders) Cancel(ctx context.Context, orderID int64) (model.Order, error) {
	updated, err := s.repo.Cancel(ctx, orderID)
	if err != nil {
		return model.Order{}, err
	}
	if !updated {
		return model.Order{}, ErrCannotCancel
	}
	return s.repo.GetOrder(ctx, orderID)
}

// submit отправляет заказ в ERP и применяет результат:
//   - 200/409 → submitted_to_erp (+ номер ERP);
//   - 400/422 → failed;
//   - временная ошибка → заказ остаётся confirmed, наружу возвращается
//     ErrERPUnavailable (HTTP 502), повтор возможен через Resend.
func (s *Orders) submit(ctx context.Context, order model.Order, idempotencyKey string) error {
	req := erp.SubmitRequest{OrderID: order.ID, CustomerID: order.CustomerID}
	for _, item := range order.Items {
		req.Items = append(req.Items, erp.Item{ProductID: item.ProductID, Qty: item.Qty})
	}

	erpOrderID, err := s.erp.Submit(ctx, idempotencyKey, req)
	switch {
	case err == nil:
		return s.repo.SetStatus(ctx, order.ID, model.StatusSubmittedToERP, erpOrderID)
	case errors.Is(err, erp.ErrPermanent):
		return s.repo.SetStatus(ctx, order.ID, model.StatusFailed, "")
	default:
		return fmt.Errorf("%w: %v", ErrERPUnavailable, err)
	}
}
