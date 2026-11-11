package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"order-service/internal/catalog"
	"order-service/internal/erp"
	"order-service/internal/model"
	"order-service/internal/repository"
)

// Ошибки бизнес-валидации. Все они превращаются в HTTP 400.
var (
	ErrNoItems             = errors.New("items must not be empty")
	ErrInvalidQty          = errors.New("qty must be >= 1")
	ErrCustomerNotFound    = errors.New("customer not found")
	ErrProductNotFound     = errors.New("product not found")
	ErrInvalidOrderType    = errors.New("invalid order type")
	ErrPreorderWithoutDate = errors.New("delivery_date is required for preorder")
)

// Ошибки переходов состояний и интеграции с ERP.
var (
	ErrCannotConfirm    = errors.New("cannot confirm")
	ErrCannotRetry      = errors.New("cannot retry")
	ErrCannotCancel     = errors.New("cannot cancel")
	ErrCancelNotAllowed = errors.New("cancel not allowed")
	ErrERPUnavailable   = errors.New("erp unavailable")
	ErrOrderNotFound    = errors.New("order not found")
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
	CustomerID   int64
	OrderType    string
	DeliveryDate *time.Time
	Items        []CreateItem
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

	orderType := in.OrderType
	if orderType == "" {
		orderType = model.OrderTypeRegular
	}
	if orderType != model.OrderTypeRegular && orderType != model.OrderTypePreorder {
		return model.Order{}, ErrInvalidOrderType
	}
	if orderType == model.OrderTypePreorder && in.DeliveryDate == nil {
		return model.Order{}, ErrPreorderWithoutDate
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

		id, err := s.repo.InsertOrder(ctx, tx, in.CustomerID, orderType, in.DeliveryDate)
		if err != nil {
			return err
		}
		orderID = id

		for _, item := range in.Items {
			// Цена фиксируется в той же транзакции, что и шапка заказа:
			// сбой каталога или вставки откатывает заказ целиком.
			price, err := s.catalog.GetPrice(ctx, item.ProductID)
			if err != nil {
				if errors.Is(err, catalog.ErrNotFound) {
					return ErrProductNotFound
				}
				return fmt.Errorf("get price for %q: %w", item.ProductID, err)
			}
			if err := s.repo.InsertItem(ctx, tx, id, item.ProductID, item.Qty, price); err != nil {
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

// listLimit — сколько заказов отдаёт список клиента в одном статусе.
const listLimit = 50

// List возвращает не больше listLimit заказов клиента в указанном статусе,
// от новых к старым.
func (s *Orders) List(ctx context.Context, customerID int64, status string) ([]model.Order, error) {
	return s.repo.ListByCustomerStatus(ctx, customerID, status, listLimit)
}

// Get возвращает заказ по идентификатору. ErrOrderNotFound — заказа нет.
func (s *Orders) Get(ctx context.Context, id int64) (model.Order, error) {
	order, err := s.repo.GetOrder(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return model.Order{}, ErrOrderNotFound
	}
	if err != nil {
		return model.Order{}, err
	}
	return order, nil
}

// Confirm переводит заказ created → confirmed и ставит задание в очередь.
// ERP из этого запроса не вызывается: отправку заберёт воркер.
func (s *Orders) Confirm(ctx context.Context, orderID int64) (model.Order, error) {
	updated, err := s.repo.Confirm(ctx, orderID, uuid.NewString(), uuid.NewString())
	if err != nil {
		return model.Order{}, err
	}
	if !updated {
		return model.Order{}, ErrCannotConfirm
	}
	return s.repo.GetOrder(ctx, orderID)
}

// Retry ставит новое задание для заказа в статусе failed.
// Ноль затронутых строк — ErrCannotRetry, новая строка в integration_jobs не появляется.
func (s *Orders) Retry(ctx context.Context, orderID int64) (model.Order, error) {
	updated, err := s.repo.Retry(ctx, orderID, uuid.NewString(), uuid.NewString())
	if err != nil {
		return model.Order{}, err
	}
	if !updated {
		return model.Order{}, ErrCannotRetry
	}
	return s.repo.GetOrder(ctx, orderID)
}

// Cancel отменяет заказ из created, confirmed и failed.
// Из submitted_to_erp, completed и cancelled отмена запрещена: UPDATE нет.
// Отмена из failed не выпускает новый ключ идемпотентности.
func (s *Orders) Cancel(ctx context.Context, orderID int64) (model.Order, error) {
	state, err := s.repo.SubmitState(ctx, orderID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return model.Order{}, ErrOrderNotFound
		}
		return model.Order{}, err
	}
	if !allow(state.Status, actionCancel) {
		return model.Order{}, ErrCancelNotAllowed
	}

	var updated bool
	if state.Status == model.StatusFailed {
		updated, err = s.repo.CancelFailed(ctx, orderID)
	} else {
		updated, err = s.repo.Cancel(ctx, orderID)
	}
	if err != nil {
		return model.Order{}, err
	}
	if !updated {
		return model.Order{}, ErrCannotCancel
	}
	return s.repo.GetOrder(ctx, orderID)
}
