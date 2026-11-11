// Package handler — HTTP-слой Order Service.
package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"order-service/internal/model"
	"order-service/internal/service"

	_ "order-service/internal/metrics"
)

type Orders struct {
	svc *service.Orders
}

// deliveryDateLayout — формат даты без времени (тип date в PostgreSQL).
const deliveryDateLayout = "2006-01-02"

func NewOrders(svc *service.Orders) *Orders {
	return &Orders{svc: svc}
}

// Router собирает маршруты API.
func (h *Orders) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/orders", h.create)
		r.Get("/orders", h.list)
		r.Get("/orders/{id}", h.get)
		r.Post("/orders/{id}/confirm", h.confirm)
		r.Post("/orders/{id}/retry", h.retry)
		r.Post("/orders/{id}/cancel", h.cancel)
	})

	return r
}

// createOrderRequest — тело POST /api/v1/orders.
type createOrderRequest struct {
	CustomerID   int64             `json:"customer_id"`
	OrderType    string            `json:"order_type"`
	DeliveryDate *string           `json:"delivery_date"`
	Items        []createOrderItem `json:"items"`
}

type createOrderItem struct {
	ProductID productID `json:"product_id"`
	Qty       int       `json:"qty"`
}

// productID принимает и число, и строку: {"product_id":1} и {"product_id":"1"}.
type productID string

func (p *productID) UnmarshalJSON(data []byte) error {
	s := strings.TrimSpace(string(data))
	if s == "null" {
		*p = ""
		return nil
	}
	if strings.HasPrefix(s, `"`) {
		var v string
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		*p = productID(v)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*p = productID(n.String())
	return nil
}

// orderResponse — форма заказа в ответе.
type orderResponse struct {
	ID           int64               `json:"id"`
	CustomerID   int64               `json:"customer_id"`
	Status       string              `json:"status"`
	OrderType    string              `json:"order_type"`
	DeliveryDate *string             `json:"delivery_date"`
	CreatedAt    time.Time           `json:"created_at"`
	Items        []orderItemResponse `json:"items"`
}

type orderItemResponse struct {
	ProductID    string `json:"product_id"`
	Qty          int    `json:"qty"`
	PriceAtOrder string `json:"price_at_order"`
}

func (h *Orders) create(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	in := service.CreateInput{
		CustomerID: req.CustomerID,
		OrderType:  strings.TrimSpace(req.OrderType),
	}

	if req.DeliveryDate != nil && strings.TrimSpace(*req.DeliveryDate) != "" {
		parsed, err := time.Parse(deliveryDateLayout, strings.TrimSpace(*req.DeliveryDate))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid delivery_date: want YYYY-MM-DD")
			return
		}
		in.DeliveryDate = &parsed
	}

	for _, item := range req.Items {
		in.Items = append(in.Items, service.CreateItem{
			ProductID: string(item.ProductID),
			Qty:       item.Qty,
		})
	}

	order, err := h.svc.Create(r.Context(), in)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrProductNotFound):
			writeError(w, http.StatusBadRequest, "product not found")
		case errors.Is(err, service.ErrCustomerNotFound):
			writeError(w, http.StatusBadRequest, "customer not found")
		case errors.Is(err, service.ErrNoItems):
			writeError(w, http.StatusBadRequest, "items must not be empty")
		case errors.Is(err, service.ErrInvalidQty):
			writeError(w, http.StatusBadRequest, "qty must be >= 1")
		case errors.Is(err, service.ErrInvalidOrderType):
			writeError(w, http.StatusBadRequest, "invalid order_type: want regular or preorder")
		case errors.Is(err, service.ErrPreorderWithoutDate):
			writeError(w, http.StatusBadRequest, "delivery_date is required for preorder")
		default:
			log.Printf("handler: create order: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	writeJSON(w, http.StatusCreated, toResponse(order))
}

func (h *Orders) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rawCustomerID := strings.TrimSpace(q.Get("customer_id"))
	status := strings.TrimSpace(q.Get("status"))
	if rawCustomerID == "" || status == "" {
		writeError(w, http.StatusBadRequest, "customer_id and status are required")
		return
	}
	customerID, err := strconv.ParseInt(rawCustomerID, 10, 64)
	if err != nil || customerID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid customer_id")
		return
	}

	orders, err := h.svc.List(r.Context(), customerID, status)
	if err != nil {
		log.Printf("handler: list orders: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := make([]orderResponse, 0, len(orders))
	for _, order := range orders {
		resp = append(resp, toResponse(order))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Orders) get(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDFromRequest(w, r)
	if !ok {
		return
	}

	order, err := h.svc.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrOrderNotFound) {
			writeError(w, http.StatusNotFound, "order not found")
			return
		}
		log.Printf("handler: get order %d: %v", id, err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toResponse(order))
}

func (h *Orders) confirm(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDFromRequest(w, r)
	if !ok {
		return
	}

	order, err := h.svc.Confirm(r.Context(), id)
	h.writeAccepted(w, id, order, err, service.ErrCannotConfirm, "cannot confirm")
}

func (h *Orders) retry(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDFromRequest(w, r)
	if !ok {
		return
	}

	order, err := h.svc.Retry(r.Context(), id)
	h.writeAccepted(w, id, order, err, service.ErrCannotRetry, "cannot retry")
}

func (h *Orders) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := orderIDFromRequest(w, r)
	if !ok {
		return
	}

	order, err := h.svc.Cancel(r.Context(), id)
	h.writeTransition(w, id, order, err, service.ErrCannotCancel, "cannot cancel")
}

// writeTransition — общий маппинг ошибок переходов состояний и вызовов ERP.
func (h *Orders) writeTransition(w http.ResponseWriter, id int64, order model.Order, err error, conflictErr error, conflictMsg string) {
	if err != nil {
		switch {
		case errors.Is(err, service.ErrOrderNotFound):
			writeError(w, http.StatusNotFound, "order not found")
		case errors.Is(err, service.ErrCancelNotAllowed):
			writeError(w, http.StatusConflict, "cancel not allowed")
		case errors.Is(err, conflictErr):
			writeError(w, http.StatusConflict, conflictMsg)
		case errors.Is(err, service.ErrERPUnavailable):
			log.Printf("handler: order %d: %v", id, err)
			writeError(w, http.StatusBadGateway, "erp unavailable")
		default:
			log.Printf("handler: order %d: %v", id, err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	writeJSON(w, http.StatusOK, toResponse(order))
}

// writeAccepted — confirm/retry: задание принято в очередь, ERP ещё не вызывалась.
func (h *Orders) writeAccepted(w http.ResponseWriter, id int64, order model.Order, err error, conflictErr error, conflictMsg string) {
	if err != nil {
		h.writeTransition(w, id, order, err, conflictErr, conflictMsg)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": order.Status})
}

func orderIDFromRequest(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return 0, false
	}
	return id, true
}

func toResponse(order model.Order) orderResponse {
	items := make([]orderItemResponse, 0, len(order.Items))
	for _, item := range order.Items {
		items = append(items, orderItemResponse{
			ProductID:    item.ProductID,
			Qty:          item.Qty,
			PriceAtOrder: item.PriceAtOrder,
		})
	}

	var deliveryDate *string
	if order.DeliveryDate != nil {
		formatted := order.DeliveryDate.Format(deliveryDateLayout)
		deliveryDate = &formatted
	}

	return orderResponse{
		ID:           order.ID,
		CustomerID:   order.CustomerID,
		Status:       order.Status,
		OrderType:    order.OrderType,
		DeliveryDate: deliveryDate,
		CreatedAt:    order.CreatedAt,
		Items:        items,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("handler: encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
