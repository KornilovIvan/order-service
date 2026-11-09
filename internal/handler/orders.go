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

	"order-service/internal/model"
	"order-service/internal/service"
)

type Orders struct {
	svc *service.Orders
}

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

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/orders", h.create)
		r.Get("/orders/{id}", h.get)
	})

	return r
}

// createOrderRequest — тело POST /api/v1/orders.
type createOrderRequest struct {
	CustomerID int64             `json:"customer_id"`
	Items      []createOrderItem `json:"items"`
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

// orderResponse — форма заказа в ответе. Цены здесь нет: она не копируется.
type orderResponse struct {
	ID         int64               `json:"id"`
	CustomerID int64               `json:"customer_id"`
	Status     string              `json:"status"`
	CreatedAt  time.Time           `json:"created_at"`
	Items      []orderItemResponse `json:"items"`
}

type orderItemResponse struct {
	ProductID string `json:"product_id"`
	Qty       int    `json:"qty"`
}

func (h *Orders) create(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	in := service.CreateInput{CustomerID: req.CustomerID}
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
		default:
			log.Printf("handler: create order: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}

	writeJSON(w, http.StatusCreated, toResponse(order))
}

func (h *Orders) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}

	order, err := h.svc.Get(r.Context(), id)
	if err != nil {
		log.Printf("handler: get order %d: %v", id, err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toResponse(order))
}

func toResponse(order model.Order) orderResponse {
	items := make([]orderItemResponse, 0, len(order.Items))
	for _, item := range order.Items {
		items = append(items, orderItemResponse{ProductID: item.ProductID, Qty: item.Qty})
	}
	return orderResponse{
		ID:         order.ID,
		CustomerID: order.CustomerID,
		Status:     order.Status,
		CreatedAt:  order.CreatedAt,
		Items:      items,
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
