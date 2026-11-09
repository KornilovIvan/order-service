// Command erpfake — фейковая ERP: принимает заказы по HTTP и дедуплицирует их
// по заголовку Idempotency-Key. Порт по умолчанию :8081.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultAddr     = ":8081"
	shutdownTimeout = 5 * time.Second
)

// orderRequest — тело POST /orders.
type orderRequest struct {
	OrderID    int64  `json:"order_id"`
	CustomerID int64  `json:"customer_id"`
	Items      []item `json:"items"`
}

type item struct {
	ProductID string `json:"product_id"`
	Qty       int    `json:"qty"`
}

// erp — состояние фейковой ERP в памяти.
type erp struct {
	mu     sync.Mutex
	orders map[string]acceptedOrder // Idempotency-Key → принятый заказ
	seq    int

	failMode string        // "" | "permanent" | "temporary"
	delay    time.Duration // искусственная задержка ответа (для таймаутов)
	limit    int           // лимит запросов в окно (0 — без лимита)
	window   time.Duration // длина окна лимита
	accepted []time.Time   // отметки принятых запросов для лимита
}

type acceptedOrder struct {
	IdempotencyKey string `json:"idempotency_key"`
	ERPOrderID     string `json:"erp_order_id"`
	OrderID        int64  `json:"order_id"`
	CustomerID     int64  `json:"customer_id"`
}

func newERP() *erp {
	return &erp{
		orders:   make(map[string]acceptedOrder),
		failMode: strings.ToLower(strings.TrimSpace(os.Getenv("ERP_FAIL_MODE"))),
		delay:    durationEnv("ERP_DELAY"),
		limit:    intEnv("ERP_LIMIT", 0),
		window:   time.Duration(intEnv("ERP_WINDOW_SECONDS", 1)) * time.Second,
	}
}

func (e *erp) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", e.handleSubmit)
	mux.HandleFunc("GET /orders", e.handleList)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// handleSubmit — единственное, что умеет ERP: принять заказ.
func (e *erp) handleSubmit(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read body")
		return
	}

	var req orderRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.OrderID <= 0 || len(req.Items) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "order_id and items are required")
		return
	}

	if e.delay > 0 {
		time.Sleep(e.delay)
	}

	switch e.failMode {
	case "permanent":
		writeError(w, http.StatusUnprocessableEntity, "erp: order rejected")
		return
	case "temporary":
		writeError(w, http.StatusServiceUnavailable, "erp: temporarily unavailable")
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Дубль по ключу: заказ уже принят — отдаём прежний номер.
	if prev, ok := e.orders[key]; ok {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":        "duplicate idempotency key",
			"erp_order_id": prev.ERPOrderID,
		})
		return
	}

	if !e.allowLocked(time.Now()) {
		writeError(w, http.StatusTooManyRequests, "erp: rate limit exceeded")
		return
	}

	e.seq++
	rec := acceptedOrder{
		IdempotencyKey: key,
		ERPOrderID:     fmt.Sprintf("ERP-%d", e.seq),
		OrderID:        req.OrderID,
		CustomerID:     req.CustomerID,
	}
	e.orders[key] = rec
	e.accepted = append(e.accepted, time.Now())

	log.Printf("erpfake: accepted order %d as %s (key %s)", req.OrderID, rec.ERPOrderID, key)
	writeJSON(w, http.StatusOK, map[string]any{"erp_order_id": rec.ERPOrderID, "status": "accepted"})
}

// handleList — отладочная ручка: что уже принято.
func (e *erp) handleList(w http.ResponseWriter, _ *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()

	list := make([]acceptedOrder, 0, len(e.orders))
	for _, rec := range e.orders {
		list = append(list, rec)
	}
	writeJSON(w, http.StatusOK, map[string]any{"orders": list, "count": len(list)})
}

// allowLocked проверяет лимит в скользящем окне. Вызывать под e.mu.
func (e *erp) allowLocked(now time.Time) bool {
	if e.limit <= 0 || e.window <= 0 {
		return true
	}

	cutoff := now.Add(-e.window)
	kept := e.accepted[:0]
	for _, t := range e.accepted {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	e.accepted = kept

	return len(e.accepted) < e.limit
}

func main() {
	addr := envOr("ERP_ADDR", defaultAddr)
	e := newERP()

	srv := &http.Server{
		Addr:              addr,
		Handler:           e.handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("erpfake: listening on %s (failMode=%q delay=%s limit=%d/%s)",
			addr, e.failMode, e.delay, e.limit, e.window)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("erpfake: serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("erpfake: shutting down: %v", ctx.Err())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("erpfake: shutdown: %v", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("erpfake: encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func durationEnv(key string) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0
	}
	return d
}
