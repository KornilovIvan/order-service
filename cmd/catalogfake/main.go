// Command catalogfake — фейковый сервис каталога: gRPC :9091 для Order Service,
// HTTP :8084 для ручных проверок и health-check'ов.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	catalogv1 "order-service/internal/gen/catalog/v1"
)

const (
	defaultGRPCAddr = ":9091"
	defaultHTTPAddr = ":8084"
	shutdownTimeout = 5 * time.Second
)

// product — доменное значение, которое хранится в памяти.
// Не proto-сообщение: значения protobuf нельзя копировать (внутри sync.Mutex).
type product struct {
	ID    string
	Name  string
	Price string
}

// catalog — «база» в памяти. Ключ — идентификатор товара.
var catalog = map[string]product{
	"1": {ID: "1", Name: "Widget", Price: "100.00"},
	"2": {ID: "2", Name: "Gadget", Price: "500.00"},
}

// catalogMu защищает catalog: цена меняется в рантайме через PATCH /prices/{id}.
var catalogMu sync.RWMutex

func getProduct(id string) (product, bool) {
	catalogMu.RLock()
	defer catalogMu.RUnlock()
	p, ok := catalog[id]
	return p, ok
}

// setPrice меняет цену товара. false — товара с таким id нет.
func setPrice(id, price string) bool {
	catalogMu.Lock()
	defer catalogMu.Unlock()
	p, ok := catalog[id]
	if !ok {
		return false
	}
	p.Price = price
	catalog[id] = p
	return true
}

// catalogServer реализует catalog.v1.Catalog.
type catalogServer struct {
	catalogv1.UnimplementedCatalogServer
}

func (catalogServer) GetProduct(_ context.Context, req *catalogv1.GetProductRequest) (*catalogv1.GetProductResponse, error) {
	p, ok := getProduct(req.GetId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "product %q not found", req.GetId())
	}
	return &catalogv1.GetProductResponse{
		Product: &catalogv1.Product{Id: p.ID, Name: p.Name, Price: p.Price},
	}, nil
}

// productJSON — форма ответа HTTP-ручки.
type productJSON struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price string `json:"price"`
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

// priceRe — цена в виде десятичной строки с двумя знаками: "100.00", "120.00".
var priceRe = regexp.MustCompile(`^[0-9]+\.[0-9]{2}$`)

// parsePrice принимает цену в трёх формах: JSON-строка "120.00",
// объект {"price":"120.00"} или голый текст 120.00.
func parsePrice(body []byte) (string, error) {
	s := strings.TrimSpace(string(body))

	switch {
	case strings.HasPrefix(s, `"`):
		if err := json.Unmarshal([]byte(s), &s); err != nil {
			return "", fmt.Errorf("invalid JSON string")
		}
	case strings.HasPrefix(s, "{"):
		var v struct {
			Price string `json:"price"`
		}
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			return "", fmt.Errorf("invalid JSON body")
		}
		s = v.Price
	}

	s = strings.TrimSpace(s)
	if !priceRe.MatchString(s) {
		return "", fmt.Errorf("invalid price %q: want decimal with 2 fraction digits, e.g. 120.00", s)
	}
	return s, nil
}

func newHTTPHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /products/{id}", func(w http.ResponseWriter, r *http.Request) {
		p, ok := getProduct(r.PathValue("id"))
		if !ok {
			http.Error(w, `{"error":"product not found"}`, http.StatusNotFound)
			return
		}
		writeJSON(w, productJSON(p))
	})

	// GET /prices/{id} — текущая цена товара, JSON-строка: "100.00".
	mux.HandleFunc("GET /prices/{id}", func(w http.ResponseWriter, r *http.Request) {
		p, ok := getProduct(r.PathValue("id"))
		if !ok {
			http.Error(w, `{"error":"product not found"}`, http.StatusNotFound)
			return
		}
		writeJSON(w, p.Price)
	})

	// PATCH /prices/{id} — изменить цену. Тело: "120.00", {"price":"120.00"} или 120.00.
	mux.HandleFunc("PATCH /prices/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		body, err := io.ReadAll(io.LimitReader(r.Body, 1024))
		if err != nil {
			http.Error(w, `{"error":"cannot read body"}`, http.StatusBadRequest)
			return
		}

		price, err := parsePrice(body)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}

		if !setPrice(id, price) {
			http.Error(w, `{"error":"product not found"}`, http.StatusNotFound)
			return
		}

		log.Printf("catalogfake: price of %q set to %s", id, price)
		writeJSON(w, price)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	return mux
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	grpcAddr := envOr("CATALOG_GRPC_ADDR", defaultGRPCAddr)
	httpAddr := envOr("CATALOG_HTTP_ADDR", defaultHTTPAddr)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen grpc %s: %v", grpcAddr, err)
	}

	grpcSrv := grpc.NewServer()
	catalogv1.RegisterCatalogServer(grpcSrv, catalogServer{})
	reflection.Register(grpcSrv)

	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           newHTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("catalogfake: grpc on %s, http on %s, products: %d", grpcAddr, httpAddr, len(catalog))

	errCh := make(chan error, 2)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			errCh <- err
		}
	}()
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("catalogfake: shutting down: %v", ctx.Err())
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("catalogfake: server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("catalogfake: http shutdown: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		grpcSrv.Stop()
	}
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
