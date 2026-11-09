// Command api — HTTP API Order Service.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"order-service/internal/catalog"
	"order-service/internal/handler"
	"order-service/internal/repository"
	"order-service/internal/service"
)

const shutdownTimeout = 10 * time.Second

func main() {
	dsn := envOr("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable")
	httpAddr := envOr("HTTP_ADDR", ":8080")
	catalogAddr := envOr("CATALOG_GRPC_ADDR", "localhost:9091")

	db, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		log.Fatalf("api: connect db: %v", err)
	}
	defer func() { _ = db.Close() }()

	prices, err := catalog.New(catalogAddr)
	if err != nil {
		log.Fatalf("api: catalog client: %v", err)
	}
	defer func() { _ = prices.Close() }()

	svc := service.NewOrders(repository.NewOrders(db), prices)

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler.NewOrders(svc).Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("api: listening on %s, catalog at %s", httpAddr, catalogAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("api: serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("api: shutting down: %v", ctx.Err())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("api: shutdown: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
