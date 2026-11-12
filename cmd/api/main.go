package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"order-service/internal/catalog"
	"order-service/internal/erp"
	jobsv1 "order-service/internal/gen/jobs/v1"
	"order-service/internal/handler"
	"order-service/internal/jobs"
	"order-service/internal/limiter"
	"order-service/internal/publisher"
	"order-service/internal/repository"
	"order-service/internal/service"
)

const shutdownTimeout = 10 * time.Second

func main() {
	dsn := envOr("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable")
	httpAddr := envOr("HTTP_ADDR", ":8080")
	grpcAddr := envOr("GRPC_ADDR", ":9090")
	catalogAddr := envOr("CATALOG_GRPC_ADDR", "localhost:9091")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	priceCacheTTL := durationEnv("PRICE_CACHE_TTL", 60*time.Second)
	erpBaseURL := envOr("ERP_BASE_URL", "http://localhost:8081")
	erpTimeout := durationEnv("ERP_TIMEOUT", 15*time.Second)
	reaperAfter := durationEnv("REAPER_AFTER", 30*time.Second)
	reaperEvery := durationEnv("REAPER_EVERY", 10*time.Second)
	if err := service.ValidateReaperAfter(reaperAfter, erpTimeout); err != nil {
		log.Fatalf("api: %v", err)
	}

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

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		log.Fatalf("api: redis %s: %v", redisAddr, err)
	}
	pingCancel()
	priceCache := catalog.NewPriceCache(prices, rdb, priceCacheTTL)

	repo := repository.NewOrders(db)

	kafkaAddrs := strings.Split(envOr("KAFKA_ADDR", "localhost:19092"), ",")
	kafkaTopic := envOr("KAFKA_TOPIC", publisher.EventOrderCreated)
	pollInterval := durationEnv("POLL_INTERVAL", time.Second)

	events := publisher.New(kafkaAddrs, kafkaTopic)
	defer func() { _ = events.Close() }()
	eventsWorker := publisher.NewWorker(repo, events, pollInterval, 50)
	jobIdle := durationEnv("POLL_IDLE", time.Second)

	svc := service.NewOrders(repo, priceCache, erp.New(erpBaseURL, erpTimeout))
	erpLimit := intEnv("ERP_LIMIT", 0)
	erpWindow := durationEnv("ERP_WINDOW", time.Second)
	if erpLimit > 0 {
		svc.UseLimiter(limiter.New(rdb, erpLimit, erpWindow))
	}

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler.NewOrders(svc).Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("api: listen grpc %s: %v", grpcAddr, err)
	}
	grpcSrv := grpc.NewServer()
	jobsv1.RegisterJobServiceServer(grpcSrv, jobs.New(repo, jobIdle))
	reflection.Register(grpcSrv)

	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		if err := grpcSrv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("api: grpc serve: %v", err)
		}
	}()

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		eventsWorker.Run(ctx)
	}()

	// Задания забирает только внешний процесс cmd/worker (gRPC ClaimJob):
	// в API горутины опроса очереди нет, иначе захватчиков было бы два.
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		svc.RunReaper(ctx, reaperEvery, reaperAfter)
	}()

	go func() {
		log.Printf("api: listening on %s, grpc on %s, catalog at %s, redis at %s (price ttl %s), erp at %s (limit %d / window %s, reaper after %s / every %s), jobs idle %s, kafka %s/%s",
			httpAddr, grpcAddr, catalogAddr, redisAddr, priceCacheTTL, erpBaseURL, erpLimit, erpWindow, reaperAfter, reaperEvery, jobIdle, strings.Join(kafkaAddrs, ","), kafkaTopic)
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

	<-workerDone
	<-reaperDone

	// gRPC останавливаем после HTTP: воркер успевает дописать результат задания.
	grpcSrv.GracefulStop()
	<-grpcDone
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

func durationEnv(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
