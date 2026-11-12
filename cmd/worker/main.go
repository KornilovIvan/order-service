// Command worker — внешний воркер: забирает задания по gRPC, отправляет их в ERP
// по HTTP и сообщает результат. База данных воркеру не нужна.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobsv1 "order-service/internal/gen/jobs/v1"
	"order-service/internal/worker"
)

func main() {
	jobsAddr := envOr("JOBS_ADDR", "localhost:9090")
	erpBaseURL := envOr("ERP_BASE_URL", "http://localhost:8081")
	pprofAddr := envOr("WORKER_PPROF_ADDR", ":8082")
	workerID := envOr("WORKER_ID", defaultWorkerID())

	cfg := worker.Config{
		WorkerID:   workerID,
		ERPBaseURL: erpBaseURL,
		PollIdle:   durationEnv("POLL_IDLE", time.Second),
		ClaimRPC:   durationEnv("RPC_DEADLINE", 5*time.Second),
		Shutdown:   durationEnv("SHUTDOWN_TIMEOUT", 20*time.Second),
	}

	conn, err := grpc.NewClient(jobsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("worker: connect jobs %s: %v", jobsAddr, err)
	}
	defer func() { _ = conn.Close() }()

	go servePprof(pprofAddr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("worker: id=%s jobs=%s erp=%s pprof=%s poll_idle=%s claim_rpc=%s shutdown=%s",
		workerID, jobsAddr, erpBaseURL, pprofAddr, cfg.PollIdle, cfg.ClaimRPC, cfg.Shutdown)

	worker.New(jobsv1.NewJobServiceClient(conn), cfg).Run(ctx)

	log.Printf("worker: stopped")
}

func servePprof(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("worker: pprof %s: %v", addr, err)
	}
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
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
