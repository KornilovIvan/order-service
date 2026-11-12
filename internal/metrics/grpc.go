package metrics

import (
	"context"
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	// GRPCServerHandled — сколько gRPC-запросов обработал API.
	// Label method — короткое имя метода (ClaimJob, CompleteJob), code — код статуса.
	// Именно по code видно, что воркер получает FailedPrecondition из-за протухшего lease.
	GRPCServerHandled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_server_handled_total",
		Help: "Total number of gRPC requests handled by the API, by method and status code",
	}, []string{"method", "code"})

	// GRPCClientRoundtripLatency — время полного клиентского вызова воркера,
	// включая сериализацию, сеть и разбор ответа. В секундах (имя без _seconds
	// зафиксировано требованиями).
	GRPCClientRoundtripLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "grpc_client_roundtrip_latency",
		Help: "Worker gRPC call roundtrip latency in seconds, by method",
		Buckets: []float64{
			0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
			0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"method"})
)

// ServerHandled учитывает один обработанный gRPC-запрос API.
func ServerHandled(method, code string) {
	GRPCServerHandled.WithLabelValues(method, code).Inc()
}

// ClientRoundtrip учитывает один клиентский gRPC-вызов воркера.
func ClientRoundtrip(method string, d time.Duration) {
	GRPCClientRoundtripLatency.WithLabelValues(method).Observe(d.Seconds())
}

// UnaryServerInterceptor считает grpc_server_handled_total{method,code}.
// Имя метода короткое (path.Base), чтобы metric совпадал с именем span'а.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		ServerHandled(path.Base(info.FullMethod), status.Code(err).String())
		return resp, err
	}
}
