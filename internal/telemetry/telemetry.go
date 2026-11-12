// Package telemetry настраивает OpenTelemetry: экспорт трейсов по OTLP/HTTP,
// gRPC-интерсептор с короткими именами span'ов и проброс контекста в HTTP.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Init включает трейсинг и возвращает функцию остановки.
// endpoint — базовый OTLP/HTTP адрес коллектора, например http://localhost:4318.
// Пустая строка выключает трейсинг (no-op).
func Init(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		// Короткий батч: трейсы должны появляться в UI сразу после confirm.
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// UnaryServerInterceptor создаёт серверный span с коротким именем метода
// (ClaimJob, CompleteJob) и подхватывает trace context из metadata.
func UnaryServerInterceptor(tracerName string) grpc.UnaryServerInterceptor {
	tracer := otel.Tracer(tracerName)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = otel.GetTextMapPropagator().Extract(ctx, mdCarrier{md: md})
		}

		ctx, span := tracer.Start(ctx, path.Base(info.FullMethod),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(semconv.RPCSystemNameGRPC),
		)
		defer span.End()

		resp, err := handler(ctx, req)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return resp, err
	}
}

// InjectGRPC кладёт trace context в исходящие gRPC metadata.
func InjectGRPC(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	} else {
		md = md.Copy()
	}
	otel.GetTextMapPropagator().Inject(ctx, mdCarrier{md: md})
	return metadata.NewOutgoingContext(ctx, md)
}

// InjectHTTP кладёт trace context в HTTP-заголовки (traceparent).
func InjectHTTP(ctx context.Context, header http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(header))
}

// mdCarrier — TextMapCarrier поверх gRPC metadata.
type mdCarrier struct {
	md metadata.MD
}

func (c mdCarrier) Get(key string) string {
	values := c.md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (c mdCarrier) Set(key, value string) {
	c.md.Set(key, value)
}

func (c mdCarrier) Keys() []string {
	keys := make([]string, 0, len(c.md))
	for key := range c.md {
		keys = append(keys, key)
	}
	return keys
}
