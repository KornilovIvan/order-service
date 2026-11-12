// Package metricsclient оборачивает сгенерированный gRPC-клиент JobService,
// чтобы измерить длительность сетевого roundtrip'а воркера.
//
// Воркер не умеет считать метрики по серверу (у него нет базы и своего gRPC),
// поэтому единственная его метрика — задержка вызовов ClaimJob/CompleteJob.
package metricsclient

import (
	"context"
	"time"

	"google.golang.org/grpc"

	jobsv1 "order-service/internal/gen/jobs/v1"
	"order-service/internal/metrics"
)

// Client — декоратор jobsv1.JobServiceClient.
type Client struct {
	jobsv1.JobServiceClient
}

// Wrap возвращает клиент, который пишет grpc_client_roundtrip_latency{method}.
func Wrap(c jobsv1.JobServiceClient) *Client {
	return &Client{JobServiceClient: c}
}

func (c *Client) ClaimJob(ctx context.Context, in *jobsv1.ClaimRequest, opts ...grpc.CallOption) (*jobsv1.ClaimResponse, error) {
	start := time.Now()
	resp, err := c.JobServiceClient.ClaimJob(ctx, in, opts...)
	metrics.ClientRoundtrip("ClaimJob", time.Since(start))
	return resp, err
}

func (c *Client) CompleteJob(ctx context.Context, in *jobsv1.CompleteRequest, opts ...grpc.CallOption) (*jobsv1.CompleteResponse, error) {
	start := time.Now()
	resp, err := c.JobServiceClient.CompleteJob(ctx, in, opts...)
	metrics.ClientRoundtrip("CompleteJob", time.Since(start))
	return resp, err
}
