// Package jobs реализует gRPC-контракт jobs.v1 поверх очереди integration_jobs.
//
// Это тот же код захвата и те же ветки завершения, что у встроенного воркера:
// ClaimJob использует repository.ClaimJob (FOR UPDATE SKIP LOCKED), а CompleteJob
// вызывает ApplyJobSuccess / ApplyJobPermanent / ApplyJobTemporary.
// Отличие — lease_token: внешний воркер обязан вернуть токен, выданный при
// захвате, иначе результат не будет применён.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobsv1 "order-service/internal/gen/jobs/v1"
	"order-service/internal/repository"
	"order-service/internal/service"
)

// releaseDelay — через сколько вернуть задание в очередь, если не удалось
// собрать тело запроса после захвата.
const releaseDelay = 30 * time.Second

// Service — gRPC-обёртка над очередью заданий.
type Service struct {
	jobsv1.UnimplementedJobServiceServer

	repo *repository.Orders
	idle time.Duration
}

// New создаёт сервис. idle — сколько ждать, если очередь пуста (retry_after).
func New(repo *repository.Orders, idle time.Duration) *Service {
	if idle <= 0 {
		idle = time.Second
	}
	return &Service{repo: repo, idle: idle}
}

// ClaimJob захватывает одно задание. Пустая очередь — job == nil и retry_after.
func (s *Service) ClaimJob(ctx context.Context, req *jobsv1.ClaimRequest) (*jobsv1.ClaimResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	job, ok, err := s.repo.ClaimJob(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "claim job: %v", err)
	}
	if !ok {
		return &jobsv1.ClaimResponse{
			RetryAfter: timestamppb.New(time.Now().Add(s.idle)),
		}, nil
	}

	order, err := s.repo.GetOrder(ctx, job.OrderID)
	if err != nil {
		if relErr := s.repo.ReleaseJob(ctx, job, releaseDelay); relErr != nil {
			return nil, status.Errorf(codes.Internal, "release job %d: %v", job.ID, relErr)
		}
		return nil, status.Errorf(codes.Internal, "load order %d: %v", job.OrderID, err)
	}

	body, err := json.Marshal(service.BuildSubmit(order))
	if err != nil {
		if relErr := s.repo.ReleaseJob(ctx, job, releaseDelay); relErr != nil {
			return nil, status.Errorf(codes.Internal, "release job %d: %v", job.ID, relErr)
		}
		return nil, status.Errorf(codes.Internal, "encode request body: %v", err)
	}

	return &jobsv1.ClaimResponse{Job: &jobsv1.Job{
		Id:             job.ID,
		OrderId:        job.OrderID,
		IdempotencyKey: job.IdempotencyKey,
		LockedAt:       timestamppb.New(job.LockedAt),
		CorrelationId:  job.CorrelationID,
		RequestBody:    string(body),
		LeaseToken:     job.LeaseToken,
	}}, nil
}

// CompleteJob применяет результат выполнения.
//
// Те же ветки, что у встроенного воркера:
//   - 2xx / 409 → ApplyJobSuccess (submitted_to_erp + номер ERP);
//   - 400 / 422 → ApplyJobPermanent (failed);
//   - всё остальное (0, 408, 429, 5xx, таймаут) → ApplyJobTemporary (queued).
//
// Перед применением проверяется lease_token и locked_at: если задание успели
// перехватить после истечения lease, результат не применяется.
func (s *Service) CompleteJob(ctx context.Context, req *jobsv1.CompleteRequest) (*jobsv1.CompleteResponse, error) {
	if req.GetJobId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	if req.GetLeaseToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_token is required")
	}
	if req.GetLockedAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "locked_at is required")
	}

	job, err := s.repo.JobByID(ctx, req.GetJobId())
	if errors.Is(err, repository.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "job %d not found", req.GetJobId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load job %d: %v", req.GetJobId(), err)
	}

	// Тот же захват? Токен обязан совпасть, время — тоже (оно входит в lease).
	if job.LeaseToken != req.GetLeaseToken() || !job.LockedAt.Equal(req.GetLockedAt().AsTime()) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"lease mismatch for job %d: job is not held by this worker", req.GetJobId())
	}

	switch classify(req.GetHttpStatus()) {
	case outcomeSuccess:
		if err := s.repo.ApplyJobSuccess(ctx, job, erpOrderID(req.GetResponseBody())); err != nil {
			return nil, status.Errorf(codes.Internal, "apply success: %v", err)
		}
	case outcomePermanent:
		if err := s.repo.ApplyJobPermanent(ctx, job); err != nil {
			return nil, status.Errorf(codes.Internal, "apply permanent: %v", err)
		}
	default:
		if err := s.repo.ApplyJobTemporary(ctx, job); err != nil {
			return nil, status.Errorf(codes.Internal, "apply temporary: %v", err)
		}
	}

	return &jobsv1.CompleteResponse{}, nil
}

type outcome int

const (
	outcomeTemporary outcome = iota
	outcomeSuccess
	outcomePermanent
)

func classify(httpStatus int32) outcome {
	switch {
	case (httpStatus >= 200 && httpStatus < 300) || httpStatus == 409:
		return outcomeSuccess
	case httpStatus == 400 || httpStatus == 422:
		return outcomePermanent
	default:
		// 0 (сеть/таймаут), 408, 429, 5xx и прочее — повторяем.
		return outcomeTemporary
	}
}

// erpOrderID достаёт номер заказа из тела ответа ERP.
func erpOrderID(responseBody string) string {
	if responseBody == "" {
		return ""
	}
	var resp struct {
		ERPOrderID string `json:"erp_order_id"`
	}
	if err := json.Unmarshal([]byte(responseBody), &resp); err != nil {
		return ""
	}
	return resp.ERPOrderID
}
