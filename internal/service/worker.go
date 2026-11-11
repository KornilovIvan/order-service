package service

import (
	"context"
	"errors"
	"log"
	"time"

	"order-service/internal/erp"
	"order-service/internal/metrics"
)

const defaultJobIdle = time.Second

// RunJobs в цикле захватывает задания и отправляет их в ERP.
// Пустая очередь — сон idle (POLL_IDLE).
func (s *Orders) RunJobs(ctx context.Context, idle time.Duration) {
	if idle <= 0 {
		idle = defaultJobIdle
	}
	for {
		if ctx.Err() != nil {
			return
		}
		ok, err := s.ProcessNextJob(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("jobs: %v", err)
		}
		if ok && err == nil {
			continue
		}
		timer := time.NewTimer(idle)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// ProcessNextJob захватывает одно задание, коммитит, собирает тело, вызывает Submit
// и коротко пишет результат. false — очередь пуста.
func (s *Orders) ProcessNextJob(ctx context.Context) (bool, error) {
	job, ok, err := s.repo.ClaimJob(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	order, err := s.repo.GetOrder(ctx, job.OrderID)
	if err != nil {
		if relErr := s.repo.ReleaseJob(ctx, job, 30*time.Second); relErr != nil {
			return true, errors.Join(err, relErr)
		}
		return true, err
	}

	if s.limiter != nil {
		slot, limErr := s.limiter.Claim(ctx)
		if limErr != nil {
			if relErr := s.repo.ReleaseJob(ctx, job, 30*time.Second); relErr != nil {
				return true, errors.Join(limErr, relErr)
			}
			return true, limErr
		}
		if !slot.Allowed {
			return true, s.repo.ReleaseJob(ctx, job, slot.RetryAfter)
		}
	}

	req := buildSubmit(order)
	if s.payload != nil {
		req = s.payload(order)
	}
	if err := validateERPPayload(req); err != nil {
		metrics.PrepareError(order.OrderType, metrics.ErrorInvalidDeliveryDate)
		return true, s.repo.ApplyJobPermanent(ctx, job)
	}

	erpOrderID, err := s.erp.Submit(ctx, job.IdempotencyKey, req)
	if err != nil {
		kind := metrics.ErrorTemporary
		if errors.Is(err, erp.ErrPermanent) {
			kind = metrics.ErrorPermanent
		}
		metrics.SubmitError(order.OrderType, kind)
	}
	switch {
	case err == nil:
		return true, s.repo.ApplyJobSuccess(ctx, job, erpOrderID)
	case errors.Is(err, erp.ErrPermanent):
		return true, s.repo.ApplyJobPermanent(ctx, job)
	default:
		return true, s.repo.ApplyJobTemporary(ctx, job)
	}
}
