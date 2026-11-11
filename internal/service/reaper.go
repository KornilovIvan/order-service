package service

import (
	"context"
	"fmt"
	"log"
	"time"
)

// ValidateReaperAfter проверяет, что порог зависания строго больше таймаута ERP.
// Иначе живой Submit ещё идёт, а reaper уже отдаст задание другому воркеру.
func ValidateReaperAfter(after, erpTimeout time.Duration) error {
	if after <= erpTimeout {
		return fmt.Errorf("REAPER_AFTER (%s) must be greater than ERP_TIMEOUT (%s)", after, erpTimeout)
	}
	return nil
}

// RunReaper раз в every снимает in_progress старше after.
func (s *Orders) RunReaper(ctx context.Context, every, after time.Duration) {
	if every <= 0 {
		every = 10 * time.Second
	}
	if err := s.ReapOnce(ctx, after); err != nil && ctx.Err() == nil {
		log.Printf("reaper: %v", err)
	}

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.ReapOnce(ctx, after); err != nil && ctx.Err() == nil {
				log.Printf("reaper: %v", err)
			}
		}
	}
}

// ReapOnce — один проход reaper.
func (s *Orders) ReapOnce(ctx context.Context, after time.Duration) error {
	n, err := s.repo.ReapStuckJobs(ctx, after)
	if err != nil {
		return err
	}
	if n > 0 {
		log.Printf("reaper: released %d stuck job(s)", n)
	}
	return nil
}
