package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"order-service/internal/model"
)

const claimJobSQL = `
UPDATE integration_jobs
SET status = 'in_progress', locked_at = now(), lease_token = gen_random_uuid()
WHERE id = (
    SELECT id FROM integration_jobs
    WHERE status = 'queued' AND run_at <= now()
    ORDER BY run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, order_id, idempotency_key::text AS idempotency_key, locked_at, lease_token::text AS lease_token, correlation_id`

const jobRetryDelay = 30 * time.Second

// ClaimJob берёт одно queued-задание, ставит in_progress и выдаёт lease_token.
// Коммит здесь же: другие воркеры видят захват до вызова ERP.
func (r *Orders) ClaimJob(ctx context.Context) (model.Job, bool, error) {
	var job model.Job
	err := r.WithTx(ctx, func(tx *sqlx.Tx) error {
		err := tx.GetContext(ctx, &job, claimJobSQL)
		if errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		}
		if err != nil {
			return fmt.Errorf("claim job: %w", err)
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return model.Job{}, false, nil
	}
	if err != nil {
		return model.Job{}, false, err
	}
	return job, true, nil
}

// ApplyJobSuccess: 200/409 ERP.
// confirmed → submitted_to_erp + erp_order_id + задание done.
// cancelled → только erp_order_id, задание done.
func (r *Orders) ApplyJobSuccess(ctx context.Context, job model.Job, erpOrderID string) error {
	return r.finishJob(ctx, job, func(tx *sqlx.Tx, orderStatus string) error {
		if err := setJobStatus(ctx, tx, job, model.JobDone, false, nil); err != nil {
			return err
		}
		if orderStatus == model.StatusCancelled {
			_, err := tx.ExecContext(ctx,
				`UPDATE orders SET erp_order_id = COALESCE(NULLIF($2, ''), erp_order_id) WHERE id = $1`,
				job.OrderID, erpOrderID)
			return err
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE orders
			    SET status = 'submitted_to_erp',
			        erp_order_id = COALESCE(NULLIF($2, ''), erp_order_id)
			  WHERE id = $1 AND status = 'confirmed'`,
			job.OrderID, erpOrderID)
		return err
	})
}

// ApplyJobTemporary: 408/429/5xx/таймаут.
// confirmed → снова queued через 30с, lease_token NULL.
// cancelled → задание cancelled, run_at не двигаем.
func (r *Orders) ApplyJobTemporary(ctx context.Context, job model.Job) error {
	return r.finishJob(ctx, job, func(tx *sqlx.Tx, orderStatus string) error {
		if orderStatus == model.StatusCancelled {
			return setJobStatus(ctx, tx, job, model.JobCancelled, true, nil)
		}
		retryAt := time.Now().Add(jobRetryDelay)
		return setJobStatus(ctx, tx, job, model.JobQueued, true, &retryAt)
	})
}

// ApplyJobPermanent: 400/422.
// confirmed → заказ и задание failed.
// cancelled → заказ не failed, задание failed.
func (r *Orders) ApplyJobPermanent(ctx context.Context, job model.Job) error {
	return r.finishJob(ctx, job, func(tx *sqlx.Tx, orderStatus string) error {
		if err := setJobStatus(ctx, tx, job, model.JobFailed, true, nil); err != nil {
			return err
		}
		if orderStatus == model.StatusCancelled {
			return nil
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE orders SET status = 'failed' WHERE id = $1 AND status = 'confirmed'`,
			job.OrderID)
		return err
	})
}

// ReleaseJob возвращает задание в очередь (лимитер / ошибка Redis после захвата).
// confirmed → queued, run_at сдвинут, токен и locked_at NULL.
// cancelled → cancelled, токен NULL.
func (r *Orders) ReleaseJob(ctx context.Context, job model.Job, retryAfter time.Duration) error {
	if retryAfter <= 0 {
		retryAfter = jobRetryDelay
	}
	return r.finishJob(ctx, job, func(tx *sqlx.Tx, orderStatus string) error {
		if orderStatus == model.StatusCancelled {
			return setJobStatus(ctx, tx, job, model.JobCancelled, true, nil)
		}
		retryAt := time.Now().Add(retryAfter)
		return setJobStatus(ctx, tx, job, model.JobQueued, true, &retryAt)
	})
}

func (r *Orders) finishJob(ctx context.Context, job model.Job, apply func(*sqlx.Tx, string) error) error {
	return r.WithTx(ctx, func(tx *sqlx.Tx) error {
		var n int
		if err := tx.GetContext(ctx, &n,
			`SELECT count(*) FROM integration_jobs WHERE id = $1 AND lease_token = $2`,
			job.ID, job.LeaseToken); err != nil {
			return fmt.Errorf("check lease: %w", err)
		}
		if n != 1 {
			return nil
		}
		var orderStatus string
		if err := tx.GetContext(ctx, &orderStatus,
			`SELECT status FROM orders WHERE id = $1`, job.OrderID); err != nil {
			return fmt.Errorf("select order for job: %w", err)
		}
		if err := apply(tx, orderStatus); err != nil {
			return fmt.Errorf("apply job result: %w", err)
		}
		return nil
	})
}

func setJobStatus(ctx context.Context, tx *sqlx.Tx, job model.Job, status string, clearLease bool, retryAt *time.Time) error {
	query := `UPDATE integration_jobs SET status = $3 WHERE id = $1 AND lease_token = $2`
	args := []any{job.ID, job.LeaseToken, status}
	if clearLease && retryAt != nil {
		query = `UPDATE integration_jobs
		            SET status = $3, lease_token = NULL, locked_at = NULL, run_at = $4
		          WHERE id = $1 AND lease_token = $2`
		args = append(args, *retryAt)
	} else if clearLease {
		query = `UPDATE integration_jobs
		            SET status = $3, lease_token = NULL, locked_at = NULL
		          WHERE id = $1 AND lease_token = $2`
	}
	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("set job %s: %w", status, err)
	}
	return nil
}
