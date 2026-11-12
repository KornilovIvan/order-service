// Package worker — внешний воркер очереди: забирает задание по gRPC,
// отправляет его в ERP по HTTP и сообщает результат через CompleteJob.
//
// Пакет намеренно не работает с базой: единственный источник заданий — gRPC,
// единственный результат — CompleteJob.
package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	jobsv1 "order-service/internal/gen/jobs/v1"
)

const (
	defaultPollIdle  = time.Second
	defaultClaimRPC  = 5 * time.Second
	defaultShutdown  = 20 * time.Second
	maxResponseBytes = 1 << 20
)

// Config — настройки воркера.
type Config struct {
	WorkerID   string
	ERPBaseURL string
	PollIdle   time.Duration
	ClaimRPC   time.Duration
	Shutdown   time.Duration
	HTTPClient *http.Client
}

// Worker — цикл обработки заданий.
type Worker struct {
	client jobsv1.JobServiceClient
	http   *http.Client
	cfg    Config

	budget shutdownBudget
}

func New(client jobsv1.JobServiceClient, cfg Config) *Worker {
	if cfg.PollIdle <= 0 {
		cfg.PollIdle = defaultPollIdle
	}
	if cfg.ClaimRPC <= 0 {
		cfg.ClaimRPC = defaultClaimRPC
	}
	if cfg.Shutdown <= 0 {
		cfg.Shutdown = defaultShutdown
	}
	if cfg.HTTPClient == nil {
		// Без Timeout: длительность Submit ограничивает контекст (бюджет shutdown).
		cfg.HTTPClient = &http.Client{}
	}
	return &Worker{client: client, http: cfg.HTTPClient, cfg: cfg}
}

// Run крутит цикл до отмены ctx. По SIGTERM новые задания не берутся:
// проверка ctx.Err() стоит перед каждым ClaimJob.
func (w *Worker) Run(ctx context.Context) {
	emptyWithoutRetry := false

	for {
		if ctx.Err() != nil {
			return
		}

		resp, err := w.claim(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("worker: claim: %v", err)
			if !sleepCtx(ctx, w.cfg.PollIdle) {
				return
			}
			continue
		}

		job := resp.GetJob()
		if job == nil {
			wait := waitAfterEmpty(resp.GetRetryAfter(), w.cfg.PollIdle, &emptyWithoutRetry)
			if !sleepCtx(ctx, wait) {
				return
			}
			continue
		}

		emptyWithoutRetry = false
		w.processJob(ctx, job)
		if ctx.Err() != nil {
			return
		}
	}
}

// waitAfterEmpty считает паузу для пустой очереди.
//
//   - есть retry_after — спим до этого момента;
//   - нет retry_after — первый раз не спим вовсе, второй подряд — pollIdle.
func waitAfterEmpty(retryAfter *timestamppb.Timestamp, pollIdle time.Duration, emptyWithoutRetry *bool) time.Duration {
	if retryAfter != nil {
		*emptyWithoutRetry = false
		if d := time.Until(retryAfter.AsTime()); d > 0 {
			return d
		}
		return 0
	}

	if !*emptyWithoutRetry {
		*emptyWithoutRetry = true
		return 0
	}
	return pollIdle
}

func (w *Worker) claim(ctx context.Context) (*jobsv1.ClaimResponse, error) {
	claimCtx, cancel := context.WithTimeout(ctx, w.cfg.ClaimRPC)
	defer cancel()
	return w.client.ClaimJob(claimCtx, &jobsv1.ClaimRequest{WorkerId: w.cfg.WorkerID})
}

// processJob — Submit и CompleteJob с отдельными контекстами:
// SIGTERM не обрывает ни отправку, ни запись результата; обоим выдаётся
// бюджет cfg.Shutdown, отсчитываемый от момента SIGTERM.
func (w *Worker) processJob(ctx context.Context, job *jobsv1.Job) {
	log.Printf("worker: job %d order %d correlation_id=%s", job.GetId(), job.GetOrderId(), job.GetCorrelationId())

	submitCtx, cancelSubmit := context.WithCancel(context.Background())
	defer cancelSubmit()

	submitFinished := make(chan struct{})
	var once sync.Once
	stopWatch := func() { once.Do(func() { close(submitFinished) }) }

	// Сторож: по SIGTERM стартуем бюджет и обрываем Submit, если он не уложился.
	go func() {
		select {
		case <-ctx.Done():
			w.budget.start(w.cfg.Shutdown)
			deadline := w.budget.deadlineOr(w.cfg.Shutdown)
			timer := time.NewTimer(time.Until(deadline))
			defer timer.Stop()
			select {
			case <-timer.C:
				cancelSubmit()
			case <-submitFinished:
			}
		case <-submitFinished:
		}
	}()

	status, body, submitErr := w.submit(submitCtx, job)
	stopWatch()

	errText := ""
	if submitErr != nil {
		errText = submitErr.Error()
		log.Printf("worker: submit job %d: %v", job.GetId(), submitErr)
	}

	completeTimeout := w.cfg.Shutdown
	if remaining, started := w.budget.remaining(); started {
		completeTimeout = remaining
	}
	if completeTimeout <= 0 {
		// Бюджет исчерпан: задание остаётся in_progress, его вернёт reaper.
		log.Printf("worker: shutdown budget exhausted before CompleteJob for job %d", job.GetId())
		return
	}

	completeCtx, cancelComplete := context.WithTimeout(context.Background(), completeTimeout)
	defer cancelComplete()

	if _, err := w.client.CompleteJob(completeCtx, &jobsv1.CompleteRequest{
		JobId:        job.GetId(),
		LockedAt:     job.GetLockedAt(),
		LeaseToken:   job.GetLeaseToken(),
		HttpStatus:   int32(status),
		ResponseBody: body,
		Error:        errText,
	}); err != nil {
		// Задание останется in_progress — его восстановит reaper.
		log.Printf("worker: complete job %d: %v", job.GetId(), err)
		return
	}

	log.Printf("worker: job %d done, http_status=%d correlation_id=%s", job.GetId(), status, job.GetCorrelationId())
}

// submit отправляет заказ в ERP. Таймаутом управляет context; тело ответа
// ограничено 1 МиБ.
func (w *Worker) submit(ctx context.Context, job *jobsv1.Job) (int, string, error) {
	url := strings.TrimRight(w.cfg.ERPBaseURL, "/") + "/orders"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(job.GetRequestBody()))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", job.GetIdempotencyKey())
	if cid := job.GetCorrelationId(); cid != "" {
		req.Header.Set("X-Correlation-ID", cid)
	}

	resp, err := w.http.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("erp request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return resp.StatusCode, "", fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, string(body), nil
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// shutdownBudget — момент, до которого воркер обязан уложиться после SIGTERM.
type shutdownBudget struct {
	mu       sync.Mutex
	deadline time.Time
}

func (b *shutdownBudget) start(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deadline.IsZero() {
		b.deadline = time.Now().Add(d)
	}
}

func (b *shutdownBudget) remaining() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deadline.IsZero() {
		return 0, false
	}
	return time.Until(b.deadline), true
}

func (b *shutdownBudget) deadlineOr(fallback time.Duration) time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deadline.IsZero() {
		return time.Now().Add(fallback)
	}
	return b.deadline
}
