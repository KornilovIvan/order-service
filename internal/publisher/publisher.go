// Package publisher публикует события заданий в Kafka/Redpanda.
//
// Outbox — integration_jobs.published_at. Publisher захватывает строки
// через publish_locked_at (lease 60s, вариант B): несколько экземпляров API
// не берут одно задание одновременно. Падение после Kafka и до published_at
// даёт повтор — at-least-once. Потребитель дедуплицирует по job_id.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"order-service/internal/model"
)

// EventOrderCreated — тип события «заказ создан и подтверждён».
const EventOrderCreated = "order.created"

type Event struct {
	Event         string `json:"event"`
	OrderID       int64  `json:"order_id"`
	JobID         int64  `json:"job_id"`
	CorrelationID string `json:"correlation_id"`
}

// Publisher пишет события в брокер.
type Publisher struct {
	writer *kafka.Writer
}

func New(brokers []string, topic string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			BatchTimeout:           10 * time.Millisecond,
		},
	}
}

func (p *Publisher) Publish(ctx context.Context, ev Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("publisher: encode event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(strconv.FormatInt(ev.JobID, 10)),
		Value: body,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publisher: write message: %w", err)
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Source — захват неопубликованных заданий и отметка published_at.
type Source interface {
	ClaimPendingPublish(ctx context.Context, limit int) ([]model.PendingPublish, error)
	MarkPublished(ctx context.Context, jobID int64, at time.Time) (bool, error)
}

type Worker struct {
	source   Source
	pub      *Publisher
	limit    int
	interval time.Duration
}

func NewWorker(source Source, pub *Publisher, interval time.Duration, limit int) *Worker {
	return &Worker{source: source, pub: pub, interval: interval, limit: limit}
}

func (w *Worker) Run(ctx context.Context) {
	if err := w.poll(ctx); err != nil && ctx.Err() == nil {
		log.Printf("publisher: poll: %v", err)
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.poll(ctx); err != nil && ctx.Err() == nil {
				log.Printf("publisher: poll: %v", err)
			}
		}
	}
}

func (w *Worker) poll(ctx context.Context) error {
	pending, err := w.source.ClaimPendingPublish(ctx, w.limit)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	n := 0
	for _, job := range pending {
		ev := Event{
			Event:         EventOrderCreated,
			OrderID:       job.OrderID,
			JobID:         job.ID,
			CorrelationID: job.CorrelationID,
		}
		if err := w.pub.Publish(ctx, ev); err != nil {
			return err
		}
		ok, err := w.source.MarkPublished(ctx, job.ID, time.Now())
		if err != nil {
			return err
		}
		if ok {
			n++
		}
	}
	if n > 0 {
		log.Printf("publisher: published %d event(s)", n)
	}
	return nil
}
