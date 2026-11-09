// Package publisher публикует события заказов в Kafka/Redpanda.
//
// Публикация устроена как упрощённый outbox: состояние «ещё не опубликовано»
// хранится в самом заказе (published_at IS NULL), поэтому событие не теряется
// при падении процесса между коммитом и отправкой. Гарантия доставки —
// at-least-once: дубли возможны, потребитель должен быть идемпотентным.
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
	CorrelationID string `json:"correlation_id"`
}

// Publisher пишет события в брокер.
type Publisher struct {
	writer *kafka.Writer
}

// New создаёт публикатора. topic должен существовать заранее либо
// брокер разрешает автосоздание.
func New(brokers []string, topic string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // ключ сообщения = order_id → порядок по заказу
			RequiredAcks: kafka.RequireAll,
			// Для локальной разработки удобно: топик создаётся сам.
			AllowAutoTopicCreation: true,
			BatchTimeout:           10 * time.Millisecond,
		},
	}
}

// Publish отправляет одно событие и ждёт подтверждения брокера.
func (p *Publisher) Publish(ctx context.Context, ev Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("publisher: encode event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(strconv.FormatInt(ev.OrderID, 10)),
		Value: body,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publisher: write message: %w", err)
	}
	return nil
}

// Close закрывает соединение с брокером.
func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Source — источник неопубликованных заказов и приёмник отметки о публикации.
type Source interface {
	PendingPublish(ctx context.Context, limit int) ([]model.PendingPublish, error)
	MarkPublished(ctx context.Context, ids []int64, at time.Time) error
}

// Worker периодически публикует события по неопубликованным заказам.
type Worker struct {
	source   Source
	pub      *Publisher
	limit    int
	interval time.Duration
}

func NewWorker(source Source, pub *Publisher, interval time.Duration, limit int) *Worker {
	return &Worker{source: source, pub: pub, interval: interval, limit: limit}
}

// Run выполняет первый проход сразу и далее по тикеру до отмены ctx.
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

// poll публикует одну пачку и отмечает успешно опубликованные заказы.
func (w *Worker) poll(ctx context.Context) error {
	pending, err := w.source.PendingPublish(ctx, w.limit)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	published := make([]int64, 0, len(pending))
	for _, order := range pending {
		ev := Event{
			Event:         EventOrderCreated,
			OrderID:       order.ID,
			CorrelationID: order.CorrelationID,
		}
		if err := w.pub.Publish(ctx, ev); err != nil {
			// То, что уже ушло в брокер, отмечаем, чтобы не публиковать повторно.
			if len(published) > 0 {
				if markErr := w.source.MarkPublished(ctx, published, time.Now()); markErr != nil {
					return fmt.Errorf("%v; mark published: %w", err, markErr)
				}
			}
			return err
		}
		published = append(published, order.ID)
	}

	if err := w.source.MarkPublished(ctx, published, time.Now()); err != nil {
		return err
	}
	log.Printf("publisher: published %d event(s)", len(published))
	return nil
}
