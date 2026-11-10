package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// PriceLoader — источник актуальной цены. Обычно это gRPC-клиент каталога.
type PriceLoader interface {
	GetPrice(ctx context.Context, productID string) (string, error)
}

// PriceCache хранит текущую цену каталога в Redis.
// Ключ — price:{id}. Промах читает цену у next и записывает её одним SET вместе с TTL.
// Попадание возвращает значение из Redis и в каталог не ходит.
type PriceCache struct {
	next  PriceLoader
	redis *redis.Client
	ttl   time.Duration
}

func NewPriceCache(next PriceLoader, rdb *redis.Client, ttl time.Duration) *PriceCache {
	return &PriceCache{next: next, redis: rdb, ttl: ttl}
}

func (c *PriceCache) GetPrice(ctx context.Context, productID string) (string, error) {
	key := "price:" + productID

	price, err := c.redis.Get(ctx, key).Result()
	if nil == err {
		return price, nil
	}
	if !errors.Is(err, redis.Nil) {
		return "", fmt.Errorf("catalog: cache get %s: %w", key, err)
	}

	price, err = c.next.GetPrice(ctx, productID)
	if err != nil {
		return "", err
	}

	if err := c.redis.Set(ctx, key, price, c.ttl).Err(); err != nil {
		return "", fmt.Errorf("catalog: cache set %s: %w", key, err)
	}
	return price, nil
}
