package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type countingLoader struct {
	calls int
	price string
}

func (c *countingLoader) GetPrice(context.Context, string) (string, error) {
	c.calls++
	return c.price, nil
}

func TestGetPriceCallsRPCOnce(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	loader := &countingLoader{price: "100.00"}
	cache := NewPriceCache(loader, rdb, time.Minute)

	for range 2 {
		got, err := cache.GetPrice(context.Background(), "1")
		if err != nil {
			t.Fatalf("GetPrice: %v", err)
		}
		if got != "100.00" {
			t.Fatalf("price = %q, want 100.00", got)
		}
	}

	if loader.calls != 1 {
		t.Fatalf("RPC calls = %d, want 1", loader.calls)
	}

	stored, err := srv.Get("price:1")
	if err != nil {
		t.Fatalf("redis get price:1: %v", err)
	}
	if stored != "100.00" {
		t.Fatalf("cached price = %q, want 100.00", stored)
	}
	if ttl := srv.TTL("price:1"); ttl != time.Minute {
		t.Fatalf("ttl = %s, want 1m", ttl)
	}
}
