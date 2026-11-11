package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testLimiter(t *testing.T, limit int, window time.Duration) (*Limiter, *miniredis.Miniredis) {
	t.Helper()
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, limit, window), srv
}

func TestClaimAllowsUntilLimit(t *testing.T) {
	lim, _ := testLimiter(t, 2, time.Second)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		got, err := lim.Claim(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Allowed {
			t.Fatalf("call %d denied", i+1)
		}
	}
	got, err := lim.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Allowed || got.RetryAfter <= 0 {
		t.Fatalf("over limit: %+v", got)
	}
}

func TestDenyUsesLivePTTL(t *testing.T) {
	lim, srv := testLimiter(t, 1, 2*time.Second)
	ctx := context.Background()
	if _, err := lim.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := lim.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Allowed {
		t.Fatal("expected deny")
	}
	ttl := srv.TTL(windowKey)
	if ttl <= 0 {
		t.Fatal("window ttl missing")
	}
	if got.RetryAfter > ttl || got.RetryAfter < ttl-100*time.Millisecond {
		t.Fatalf("retry_after = %s, pttl = %s", got.RetryAfter, ttl)
	}
}

func TestPersistThenExpireOnDeny(t *testing.T) {
	lim, srv := testLimiter(t, 1, 2*time.Second)
	ctx := context.Background()
	if _, err := lim.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Persist(ctx, windowKey).Err(); err != nil {
		t.Fatal(err)
	}
	if srv.TTL(windowKey) != 0 {
		t.Fatalf("after persist ttl = %s, want none", srv.TTL(windowKey))
	}

	got, err := lim.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Allowed {
		t.Fatal("expected deny")
	}
	if !srv.Exists(windowKey) {
		t.Fatal("DEL must not be called")
	}
	if srv.TTL(windowKey) <= 0 {
		t.Fatalf("expire not set, ttl = %s", srv.TTL(windowKey))
	}
	if got.RetryAfter <= 0 {
		t.Fatal("retry_after from new PTTL")
	}
}

func TestMissingKeyPTTLDoesNotIncrAgain(t *testing.T) {
	lim, srv := testLimiter(t, 1, time.Second)
	ctx := context.Background()
	wait, err := lim.retryAfter(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if wait != missingPause {
		t.Fatalf("retry = %s, want 30s", wait)
	}
	if srv.Exists(windowKey) {
		t.Fatal("second INCR must not recreate erp:window")
	}
}
