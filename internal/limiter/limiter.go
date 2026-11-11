package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	windowKey     = "erp:window"
	missingPause  = 30 * time.Second
	incrAndExpire = `
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("EXPIRE", KEYS[1], tonumber(ARGV[1]))
end
return current
`
)

// ClaimResponse — результат слота: можно слать в ERP или ждать retry_after.
type ClaimResponse struct {
	Allowed    bool
	RetryAfter time.Duration
}

// Limiter — скользящее окно на ERP: ключ erp:window, лимит ERP_LIMIT.
type Limiter struct {
	rdb    *redis.Client
	limit  int64
	window time.Duration
}

func New(rdb *redis.Client, limit int, window time.Duration) *Limiter {
	if window <= 0 {
		window = time.Second
	}
	return &Limiter{rdb: rdb, limit: int64(limit), window: window}
}

// Claim считает INCR. current > limit — слота нет, пауза из PTTL.
// DEL не вызывается. При PTTL=-2 второго INCR нет.
func (l *Limiter) Claim(ctx context.Context) (ClaimResponse, error) {
	secs := int(l.window.Seconds())
	if secs < 1 {
		secs = 1
	}
	current, err := l.rdb.Eval(ctx, incrAndExpire, []string{windowKey}, secs).Int64()
	if err != nil {
		return ClaimResponse{}, fmt.Errorf("limiter incr: %w", err)
	}
	if current <= l.limit {
		return ClaimResponse{Allowed: true}, nil
	}
	wait, err := l.retryAfter(ctx, secs)
	if err != nil {
		return ClaimResponse{}, err
	}
	return ClaimResponse{RetryAfter: wait}, nil
}

func (l *Limiter) retryAfter(ctx context.Context, windowSecs int) (time.Duration, error) {
	pttl, err := l.rdb.PTTL(ctx, windowKey).Result()
	if err != nil {
		return 0, fmt.Errorf("limiter pttl: %w", err)
	}
	switch redisPTTL(pttl) {
	case -2:
		// Ключа нет. Не INCR: это открыло бы новое окно и съело слот.
		return missingPause, nil
	case -1:
		if err := l.rdb.Expire(ctx, windowKey, time.Duration(windowSecs)*time.Second).Err(); err != nil {
			return 0, fmt.Errorf("limiter expire: %w", err)
		}
		pttl, err = l.rdb.PTTL(ctx, windowKey).Result()
		if err != nil {
			return 0, fmt.Errorf("limiter pttl after expire: %w", err)
		}
		if redisPTTL(pttl) <= 0 {
			return missingPause, nil
		}
		return pttl, nil
	default:
		if pttl < 0 {
			return missingPause, nil
		}
		return pttl, nil
	}
}

// redisPTTL: go-redis кладёт Redis -1/-2 как duration(-1)/(-2) нс, не как миллисекунды.
func redisPTTL(d time.Duration) int64 {
	if d == -1 || d == -2 {
		return int64(d)
	}
	return d.Milliseconds()
}
