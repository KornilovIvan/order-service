package worker

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Два retry_after подряд — две паузы до указанных моментов.
func TestWaitAfterEmpty_TwoRetryAfter(t *testing.T) {
	pollIdle := 200 * time.Millisecond
	emptyFlag := false

	for i := 0; i < 2; i++ {
		retryAt := timestamppb.New(time.Now().Add(150 * time.Millisecond))
		wait := waitAfterEmpty(retryAt, pollIdle, &emptyFlag)

		if wait <= 0 {
			t.Fatalf("sleep #%d: want positive duration, got %s", i+1, wait)
		}
		if wait > 150*time.Millisecond+50*time.Millisecond {
			t.Fatalf("sleep #%d: want about 150ms, got %s", i+1, wait)
		}
		if emptyFlag {
			t.Fatalf("sleep #%d: retry_after must reset emptyWithoutRetry", i+1)
		}
	}
}

// Первый ответ без retry_after — без сна; второй подряд — POLL_IDLE.
func TestWaitAfterEmpty_NoField(t *testing.T) {
	pollIdle := 200 * time.Millisecond
	emptyFlag := false

	if wait := waitAfterEmpty(nil, pollIdle, &emptyFlag); wait != 0 {
		t.Fatalf("first empty response: want 0 sleep, got %s", wait)
	}
	if wait := waitAfterEmpty(nil, pollIdle, &emptyFlag); wait != pollIdle {
		t.Fatalf("second empty response: want %s, got %s", pollIdle, wait)
	}
	if wait := waitAfterEmpty(nil, pollIdle, &emptyFlag); wait != pollIdle {
		t.Fatalf("third empty response: want %s, got %s", pollIdle, wait)
	}

	// Задание пришло — флаг сбрасывается, следующий пустой ответ снова без сна.
	emptyFlag = false
	if wait := waitAfterEmpty(nil, pollIdle, &emptyFlag); wait != 0 {
		t.Fatalf("after job: want 0 sleep, got %s", wait)
	}
}

// retry_after в прошлом — не спим.
func TestWaitAfterEmpty_PastRetryAfter(t *testing.T) {
	emptyFlag := false
	retryAt := timestamppb.New(time.Now().Add(-time.Second))

	if wait := waitAfterEmpty(retryAt, 200*time.Millisecond, &emptyFlag); wait != 0 {
		t.Fatalf("past retry_after: want 0, got %s", wait)
	}
}

// Бюджет shutdown считается один раз и не продлевается повторным SIGTERM.
func TestShutdownBudget_StartOnce(t *testing.T) {
	var b shutdownBudget

	if _, started := b.remaining(); started {
		t.Fatal("budget must not be started before SIGTERM")
	}

	b.start(100 * time.Millisecond)
	first, started := b.remaining()
	if !started || first <= 0 {
		t.Fatalf("first remaining: got %s, started=%v", first, started)
	}

	b.start(time.Hour) // повторный сигнал не должен продлевать бюджет
	second, _ := b.remaining()
	if second > first {
		t.Fatalf("budget was extended: first %s, second %s", first, second)
	}
}
