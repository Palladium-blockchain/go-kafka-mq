package retry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWithExpSleepSuccessFirstAttempt(t *testing.T) {
	calls := 0

	res, err := WithExpSleep(3, time.Nanosecond, time.Nanosecond, func() (int, error) {
		calls++
		return 42, nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res != 42 {
		t.Fatalf("expected result 42, got %d", res)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestWithExpSleepRetriesUntilSuccess(t *testing.T) {
	calls := 0

	res, err := WithExpSleep(3, time.Nanosecond, time.Nanosecond, func() (string, error) {
		calls++
		if calls < 3 {
			return "", errors.New("temporary")
		}
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected result ok, got %q", res)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestWithExpSleepReturnsWrappedError(t *testing.T) {
	calls := 0
	errSentinel := errors.New("boom")

	_, err := WithExpSleep(2, time.Nanosecond, time.Nanosecond, func() (int, error) {
		calls++
		return 0, errSentinel
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
	if !strings.Contains(err.Error(), "after 2 attempts") {
		t.Fatalf("expected wrapped attempt count in error, got %v", err)
	}
	if !errors.Is(err, errSentinel) {
		t.Fatalf("expected wrapped sentinel error, got %v", err)
	}
}

func TestWithExpSleepCtxCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	_, err := WithExpSleepCtx(ctx, 3, time.Nanosecond, time.Nanosecond, func() (int, error) {
		calls++
		return 0, nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected 0 calls, got %d", calls)
	}
}

func TestWithExpSleepCtxCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	done := make(chan error, 1)
	go func() {
		_, err := WithExpSleepCtx(ctx, 3, 50*time.Millisecond, 50*time.Millisecond, func() (int, error) {
			calls++
			return 0, errors.New("temporary")
		})
		done <- err
	}()

	time.Sleep(5 * time.Millisecond)
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call before cancel, got %d", calls)
	}
}
