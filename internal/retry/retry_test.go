package retry

import (
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
