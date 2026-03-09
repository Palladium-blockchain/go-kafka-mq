package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func WithExpSleep[ResT any](attempts int, initialBackoff time.Duration, maxBackoff time.Duration, fn func() (ResT, error)) (ResT, error) {
	var res ResT
	var err error
	for i := 0; i < attempts; i++ {
		res, err = fn()
		if err == nil {
			return res, nil
		}

		sleep := min(initialBackoff*(1<<i), maxBackoff)

		if i < attempts-1 {
			time.Sleep(sleep)
		}
	}
	return res, fmt.Errorf("after %d attempts, last error: %w", attempts, err)
}

func WithExpSleepCtx[ResT any](ctx context.Context, attempts int, initialBackoff time.Duration, maxBackoff time.Duration, fn func() (ResT, error)) (ResT, error) {
	var res ResT
	var err error

	for i := 0; i < attempts; i++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return res, ctxErr
		}

		res, err = fn()
		if err == nil {
			return res, nil
		}

		sleep := min(initialBackoff*(1<<i), maxBackoff)
		if i < attempts-1 {
			timer := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return res, ctx.Err()
			case <-timer.C:
			}
		}
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return res, err
	}
	return res, fmt.Errorf("after %d attempts, last error: %w", attempts, err)
}
