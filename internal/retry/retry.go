package retry

import (
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
