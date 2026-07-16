// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"math/rand/v2"
	"time"
)

// BackoffFunc returns the delay after a failed attempt. The attempt argument
// is one-based, so one identifies the initial call's failure. Implementations
// may use values from ctx and must be safe for concurrent use.
type BackoffFunc func(ctx context.Context, attempt uint) time.Duration

// ExponentialBackoff returns a full-jitter exponential backoff. For each
// attempt it chooses a delay from zero through base*2^(attempt-1), capped at
// maximum. It panics unless base and maximum are positive and base <= maximum.
func ExponentialBackoff(base, maximum time.Duration) BackoffFunc {
	if base <= 0 {
		panic("retry: backoff base must be positive")
	}
	if maximum <= 0 {
		panic("retry: backoff maximum must be positive")
	}
	if base > maximum {
		panic("retry: backoff base must not exceed maximum")
	}
	return func(_ context.Context, attempt uint) time.Duration {
		limit := base
		for step := uint(1); step < attempt && limit < maximum; step++ {
			if limit > maximum/2 {
				limit = maximum
			} else {
				limit *= 2
			}
		}
		if limit <= 1 {
			return 0
		}
		return time.Duration(rand.Int64N(int64(limit)))
	}
}
