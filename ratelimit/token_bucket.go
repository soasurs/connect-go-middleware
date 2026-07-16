// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// TokenBucketConfig configures a local token bucket. A newly created bucket is
// full, allowing an initial burst of Burst RPCs.
type TokenBucketConfig struct {
	// Rate is the number of tokens added per second and must be positive.
	Rate float64
	// Burst is the bucket capacity and must be positive.
	Burst int
}

// NewTokenBucket returns a process-local Limiter using a token bucket.
// It panics if Rate or Burst is not positive, or if Rate is NaN or infinite.
func NewTokenBucket(config TokenBucketConfig) Limiter {
	return newTokenBucket(config, time.Now)
}

type tokenBucket struct {
	mu sync.Mutex

	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newTokenBucket(config TokenBucketConfig, now func() time.Time) *tokenBucket {
	if config.Rate <= 0 || math.IsNaN(config.Rate) || math.IsInf(config.Rate, 0) {
		panic("ratelimit: token bucket rate must be positive and finite")
	}
	if config.Burst <= 0 {
		panic("ratelimit: token bucket burst must be positive")
	}
	current := now()
	return &tokenBucket{
		rate:   config.Rate,
		burst:  float64(config.Burst),
		tokens: float64(config.Burst),
		last:   current,
		now:    now,
	}
}

func (b *tokenBucket) Allow(context.Context, string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = min(b.burst, b.tokens+elapsed.Seconds()*b.rate)
		b.last = now
	}
	if b.tokens < 1 {
		return ErrRateLimited
	}
	b.tokens--
	return nil
}
