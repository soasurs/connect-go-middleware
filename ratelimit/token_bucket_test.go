// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTokenBucketPanicsForInvalidConfig(t *testing.T) {
	tests := map[string]TokenBucketConfig{
		"zero rate":      {Rate: 0, Burst: 1},
		"negative rate":  {Rate: -1, Burst: 1},
		"NaN rate":       {Rate: math.NaN(), Burst: 1},
		"infinite rate":  {Rate: math.Inf(1), Burst: 1},
		"zero burst":     {Rate: 1, Burst: 0},
		"negative burst": {Rate: 1, Burst: -1},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewTokenBucket did not panic")
				}
			}()
			NewTokenBucket(config)
		})
	}
}

func TestTokenBucketAllowsInitialBurst(t *testing.T) {
	clock := newFakeClock()
	limiter := newTokenBucket(TokenBucketConfig{Rate: 1, Burst: 2}, clock.now)

	if err := limiter.Allow(t.Context(), testProcedure); err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}
	if err := limiter.Allow(t.Context(), testProcedure); err != nil {
		t.Fatalf("second Allow() error = %v", err)
	}
	if err := limiter.Allow(t.Context(), testProcedure); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("third Allow() error = %v, want ErrRateLimited", err)
	}
}

func TestTokenBucketRefillsOverTime(t *testing.T) {
	clock := newFakeClock()
	limiter := newTokenBucket(TokenBucketConfig{Rate: 2, Burst: 1}, clock.now)

	if err := limiter.Allow(t.Context(), testProcedure); err != nil {
		t.Fatalf("initial Allow() error = %v", err)
	}
	clock.advance(499 * time.Millisecond)
	if err := limiter.Allow(t.Context(), testProcedure); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("Allow() before refill error = %v, want ErrRateLimited", err)
	}
	clock.advance(time.Millisecond)
	if err := limiter.Allow(t.Context(), testProcedure); err != nil {
		t.Fatalf("Allow() after refill error = %v", err)
	}
}

func TestTokenBucketCapsRefillAtBurst(t *testing.T) {
	clock := newFakeClock()
	limiter := newTokenBucket(TokenBucketConfig{Rate: 100, Burst: 2}, clock.now)
	clock.advance(time.Hour)

	for i := range 2 {
		if err := limiter.Allow(t.Context(), testProcedure); err != nil {
			t.Fatalf("Allow() %d error = %v", i, err)
		}
	}
	if err := limiter.Allow(t.Context(), testProcedure); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("third Allow() error = %v, want ErrRateLimited", err)
	}
}

func TestTokenBucketConcurrentUse(t *testing.T) {
	const burst = 100
	clock := newFakeClock()
	limiter := newTokenBucket(TokenBucketConfig{Rate: 1, Burst: burst}, clock.now)
	var allowed atomic.Int64
	var wait sync.WaitGroup
	for range 1000 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if limiter.Allow(t.Context(), testProcedure) == nil {
				allowed.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := allowed.Load(); got != burst {
		t.Fatalf("allowed calls = %d, want %d", got, burst)
	}
}

type fakeClock struct {
	mu      sync.Mutex
	current time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{current: time.Unix(0, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *fakeClock) advance(duration time.Duration) {
	c.mu.Lock()
	c.current = c.current.Add(duration)
	c.mu.Unlock()
}
