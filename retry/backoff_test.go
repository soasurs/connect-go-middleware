// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"testing"
	"time"
)

func TestExponentialBackoffBounds(t *testing.T) {
	backoff := ExponentialBackoff(10*time.Millisecond, 25*time.Millisecond)
	tests := []struct {
		attempt uint
		limit   time.Duration
	}{
		{attempt: 0, limit: 10 * time.Millisecond},
		{attempt: 1, limit: 10 * time.Millisecond},
		{attempt: 2, limit: 20 * time.Millisecond},
		{attempt: 3, limit: 25 * time.Millisecond},
		{attempt: 1000, limit: 25 * time.Millisecond},
	}
	for _, test := range tests {
		for range 100 {
			got := backoff(context.Background(), test.attempt)
			if got < 0 || got >= test.limit {
				t.Fatalf("attempt %d backoff = %v, want [0, %v)", test.attempt, got, test.limit)
			}
		}
	}
}

func TestExponentialBackoffPanicsForInvalidConfig(t *testing.T) {
	tests := map[string]struct {
		base    time.Duration
		maximum time.Duration
	}{
		"zero base":         {base: 0, maximum: time.Second},
		"negative base":     {base: -time.Second, maximum: time.Second},
		"zero maximum":      {base: time.Second, maximum: 0},
		"negative maximum":  {base: time.Second, maximum: -time.Second},
		"base over maximum": {base: 2 * time.Second, maximum: time.Second},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("ExponentialBackoff did not panic")
				}
			}()
			ExponentialBackoff(test.base, test.maximum)
		})
	}
}
