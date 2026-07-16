// SPDX-License-Identifier: Apache-2.0

package breaker

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
)

func TestNewGoogleUsesDefaults(t *testing.T) {
	google := NewGoogle(GoogleConfig{}).(*googleBreaker)
	if got, want := len(google.buckets), defaultGoogleBuckets; got != want {
		t.Fatalf("buckets = %d, want %d", got, want)
	}
	if got, want := google.bucketWidth, defaultGoogleWindow/defaultGoogleBuckets; got != want {
		t.Fatalf("bucket width = %v, want %v", got, want)
	}
	if google.k != defaultGoogleK || google.minRequests != defaultGoogleMinRequests || google.probeInterval != defaultGoogleProbeInterval {
		t.Fatalf("unexpected defaults: k=%v minRequests=%v probeInterval=%v", google.k, google.minRequests, google.probeInterval)
	}
}

func TestNewGooglePanicsForInvalidConfig(t *testing.T) {
	tests := map[string]GoogleConfig{
		"negative window":         {Window: -time.Second},
		"negative buckets":        {Buckets: -1},
		"K too small":             {K: 1},
		"K NaN":                   {K: math.NaN()},
		"K infinite":              {K: math.Inf(1)},
		"negative probe interval": {ProbeInterval: -time.Second},
		"bucket below nanosecond": {Window: time.Nanosecond, Buckets: 2},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewGoogle did not panic")
				}
			}()
			NewGoogle(config)
		})
	}
}

func TestGoogleRejectsWithCalculatedProbability(t *testing.T) {
	clock := newFakeClock()
	google := newTestGoogle(clock, func() float64 { return 0.49 }, 1, time.Hour)
	done, err := google.Allow()
	if err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}
	done(connect.NewError(connect.CodeUnavailable, errors.New("unavailable")))

	if _, err := google.Allow(); !errors.Is(err, ErrRejected) {
		t.Fatalf("second Allow() error = %v, want ErrRejected", err)
	}
	requests, accepts := google.history()
	if requests != 2 || accepts != 0 {
		t.Fatalf("history = (%d requests, %d accepts), want (2, 0)", requests, accepts)
	}
}

func TestGoogleWaitsForMinimumRequests(t *testing.T) {
	clock := newFakeClock()
	google := newTestGoogle(clock, func() float64 { return 0 }, 2, time.Hour)
	for i := 0; i < 2; i++ {
		done, err := google.Allow()
		if err != nil {
			t.Fatalf("Allow() call %d error = %v", i+1, err)
		}
		done(connect.NewError(connect.CodeUnavailable, errors.New("unavailable")))
	}
	if _, err := google.Allow(); !errors.Is(err, ErrRejected) {
		t.Fatalf("third Allow() error = %v, want ErrRejected", err)
	}
}

func TestGoogleAllowsProbe(t *testing.T) {
	clock := newFakeClock()
	google := newTestGoogle(clock, func() float64 { return 0 }, 1, time.Second)
	done, err := google.Allow()
	if err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}
	done(connect.NewError(connect.CodeUnavailable, errors.New("unavailable")))
	if _, err := google.Allow(); !errors.Is(err, ErrRejected) {
		t.Fatalf("second Allow() error = %v, want ErrRejected", err)
	}

	clock.Advance(time.Second)
	done, err = google.Allow()
	if err != nil {
		t.Fatalf("probe Allow() error = %v", err)
	}
	done(nil)
}

func TestGoogleExpiresRollingWindow(t *testing.T) {
	clock := newFakeClock()
	google := newTestGoogle(clock, func() float64 { return 0 }, 1, time.Hour)
	done, err := google.Allow()
	if err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}
	done(connect.NewError(connect.CodeUnavailable, errors.New("unavailable")))

	clock.Advance(11 * time.Second)
	done, err = google.Allow()
	if err != nil {
		t.Fatalf("Allow() after window expiry error = %v", err)
	}
	done(nil)
}

func TestGoogleRecordsCallbackOnce(t *testing.T) {
	clock := newFakeClock()
	google := newTestGoogle(clock, func() float64 { return 1 }, 1, time.Hour)
	done, err := google.Allow()
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	done(nil)
	done(nil)
	requests, accepts := google.history()
	if requests != 1 || accepts != 1 {
		t.Fatalf("history = (%d requests, %d accepts), want (1, 1)", requests, accepts)
	}
}

func TestGoogleIgnoresAcceptedResultAfterRequestExpires(t *testing.T) {
	clock := newFakeClock()
	google := newTestGoogle(clock, func() float64 { return 1 }, 1, time.Hour)
	done, err := google.Allow()
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	clock.Advance(11 * time.Second)
	done(nil)
	requests, accepts := google.history()
	if requests != 0 || accepts != 0 {
		t.Fatalf("history = (%d requests, %d accepts), want (0, 0)", requests, accepts)
	}
}

func TestGoogleUsesAcceptedClassifier(t *testing.T) {
	clock := newFakeClock()
	google := newGoogle(GoogleConfig{
		Window:        10 * time.Second,
		Buckets:       10,
		K:             2,
		MinRequests:   1,
		ProbeInterval: time.Hour,
		IsAccepted:    func(error) bool { return true },
	}, clock.Now, func() float64 { return 1 })
	done, err := google.Allow()
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	done(connect.NewError(connect.CodeUnavailable, errors.New("unavailable")))
	requests, accepts := google.history()
	if requests != 1 || accepts != 1 {
		t.Fatalf("history = (%d requests, %d accepts), want (1, 1)", requests, accepts)
	}
}

func TestDefaultGoogleIsAccepted(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "success", want: true},
		{name: "invalid argument", err: connect.NewError(connect.CodeInvalidArgument, errors.New("invalid")), want: true},
		{name: "deadline exceeded", err: connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded), want: true},
		{name: "unavailable", err: connect.NewError(connect.CodeUnavailable, errors.New("unavailable")), want: false},
		{name: "resource exhausted", err: connect.NewError(connect.CodeResourceExhausted, errors.New("overloaded")), want: false},
		{name: "panic", err: connect.NewError(connect.CodeInternal, errCallPanicked), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DefaultGoogleIsAccepted(test.err); got != test.want {
				t.Fatalf("DefaultGoogleIsAccepted() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGoogleConcurrentUse(t *testing.T) {
	google := NewGoogle(GoogleConfig{MinRequests: 1})
	var waitGroup sync.WaitGroup
	for i := 0; i < 1_000; i++ {
		waitGroup.Go(func() {
			done, err := google.Allow()
			if err == nil {
				done(nil)
			}
		})
	}
	waitGroup.Wait()
}

func newTestGoogle(clock *fakeClock, random func() float64, minRequests uint64, probeInterval time.Duration) *googleBreaker {
	return newGoogle(GoogleConfig{
		Window:        10 * time.Second,
		Buckets:       10,
		K:             2,
		MinRequests:   minRequests,
		ProbeInterval: probeInterval,
	}, clock.Now, random)
}

func (b *googleBreaker) history() (uint64, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.historyLocked(b.now())
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}
