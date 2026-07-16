// SPDX-License-Identifier: Apache-2.0

package breaker

import (
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"connectrpc.com/connect"
)

const (
	defaultGoogleWindow        = 2 * time.Minute
	defaultGoogleBuckets       = 40
	defaultGoogleK             = 2.0
	defaultGoogleMinRequests   = 20
	defaultGoogleProbeInterval = time.Second
)

// GoogleConfig configures the adaptive circuit breaker described in the
// Google SRE book's client-side throttling section. Zero-valued fields use
// their documented defaults.
type GoogleConfig struct {
	// Window is the duration of request history to retain. The default is two minutes.
	Window time.Duration
	// Buckets is the number of segments in the rolling window. The default is 40.
	Buckets int
	// K controls rejection aggressiveness and must be greater than one. The default is 2.
	K float64
	// MinRequests is the number of requests collected before rejection begins. The default is 20.
	MinRequests uint64
	// ProbeInterval guarantees an allowed probe after this long without an
	// allowed request. The default is one second.
	ProbeInterval time.Duration
	// IsAccepted reports whether the backend accepted an RPC. It may be called
	// concurrently. By default, Unavailable and ResourceExhausted errors are
	// not accepted; all other results are accepted.
	IsAccepted func(error) bool
}

// NewGoogle returns a CircuitBreaker using Google SRE adaptive throttling as
// described at https://sre.google/sre-book/handling-overload/.
// It panics if config contains an invalid negative value, K is not greater
// than one, or the configured bucket duration is less than one nanosecond.
func NewGoogle(config GoogleConfig) CircuitBreaker {
	return newGoogle(config, time.Now, rand.Float64)
}

type googleBreaker struct {
	mu sync.Mutex

	buckets       []googleBucket
	bucketWidth   time.Duration
	k             float64
	minRequests   uint64
	probeInterval time.Duration
	isAccepted    func(error) bool
	now           func() time.Time
	random        func() float64
	lastPass      time.Time
}

type googleBucket struct {
	valid    bool
	tick     int64
	requests uint64
	accepts  uint64
}

func newGoogle(config GoogleConfig, now func() time.Time, random func() float64) *googleBreaker {
	config = withGoogleDefaults(config)
	validateGoogleConfig(config)
	return &googleBreaker{
		buckets:       make([]googleBucket, config.Buckets),
		bucketWidth:   config.Window / time.Duration(config.Buckets),
		k:             config.K,
		minRequests:   config.MinRequests,
		probeInterval: config.ProbeInterval,
		isAccepted:    config.IsAccepted,
		now:           now,
		random:        random,
	}
}

func withGoogleDefaults(config GoogleConfig) GoogleConfig {
	if config.Window == 0 {
		config.Window = defaultGoogleWindow
	}
	if config.Buckets == 0 {
		config.Buckets = defaultGoogleBuckets
	}
	if config.K == 0 {
		config.K = defaultGoogleK
	}
	if config.MinRequests == 0 {
		config.MinRequests = defaultGoogleMinRequests
	}
	if config.ProbeInterval == 0 {
		config.ProbeInterval = defaultGoogleProbeInterval
	}
	if config.IsAccepted == nil {
		config.IsAccepted = DefaultGoogleIsAccepted
	}
	return config
}

func validateGoogleConfig(config GoogleConfig) {
	if config.Window < 0 {
		panic("breaker: Google window must not be negative")
	}
	if config.Buckets < 0 {
		panic("breaker: Google buckets must not be negative")
	}
	if config.K <= 1 || math.IsNaN(config.K) || math.IsInf(config.K, 0) {
		panic("breaker: Google K must be greater than one")
	}
	if config.ProbeInterval < 0 {
		panic("breaker: Google probe interval must not be negative")
	}
	if config.Window/time.Duration(config.Buckets) <= 0 {
		panic("breaker: Google bucket duration must be at least one nanosecond")
	}
}

// DefaultGoogleIsAccepted reports whether an RPC result counts as accepted by
// the backend. Unavailable and ResourceExhausted errors, along with RPC panics
// reported by this package, are not accepted. All other results are accepted.
func DefaultGoogleIsAccepted(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, errCallPanicked) {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeResourceExhausted:
		return false
	default:
		return true
	}
}

func (b *googleBreaker) Allow() (func(error), error) {
	now := b.now()
	b.mu.Lock()
	requests, accepts := b.historyLocked(now)
	dropProbability := b.dropProbability(requests, accepts)
	if dropProbability > 0 && !b.shouldProbeLocked(now) && b.random() < dropProbability {
		b.addRequestLocked(now)
		b.mu.Unlock()
		return nil, ErrRejected
	}
	b.addRequestLocked(now)
	requestTick := b.tick(now)
	b.lastPass = now
	b.mu.Unlock()

	var once sync.Once
	return func(err error) {
		once.Do(func() {
			if b.isAccepted(err) {
				b.mu.Lock()
				b.addAcceptLocked(requestTick)
				b.mu.Unlock()
			}
		})
	}, nil
}

func (b *googleBreaker) dropProbability(requests, accepts uint64) float64 {
	if requests < b.minRequests {
		return 0
	}
	probability := (float64(requests) - b.k*float64(accepts)) / (float64(requests) + 1)
	if probability <= 0 {
		return 0
	}
	if probability >= 1 {
		return 1
	}
	return probability
}

func (b *googleBreaker) shouldProbeLocked(now time.Time) bool {
	return b.lastPass.IsZero() || now.Sub(b.lastPass) >= b.probeInterval
}

func (b *googleBreaker) historyLocked(now time.Time) (requests, accepts uint64) {
	currentTick := b.tick(now)
	for i := range b.buckets {
		bucket := &b.buckets[i]
		age := currentTick - bucket.tick
		if bucket.valid && age >= 0 && age < int64(len(b.buckets)) {
			requests += bucket.requests
			accepts += bucket.accepts
		}
	}
	return requests, accepts
}

func (b *googleBreaker) addRequestLocked(now time.Time) {
	tick := b.tick(now)
	index := tick % int64(len(b.buckets))
	if index < 0 {
		index += int64(len(b.buckets))
	}
	bucket := &b.buckets[index]
	if !bucket.valid || bucket.tick != tick {
		*bucket = googleBucket{valid: true, tick: tick}
	}
	bucket.requests++
}

func (b *googleBreaker) addAcceptLocked(tick int64) {
	index := tick % int64(len(b.buckets))
	if index < 0 {
		index += int64(len(b.buckets))
	}
	bucket := &b.buckets[index]
	if bucket.valid && bucket.tick == tick {
		bucket.accepts++
	}
}

func (b *googleBreaker) tick(now time.Time) int64 {
	return now.UnixNano() / int64(b.bucketWidth)
}
