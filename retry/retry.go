// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
)

const defaultMaxAttempts = 3

// RetryableFunc reports whether an RPC error may be retried. It may be called
// concurrently. Context cancellation and the parent deadline always take
// precedence over its result.
type RetryableFunc func(error) bool

// OnRetryFunc is called after a retryable failure and before its backoff. The
// attempt is the one-based number of the failed attempt. It may be called
// concurrently for different RPCs.
type OnRetryFunc func(ctx context.Context, attempt uint, err error)

// Config configures a retry interceptor. Zero-valued fields use their
// documented defaults.
type Config struct {
	// MaxAttempts is the total number of attempts, including the initial call.
	// The default is three.
	MaxAttempts uint
	// Backoff determines how long to wait after a failed attempt. The attempt
	// argument starts at one. The default is exponential full-jitter backoff
	// starting at 50 milliseconds and capped at one second.
	Backoff BackoffFunc
	// PerAttemptTimeout limits each attempt. Zero disables it. A parent context
	// with an earlier deadline always takes precedence. When enabled,
	// DeadlineExceeded results are eligible for retry.
	PerAttemptTimeout time.Duration
	// IsRetryable determines which errors may be retried. By default,
	// Unavailable and ResourceExhausted errors are retried.
	IsRetryable RetryableFunc
	// OnRetry is called immediately before waiting for the next attempt.
	OnRetry OnRetryFunc
	// AllowNonIdempotent permits retries when the procedure does not declare an
	// idempotency level. It is false by default.
	AllowNonIdempotent bool
}

type interceptor struct {
	maxAttempts        uint
	backoff            BackoffFunc
	perAttemptTimeout  time.Duration
	isRetryable        RetryableFunc
	onRetry            OnRetryFunc
	allowNonIdempotent bool
	wait               func(context.Context, time.Duration) error
}

// NewInterceptor returns an interceptor that retries eligible client-side
// unary RPCs. By default, only procedures declared idempotent or free of side
// effects are eligible.
//
// The interceptor has no effect on handler invocations or streaming RPCs. It
// panics if config contains a negative PerAttemptTimeout.
func NewInterceptor(config Config) connect.Interceptor {
	return newInterceptor(config, waitContext)
}

func newInterceptor(config Config, wait func(context.Context, time.Duration) error) *interceptor {
	if config.PerAttemptTimeout < 0 {
		panic("retry: per-attempt timeout must not be negative")
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
	if config.Backoff == nil {
		config.Backoff = ExponentialBackoff(50*time.Millisecond, time.Second)
	}
	if config.IsRetryable == nil {
		config.IsRetryable = DefaultIsRetryable
	}
	return &interceptor{
		maxAttempts:        config.MaxAttempts,
		backoff:            config.Backoff,
		perAttemptTimeout:  config.PerAttemptTimeout,
		isRetryable:        config.IsRetryable,
		onRetry:            config.OnRetry,
		allowNonIdempotent: config.AllowNonIdempotent,
		wait:               wait,
	}
}

// DefaultIsRetryable reports whether err has connect.CodeUnavailable or
// connect.CodeResourceExhausted. Context cancellation errors are never
// retryable.
func DefaultIsRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeResourceExhausted:
		return true
	default:
		return false
	}
}

func (i *interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		spec := request.Spec()
		if !spec.IsClient || !i.eligible(spec.IdempotencyLevel) {
			return next(ctx, request)
		}
		return i.call(ctx, request, next)
	}
}

func (*interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (*interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func (i *interceptor) eligible(level connect.IdempotencyLevel) bool {
	if i.allowNonIdempotent {
		return true
	}
	return level == connect.IdempotencyNoSideEffects || level == connect.IdempotencyIdempotent
}

func (i *interceptor) call(
	ctx context.Context,
	request connect.AnyRequest,
	next connect.UnaryFunc,
) (connect.AnyResponse, error) {
	for attempt := uint(1); ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, connectContextError(err)
		}

		attemptCtx, cancel := i.attemptContext(ctx)
		response, err := invokeAttempt(attemptCtx, cancel, request, next)
		if err == nil {
			return response, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, connectContextError(ctxErr)
		}
		perAttemptDeadline := i.perAttemptTimeout > 0 && connect.CodeOf(err) == connect.CodeDeadlineExceeded
		if attempt >= i.maxAttempts || (!perAttemptDeadline && !i.isRetryable(err)) {
			return response, err
		}

		if i.onRetry != nil {
			i.onRetry(ctx, attempt, err)
		}
		if err := i.wait(ctx, i.backoff(ctx, attempt)); err != nil {
			return nil, connectContextError(err)
		}
	}
}

func invokeAttempt(
	attemptCtx context.Context,
	cancel context.CancelFunc,
	request connect.AnyRequest,
	next connect.UnaryFunc,
) (response connect.AnyResponse, err error) {
	defer cancel()
	return next(attemptCtx, request)
}

func (i *interceptor) attemptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if i.perAttemptTimeout == 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, i.perAttemptTimeout)
}

func connectContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	return connect.NewError(connect.CodeCanceled, err)
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
