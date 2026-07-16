// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
)

// ErrRateLimited indicates that a limiter rejected an RPC.
var ErrRateLimited = errors.New("rate limit exceeded")

// Limiter decides whether a unary RPC may proceed. Implementations must be
// safe for concurrent use. The procedure uses Connect's fully qualified form,
// for example "/acme.user.v1.UserService/GetUser".
type Limiter interface {
	Allow(ctx context.Context, procedure string) error
}

type interceptor struct {
	limiter Limiter
}

// NewInterceptor returns an interceptor that applies limiter to server-side
// unary RPCs. Rejected RPCs fail with connect.CodeResourceExhausted and an
// error matching ErrRateLimited.
//
// The interceptor has no effect on client or streaming invocations.
// NewInterceptor panics if limiter is nil.
func NewInterceptor(limiter Limiter) connect.Interceptor {
	if limiter == nil {
		panic("ratelimit: limiter must not be nil")
	}
	return &interceptor{limiter: limiter}
}

func (i *interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if request.Spec().IsClient {
			return next(ctx, request)
		}
		if err := i.limiter.Allow(ctx, request.Spec().Procedure); err != nil {
			return nil, connect.NewError(connect.CodeResourceExhausted, newRateLimitError(err))
		}
		return next(ctx, request)
	}
}

func (*interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (*interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func newRateLimitError(cause error) error {
	if errors.Is(cause, ErrRateLimited) {
		return cause
	}
	return &rateLimitError{cause: cause}
}

type rateLimitError struct {
	cause error
}

func (e *rateLimitError) Error() string {
	return fmt.Sprintf("%s: %v", ErrRateLimited, e.cause)
}

func (e *rateLimitError) Unwrap() []error {
	return []error{ErrRateLimited, e.cause}
}
