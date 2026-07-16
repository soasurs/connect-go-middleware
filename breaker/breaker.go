// SPDX-License-Identifier: Apache-2.0

package breaker

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
)

// ErrRejected indicates that a circuit breaker rejected an RPC before it was
// sent to the server.
var ErrRejected = errors.New("circuit breaker rejected request")

var errCallPanicked = errors.New("rpc call panicked")

// CircuitBreaker decides whether an RPC may proceed and returns a callback
// that records its result. Implementations must be safe for concurrent use.
// The caller invokes done exactly once for every allowed RPC.
type CircuitBreaker interface {
	Allow() (done func(error), err error)
}

type interceptor struct {
	breaker CircuitBreaker
}

// NewInterceptor returns an interceptor that applies circuitBreaker to
// client-side unary RPCs. Rejected RPCs fail with connect.CodeUnavailable and
// an error matching ErrRejected.
//
// The interceptor has no effect on handler invocations or streaming RPCs.
// NewInterceptor panics if circuitBreaker is nil.
func NewInterceptor(circuitBreaker CircuitBreaker) connect.Interceptor {
	if circuitBreaker == nil {
		panic("breaker: circuit breaker must not be nil")
	}
	return &interceptor{breaker: circuitBreaker}
}

func (i *interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if !request.Spec().IsClient {
			return next(ctx, request)
		}
		done, err := i.breaker.Allow()
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, newRejectionError(err))
		}
		return invoke(done, func() (connect.AnyResponse, error) {
			return next(ctx, request)
		})
	}
}

func (*interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (*interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func invoke(done func(error), call func() (connect.AnyResponse, error)) (response connect.AnyResponse, err error) {
	completed := false
	defer func() {
		if !completed {
			done(connect.NewError(connect.CodeInternal, errCallPanicked))
		}
	}()
	response, err = call()
	completed = true
	done(err)
	return response, err
}

func newRejectionError(cause error) error {
	if errors.Is(cause, ErrRejected) {
		return cause
	}
	return &rejectionError{cause: cause}
}

type rejectionError struct {
	cause error
}

func (e *rejectionError) Error() string {
	return fmt.Sprintf("%s: %v", ErrRejected, e.cause)
}

func (e *rejectionError) Unwrap() []error {
	return []error{ErrRejected, e.cause}
}
