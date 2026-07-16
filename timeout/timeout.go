// SPDX-License-Identifier: Apache-2.0

package timeout

import (
	"context"
	"time"

	"connectrpc.com/connect"
)

type interceptor struct {
	duration time.Duration
}

// NewInterceptor returns an interceptor that limits client-side RPCs to
// duration. For streaming RPCs, the timeout covers the whole stream lifetime.
// An existing earlier context deadline always takes precedence.
//
// The interceptor has no effect on handler invocations. NewInterceptor panics
// if duration is not positive.
func NewInterceptor(duration time.Duration) connect.Interceptor {
	if duration <= 0 {
		panic("timeout: duration must be positive")
	}
	return &interceptor{duration: duration}
}

func (i *interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if !request.Spec().IsClient {
			return next(ctx, request)
		}
		timedCtx, cancel := context.WithTimeout(ctx, i.duration)
		defer cancel()
		return next(timedCtx, request)
	}
}

func (i *interceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		timedCtx, cancel := context.WithTimeout(ctx, i.duration)
		return newStreamingClientConn(next(timedCtx, spec), cancel)
	}
}

func (*interceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
