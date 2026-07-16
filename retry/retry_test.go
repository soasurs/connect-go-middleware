// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"
)

const testProcedure = "/retry.v1.RetryService/Call"

func TestInterceptorRetriesIdempotentClientUnaryCall(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyIdempotent, Config{
		Backoff: noBackoff,
	}, func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		if calls.Add(1) < 3 {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
		}
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
}

func TestInterceptorRetriesNoSideEffectsClientUnaryCall(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyNoSideEffects, Config{
		MaxAttempts: 2,
		Backoff:     noBackoff,
	}, func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		if calls.Add(1) == 1 {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("overloaded"))
		}
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
}

func TestInterceptorDoesNotRetryUnknownIdempotency(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyUnknown, Config{
		Backoff: noBackoff,
	}, func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		calls.Add(1)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
	})

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeUnavailable)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestInterceptorCanRetryUnknownIdempotency(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyUnknown, Config{
		MaxAttempts:        2,
		Backoff:            noBackoff,
		AllowNonIdempotent: true,
	}, func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		if calls.Add(1) == 1 {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
		}
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
}

func TestInterceptorDoesNotRetryNonRetryableError(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyIdempotent, Config{
		Backoff: noBackoff,
	}, func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		calls.Add(1)
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid request"))
	})

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeInvalidArgument)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestInterceptorStopsAtMaxAttempts(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyIdempotent, Config{
		MaxAttempts: 2,
		Backoff:     noBackoff,
	}, func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		calls.Add(1)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
	})

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeUnavailable)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
}

func TestInterceptorAppliesPerAttemptTimeout(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyIdempotent, Config{
		MaxAttempts:       2,
		Backoff:           noBackoff,
		PerAttemptTimeout: 20 * time.Millisecond,
	}, func(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		if calls.Add(1) == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
}

func TestInterceptorStopsWhenParentContextCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyIdempotent, Config{
		Backoff: func(context.Context, uint) time.Duration { return time.Hour },
		OnRetry: func(context.Context, uint, error) { cancel() },
	}, func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		calls.Add(1)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
	})

	_, err := client.CallUnary(ctx, connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeCanceled {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeCanceled)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestInterceptorStopsWhenParentDeadlineExpires(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, connect.IdempotencyIdempotent, Config{
		Backoff: noBackoff,
	}, func(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err := client.CallUnary(ctx, connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeDeadlineExceeded)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestInterceptorReentersFollowingInterceptorForEveryAttempt(t *testing.T) {
	var handlerCalls atomic.Int64
	var innerCalls atomic.Int64
	inner := countingInterceptor{calls: &innerCalls}
	handler := connect.NewUnaryHandler(
		testProcedure,
		func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			if handlerCalls.Add(1) == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
			}
			return connect.NewResponse(&emptypb.Empty{}), nil
		},
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithIdempotency(connect.IdempotencyIdempotent),
		connect.WithInterceptors(
			NewInterceptor(Config{MaxAttempts: 2, Backoff: noBackoff}),
			inner,
		),
	)

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	if got := innerCalls.Load(); got != 2 {
		t.Fatalf("inner interceptor calls = %d, want 2", got)
	}
}

func TestInterceptorReportsRetryAndBackoffAttempts(t *testing.T) {
	var mu sync.Mutex
	var backoffAttempts []uint
	var callbackAttempts []uint
	var waits []time.Duration
	interceptor := newInterceptor(Config{
		MaxAttempts: 3,
		Backoff: func(_ context.Context, attempt uint) time.Duration {
			mu.Lock()
			defer mu.Unlock()
			backoffAttempts = append(backoffAttempts, attempt)
			return time.Duration(attempt) * time.Millisecond
		},
		OnRetry: func(_ context.Context, attempt uint, _ error) {
			mu.Lock()
			callbackAttempts = append(callbackAttempts, attempt)
			mu.Unlock()
		},
	}, func(_ context.Context, duration time.Duration) error {
		mu.Lock()
		waits = append(waits, duration)
		mu.Unlock()
		return nil
	})
	var calls atomic.Int64
	client := newTestClientWithInterceptor(t, connect.IdempotencyIdempotent, interceptor, func(
		context.Context,
		*connect.Request[emptypb.Empty],
	) (*connect.Response[emptypb.Empty], error) {
		if calls.Add(1) < 3 {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
		}
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !equalUint(backoffAttempts, []uint{1, 2}) {
		t.Fatalf("backoff attempts = %v, want [1 2]", backoffAttempts)
	}
	if !equalUint(callbackAttempts, []uint{1, 2}) {
		t.Fatalf("callback attempts = %v, want [1 2]", callbackAttempts)
	}
	if len(waits) != 2 || waits[0] != time.Millisecond || waits[1] != 2*time.Millisecond {
		t.Fatalf("waits = %v, want [1ms 2ms]", waits)
	}
}

func TestInterceptorDoesNotApplyToHandlerUnaryCall(t *testing.T) {
	var calls atomic.Int64
	handler := connect.NewUnaryHandler(
		testProcedure,
		func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			calls.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable"))
		},
		connect.WithInterceptors(NewInterceptor(Config{
			AllowNonIdempotent: true,
			Backoff:            noBackoff,
		})),
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](http.DefaultClient, server.URL+testProcedure)

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeUnavailable)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestInterceptorDoesNotApplyToStreamingCalls(t *testing.T) {
	interceptor := NewInterceptor(Config{AllowNonIdempotent: true})
	ctx := t.Context()
	spec := connect.Spec{Procedure: testProcedure, IsClient: true, StreamType: connect.StreamTypeBidi}
	clientCalled := false
	clientNext := func(gotCtx context.Context, gotSpec connect.Spec) connect.StreamingClientConn {
		clientCalled = true
		if gotCtx != ctx || gotSpec != spec {
			t.Error("WrapStreamingClient changed arguments")
		}
		return nil
	}
	interceptor.WrapStreamingClient(clientNext)(ctx, spec)
	if !clientCalled {
		t.Error("WrapStreamingClient did not call next")
	}

	handlerCalled := false
	handlerNext := func(gotCtx context.Context, gotConn connect.StreamingHandlerConn) error {
		handlerCalled = true
		if gotCtx != ctx || gotConn != nil {
			t.Error("WrapStreamingHandler changed arguments")
		}
		return nil
	}
	if err := interceptor.WrapStreamingHandler(handlerNext)(ctx, nil); err != nil {
		t.Fatalf("WrapStreamingHandler() error = %v", err)
	}
	if !handlerCalled {
		t.Error("WrapStreamingHandler did not call next")
	}
}

func TestNewInterceptorPanicsForNegativePerAttemptTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewInterceptor did not panic")
		}
	}()
	NewInterceptor(Config{PerAttemptTimeout: -time.Second})
}

func TestDefaultIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "unavailable", err: connect.NewError(connect.CodeUnavailable, errors.New("unavailable")), want: true},
		{name: "resource exhausted", err: connect.NewError(connect.CodeResourceExhausted, errors.New("overloaded")), want: true},
		{name: "invalid argument", err: connect.NewError(connect.CodeInvalidArgument, errors.New("invalid")), want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DefaultIsRetryable(test.err); got != test.want {
				t.Fatalf("DefaultIsRetryable() = %v, want %v", got, test.want)
			}
		})
	}
}

func newTestClient(
	t *testing.T,
	idempotency connect.IdempotencyLevel,
	config Config,
	handlerFunc func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error),
) *connect.Client[emptypb.Empty, emptypb.Empty] {
	t.Helper()
	return newTestClientWithInterceptor(t, idempotency, NewInterceptor(config), handlerFunc)
}

func newTestClientWithInterceptor(
	t *testing.T,
	idempotency connect.IdempotencyLevel,
	interceptor connect.Interceptor,
	handlerFunc func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error),
) *connect.Client[emptypb.Empty, emptypb.Empty] {
	t.Helper()
	handler := connect.NewUnaryHandler(testProcedure, handlerFunc)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithIdempotency(idempotency),
		connect.WithInterceptors(interceptor),
	)
}

func noBackoff(context.Context, uint) time.Duration {
	return 0
}

func equalUint(left, right []uint) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type countingInterceptor struct {
	calls *atomic.Int64
}

func (i countingInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		i.calls.Add(1)
		return next(ctx, request)
	}
}

func (countingInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (countingInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
