// SPDX-License-Identifier: Apache-2.0

package breaker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"
)

const testProcedure = "/breaker.v1.BreakerService/Call"

func TestNewInterceptorPanicsForNilBreaker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewInterceptor did not panic")
		}
	}()
	NewInterceptor(nil)
}

func TestInterceptorRecordsSuccessfulClientUnaryCall(t *testing.T) {
	circuitBreaker := newRecordingBreaker(nil)
	client := newTestClient(t, circuitBreaker, func(
		context.Context,
		*connect.Request[emptypb.Empty],
	) (*connect.Response[emptypb.Empty], error) {
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	allowCalls, results := circuitBreaker.snapshot()
	if allowCalls != 1 {
		t.Fatalf("Allow calls = %d, want 1", allowCalls)
	}
	if len(results) != 1 || results[0] != nil {
		t.Fatalf("recorded results = %v, want [nil]", results)
	}
}

func TestInterceptorRecordsClientUnaryError(t *testing.T) {
	circuitBreaker := newRecordingBreaker(nil)
	client := newTestClient(t, circuitBreaker, func(
		context.Context,
		*connect.Request[emptypb.Empty],
	) (*connect.Response[emptypb.Empty], error) {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("overloaded"))
	})

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeResourceExhausted)
	}
	_, results := circuitBreaker.snapshot()
	if len(results) != 1 || connect.CodeOf(results[0]) != connect.CodeResourceExhausted {
		t.Fatalf("recorded results = %v, want one ResourceExhausted error", results)
	}
}

func TestInterceptorRejectsClientUnaryCall(t *testing.T) {
	providerErr := errors.New("circuit open")
	circuitBreaker := newRecordingBreaker(providerErr)
	var handlerCalls atomic.Int64
	client := newTestClient(t, circuitBreaker, func(
		context.Context,
		*connect.Request[emptypb.Empty],
	) (*connect.Response[emptypb.Empty], error) {
		handlerCalls.Add(1)
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeUnavailable {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeUnavailable)
	}
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("CallUnary() error = %v, want error matching ErrRejected", err)
	}
	if !errors.Is(err, providerErr) {
		t.Fatalf("CallUnary() error = %v, want error matching provider error", err)
	}
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0", got)
	}
}

func TestInterceptorDoesNotApplyToHandlerUnaryCall(t *testing.T) {
	circuitBreaker := newRecordingBreaker(errors.New("circuit open"))
	handler := connect.NewUnaryHandler(
		testProcedure,
		func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			return connect.NewResponse(&emptypb.Empty{}), nil
		},
		connect.WithInterceptors(NewInterceptor(circuitBreaker)),
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](http.DefaultClient, server.URL+testProcedure)

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	allowCalls, _ := circuitBreaker.snapshot()
	if allowCalls != 0 {
		t.Fatalf("Allow calls = %d, want 0", allowCalls)
	}
}

func TestInterceptorDoesNotApplyToStreamingCalls(t *testing.T) {
	circuitBreaker := newRecordingBreaker(errors.New("circuit open"))
	interceptor := NewInterceptor(circuitBreaker)
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
	allowCalls, _ := circuitBreaker.snapshot()
	if allowCalls != 0 {
		t.Fatalf("Allow calls = %d, want 0", allowCalls)
	}
}

func TestInvokeRecordsPanicAndRepanics(t *testing.T) {
	var recorded error
	panicValue := errors.New("boom")
	defer func() {
		if recovered := recover(); recovered != panicValue {
			t.Fatalf("recovered = %v, want %v", recovered, panicValue)
		}
		if connect.CodeOf(recorded) != connect.CodeInternal || !errors.Is(recorded, errCallPanicked) {
			t.Fatalf("recorded error = %v, want internal panic error", recorded)
		}
	}()
	_, _ = invoke(func(err error) { recorded = err }, func() (connect.AnyResponse, error) {
		panic(panicValue)
	})
}

func newTestClient(
	t *testing.T,
	circuitBreaker CircuitBreaker,
	handlerFunc func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error),
) *connect.Client[emptypb.Empty, emptypb.Empty] {
	t.Helper()
	handler := connect.NewUnaryHandler(testProcedure, handlerFunc)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithInterceptors(NewInterceptor(circuitBreaker)),
	)
}

type recordingBreaker struct {
	mu         sync.Mutex
	allowErr   error
	allowCalls int
	results    []error
}

func newRecordingBreaker(allowErr error) *recordingBreaker {
	return &recordingBreaker{allowErr: allowErr}
}

func (b *recordingBreaker) Allow() (func(error), error) {
	b.mu.Lock()
	b.allowCalls++
	b.mu.Unlock()
	if b.allowErr != nil {
		return nil, b.allowErr
	}
	return func(err error) {
		b.mu.Lock()
		b.results = append(b.results, err)
		b.mu.Unlock()
	}, nil
}

func (b *recordingBreaker) snapshot() (int, []error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.allowCalls, append([]error(nil), b.results...)
}
