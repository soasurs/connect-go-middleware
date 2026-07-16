// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"
)

const testProcedure = "/ratelimit.v1.RateLimitService/Call"

func TestNewInterceptorPanicsForNilLimiter(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewInterceptor did not panic")
		}
	}()
	NewInterceptor(nil)
}

func TestInterceptorAllowsHandlerUnaryCall(t *testing.T) {
	limiter := &recordingLimiter{}
	handlerCalls := 0
	client := newTestClient(t, limiter, func(
		context.Context,
		*connect.Request[emptypb.Empty],
	) (*connect.Response[emptypb.Empty], error) {
		handlerCalls++
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}
	ctx, procedures := limiter.snapshot()
	if ctx == nil || len(procedures) != 1 || procedures[0] != testProcedure {
		t.Fatalf("limiter calls = (%v, %v), want context and %q", ctx, procedures, testProcedure)
	}
}

func TestInterceptorRejectsHandlerUnaryCall(t *testing.T) {
	providerErr := errors.New("quota exhausted")
	limiter := &recordingLimiter{err: providerErr}
	handlerCalls := 0
	client := newTestClient(t, limiter, func(
		context.Context,
		*connect.Request[emptypb.Empty],
	) (*connect.Response[emptypb.Empty], error) {
		handlerCalls++
		return connect.NewResponse(&emptypb.Empty{}), nil
	})

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("CallUnary() code = %v, want %v", got, connect.CodeResourceExhausted)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want 0", handlerCalls)
	}
}

func TestRateLimitErrorPreservesCauses(t *testing.T) {
	providerErr := errors.New("quota exhausted")
	err := newRateLimitError(providerErr)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("error = %v, want error matching ErrRateLimited", err)
	}
	if !errors.Is(err, providerErr) {
		t.Fatalf("error = %v, want error matching provider error", err)
	}
}

func TestInterceptorDoesNotApplyToClientUnaryCall(t *testing.T) {
	limiter := &recordingLimiter{err: ErrRateLimited}
	handler := connect.NewUnaryHandler(
		testProcedure,
		func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			return connect.NewResponse(&emptypb.Empty{}), nil
		},
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithInterceptors(NewInterceptor(limiter)),
	)

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	_, procedures := limiter.snapshot()
	if len(procedures) != 0 {
		t.Fatalf("limiter calls = %d, want 0", len(procedures))
	}
}

func TestInterceptorDoesNotApplyToStreamingCalls(t *testing.T) {
	limiter := &recordingLimiter{err: ErrRateLimited}
	interceptor := NewInterceptor(limiter)
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
	_, procedures := limiter.snapshot()
	if len(procedures) != 0 {
		t.Fatalf("limiter calls = %d, want 0", len(procedures))
	}
}

func newTestClient(
	t *testing.T,
	limiter Limiter,
	handlerFunc func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error),
) *connect.Client[emptypb.Empty, emptypb.Empty] {
	t.Helper()
	handler := connect.NewUnaryHandler(
		testProcedure,
		handlerFunc,
		connect.WithInterceptors(NewInterceptor(limiter)),
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return connect.NewClient[emptypb.Empty, emptypb.Empty](http.DefaultClient, server.URL+testProcedure)
}

type recordingLimiter struct {
	mu         sync.Mutex
	err        error
	ctx        context.Context
	procedures []string
}

func (l *recordingLimiter) Allow(ctx context.Context, procedure string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ctx = ctx
	l.procedures = append(l.procedures, procedure)
	return l.err
}

func (l *recordingLimiter) snapshot() (context.Context, []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ctx, append([]string(nil), l.procedures...)
}
