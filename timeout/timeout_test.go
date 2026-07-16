// SPDX-License-Identifier: Apache-2.0

package timeout

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"
)

const testProcedure = "/timeout.v1.TimeoutService/Wait"

func TestNewInterceptorPanicsForNonPositiveDuration(t *testing.T) {
	for _, duration := range []time.Duration{0, -time.Second} {
		t.Run(duration.String(), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewInterceptor did not panic")
				}
			}()
			NewInterceptor(duration)
		})
	}
}

func TestInterceptorSetsDefaultClientDeadline(t *testing.T) {
	const timeout = 5 * time.Second
	remaining := callAndObserveDeadline(t, NewInterceptor(timeout), context.Background())
	if remaining <= 0 || remaining > timeout {
		t.Fatalf("remaining deadline = %v, want within (0, %v]", remaining, timeout)
	}
}

func TestInterceptorPreservesEarlierClientDeadline(t *testing.T) {
	const (
		interceptorTimeout = 10 * time.Second
		callerTimeout      = 2 * time.Second
	)
	ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
	defer cancel()

	remaining := callAndObserveDeadline(t, NewInterceptor(interceptorTimeout), ctx)
	if remaining <= 0 || remaining > callerTimeout {
		t.Fatalf("remaining deadline = %v, want within (0, %v]", remaining, callerTimeout)
	}
}

func TestInterceptorReturnsDeadlineExceeded(t *testing.T) {
	handler := connect.NewUnaryHandler(
		testProcedure,
		func(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithInterceptors(NewInterceptor(20*time.Millisecond)),
	)

	_, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if err == nil {
		t.Fatal("CallUnary returned nil error")
	}
	if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
		t.Fatalf("error code = %v, want %v: %v", got, connect.CodeDeadlineExceeded, err)
	}
}

func TestInterceptorDoesNotSetHandlerDeadline(t *testing.T) {
	handler := connect.NewUnaryHandler(
		testProcedure,
		func(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			if _, ok := ctx.Deadline(); ok {
				return nil, errors.New("handler context unexpectedly has a deadline")
			}
			return connect.NewResponse(&emptypb.Empty{}), nil
		},
		connect.WithInterceptors(NewInterceptor(time.Second)),
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](http.DefaultClient, server.URL+testProcedure)

	if _, err := client.CallUnary(t.Context(), connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
}

func TestInterceptorSetsStreamingClientDeadline(t *testing.T) {
	interceptor := NewInterceptor(time.Second)
	ctx := t.Context()
	spec := connect.Spec{Procedure: testProcedure, IsClient: true, StreamType: connect.StreamTypeBidi}
	clientConn := &stubStreamingClientConn{spec: spec}
	var streamCtx context.Context
	clientNext := func(gotCtx context.Context, gotSpec connect.Spec) connect.StreamingClientConn {
		streamCtx = gotCtx
		if gotSpec != spec {
			t.Errorf("streaming client spec = %#v, want %#v", gotSpec, spec)
		}
		return clientConn
	}
	wrapped := interceptor.WrapStreamingClient(clientNext)(ctx, spec)
	deadline, ok := streamCtx.Deadline()
	if !ok {
		t.Fatal("streaming client context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > time.Second {
		t.Fatalf("remaining deadline = %v, want within (0, %v]", remaining, time.Second)
	}
	if err := wrapped.CloseRequest(); err != nil {
		t.Fatalf("CloseRequest() error = %v", err)
	}
	if err := streamCtx.Err(); err != nil {
		t.Fatalf("CloseRequest canceled stream context: %v", err)
	}
	if err := wrapped.CloseResponse(); err != nil {
		t.Fatalf("CloseResponse() error = %v", err)
	}
	if !errors.Is(streamCtx.Err(), context.Canceled) {
		t.Fatalf("stream context error = %v, want context.Canceled", streamCtx.Err())
	}
}

func TestInterceptorPreservesEarlierStreamingClientDeadline(t *testing.T) {
	callerDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(t.Context(), callerDeadline)
	defer cancel()
	var streamCtx context.Context
	next := func(gotCtx context.Context, spec connect.Spec) connect.StreamingClientConn {
		streamCtx = gotCtx
		return &stubStreamingClientConn{spec: spec}
	}

	conn := NewInterceptor(time.Hour).WrapStreamingClient(next)(ctx, connect.Spec{IsClient: true})
	t.Cleanup(func() { _ = conn.CloseResponse() })
	gotDeadline, ok := streamCtx.Deadline()
	if !ok {
		t.Fatal("streaming client context has no deadline")
	}
	if !gotDeadline.Equal(callerDeadline) {
		t.Fatalf("streaming client deadline = %v, want %v", gotDeadline, callerDeadline)
	}
}

func TestStreamingClientTimeoutEndsOnReceiveError(t *testing.T) {
	var streamCtx context.Context
	next := func(gotCtx context.Context, spec connect.Spec) connect.StreamingClientConn {
		streamCtx = gotCtx
		return &stubStreamingClientConn{spec: spec, receiveErr: io.EOF}
	}
	conn := NewInterceptor(time.Hour).WrapStreamingClient(next)(t.Context(), connect.Spec{IsClient: true})

	if err := conn.Receive(&emptypb.Empty{}); !errors.Is(err, io.EOF) {
		t.Fatalf("Receive() error = %v, want io.EOF", err)
	}
	if !errors.Is(streamCtx.Err(), context.Canceled) {
		t.Fatalf("stream context error = %v, want context.Canceled", streamCtx.Err())
	}
}

func TestStreamingClientTimeoutEndsOnResponseTrailer(t *testing.T) {
	var streamCtx context.Context
	next := func(gotCtx context.Context, spec connect.Spec) connect.StreamingClientConn {
		streamCtx = gotCtx
		return &stubStreamingClientConn{spec: spec}
	}
	conn := NewInterceptor(time.Hour).WrapStreamingClient(next)(t.Context(), connect.Spec{IsClient: true})

	conn.ResponseTrailer()
	if !errors.Is(streamCtx.Err(), context.Canceled) {
		t.Fatalf("stream context error = %v, want context.Canceled", streamCtx.Err())
	}
}

func TestStreamingClientTimeoutReturnsDeadlineExceeded(t *testing.T) {
	handler := connect.NewServerStreamHandler(
		testProcedure,
		func(ctx context.Context, _ *connect.Request[emptypb.Empty], _ *connect.ServerStream[emptypb.Empty]) error {
			<-ctx.Done()
			return ctx.Err()
		},
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithInterceptors(NewInterceptor(20*time.Millisecond)),
	)

	stream, err := client.CallServerStream(t.Context(), connect.NewRequest(&emptypb.Empty{}))
	if err == nil {
		stream.Receive()
		err = stream.Err()
	}
	if err == nil {
		t.Fatal("server stream returned nil error")
	}
	if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
		t.Fatalf("error code = %v, want %v: %v", got, connect.CodeDeadlineExceeded, err)
	}
}

func TestClientStreamTimeoutReturnsDeadlineExceeded(t *testing.T) {
	handler := connect.NewClientStreamHandler(
		testProcedure,
		func(ctx context.Context, _ *connect.ClientStream[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithInterceptors(NewInterceptor(20*time.Millisecond)),
	)
	stream := client.CallClientStream(t.Context())

	_, err := stream.CloseAndReceive()
	if err == nil {
		t.Fatal("client stream returned nil error")
	}
	if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
		t.Fatalf("error code = %v, want %v: %v", got, connect.CodeDeadlineExceeded, err)
	}
}

func TestBidiStreamTimeoutReturnsDeadlineExceeded(t *testing.T) {
	handler := connect.NewBidiStreamHandler(
		testProcedure,
		func(ctx context.Context, _ *connect.BidiStream[emptypb.Empty, emptypb.Empty]) error {
			<-ctx.Done()
			return ctx.Err()
		},
	)
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		server.Client(),
		server.URL+testProcedure,
		connect.WithInterceptors(NewInterceptor(20*time.Millisecond)),
	)
	stream := client.CallBidiStream(t.Context())
	if err := stream.Send(&emptypb.Empty{}); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Send() error = %v", err)
	}

	_, err := stream.Receive()
	if err == nil {
		t.Fatal("bidi stream returned nil error")
	}
	if got := connect.CodeOf(err); got != connect.CodeDeadlineExceeded {
		t.Fatalf("error code = %v, want %v: %v", got, connect.CodeDeadlineExceeded, err)
	}
}

func TestInterceptorDoesNotWrapStreamingHandler(t *testing.T) {
	interceptor := NewInterceptor(time.Second)
	ctx := t.Context()
	spec := connect.Spec{Procedure: testProcedure, StreamType: connect.StreamTypeBidi}
	handlerConn := &stubStreamingHandlerConn{spec: spec}
	handlerCalled := false
	handlerNext := func(gotCtx context.Context, gotConn connect.StreamingHandlerConn) error {
		handlerCalled = true
		if gotCtx != ctx {
			t.Error("streaming handler context was replaced")
		}
		if gotConn != handlerConn {
			t.Error("WrapStreamingHandler changed the connection")
		}
		return nil
	}
	if err := interceptor.WrapStreamingHandler(handlerNext)(ctx, handlerConn); err != nil {
		t.Fatalf("WrapStreamingHandler() error = %v", err)
	}
	if !handlerCalled {
		t.Error("WrapStreamingHandler did not call next")
	}
}

func callAndObserveDeadline(t *testing.T, interceptor connect.Interceptor, ctx context.Context) time.Duration {
	t.Helper()
	deadlines := make(chan time.Duration, 1)
	handler := connect.NewUnaryHandler(
		testProcedure,
		func(ctx context.Context, _ *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				return nil, errors.New("handler context has no deadline")
			}
			deadlines <- time.Until(deadline)
			return connect.NewResponse(&emptypb.Empty{}), nil
		},
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := connect.NewClient[emptypb.Empty, emptypb.Empty](
		http.DefaultClient,
		server.URL+testProcedure,
		connect.WithInterceptors(interceptor),
	)

	if _, err := client.CallUnary(ctx, connect.NewRequest(&emptypb.Empty{})); err != nil {
		t.Fatalf("CallUnary() error = %v", err)
	}
	return <-deadlines
}

type stubStreamingClientConn struct {
	spec       connect.Spec
	receiveErr error
}

func (c *stubStreamingClientConn) Spec() connect.Spec         { return c.spec }
func (*stubStreamingClientConn) Peer() connect.Peer           { return connect.Peer{} }
func (*stubStreamingClientConn) Send(any) error               { return nil }
func (*stubStreamingClientConn) RequestHeader() http.Header   { return make(http.Header) }
func (*stubStreamingClientConn) CloseRequest() error          { return nil }
func (c *stubStreamingClientConn) Receive(any) error          { return c.receiveErr }
func (*stubStreamingClientConn) ResponseHeader() http.Header  { return make(http.Header) }
func (*stubStreamingClientConn) ResponseTrailer() http.Header { return make(http.Header) }
func (*stubStreamingClientConn) CloseResponse() error         { return nil }

type stubStreamingHandlerConn struct {
	spec connect.Spec
}

func (c *stubStreamingHandlerConn) Spec() connect.Spec         { return c.spec }
func (*stubStreamingHandlerConn) Peer() connect.Peer           { return connect.Peer{} }
func (*stubStreamingHandlerConn) Receive(any) error            { return nil }
func (*stubStreamingHandlerConn) RequestHeader() http.Header   { return make(http.Header) }
func (*stubStreamingHandlerConn) Send(any) error               { return nil }
func (*stubStreamingHandlerConn) ResponseHeader() http.Header  { return make(http.Header) }
func (*stubStreamingHandlerConn) ResponseTrailer() http.Header { return make(http.Header) }
