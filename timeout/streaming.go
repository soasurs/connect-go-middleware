// SPDX-License-Identifier: Apache-2.0

package timeout

import (
	"context"
	"net/http"
	"sync"

	"connectrpc.com/connect"
)

type streamingClientConn struct {
	connect.StreamingClientConn

	cancel context.CancelFunc
	once   sync.Once
}

func newStreamingClientConn(conn connect.StreamingClientConn, cancel context.CancelFunc) *streamingClientConn {
	return &streamingClientConn{
		StreamingClientConn: conn,
		cancel:              cancel,
	}
}

func (c *streamingClientConn) Receive(message any) error {
	err := c.StreamingClientConn.Receive(message)
	if err != nil {
		c.finish()
	}
	return err
}

func (c *streamingClientConn) ResponseTrailer() http.Header {
	trailer := c.StreamingClientConn.ResponseTrailer()
	c.finish()
	return trailer
}

func (c *streamingClientConn) CloseResponse() error {
	err := c.StreamingClientConn.CloseResponse()
	c.finish()
	return err
}

func (c *streamingClientConn) finish() {
	c.once.Do(c.cancel)
}
