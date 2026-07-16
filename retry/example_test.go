// SPDX-License-Identifier: Apache-2.0

package retry_test

import (
	"time"

	"connectrpc.com/connect"
	"github.com/soasurs/connect-go-middleware/retry"
)

func ExampleNewInterceptor() {
	clientOption := connect.WithInterceptors(retry.NewInterceptor(retry.Config{
		MaxAttempts:       3,
		PerAttemptTimeout: time.Second,
	}))
	_ = clientOption
}
