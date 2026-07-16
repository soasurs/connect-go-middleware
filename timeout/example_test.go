// SPDX-License-Identifier: Apache-2.0

package timeout_test

import (
	"time"

	"connectrpc.com/connect"
	"github.com/soasurs/connect-go-middleware/timeout"
)

func ExampleNewInterceptor() {
	clientOption := connect.WithInterceptors(timeout.NewInterceptor(3 * time.Second))
	_ = clientOption
}
