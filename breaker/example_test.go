// SPDX-License-Identifier: Apache-2.0

package breaker_test

import (
	"time"

	"connectrpc.com/connect"
	"github.com/soasurs/connect-go-middleware/breaker"
)

func ExampleNewGoogle() {
	circuitBreaker := breaker.NewGoogle(breaker.GoogleConfig{
		Window:      2 * time.Minute,
		Buckets:     40,
		K:           2,
		MinRequests: 20,
	})
	clientOption := connect.WithInterceptors(breaker.NewInterceptor(circuitBreaker))
	_ = clientOption
}
