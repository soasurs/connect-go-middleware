// SPDX-License-Identifier: Apache-2.0

package ratelimit_test

import (
	"connectrpc.com/connect"
	"github.com/soasurs/connect-go-middleware/ratelimit"
)

func ExampleNewTokenBucket() {
	limiter := ratelimit.NewTokenBucket(ratelimit.TokenBucketConfig{
		Rate:  100,
		Burst: 200,
	})
	handlerOption := connect.WithInterceptors(ratelimit.NewInterceptor(limiter))
	_ = handlerOption
}
