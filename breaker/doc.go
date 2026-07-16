// SPDX-License-Identifier: Apache-2.0

// Package breaker provides client-side circuit breaker interceptors for Connect RPCs.
// It adapts two-step circuit breaker implementations and includes Google SRE
// adaptive throttling. Interceptors currently apply only to unary client calls.
package breaker
