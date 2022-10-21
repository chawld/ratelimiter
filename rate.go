// This file implements a lock-less thread-safe rate limiter with limited functionality
package ratelimiter

import (
	"context"
	"sync/atomic"
	"time"
)

type RateLimiter interface {
	Wait(ctx context.Context)
	Allow() bool
}

func NewRateLimiter(tokens int64, interval time.Duration) RateLimiter {
	return &rateLimiter{
		timeUnit: float64(interval) / float64(tokens),
		interval: interval,
	}
}

// rateLimiter is a lockless thread-safe implementation of the RateLimiter
//   * It is thread-safe
//   * It is fair
//   * It supports a wait API
//   * It supports contexts and honors context cancellations
//
// Non-features
//   * Rate requests once granted can't be cancelled and will impact future observed throughputs
//   * Doesn't support bursts larger than the rate
//   * Doesn't allow changing the rate after initial setup
type rateLimiter struct {
	token    int64         // monotonically increasing token
	timeUnit float64       // number of tokens per unit of time (ns)
	interval time.Duration // granularity
}

// Wait waits until the caller is allowed to perform the rate-limited operation.
func (r *rateLimiter) Wait(ctx context.Context) {
	r.waitN(ctx, 1)
}

// Allow returns true if the caller is allowed to perform the rate-limited operation,
// false otherwise.
func (r *rateLimiter) Allow() bool {
	return r.allowN(1)
}

// waitN waits until the caller is allowed to perform the rate-limited operation.
func (r *rateLimiter) waitN(ctx context.Context, n int64) {
	var waitTime time.Duration
	done := false
	for !done {
		select {
		case <-ctx.Done():
			return
		default:
			oldToken, newToken := r.getNextToken(n)
			if atomic.CompareAndSwapInt64(&r.token, oldToken, newToken) {
				waitTime = r.timeFromToken(newToken)
				done = true
			}
		}
	}
	r.waitTill(ctx, waitTime)
}

// allowN returns true if the caller is allowed to perform the rate-limited operation,
// false otherwise.
func (r *rateLimiter) allowN(n int64) bool {
	done := false
	for !done {
		oldToken, newToken := r.getNextToken(n)
		if r.timeFromToken(newToken) > time.Duration(0) {
			// will have to wait :(
			return false
		}
		if atomic.CompareAndSwapInt64(&r.token, oldToken, newToken) {
			done = true
		}
	}
	return true
}

// getNextToken returns the current token and the next available token.
func (r *rateLimiter) getNextToken(n int64) (int64, int64) {
	oldToken := atomic.LoadInt64(&r.token)
	now := time.Now().UnixNano()
	newToken := int64(float64(now) / r.timeUnit)
	if newToken < oldToken+n {
		newToken = oldToken + n
	}
	return oldToken, newToken
}

// timeFromToken returns the duration after which the token can be used. If the duration
// is not a positive value, the token can be used immediately.
func (r *rateLimiter) timeFromToken(token int64) time.Duration {
	nanos := (token * int64(r.timeUnit) / int64(r.interval)) * int64(r.interval)
	then := time.Unix(0, nanos)
	return time.Until(then)
}

// waitTill waits for the given time duration while honoring context expirations.
func (r *rateLimiter) waitTill(ctx context.Context, waitTime time.Duration) {
	if waitTime <= 0 {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(waitTime):
		return
	}
}
