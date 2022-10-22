// This file implements a lock-less thread-safe rate limiter with limited functionality
package ratelimiter

import (
	"context"
	"sync/atomic"
	"time"
)

type RateLimiter interface {
	Wait(ctx context.Context)
	WaitN(ctx context.Context, n int64)
	GetCurrentUsage() int64
}

func New(tokens int64, interval time.Duration) RateLimiter {
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
	r.WaitN(ctx, 1)
}

// allow returns true if the caller is allowed to perform the rate-limited operation,
// false otherwise.
func (r *rateLimiter) allow() bool {
	return r.allowN(1)
}

// waitN waits until the caller is allowed to perform the rate-limited operation.
func (r *rateLimiter) WaitN(ctx context.Context, n int64) {
	var waitTime time.Duration
	done := false
	for !done {
		select {
		case <-ctx.Done():
			return
		default:
			oldToken, newToken := r.getNextToken(n)
			if atomic.CompareAndSwapInt64(&r.token, oldToken, newToken) {
				waitTime = r.timeFromToken(newToken - n)
				done = true
			}
		}
	}
	r.waitTill(ctx, waitTime)
}

// allowN returns true if the caller is allowed to perform the rate-limited operation,
// false otherwise.
func (r *rateLimiter) allowN(n int64) bool {
	for {
		oldToken, newToken := r.getNextToken(n)
		if r.timeFromToken(newToken-n) > time.Duration(0) {
			// will have to wait :(
			return false
		}
		if atomic.CompareAndSwapInt64(&r.token, oldToken, newToken) {
			return true
		}
	}
}

// getNextToken returns the current token and the next available token.
func (r *rateLimiter) getNextToken(n int64) (int64, int64) {
	oldToken := atomic.LoadInt64(&r.token)
	now := time.Now().UnixNano()
	newToken := int64(float64(now)/r.timeUnit) + n
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

// GetCurrentUsage returns the number of calls in the current window. The value returned
// is between 0 and <limit>.
func (r *rateLimiter) GetCurrentUsage() int64 {
	curToken := atomic.LoadInt64(&r.token)
	delta := time.Unix(0, curToken*int64(r.timeUnit)).Sub(time.Now())
	if delta > r.interval {
		// requests overflowed into next interval
		// Not entirely true.. doesn't factor in token jumps!
		delta = r.interval
	} else if delta <= 0 {
		// no pending requests
		return 0
	}
	return int64(float64(delta) / r.timeUnit)
}
