// This file implements a lock-less thread-safe rate limiter with limited functionality
package ratelimiter

import (
	"context"
	"sync/atomic"
	"time"
	"unsafe"
)

type RateLimiter interface {
	// Wait waits until the caller is allowed to perform the rate-limited operation
	// that requires one token.
	Wait(ctx context.Context)

	// WaitN waits until the caller is allowed to perform the rate-limited operation
	// that requires 'n' tokens.
	WaitN(ctx context.Context, n int64)

	// GetUsage returns the number of allocated tokens, the current token, and the
	// number of tokens unused.
	GetUsage() (int64, int64, int64)

	// Deprecated: Use GetUsage instead.
	GetCurrentUsage() int64
}

func New(tokens int64, interval time.Duration) RateLimiter {
	return &rateLimiter{
		timeUnit: float64(interval) / float64(tokens),
		interval: int64(interval),
		limit:    tokens,
	}
}

// rateLimiter is a lockless thread-safe implementation of the RateLimiter
//   - It is thread-safe
//   - It is fair
//   - It supports a wait API
//   - It supports contexts and honors context cancellations
//
// Non-features
//   - Rate requests once granted can't be cancelled and will impact future observed throughputs
//   - Doesn't support bursts larger than the rate
//   - Doesn't allow changing the rate after initial setup
type rateLimiter struct {
	token      *tokenInfo
	timeUnit   float64 // number of tokens per unit of time (ns)
	interval   int64   // granularity
	limit      int64   // max number of tokens to allocate per interval
	startToken int64
}

type tokenInfo struct {
	token         int64 // monotonically increasing token
	skippedTokens int64 // total number of tokens skipped
}

func (t *tokenInfo) getToken() int64 {
	if t == nil {
		return 0
	}
	return t.token
}

func (t *tokenInfo) getSkippedTokens() int64 {
	if t == nil {
		return 0
	}
	return t.skippedTokens
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

// WaitN waits until the caller is allowed to perform the rate-limited operation.
func (r *rateLimiter) WaitN(ctx context.Context, n int64) {
	var waitTime time.Duration
	done := false
	for !done {
		select {
		case <-ctx.Done():
			return
		default:
			oldToken, newToken := r.getNextToken(n)
			done, waitTime = r.tryUpdate(oldToken, newToken, n)
		}
	}
	r.waitTill(ctx, waitTime)
}

// allowN returns true if the caller is allowed to perform the rate-limited operation,
// false otherwise.
func (r *rateLimiter) allowN(n int64) bool {
	for {
		oldToken, newToken := r.getNextToken(n)
		if r.timeFromToken((*tokenInfo)(newToken).token-n) > time.Duration(0) {
			// will have to wait :(
			return false
		}
		done, _ := r.tryUpdate(oldToken, newToken, n)
		if done {
			return true
		}
	}
}

// tryUpdate tries to update the token and returns true if it succeeds, along with the wait time,
func (r *rateLimiter) tryUpdate(oldToken, newToken unsafe.Pointer, n int64) (bool, time.Duration) {
	upp := (*unsafe.Pointer)(unsafe.Pointer(&r.token))
	if atomic.CompareAndSwapPointer(upp, oldToken, newToken) {
		ot := (*tokenInfo)(oldToken)
		nt := (*tokenInfo)(newToken)
		if ot.getToken() == 0 {
			// this is the first token being handed out,
			// set the start token. (see comment in
			// GetUsage() below.
			atomic.StoreInt64(&r.startToken, nt.skippedTokens)
		}
		return true, r.timeFromToken(nt.token - n)
	}
	return false, 0
}

func (r *rateLimiter) startOfInterval(t int64) int64 {
	return (t / r.interval) * r.interval
}

func (r *rateLimiter) endOfInterval(t int64) int64 {
	return r.startOfInterval(t) + r.interval
}

// getNextToken returns the current token and the next available token.
func (r *rateLimiter) getNextToken(n int64) (unsafe.Pointer, unsafe.Pointer) {
	upp := (*unsafe.Pointer)(unsafe.Pointer(&r.token))
	oldTokenPtr := (*tokenInfo)(atomic.LoadPointer(upp))

	oldToken := oldTokenPtr.getToken()
	minToken := r.tokenAt(r.startOfInterval(time.Now().UnixNano()))
	newToken := minToken + n
	skippedTokens := oldTokenPtr.getSkippedTokens()
	if newToken < oldToken+n {
		newToken = oldToken + n
		skippedTokens = oldTokenPtr.getSkippedTokens()
	} else {
		skippedTokens += (newToken - n) - oldToken
	}
	return unsafe.Pointer(oldTokenPtr), unsafe.Pointer(&tokenInfo{newToken, skippedTokens})
}

func (r *rateLimiter) tokenAt(atNs int64) int64 {
	return int64(float64(atNs) / r.timeUnit)
}

// timeFromToken returns the duration after which the token can be used. If the duration
// is not a positive value, the token can be used immediately.
func (r *rateLimiter) timeFromToken(token int64) time.Duration {
	nanos := (token * int64(r.timeUnit) / r.interval) * r.interval
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
// Deprecated: Use GetUsage instead.
func (r *rateLimiter) GetCurrentUsage() int64 {
	upp := (*unsafe.Pointer)(unsafe.Pointer(&r.token))
	curTokenPtr := (*tokenInfo)(atomic.LoadPointer(upp))
	curToken := curTokenPtr.getToken()
	now := time.Now().UnixNano()
	intStart := r.startOfInterval(now)
	delta := curToken*int64(r.timeUnit) - intStart
	u := delta / int64(r.timeUnit)
	if u < 0 {
		return 0
	}
	if u > r.limit {
		return r.limit
	}
	return u
}

// GetUsage returns the number of allocated tokens, the current token, and the number of tokens unused.
func (r *rateLimiter) GetUsage() (int64, int64, int64) {
	for {
		startToken := atomic.LoadInt64(&r.startToken)
		oldTokenPtr, newTokenPtr := r.getNextToken(0)
		ot := (*tokenInfo)(oldTokenPtr)
		nt := (*tokenInfo)(newTokenPtr)
		if ot.getToken() == 0 {
			startToken = nt.skippedTokens
		}
		currentToken := r.tokenAt(time.Now().UnixNano())
		if currentToken > nt.token {
			currentToken = nt.token
		}
		return nt.token - nt.skippedTokens, currentToken - startToken, nt.skippedTokens - startToken
	}
}
