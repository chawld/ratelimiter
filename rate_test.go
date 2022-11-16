package ratelimiter

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRateLimiterBandwidth(t *testing.T) {
	limit := int64(10)
	r := New(limit, time.Second)
	require.Equal(t, r.GetCurrentUsage(), int64(0))

	// wait for the next interval to start
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	startTime := time.Now()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 4*int(limit); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.Wait(ctx)
		}(i)
	}
	time.Sleep(time.Second)
	require.Equal(t, limit, r.GetCurrentUsage())

	wg.Wait()
	require.Less(t, 2900*time.Millisecond, time.Since(startTime))
}

func TestRateLimiterContextCancel(t *testing.T) {
	limit := int64(10)
	r := New(10, time.Second)

	startTime := time.Now()
	timeout := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 10*int(limit); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.Wait(ctx)
		}(i)
	}
	wg.Wait()
	require.Greater(t, timeout+time.Second, time.Since(startTime))
}

func TestGetCurrentUsage(t *testing.T) {
	limit := int64(10)
	r := New(limit, time.Second)
	ctx := context.Background()
	for i := int64(0); i < 2*limit; i++ {
		r.Wait(ctx)
		exp := i%limit + 1
		require.Equal(t, exp, r.GetCurrentUsage())
	}

	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := int64(0); i < 3*limit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Wait(ctx)
		}()
	}
	time.Sleep(100 * time.Millisecond)
	u := r.GetCurrentUsage()
	require.Equal(t, limit, u)
	wg.Wait()

	limit = int64(100000)
	r = New(limit, time.Second).(*rateLimiter)
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	for i := int64(0); i < 10; i++ {
		r.Wait(ctx)
		exp := i%limit + 1
		require.Equal(t, exp, r.GetCurrentUsage())
	}
}

func BenchmarkRateLimiter(b *testing.B) {
	r := New(math.MaxInt64, time.Second)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				r.Wait(ctx)
			}
		}()
	}
	wg.Wait()
}

func TestWaitN(t *testing.T) {
	limit := int64(10)
	r := New(limit, time.Second)
	ctx := context.Background()
	// wait for the next interval
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	startTime := time.Now()
	var wg sync.WaitGroup
	for i := int64(0); i < limit; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			r.WaitN(ctx, i+1)
		}(i)
	}
	wg.Wait()
	// worst case ~= floor((10 + 8 + 7 + ... + 2) / 10) = floor(5.4) = 5
	// best case ~= floor((1 + 2 + ... + 9) / 10) = floor(4.5) = 4
	require.Less(t, 3900*time.Millisecond, time.Since(startTime))
	require.Greater(t, 5900*time.Millisecond, time.Since(startTime))
}

func TestGetUsage(t *testing.T) {
	limit := int64(10)
	r := New(limit, time.Second).(*rateLimiter)
	ctx := context.Background()

	// wait for the next interval
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	used, current, unused := r.GetUsage()
	require.EqualValues(t, 0, used)
	require.EqualValues(t, 0, current)

	for i := 0; i < int(limit-1); i++ {
		r.Wait(ctx)
	} // used 'limit-1' tokens

	used, current, unused = r.GetUsage()
	require.EqualValues(t, limit-1, used)
	require.EqualValues(t, 0, current)
	require.EqualValues(t, 0, unused)

	// wait for the next interval
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	r.Wait(ctx) // used 'limit' tokens

	used, current, unused = r.GetUsage()
	require.EqualValues(t, limit, used)
	require.EqualValues(t, limit, current)
	require.EqualValues(t, 1, unused)

	// wait for the next interval
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	r.WaitN(ctx, limit-1) // used '2*limit-1' tokens
	r.WaitN(ctx, limit+1) // used '3*limit' tokens

	used, current, unused = r.GetUsage()
	require.EqualValues(t, 3*limit, used)
	require.EqualValues(t, 2*limit, current)
	require.EqualValues(t, 1+9, unused)

	// wait for the next interval
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	used, current, unused = r.GetUsage()
	require.EqualValues(t, 3*limit, used)
	require.EqualValues(t, 3*limit, current)
	require.EqualValues(t, 1+9, unused)

	// wait for the next interval
	time.Sleep(time.Second - time.Duration(time.Now().UnixNano())%time.Second)

	used, current, unused = r.GetUsage()
	require.EqualValues(t, 3*limit, used)
	require.EqualValues(t, 4*limit, current)
	require.EqualValues(t, 1+9, unused)
}

func TestRace(t *testing.T) {
	limit := int64(100)
	r := New(limit, time.Second).(*rateLimiter)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := int64(0); i < limit*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				r.Wait(ctx)
				r.GetUsage()
			}
		}()
	}
	wg.Wait()
}
