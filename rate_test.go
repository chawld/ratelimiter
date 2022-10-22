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

	startTime := time.Now()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < int(limit); j++ {
				r.Wait(ctx)
				t.Logf("%v> [%v, %v]", time.Now(), i, j)
			}
		}(i)
	}
	time.Sleep(time.Second)
	require.Greater(t, r.GetCurrentUsage(), int64(0))
	require.LessOrEqual(t, r.GetCurrentUsage(), limit)

	time.Sleep(10 * time.Second)
	require.Equal(t, r.GetCurrentUsage(), int64(0))
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				r.Wait(ctx)
				t.Logf("%v> [%v, %v]", time.Now(), i, j)
			}
		}(i)
	}
	time.Sleep(time.Second)
	require.Greater(t, r.GetCurrentUsage(), int64(0))
	require.LessOrEqual(t, r.GetCurrentUsage(), limit)

	wg.Wait()
	require.GreaterOrEqual(t, time.Since(startTime), 14*time.Second)
}

func TestRateLimiterContextCancel(t *testing.T) {
	r := New(10, time.Second)

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				r.Wait(ctx)
				if ctx.Err() != nil {
					return
				}
				t.Logf("%v> [%v, %v]", time.Now(), i, j)
			}
		}(i)
	}
	wg.Wait()
	require.GreaterOrEqual(t, time.Since(startTime), 2*time.Second)
	require.LessOrEqual(t, time.Since(startTime), 3*time.Second)
}

func TestGetCurrentUsage(t *testing.T) {
	limit := int64(10)
	r := New(limit, time.Second)
	ctx := context.Background()
	for i := int64(0); i < 2*limit; i++ {
		r.Wait(ctx)
		u := r.GetCurrentUsage()
		t.Logf("Usage: %v", u)
		require.GreaterOrEqual(t, u, int64(0))
		require.LessOrEqual(t, u, limit)
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
	t.Logf("Usage: %v", u)
	require.Equal(t, u, limit)
	wg.Wait()
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

	startTime := time.Now()
	var wg sync.WaitGroup
	for i := int64(0); i < limit; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			r.WaitN(ctx, i+1)
			t.Logf("%v> %v", time.Now(), i+1)
		}(i)
	}
	wg.Wait()
	// worst case ~= (10 + 8 + 7 + ... + 1) / 10 = ((10 * 11)/2) / 10 = 5.5
	// best case ~= (1 + 2 + ... + 9) / 10 = 4.5
	require.Greater(t, time.Since(startTime), 4*time.Second)
}
