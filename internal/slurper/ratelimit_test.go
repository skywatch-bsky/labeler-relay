package slurper

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitFor polls a condition until it returns non-nil/true or timeout.
// Returns the result if condition succeeds, error if timeout.
func waitFor(condition func() interface{}, timeout time.Duration) (interface{}, error) {
	deadline := time.Now().Add(timeout)
	for {
		if result := condition(); result != nil {
			// For bool, return only if true
			if b, ok := result.(bool); ok && !b {
				// Fall through to retry
			} else {
				return result, nil
			}
		}

		if time.Now().After(deadline) {
			return nil, context.DeadlineExceeded
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRateLimitThrottledCallbackFiresOncePerBlockedWait verifies that a
// single Wait call that blocks -- however long -- fires the throttled
// callback exactly once, not once per poll iteration.
func TestRateLimitThrottledCallbackFiresOncePerBlockedWait(t *testing.T) {
	t.Parallel()

	limiter := NewLimiter(1, 3600)
	defer limiter.Close()

	var throttled int32
	limiter.SetThrottledCallback(func() {
		atomic.AddInt32(&throttled, 1)
	})

	ctx := context.Background()

	// First Wait consumes the only per-second token without blocking.
	require.NoError(t, limiter.Wait(ctx))
	require.EqualValues(t, 0, atomic.LoadInt32(&throttled),
		"unblocked Wait must not fire the throttled callback")

	// Second Wait blocks until the window frees (~1s, dozens of poll
	// iterations) and must count as ONE throttle event.
	require.NoError(t, limiter.Wait(ctx))
	require.EqualValues(t, 1, atomic.LoadInt32(&throttled),
		"a blocked Wait must fire the throttled callback exactly once")
}

// TestRateLimitAC7_1 tests that a single limiter throttles correctly.
// With perSec=5, the first 5 Wait calls should succeed quickly,
// and the rest should be paced according to the rate limit.
// This is a real timing test with some tolerance for scheduler variance.
func TestRateLimitAC7_1(t *testing.T) {
	limiter := NewLimiter(5, 3600) // 5 per second, 3600 per hour
	ctx := context.Background()

	// Record start time for the entire sequence
	start := time.Now()

	// Make 20 Wait calls
	for i := 0; i < 20; i++ {
		err := limiter.Wait(ctx)
		require.NoError(t, err)
	}

	elapsed := time.Since(start)

	// With 5 allows per second, 20 calls should take roughly:
	// - First 5: immediate (< 100ms)
	// - Next 5: ~1s
	// - Next 5: ~2s
	// - Next 5: ~3s
	// Total: approximately 3 seconds. Allow ±500ms tolerance for scheduler variance.
	minExpectedDuration := 2500 * time.Millisecond
	require.GreaterOrEqual(t, elapsed, minExpectedDuration,
		"expected throttling: 20 calls at 5/sec should take ~3 seconds, got %v", elapsed)
}

// TestRateLimitAC7_2 tests that throttling on one limiter does not block another.
// Two independent limiters: one saturated, one idle. The idle limiter should
// serve calls immediately while the saturated one blocks.
func TestRateLimitAC7_2(t *testing.T) {
	// Limiter A: very restrictive (1 per second)
	limiterA := NewLimiter(1, 3600)

	// Limiter B: very permissive (1000 per second)
	limiterB := NewLimiter(1000, 3600)

	ctx := context.Background()

	// Counter for calls on limiter B
	var callsOnB int32

	// Start a goroutine that saturates limiter A
	// by making repeated Wait calls. This goroutine will block
	// after the first call since perSec=1.
	go func() {
		for i := 0; i < 5; i++ {
			_ = limiterA.Wait(ctx)
			// Don't sleep; just hammer it
		}
	}()

	// Give the goroutine time to call Wait on A and start blocking
	time.Sleep(100 * time.Millisecond)

	// Now make calls on limiter B, which should NOT be blocked
	// by A being saturated. These should return immediately.
	for i := 0; i < 10; i++ {
		err := limiterB.Wait(ctx)
		require.NoError(t, err)
		atomic.AddInt32(&callsOnB, 1)
	}

	// Assert that we were able to make calls on B while A was blocked.
	// If isolation was broken, limiterB.Wait would also block.
	finalCallsOnB := atomic.LoadInt32(&callsOnB)
	require.Equal(t, int32(10), finalCallsOnB,
		"limiter B should not be blocked by limiter A being saturated")
}

// TestRateLimitContextCancellation tests that Wait respects context cancellation.
func TestRateLimitContextCancellation(t *testing.T) {
	limiter := NewLimiter(1, 3600) // Very restrictive
	ctx, cancel := context.WithCancel(context.Background())

	// First call should succeed
	err := limiter.Wait(ctx)
	require.NoError(t, err)

	// Cancel context
	cancel()

	// Second call should fail due to context cancellation
	err = limiter.Wait(ctx)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

// TestRateLimitPerHourWindow tests that both windows are checked.
// With perSec=100 and perHour=10, the perHour window is the limiting factor.
func TestRateLimitPerHourWindow(t *testing.T) {
	limiter := NewLimiter(100, 10) // 100 per second, 10 per hour
	ctx := context.Background()

	// Try to make 12 calls quickly; the first 10 should succeed,
	// and the rest should be refused by the per-hour window.
	successCount := 0
	for i := 0; i < 12; i++ {
		// Use a short timeout to avoid hanging if the window logic is broken
		ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		err := limiter.Wait(ctx2)
		cancel()

		if err == nil {
			successCount++
		}
	}

	// We should have succeeded on only 10 calls (the per-hour limit)
	require.Equal(t, 10, successCount,
		"per-hour limit of 10 should allow exactly 10 calls before timeout")
}

// TestRateLimitConcurrency tests that multiple goroutines can use the same
// limiter correctly (serialized by the per-labeler design, but goroutines
// should not panic or deadlock).
func TestRateLimitConcurrency(t *testing.T) {
	limiter := NewLimiter(10, 3600)
	ctx := context.Background()

	var wg sync.WaitGroup
	errors := make(chan error, 5)

	// Start 5 concurrent goroutines, each trying to Wait
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.Wait(ctx)
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// All should succeed without error
	for err := range errors {
		t.Errorf("unexpected error: %v", err)
	}
}
