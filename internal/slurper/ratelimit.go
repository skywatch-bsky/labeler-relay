// pattern: Imperative Shell

package slurper

import (
	"context"
	"time"

	"github.com/RussellLuo/slidingwindow"
)

// Limiter wraps github.com/RussellLuo/slidingwindow to provide per-labeler
// rate limiting. Each labeler owns its own Limiter instance, so throttling
// is isolated per goroutine (AC7.2).
type Limiter struct {
	perSecond   *slidingwindow.Limiter
	perSecStop  slidingwindow.StopFunc
	perHour     *slidingwindow.Limiter
	perHourStop slidingwindow.StopFunc
	onThrottled func() // called when Wait has to block; may be nil
}

// NewLimiter creates a new rate limiter with the given per-second and per-hour
// token budgets.
func NewLimiter(perSec, perHour int) *Limiter {
	// Create a wrapper function that matches the NewWindow signature
	newWindow := func() (slidingwindow.Window, slidingwindow.StopFunc) {
		return slidingwindow.NewLocalWindow()
	}

	perSecLimiter, perSecStop := slidingwindow.NewLimiter(
		time.Duration(1)*time.Second,
		int64(perSec),
		newWindow,
	)
	perHourLimiter, perHourStop := slidingwindow.NewLimiter(
		time.Duration(1)*time.Hour,
		int64(perHour),
		newWindow,
	)

	return &Limiter{
		perSecond:   perSecLimiter,
		perSecStop:  perSecStop,
		perHour:     perHourLimiter,
		perHourStop: perHourStop,
	}
}

// SetThrottledCallback registers a function called whenever Wait has to block
// due to rate limiting. Used to increment Prometheus counters without importing
// the metrics package from slurper (FCIS: callback injection).
func (l *Limiter) SetThrottledCallback(fn func()) {
	l.onThrottled = fn
}

// Wait blocks until a token is available across both windows or ctx is done.
// It polls the limiters with a short backoff until both Allow() return true
// or the context is cancelled. This blocks only the calling goroutine,
// never a shared resource.
func (l *Limiter) Wait(ctx context.Context) error {
	for {
		// Check if both windows allow a token
		if l.perSecond.Allow() && l.perHour.Allow() {
			return nil
		}

		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Notify observer that throttling occurred (AC7.1 observability).
		if l.onThrottled != nil {
			l.onThrottled()
		}

		// Short backoff before retry
		select {
		case <-time.After(10 * time.Millisecond):
			// Retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
