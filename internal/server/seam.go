// pattern: Imperative Shell
// StreamFrom composes durable Playback + live Hub into a single stream with
// no gap and no duplicate at the seam. Both backfill and live are keyed on
// the monotonic relay seq; the seam dedup boundary filters live events that
// were already sent during backfill.

package server

import (
	"context"

	"github.com/scarndp/labeler-relay/internal/store"
)

// StreamFrom delivers events with relay_seq > since in ascending order,
// initially from the durable store via Playback, then switching to live
// events from the Hub. It returns a channel of LiveEvent and a cleanup func.
//
// Algorithm (load-bearing order):
// 1. Subscribe to live BEFORE draining backfill to close the gap window.
// 2. Spawn a goroutine that:
//    a. Drain Playback(ctx, since, ...) in ascending relay_seq.
//    b. Track lastBackfill = the highest relay_seq sent during backfill.
//    c. Then range over live: DROP any e.RelaySeq <= lastBackfill (already sent).
//    d. Forward the rest to out.
//    e. On ctx-done or live-closed (slow drop), close out and cancel live sub.
// 3. Return out and a cleanup that cancels live and drains out.
//
// Because both backfill and live are ordered by the SAME monotonic relay seq,
// and live events broadcast in seq order (Phase 2 Task 4: under persist mu),
// the dedup boundary guarantees exactly-once delivery across the seam.
func StreamFrom(ctx context.Context, p *store.LabelPersist, hub *Hub, since int64, bufSize int) (<-chan store.LiveEvent, func(), error) {
	// Step 1: Subscribe to live BEFORE draining backfill.
	// This captures all events from this moment onward (after head at subscribe time).
	live, liveCancel := hub.Subscribe(bufSize)

	// out: channel for the caller to consume from.
	out := make(chan store.LiveEvent, bufSize)

	// Spawn the backfill→live stitcher goroutine.
	go func() {
		defer close(out)
		defer liveCancel()

		var lastBackfill int64 = since

		// Step 2a: Drain Playback in ascending order.
		err := p.PlaybackFrames(ctx, since, func(e store.LiveEvent) error {
			select {
			case out <- e:
				// Update lastBackfill to track how far we've sent.
				lastBackfill = e.RelaySeq
				return nil
			case <-ctx.Done():
				// Context cancelled; stop draining and let cleanup close out.
				return ctx.Err()
			}
		})
		if err != nil && ctx.Err() == nil {
			// PlaybackFrames failed (not due to ctx cancel).
			// Just exit and let out close.
			return
		}

		// Step 2b: Switch to live feed.
		// Range over live until it closes (slow drop) or ctx done.
		for {
			select {
			case e, ok := <-live:
				if !ok {
					// Live closed due to slow drop; exit and close out.
					return
				}
				// Step 2c: Dedup boundary — drop any e.RelaySeq <= lastBackfill.
				// These were already sent during backfill.
				if e.RelaySeq <= lastBackfill {
					// Already sent; skip.
					continue
				}
				// Step 2d: Forward to out.
				select {
				case out <- e:
					// sent
				case <-ctx.Done():
					// Context cancelled; exit and let cleanup close out.
					return
				}
			case <-ctx.Done():
				// Context cancelled; exit and let cleanup close out.
				return
			}
		}
	}()

	// Cleanup: cancel live subscription and drain out channel.
	cleanup := func() {
		liveCancel()
		// Drain out to unblock the goroutine if it's waiting on a send.
		for range out {
		}
	}

	return out, cleanup, nil
}
