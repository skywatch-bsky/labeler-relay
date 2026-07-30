// pattern: Imperative Shell
// StreamFrom composes durable Playback + live Hub into a single stream with
// no gap and no duplicate at the seam. Backfill is chunked: a background
// drainer protects the live channel during bulk backfill, then bounded
// chunked reads handle the final seam without unbounded DB queries.

package server

import (
	"context"

	"github.com/scarndp/labeler-relay/internal/store"
)

// StreamFrom delivers events with relay_seq > since in ascending order,
// initially from the durable store via chunked playback, then switching to live
// events from the Hub. It returns a channel of LiveEvent and a cleanup func.
//
// Algorithm (chunked backfill):
//  1. Subscribe to live BEFORE returning (synchronous) — same as the original.
//     This guarantees no events are missed after StreamFrom returns.
//  2. Drain the backlog in chunks of bufSize events from the durable store.
//     During this phase a background drainer keeps the live channel from
//     overflowing by discarding buffered events (they'll be read from the DB).
//  3. Once within bufSize of head, continue with bounded chunked reads (still
//     with the drainer running) until the DB returns 0 rows (fully drained).
//  4. Stop the drainer and do a final straggler read loop: bounded chunks
//     without the drainer until the DB returns 0 rows again. This catches
//     events that arrived between the last Phase 2 query and the drainer stop
//     (drained from live, so only in the DB). If the consumer is too slow,
//     the Hub closes live and Phase 3 exits cleanly.
//  5. Switch to live with the dedup boundary dropping any events already sent
//     during backfill.
//
// An internal cancellable context (streamCtx) ensures cleanup can terminate
// all loops even if the caller's context is still alive.
func StreamFrom(ctx context.Context, p *store.LabelPersist, hub *Hub, since int64, bufSize int) (<-chan store.LiveEvent, func(), error) {
	// Internal cancellable context so cleanup can signal the goroutine to
	// stop even if the caller's ctx is still alive (e.g., the HTTP handler
	// returns early but the request context hasn't been cancelled yet).
	streamCtx, streamCancel := context.WithCancel(ctx)

	// Subscribe to live synchronously — captures all events from this moment.
	live, liveCancel := hub.Subscribe(bufSize)

	out := make(chan store.LiveEvent, bufSize)

	go func() {
		defer close(out)
		defer liveCancel()
		defer streamCancel()

		cursor := since

		// Phase 1: Chunked backfill. A background goroutine drains the live
		// channel to prevent the Hub from dropping us while we read from the DB.
		drainDone := make(chan struct{})
		stopDrain := make(chan struct{})
		go func() {
			defer close(drainDone)
			for {
				select {
				case _, ok := <-live:
					if !ok {
						return
					}
				case <-stopDrain:
					return
				case <-streamCtx.Done():
					return
				}
			}
		}()

		for {
			if streamCtx.Err() != nil {
				close(stopDrain)
				<-drainDone
				return
			}

			head, err := p.Head(streamCtx)
			if err != nil || streamCtx.Err() != nil {
				close(stopDrain)
				<-drainDone
				return
			}

			gap := head - cursor
			if gap <= int64(bufSize) {
				break
			}

			n, err := p.PlaybackFramesChunk(streamCtx, cursor, bufSize, func(e store.LiveEvent) error {
				select {
				case out <- e:
					cursor = e.RelaySeq
					return nil
				case <-streamCtx.Done():
					return streamCtx.Err()
				}
			})
			if err != nil && streamCtx.Err() == nil {
				close(stopDrain)
				<-drainDone
				return
			}
			if n == 0 {
				break
			}
		}

		// Phase 2: Final seam — continue draining the DB in bounded chunks
		// with the drainer STILL RUNNING. We break when a chunk returns
		// 0 rows, meaning the DB is fully drained at that moment.
		for {
			if streamCtx.Err() != nil {
				close(stopDrain)
				<-drainDone
				return
			}

			n, err := p.PlaybackFramesChunk(streamCtx, cursor, bufSize, func(e store.LiveEvent) error {
				select {
				case out <- e:
					cursor = e.RelaySeq
					return nil
				case <-streamCtx.Done():
					return streamCtx.Err()
				}
			})
			if err != nil && streamCtx.Err() == nil {
				close(stopDrain)
				<-drainDone
				return
			}
			if n == 0 {
				break // DB drained — no more events at this moment
			}
		}

		// Stop the drainer. After this, events arriving on the live channel
		// accumulate in the live buffer (capacity bufSize).
		close(stopDrain)
		<-drainDone

		if streamCtx.Err() != nil {
			return
		}

		// Straggler read: events may have been persisted between the last
		// PlaybackFramesChunk query (which returned 0) and the drainer stop.
		// Those events were drained from the live channel by the drainer
		// (so they're NOT in live) but ARE in the DB. Read them now in
		// bounded chunks until the DB is drained again.
		//
		// This loop is safe without the drainer because:
		// - Each chunk is bounded (bufSize), so out can only block briefly.
		// - If the consumer is too slow and the Hub drops the subscription,
		//   live is closed — Phase 3 will detect this and exit. The events
		//   already delivered to out from the DB are still valid and ordered.
		// - If cleanup is called, streamCtx is cancelled and we exit.
		// - The loop terminates when the DB returns 0 rows (no stragglers).
		for {
			if streamCtx.Err() != nil {
				return
			}

			n, err := p.PlaybackFramesChunk(streamCtx, cursor, bufSize, func(e store.LiveEvent) error {
				select {
				case out <- e:
					cursor = e.RelaySeq
					return nil
				case <-streamCtx.Done():
					return streamCtx.Err()
				}
			})
			if err != nil && streamCtx.Err() == nil {
				return
			}
			if n == 0 {
				break // DB fully drained — no stragglers
			}
		}

		// Phase 3: Live with dedup — drop any live events already sent during
		// backfill (their relay_seq <= cursor). Events that arrived after the
		// drainer stopped are in the live channel and will be delivered here.
		lastBackfill := cursor
		for {
			select {
			case e, ok := <-live:
				if !ok {
					return
				}
				if e.RelaySeq <= lastBackfill {
					continue
				}
				select {
				case out <- e:
				case <-streamCtx.Done():
					return
				}
			case <-streamCtx.Done():
				return
			}
		}
	}()

	cleanup := func() {
		streamCancel()
		liveCancel()
		for range out {
		}
	}

	return out, cleanup, nil
}
