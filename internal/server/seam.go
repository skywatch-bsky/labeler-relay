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
//  4. Stop the drainer and read remaining stragglers from the DB in bounded
//     chunks until the DB returns 0 rows again. If the consumer can't keep up,
//     the Hub drops the subscription (ConsumerTooSlow) and Phase 4 exits.
//  5. Pure live with dedup: deliver events from the live channel, dropping any
//     already sent during backfill (relay_seq <= last DB seq).
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

		// Phase 3: Straggler read + pure live with dedup.
		//
		// After stopping the drainer, some events may have been persisted
		// between the last Phase 2 empty query and the drainer stop. Those
		// were drained from live (not in the live channel) but ARE in the DB.
		// Read them in bounded chunks until the DB is drained again.
		//
		// This loop terminates because:
		// - If the consumer is keeping up, out doesn't block and the DB
		//   drains quickly (bounded chunks, fast SQLite reads).
		// - If the consumer can't keep up, out blocks, the live channel fills,
		//   the Hub drops the subscription (ConsumerTooSlow), and Phase 4
		//   detects the closed live channel and exits.
		// - If cleanup is called, streamCtx is cancelled and we exit.
		//
		// Events that arrive during this loop go to the live channel. They'll
		// be delivered by Phase 4 with dedup (e.RelaySeq <= lastBackfill).
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

		// Phase 4: Pure live with dedup — drop any live events already sent
		// during backfill (their relay_seq <= cursor, the last DB seq delivered).
		// Events that arrived after the drainer stopped are in the live channel
		// and will be delivered here. The dedup boundary ensures exactly-once.
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
