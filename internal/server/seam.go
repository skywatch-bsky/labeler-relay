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
//     with the drainer running) until the DB is drained. Then stop the drainer
//     and do one final straggler read to catch events that arrived between the
//     last chunk query and the drainer stop (these were drained from live, so
//     they're only in the DB). This replaces the original unbounded
//     PlaybackFrames call, which could overflow the live channel under
//     sustained ingest.
//  4. Switch to live with the dedup boundary dropping any events already sent
//     during backfill.
//
// This bounds the live buffer requirement: the drainer prevents overflow
// throughout all DB-reading phases, and the live phase starts only after the
// DB is fully drained.
func StreamFrom(ctx context.Context, p *store.LabelPersist, hub *Hub, since int64, bufSize int) (<-chan store.LiveEvent, func(), error) {
	// Subscribe to live synchronously — captures all events from this moment.
	live, liveCancel := hub.Subscribe(bufSize)

	out := make(chan store.LiveEvent, bufSize)

	go func() {
		defer close(out)
		defer liveCancel()

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
				}
			}
		}()

		for {
			if ctx.Err() != nil {
				close(stopDrain)
				<-drainDone
				return
			}

			head, err := p.Head(ctx)
			if err != nil || ctx.Err() != nil {
				close(stopDrain)
				<-drainDone
				return
			}

			gap := head - cursor
			if gap <= int64(bufSize) {
				break
			}

			n, err := p.PlaybackFramesChunk(ctx, cursor, bufSize, func(e store.LiveEvent) error {
				select {
				case out <- e:
					cursor = e.RelaySeq
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if err != nil && ctx.Err() == nil {
				close(stopDrain)
				<-drainDone
				return
			}
			if n == 0 {
				break
			}
		}

		// Phase 2: Final seam — continue draining the DB in bounded chunks
		// with the drainer STILL RUNNING. This is critical: if we stopped the
		// drainer here and used unbounded PlaybackFrames (as the original code
		// did), a slow consumer could cause out to block, the live channel
		// would fill up with no drainer to absorb it, and the Hub would drop
		// the subscription — permanently losing events.
		//
		// By keeping the drainer running and using bounded chunks, we get the
		// same overflow protection as Phase 1. We break when a chunk returns
		// fewer than bufSize events, meaning the DB is drained (for now).
		for {
			if ctx.Err() != nil {
				close(stopDrain)
				<-drainDone
				return
			}

			n, err := p.PlaybackFramesChunk(ctx, cursor, bufSize, func(e store.LiveEvent) error {
				select {
				case out <- e:
					cursor = e.RelaySeq
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if err != nil && ctx.Err() == nil {
				close(stopDrain)
				<-drainDone
				return
			}
			if n < bufSize {
				break // DB drained — fewer than a full chunk means we're caught up
			}
		}

		// Stop the drainer before entering the live phase.
		close(stopDrain)
		<-drainDone

		if ctx.Err() != nil {
			return
		}

		// Straggler read: events may have been persisted between the last
		// PlaybackFramesChunk query and the drainer stop. Those events were
		// drained from the live channel by the drainer (so they're NOT in live)
		// but ARE in the DB. Read them now with a bounded chunk.
		for {
			if ctx.Err() != nil {
				return
			}

			n, err := p.PlaybackFramesChunk(ctx, cursor, bufSize, func(e store.LiveEvent) error {
				select {
				case out <- e:
					cursor = e.RelaySeq
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if err != nil && ctx.Err() == nil {
				return
			}
			if n == 0 {
				break // no stragglers — DB fully drained
			}
		}

		// Phase 3: Live with dedup — drop any live events already sent during
		// backfill (their relay_seq <= cursor). Events that arrived during
		// backfill were drained by the drainer and re-read from the DB above;
		// events that arrived after the drainer stopped are in the live channel
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
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	cleanup := func() {
		liveCancel()
		for range out {
		}
	}

	return out, cleanup, nil
}
