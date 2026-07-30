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
//  4. Stop the drainer and enter an interleaved DB + live phase: read one
//     bounded chunk from the DB, then drain available live events (with dedup).
//     This catches stragglers (events drained from live but in the DB) while
//     preventing live channel overflow under sustained ingest. When the DB
//     returns < bufSize rows, switch to pure live with dedup.
//  5. Pure live with dedup: deliver events from the live channel, dropping any
//     already sent during backfill.
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

		// Phase 3: Interleaved DB + live with dedup.
		//
		// After stopping the drainer, some events may have been persisted
		// between the last Phase 2 empty query and the drainer stop. Those
		// were drained from live (not in the live channel) but ARE in the DB.
		// We must read them from the DB. But we also must drain the live
		// channel to prevent overflow under sustained ingest.
		//
		// Solution: interleave bounded DB reads with live draining. Read one
		// chunk from the DB, then drain all available live events (with dedup).
		// If the DB returned a full chunk, there may be more — loop. If the DB
		// returned < bufSize (or 0), the DB is drained and we switch to pure
		// live (Phase 4 below).
		dbDrained := false
		for {
			if streamCtx.Err() != nil {
				return
			}

			if !dbDrained {
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
				if n < bufSize {
					dbDrained = true
				}

				// Drain available live events (non-blocking) to prevent
				// overflow while we loop back for more DB reads.
				drained := true
				for drained {
					select {
					case e, ok := <-live:
						if !ok {
							return // Hub dropped the subscription
						}
						if e.RelaySeq <= cursor {
							continue // dedup: already sent from DB
						}
						select {
						case out <- e:
							cursor = e.RelaySeq
						case <-streamCtx.Done():
							return
						}
					default:
						drained = false
					}
				}
			} else {
				// Phase 4: DB fully drained — pure live with dedup.
				select {
				case e, ok := <-live:
					if !ok {
						return
					}
					if e.RelaySeq <= cursor {
						continue
					}
					select {
					case out <- e:
						cursor = e.RelaySeq
					case <-streamCtx.Done():
						return
					}
				case <-streamCtx.Done():
					return
				}
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
