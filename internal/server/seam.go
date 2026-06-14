// pattern: Imperative Shell
// StreamFrom composes durable Playback + live Hub into a single stream with
// no gap and no duplicate at the seam. Backfill is chunked so the live Hub
// subscription only needs to buffer events during the final chunk, not the
// entire backfill duration.

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
//  3. Once within bufSize of head, stop the drainer and do the final seam:
//     drain remaining backfill from the DB, then switch to live with the
//     dedup boundary dropping any events already sent during backfill.
//
// This bounds the live buffer requirement: during bulk backfill the drainer
// prevents overflow, and during the final seam only one chunk's worth of
// events can accumulate.
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

		// Stop the drainer before entering Phase 2 so we can read live events.
		close(stopDrain)
		<-drainDone

		if ctx.Err() != nil {
			return
		}

		// Phase 2: Final seam — drain remaining backfill from the DB, then
		// switch to live with dedup. The live channel may already contain
		// events from during the chunk phase; the dedup boundary handles them.
		var lastBackfill int64 = cursor

		err := p.PlaybackFrames(ctx, cursor, func(e store.LiveEvent) error {
			select {
			case out <- e:
				lastBackfill = e.RelaySeq
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil && ctx.Err() == nil {
			return
		}

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
