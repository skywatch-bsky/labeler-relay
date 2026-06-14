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
//  1. Drain the backlog in chunks of bufSize events from the durable store,
//     sending each directly to the output channel. No live subscription is
//     active during this phase, so there is no buffer pressure from live events.
//  2. After each chunk, check how far behind head we are. If the remaining gap
//     is <= bufSize, we are close enough to attach live safely.
//  3. For the final seam: subscribe to live BEFORE draining the last chunk,
//     exactly like the original algorithm. The dedup boundary drops any live
//     events already sent during this final playback.
//
// This bounds the live buffer requirement to at most one chunk's worth of live
// events, regardless of total backfill size.
func StreamFrom(ctx context.Context, p *store.LabelPersist, hub *Hub, since int64, bufSize int) (<-chan store.LiveEvent, func(), error) {
	out := make(chan store.LiveEvent, bufSize)

	innerCtx, innerCancel := context.WithCancel(ctx)

	go func() {
		defer close(out)

		cursor := since

		// Phase 1: Chunked backfill — drain in bufSize-sized chunks without a
		// live subscription. This is the key change: no live buffer pressure
		// during the bulk of the backfill.
		for {
			if innerCtx.Err() != nil {
				return
			}

			head, err := p.Head(innerCtx)
			if err != nil || innerCtx.Err() != nil {
				return
			}

			gap := head - cursor
			if gap <= int64(bufSize) {
				break
			}

			n, err := p.PlaybackFramesChunk(innerCtx, cursor, bufSize, func(e store.LiveEvent) error {
				select {
				case out <- e:
					cursor = e.RelaySeq
					return nil
				case <-innerCtx.Done():
					return innerCtx.Err()
				}
			})
			if err != nil && innerCtx.Err() == nil {
				return
			}
			if n == 0 {
				break
			}
		}

		if innerCtx.Err() != nil {
			return
		}

		// Phase 2: Final seam — subscribe to live BEFORE draining the last
		// chunk, then dedup at the boundary. Same logic as the original
		// StreamFrom: this is the only window where live events buffer.
		live, liveCancel := hub.Subscribe(bufSize)
		defer liveCancel()

		var lastBackfill int64 = cursor

		err := p.PlaybackFrames(innerCtx, cursor, func(e store.LiveEvent) error {
			select {
			case out <- e:
				lastBackfill = e.RelaySeq
				return nil
			case <-innerCtx.Done():
				return innerCtx.Err()
			}
		})
		if err != nil && innerCtx.Err() == nil {
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
				case <-innerCtx.Done():
					return
				}
			case <-innerCtx.Done():
				return
			}
		}
	}()

	cleanup := func() {
		innerCancel()
		for range out {
		}
	}

	return out, cleanup, nil
}
