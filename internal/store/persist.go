// pattern: Imperative Shell

package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
)

// CursorState represents the status of a cursor relative to the retention window.
type CursorState int

const (
	CursorOK CursorState = iota
	CursorFuture
	CursorOutdated
)

// IngestEvent is what the slurper (Phase 3) and firehose watcher (Phase 4)
// hand to PersistIngest. Exported: it is the cross-package ingest entrypoint.
type IngestEvent struct {
	Kind        string // "labels" | "service"
	LabelerDID  string
	UpstreamSeq *int64
	Labels      []*comatproto.LabelDefs_Label // for kind="labels"
	Record      *bsky.LabelerService          // for kind="service"
}

// LiveEvent is what the broadcaster delivers to live subscribers. It carries
// the relay seq explicitly so the Phase-5 seam keys on it directly — we never
// rely on indigo's XRPCStreamEvent.Sequence() (which returns -1 for labels).
type LiveEvent struct {
	RelaySeq   int64
	Kind       string
	LabelerDID string
	FrameCBOR  []byte // the stored output frame body (relay seq already embedded)
}

// LabelPersist is the serialized single writer. Its primary entrypoint is
// PersistIngest, which mints the relay seq, stores the relay-seq'd frame,
// and broadcasts to live subscribers.
type LabelPersist struct {
	store       *Store
	mu          sync.Mutex             // serializes seq minting; single-writer
	broadcaster func(LiveEvent)
}

// NewLabelPersist creates a new LabelPersist backed by the given store.
func NewLabelPersist(s *Store) *LabelPersist {
	return &LabelPersist{
		store:       s,
		broadcaster: nil,
	}
}

// SetBroadcaster registers the live event broadcaster. The broadcaster
// MUST be non-blocking: it may not stall the write path. It will be called
// while holding the seq minting mutex, so it must return immediately.
func (p *LabelPersist) SetBroadcaster(fn func(LiveEvent)) {
	p.broadcaster = fn
}

// PersistIngest mints the relay seq, stores the relay-seq'd frame body,
// and broadcasts to live subscribers. The entire body runs under mu, including
// the broadcast, guaranteeing: broadcast order == commit order == seq order.
//
// AC3.1: Interleaved events from multiple labelers receive strictly-increasing relay_seq.
// AC3.2: relay_seq is monotonic under concurrent ingest.
func (p *LabelPersist) PersistIngest(ctx context.Context, e IngestEvent) (relaySeq int64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current time for ingest_ts
	ingestTs := timestampMs()

	// Begin transaction
	tx, err := p.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Step 1: Insert placeholder with empty frame_cbor to reserve the seq
	upstreamSeq := (*int64)(nil)
	if e.UpstreamSeq != nil {
		upstreamSeq = e.UpstreamSeq
	}

	err = tx.QueryRowContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'', ?)
		 RETURNING relay_seq`,
		e.Kind, e.LabelerDID, upstreamSeq, ingestTs,
	).Scan(&relaySeq)
	if err != nil {
		return 0, fmt.Errorf("failed to insert placeholder: %w", err)
	}

	// Step 2: Encode the output frame with the minted seq
	var frameBytes []byte
	switch e.Kind {
	case "labels":
		frameBytes, err = EncodeLabelsFrame(relaySeq, e.LabelerDID, e.Labels)
		if err != nil {
			return 0, fmt.Errorf("failed to encode labels frame: %w", err)
		}

	case "service":
		frameBytes, err = EncodeServiceFrame(relaySeq, e.LabelerDID, e.Record)
		if err != nil {
			return 0, fmt.Errorf("failed to encode service frame: %w", err)
		}

	default:
		return 0, fmt.Errorf("unknown event kind: %s", e.Kind)
	}

	// Step 3: Update the frame_cbor with the actual encoded frame
	_, err = tx.ExecContext(ctx,
		`UPDATE events SET frame_cbor = ? WHERE relay_seq = ?`,
		frameBytes, relaySeq,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to update frame: %w", err)
	}

	// Step 4: Commit transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Step 5: Still holding mu, call broadcaster if registered (guarantees broadcast order)
	if p.broadcaster != nil {
		p.broadcaster(LiveEvent{
			RelaySeq:   relaySeq,
			Kind:       e.Kind,
			LabelerDID: e.LabelerDID,
			FrameCBOR:  frameBytes,
		})
	}

	return relaySeq, nil
}

// Playback queries all events with relay_seq > since in ascending order,
// invoking cb for each LiveEvent. If cb returns an error, Playback stops early.
// AC2.2: consumer connecting with cursor=N receives backfill from relay_seq > N.
func (p *LabelPersist) Playback(ctx context.Context, since int64, cb func(LiveEvent) error) error {
	rows, err := p.store.DB().QueryContext(ctx,
		`SELECT relay_seq, kind, labeler_did, frame_cbor FROM events
		 WHERE relay_seq > ? ORDER BY relay_seq ASC`,
		since,
	)
	if err != nil {
		return fmt.Errorf("failed to query events for playback: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var seq int64
		var kind string
		var did string
		var frameBytes []byte

		if err := rows.Scan(&seq, &kind, &did, &frameBytes); err != nil {
			return fmt.Errorf("failed to scan event: %w", err)
		}

		le := LiveEvent{
			RelaySeq:   seq,
			Kind:       kind,
			LabelerDID: did,
			FrameCBOR:  frameBytes,
		}

		if err := cb(le); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating events: %w", err)
	}

	return nil
}

// Head returns the maximum relay_seq (the current head of the stream).
// Returns 0 if no events exist.
func (p *LabelPersist) Head(ctx context.Context) (int64, error) {
	var seq int64
	err := p.store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(MAX(relay_seq), 0) FROM events`,
	).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("failed to query head: %w", err)
	}
	return seq, nil
}

// RetentionFloor returns the minimum relay_seq (the oldest surviving event).
// Returns 0 if no events exist. After prune, this represents the floor of
// events available for playback.
func (p *LabelPersist) RetentionFloor(ctx context.Context) (int64, error) {
	var seq int64
	err := p.store.DB().QueryRowContext(ctx,
		`SELECT COALESCE(MIN(relay_seq), 0) FROM events`,
	).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("failed to query retention floor: %w", err)
	}
	return seq, nil
}

// CursorStatus evaluates the given cursor against the retention window.
// AC2.2: cursor in [floor-1, head] is OK (get backfill starting from cursor+1).
// AC2.3: cursor > head is FutureCursor (client jumped ahead, wait for live).
// AC2.4: cursor < floor-1 is OutdatedCursor (cursor is stale, resume from floor).
func (p *LabelPersist) CursorStatus(ctx context.Context, cursor int64) (CursorState, error) {
	head, err := p.Head(ctx)
	if err != nil {
		return 0, err
	}

	floor, err := p.RetentionFloor(ctx)
	if err != nil {
		return 0, err
	}

	// Cursor above head: future cursor
	if cursor > head {
		return CursorFuture, nil
	}

	// Cursor below floor-1: outdated
	if floor > 0 && cursor < floor-1 {
		return CursorOutdated, nil
	}

	// Otherwise: OK (cursor is valid within the retention window)
	return CursorOK, nil
}

// timestampMs returns current time in milliseconds since epoch.
func timestampMs() int64 {
	return time.Now().UnixMilli()
}
