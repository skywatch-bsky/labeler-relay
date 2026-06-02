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

// timestampMs returns current time in milliseconds since epoch.
func timestampMs() int64 {
	return time.Now().UnixMilli()
}
