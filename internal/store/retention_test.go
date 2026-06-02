package store

import (
	"context"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/stretchr/testify/require"
)

// TestPruneRemovesOldEvents verifies that Prune deletes events older than cutoff.
// AC6.1: events older than the configured window are pruned and floor advances.
func TestPruneRemovesOldEvents(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	// Insert event 1 with ts=1000 (old)
	_, err := store.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'00', ?)`,
		"labels", "did:plc:labeler", nil, 1000,
	)
	require.NoError(t, err)

	// Insert event 2 with ts=2000 (old)
	_, err = store.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'00', ?)`,
		"labels", "did:plc:labeler", nil, 2000,
	)
	require.NoError(t, err)

	// Insert event 3 with ts=3000 (recent)
	_, err = store.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'00', ?)`,
		"labels", "did:plc:labeler", nil, 3000,
	)
	require.NoError(t, err)

	// Insert event 4 with ts=4000 (recent)
	_, err = store.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'00', ?)`,
		"labels", "did:plc:labeler", nil, 4000,
	)
	require.NoError(t, err)

	// Prune events older than ts=2500 (should remove 1, 2)
	deleted, newFloor, err := persist.Prune(ctx, 2500)
	require.NoError(t, err)

	require.Equal(t, int64(2), deleted)
	require.Equal(t, int64(3), newFloor)

	// Verify only events 3, 4 remain
	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), head)

	// Verify playback from 0 yields only seqs 3, 4
	var seqs []int64
	err = persist.Playback(ctx, 0, func(le LiveEvent) error {
		seqs = append(seqs, le.RelaySeq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, seqs)
}

// TestPruneBothKinds verifies that both labels and service events are pruned.
// AC6.2: Both #labels and #service events are subject to the same window.
func TestPruneBothKinds(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	// Insert labels event with ts=1000
	_, err := store.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'00', ?)`,
		"labels", "did:plc:labeler", nil, 1000,
	)
	require.NoError(t, err)

	// Insert service event with ts=2000
	_, err = store.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'00', ?)`,
		"service", "did:plc:labeler", nil, 2000,
	)
	require.NoError(t, err)

	// Insert labels event with ts=3000 (recent)
	_, err = store.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, ?, ?, x'00', ?)`,
		"labels", "did:plc:labeler", nil, 3000,
	)
	require.NoError(t, err)

	// Prune everything older than ts=2500 (should remove both 1 and 2)
	deleted, newFloor, err := persist.Prune(ctx, 2500)
	require.NoError(t, err)

	require.Equal(t, int64(2), deleted)
	require.Equal(t, int64(3), newFloor)
}

// TestSeqNeverRewindsAfterPrune verifies AC3.3: seq never reuses after prune.
func TestSeqNeverRewindsAfterPrune(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	cidStr := "bafy1"

	// Persist events 1-5
	for i := 1; i <= 5; i++ {
		event := IngestEvent{
			Kind:        "labels",
			LabelerDID:  "did:plc:labeler",
			UpstreamSeq: nil,
			Labels: []*comatproto.LabelDefs_Label{
				{
					Src: "did:plc:labeler",
					Uri: "at://did:plc:user/app.bsky.feed.post/1",
					Cid: &cidStr,
					Val: "label",
					Cts: "2026-06-01T00:00:00Z",
					Sig: []byte{byte(i)},
				},
			},
		}

		_, err := persist.PersistIngest(ctx, event)
		require.NoError(t, err)
	}

	// Prune all 5 events
	_, _, err := persist.Prune(ctx, time.Now().UnixMilli())
	require.NoError(t, err)

	// Persist new event: seq should be 6, not 1
	event := IngestEvent{
		Kind:        "labels",
		LabelerDID:  "did:plc:labeler",
		UpstreamSeq: nil,
		Labels: []*comatproto.LabelDefs_Label{
			{
				Src: "did:plc:labeler",
				Uri: "at://did:plc:user/app.bsky.feed.post/1",
				Cid: &cidStr,
				Val: "label",
				Cts: "2026-06-01T00:00:00Z",
				Sig: []byte{0x06},
			},
		},
	}

	seq, err := persist.PersistIngest(ctx, event)
	require.NoError(t, err)
	require.Equal(t, int64(6), seq, "seq should continue from high-water mark, not reuse")
}

// TestSeqNeverRewindsAfterRestart verifies seq continues after DB close/reopen.
func TestSeqNeverRewindsAfterRestart(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"

	// Phase 1: Create DB, persist 5 events, prune all
	{
		store, err := Open(dbPath)
		require.NoError(t, err)

		persist := NewLabelPersist(store)
		ctx := context.Background()

		cidStr := "bafy1"

		// Persist events 1-5
		for i := 1; i <= 5; i++ {
			event := IngestEvent{
				Kind:        "labels",
				LabelerDID:  "did:plc:labeler",
				UpstreamSeq: nil,
				Labels: []*comatproto.LabelDefs_Label{
					{
						Src: "did:plc:labeler",
						Uri: "at://did:plc:user/app.bsky.feed.post/1",
						Cid: &cidStr,
						Val: "label",
						Cts: "2026-06-01T00:00:00Z",
						Sig: []byte{byte(i)},
					},
				},
			}

			_, err := persist.PersistIngest(ctx, event)
			require.NoError(t, err)
		}

		// Prune all 5
		_, _, err = persist.Prune(ctx, time.Now().UnixMilli())
		require.NoError(t, err)

		store.Close()
	}

	// Phase 2: Reopen same DB, persist new event
	{
		store, err := Open(dbPath)
		require.NoError(t, err)

		persist := NewLabelPersist(store)
		ctx := context.Background()

		cidStr := "bafy1"

		event := IngestEvent{
			Kind:        "labels",
			LabelerDID:  "did:plc:labeler",
			UpstreamSeq: nil,
			Labels: []*comatproto.LabelDefs_Label{
				{
					Src: "did:plc:labeler",
					Uri: "at://did:plc:user/app.bsky.feed.post/1",
					Cid: &cidStr,
					Val: "label",
					Cts: "2026-06-01T00:00:00Z",
					Sig: []byte{0x06},
				},
			},
		}

		seq, err := persist.PersistIngest(ctx, event)
		require.NoError(t, err)

		// Seq should be 6, proving AUTOINCREMENT high-water mark is persistent
		require.Equal(t, int64(6), seq,
			"seq should continue from high-water mark after restart, not rewind")

		store.Close()
	}
}
