package store

import (
	"context"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/stretchr/testify/require"
)

// TestPlaybackOrdering verifies Playback yields events in ascending relay_seq order.
// AC2.2: consumer connecting with cursor=N receives backfill from relay_seq > N.
func TestPlaybackOrdering(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	// Persist 10 events
	cidStr := "bafy1"
	for i := 1; i <= 10; i++ {
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

	// Playback from cursor=4 should yield seqs 5..10
	var playedSeqs []int64
	err := persist.PlaybackFrames(ctx, 4, func(le LiveEvent) error {
		playedSeqs = append(playedSeqs, le.RelaySeq)
		return nil
	})
	require.NoError(t, err)

	expected := []int64{5, 6, 7, 8, 9, 10}
	require.Equal(t, expected, playedSeqs)
}

// TestHeadEmpty verifies Head returns 0 on empty database.
func TestHeadEmpty(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), head)
}

// TestHead verifies Head returns the maximum relay_seq.
func TestHead(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	cidStr := "bafy1"
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

	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), head)
}

// TestRetentionFloorEmpty verifies RetentionFloor returns 0 on empty database.
func TestRetentionFloorEmpty(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	floor, err := persist.RetentionFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), floor)
}

// TestRetentionFloorAfterDelete verifies RetentionFloor returns the minimum
// relay_seq after some events are deleted.
func TestRetentionFloorAfterDelete(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	cidStr := "bafy1"
	for i := 1; i <= 10; i++ {
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

	// Simulate prune by deleting events 1..5
	_, err := store.DB().ExecContext(ctx, `DELETE FROM events WHERE relay_seq <= 5`)
	require.NoError(t, err)

	// Floor should now be 6
	floor, err := persist.RetentionFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(6), floor)
}

// TestCursorStatusOK verifies CursorOK for valid cursors.
func TestCursorStatusOK(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	cidStr := "bafy1"
	for i := 1; i <= 10; i++ {
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

	// cursor=0 (before any) should be OK (will get everything)
	status, err := persist.CursorStatus(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, CursorOK, status)

	// cursor=5 (middle) should be OK
	status, err = persist.CursorStatus(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, CursorOK, status)

	// cursor=10 (at head) should be OK
	status, err = persist.CursorStatus(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, CursorOK, status)
}

// TestCursorStatusFuture verifies CursorFuture for cursors above head.
// AC2.3: cursor > head returns FutureCursor error frame.
func TestCursorStatusFuture(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	cidStr := "bafy1"
	for i := 1; i <= 10; i++ {
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

	// cursor=15 (above head=10) should be FutureCursor
	status, err := persist.CursorStatus(ctx, 15)
	require.NoError(t, err)
	require.Equal(t, CursorFuture, status)
}

// TestCursorStatusOutdated verifies CursorOutdated for cursors below floor.
// AC2.4: cursor < floor returns OutdatedCursor and resumes from floor.
func TestCursorStatusOutdated(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	cidStr := "bafy1"
	for i := 1; i <= 10; i++ {
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

	// Simulate prune by deleting events 1..5, so floor=6
	_, err := store.DB().ExecContext(ctx, `DELETE FROM events WHERE relay_seq <= 5`)
	require.NoError(t, err)

	// cursor=2 (below floor=6) should be CursorOutdated
	status, err := persist.CursorStatus(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, CursorOutdated, status)
}

// TestPlaybackEmpty verifies Playback yields nothing on empty database.
func TestPlaybackEmpty(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	var count int
	err := persist.PlaybackFrames(ctx, 0, func(le LiveEvent) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
