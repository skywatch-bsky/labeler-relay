package store

import (
	"context"
	"sync"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/stretchr/testify/require"
)

// TestPersistIngestSeqMonotonicity verifies that sequential PersistIngest
// calls return strictly increasing relay_seq values (AC3.1).
func TestPersistIngestSeqMonotonicity(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)

	// Persist mix of events from two labelers
	ctx := context.Background()

	cidStr := "bafy1"
	events := []IngestEvent{
		{
			Kind:        "labels",
			LabelerDID:  "did:plc:labeler1",
			UpstreamSeq: nil,
			Labels: []*comatproto.LabelDefs_Label{
				{
					Src: "did:plc:labeler1",
					Uri: "at://did:plc:user1/app.bsky.feed.post/1",
					Cid: &cidStr,
					Val: "spam",
					Cts: "2026-06-01T00:00:00Z",
					Sig: []byte{0x01},
				},
			},
		},
		{
			Kind:        "service",
			LabelerDID:  "did:plc:labeler2",
			UpstreamSeq: nil,
			Record: &bsky.LabelerService{
				CreatedAt: "2026-06-01T00:00:00Z",
				Policies:  &bsky.LabelerDefs_LabelerPolicies{},
			},
		},
		{
			Kind:        "labels",
			LabelerDID:  "did:plc:labeler1",
			UpstreamSeq: nil,
			Labels: []*comatproto.LabelDefs_Label{
				{
					Src: "did:plc:labeler1",
					Uri: "at://did:plc:user2/app.bsky.feed.post/2",
					Cid: &cidStr,
					Val: "label",
					Cts: "2026-06-01T00:00:00Z",
					Sig: []byte{0x02},
				},
			},
		},
	}

	seqs := []int64{}
	for _, event := range events {
		seq, err := persist.PersistIngest(ctx, event)
		require.NoError(t, err)
		seqs = append(seqs, seq)
	}

	// Verify strictly increasing: 1, 2, 3
	require.Equal(t, []int64{1, 2, 3}, seqs)
}

// TestPersistIngestConcurrencySeqUniqueness verifies that concurrent
// PersistIngest calls produce strictly increasing unique seq values (AC3.2).
func TestPersistIngestConcurrencySeqUniqueness(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	const N = 5  // goroutines
	const M = 10 // events per goroutine

	seqs := make([]int64, 0, N*M)
	var seqsMu sync.Mutex
	var wg sync.WaitGroup

	// Spawn N goroutines, each calling PersistIngest M times
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			cidStr := "bafy1"
			for j := 0; j < M; j++ {
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
							Sig: []byte{byte(goroutineID*M + j)},
						},
					},
				}

				seq, err := persist.PersistIngest(ctx, event)
				require.NoError(t, err)

				seqsMu.Lock()
				seqs = append(seqs, seq)
				seqsMu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify we got N*M seqs
	require.Len(t, seqs, N*M)

	// Verify they are exactly 1..N*M (all unique, all present)
	seqSet := make(map[int64]bool)
	for _, seq := range seqs {
		seqSet[seq] = true
	}

	for i := int64(1); i <= int64(N*M); i++ {
		require.True(t, seqSet[i], "missing seq %d", i)
	}
}

// TestPersistIngestBroadcastOrdering verifies that broadcast order equals
// commit order equals seq order (load-bearing invariant for AC2.1/AC2.2 seam).
func TestPersistIngestBroadcastOrdering(t *testing.T) {
	store, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(store)
	ctx := context.Background()

	// Register broadcaster to capture live events
	var capturedSeqs []int64
	var capturedMu sync.Mutex

	broadcaster := func(le LiveEvent) {
		capturedMu.Lock()
		capturedSeqs = append(capturedSeqs, le.RelaySeq)
		capturedMu.Unlock()
	}

	persist.SetBroadcaster(broadcaster)

	const N = 3  // goroutines
	const M = 10 // events per goroutine

	var wg sync.WaitGroup

	// Spawn N goroutines, each calling PersistIngest M times
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			cidStr := "bafy1"
			for j := 0; j < M; j++ {
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
							Sig: []byte{byte(goroutineID*M + j)},
						},
					},
				}

				_, err := persist.PersistIngest(ctx, event)
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify broadcast order is strictly ascending 1..N*M
	// This is the critical test: if broadcast moved outside mu, order would be random
	require.Len(t, capturedSeqs, N*M)

	expected := make([]int64, N*M)
	for i := 0; i < N*M; i++ {
		expected[i] = int64(i + 1)
	}

	require.Equal(t, expected, capturedSeqs,
		"broadcast sequence must be strictly ascending (proves broadcast inside mu)")
}

// testStore creates a temporary SQLite store for testing.
func testStore(t *testing.T) (*Store, func()) {
	dbPath := t.TempDir() + "/test.db"
	store, err := Open(dbPath)
	require.NoError(t, err)

	cleanup := func() {
		store.Close()
	}

	return store, cleanup
}
