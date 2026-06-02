package server_test

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
	"github.com/stretchr/testify/require"
)

// testPersist creates a real LabelPersist backed by a temp SQLite store.
func testPersist(t *testing.T) (*store.LabelPersist, func()) {
	t.Helper()
	s, err := store.Open(t.TempDir() + "/test.db")
	require.NoError(t, err)
	p := store.NewLabelPersist(s)
	return p, func() { s.Close() }
}

// labelsEvent builds a minimal IngestEvent of kind "labels" for testing.
func labelsEvent(did string, sig byte) store.IngestEvent {
	cidStr := "bafy1"
	return store.IngestEvent{
		Kind:       "labels",
		LabelerDID: did,
		Labels: []*comatproto.LabelDefs_Label{
			{
				Src: did,
				Uri: "at://did:plc:user/app.bsky.feed.post/1",
				Cid: &cidStr,
				Val: "test",
				Cts: "2026-06-01T00:00:00Z",
				Sig: []byte{sig},
			},
		},
	}
}

// drainSlowChanUntilClosed drains any buffered items from ch, then blocks
// until ch is closed, asserting that it is indeed closed.
func drainSlowChanUntilClosed(t *testing.T, ch <-chan store.LiveEvent) {
	t.Helper()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed
			}
			// still open, keep draining
		default:
			// buffer empty; block-wait for close
			_, ok := <-ch
			require.False(t, ok, "slow subscriber channel must be closed after drop")
			return
		}
	}
}

// TestHubLiveSingleOrdered verifies that a single subscriber receives events
// in the order they are persisted, with correct fields (AC2.1 live path).
func TestHubLiveSingleOrdered(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ch, unsub := h.Subscribe(8)
	defer unsub()

	ctx := context.Background()

	seq1, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", 0x01))
	require.NoError(t, err)

	seq2, err := p.PersistIngest(ctx, labelsEvent("did:plc:b", 0x02))
	require.NoError(t, err)

	got1 := <-ch
	got2 := <-ch

	require.Equal(t, seq1, got1.RelaySeq)
	require.Equal(t, seq2, got2.RelaySeq)
	require.Equal(t, "labels", got1.Kind)
	require.Equal(t, "labels", got2.Kind)
	require.Equal(t, "did:plc:a", got1.LabelerDID)
	require.Equal(t, "did:plc:b", got2.LabelerDID)
	require.True(t, got1.RelaySeq < got2.RelaySeq, "events must arrive in ascending seq order")
}

// TestHubConcurrentOrdering verifies that N goroutines × M concurrent
// PersistIngest calls are observed by a draining subscriber in STRICTLY
// ASCENDING relay_seq order (AC2.1 concurrent reorder-detection test).
//
// This test ONLY passes when broadcast runs under the persist mutex, because
// that's what prevents out-of-order delivery. A single-goroutine sequential
// test cannot catch a reorder.
func TestHubConcurrentOrdering(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	const N = 5  // goroutines
	const M = 20 // events per goroutine
	const total = N * M

	// Buffer large enough to hold all events so no drop occurs.
	ch, unsub := h.Subscribe(total + 10)
	defer unsub()

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < M; j++ {
				_, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(id*M+j)))
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	// Drain all N*M events from the channel.
	seqs := make([]int64, 0, total)
	for len(seqs) < total {
		e := <-ch
		seqs = append(seqs, e.RelaySeq)
	}

	require.Len(t, seqs, total)

	// Assert strictly ascending: each seq must equal prev+1.
	// This only holds if broadcast runs under mu (Phase 2 Task 4 guarantee).
	for i := 1; i < len(seqs); i++ {
		require.Equal(t, seqs[i-1]+1, seqs[i],
			"broadcast must be strictly ascending: got %v at [%d] after %v",
			seqs[i], i, seqs[i-1])
	}

	// Verify range is exactly 1..total (no gaps, no dups).
	require.Equal(t, int64(1), seqs[0])
	require.Equal(t, int64(total), seqs[total-1])
}

// TestHubSlowConsumerDropped verifies AC9.1: a subscriber too slow to drain
// is dropped (its channel closed), and a second draining subscriber receives
// all events unimpeded — the slow one did NOT block the broadcast.
func TestHubSlowConsumerDropped(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	// Slow subscriber: tiny buffer, never drained.
	slowCh, slowUnsub := h.Subscribe(1)
	defer slowUnsub()

	const total = 20

	// Healthy subscriber: large buffer, actively drained.
	healthyCh, healthyUnsub := h.Subscribe(total + 10)
	defer healthyUnsub()

	var healthyCount atomic.Int64

	// Drain healthy subscriber in background.
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for range healthyCh {
			healthyCount.Add(1)
		}
	}()

	// Flood: persist enough events to overflow the slow sub's buffer.
	ctx := context.Background()
	for i := 0; i < total; i++ {
		_, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
	}

	// Condition-based wait: healthy sub must see all events.
	deadline := time.After(5 * time.Second)
	for healthyCount.Load() < int64(total) {
		select {
		case <-deadline:
			t.Fatalf("timeout: healthy sub got %d of %d events", healthyCount.Load(), total)
		default:
			runtime.Gosched()
		}
	}

	// Signal healthy drain goroutine to stop.
	healthyUnsub()
	drainWg.Wait()

	// The slow subscriber's channel must be closed (dropped by Hub overflow).
	drainSlowChanUntilClosed(t, slowCh)

	require.Equal(t, int64(total), healthyCount.Load(),
		"healthy subscriber must receive all %d events", total)
}
