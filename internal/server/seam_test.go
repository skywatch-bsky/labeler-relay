package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/stretchr/testify/require"
)

// TestSeamBackfillLiveSeqOrdering verifies AC2.2: backfill from a mid-stream
// cursor replays in relay_seq order, then continues live with no gap or
// duplicate at the seam boundary.
//
// Test setup: PersistIngest 1..10, StreamFrom(since=4), then PersistIngest 11..15.
// Expected: out yields 5..15 strictly ascending, no gaps, no dups.
func TestSeamBackfillLiveSeqOrdering(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ctx := context.Background()

	// Pre-persist events 1..10
	for i := 1; i <= 10; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Start the seam at since=4 (will backfill 5..10 from durable store,
	// then switch to live).
	out, cleanup2, err := server.StreamFrom(ctx, p, h, 4, 64)
	require.NoError(t, err)
	defer cleanup2()

	// Give backfill time to drain (no fixed sleep; we'll condition-based on drains).
	// For this simple case, backfill should be fast. Persist 11..15 and consume.
	for i := 11; i <= 15; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Consume all expected 5..15 from out, verify order.
	seqs := make([]int64, 0, 11)
	for len(seqs) < 11 {
		select {
		case e, ok := <-out:
			require.True(t, ok, "out should not close before all events")
			seqs = append(seqs, e.RelaySeq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for events; got %d, expected 11", len(seqs))
		}
	}

	// Verify strictly ascending 5..15, no gaps, no dups.
	expected := make([]int64, 0, 11)
	for i := int64(5); i <= 15; i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, seqs,
		"backfill→live seam must deliver all events in ascending order with no gaps or dups")
}

// TestSeamDuringBackfillPersist verifies AC2.2 boundary case: an event
// persisted DURING backfill (that will appear in both Playback and the live
// buffer) is delivered EXACTLY ONCE, not twice.
//
// Mechanism to force during-backfill timing: use a custom PlaybackFrames
// implementation via a slow callback that we control with a channel. This
// lets us block the backfill mid-drain, persist a new event (which lands in
// the live buffer), then resume backfill and verify the dedup boundary
// correctly filters it out.
//
// Since LabelPersist doesn't expose a hook, we'll use a pattern: persist a
// large backlog (1..30), then StreamFrom(since=4) with a small bufSize.
// Spawn concurrent PersistIngest(31..40) on a separate goroutine while
// actively draining from out. The concurrent events will race with backfill
// drain. By controlling the drain pace (using a channel-blocked consume),
// we can force overlap.
//
// Actually, the simplest approach: use a deliberate delay in the drain loop
// to create a race window. Persist 1..10, StreamFrom(since=4), then spawn a
// goroutine that persists 11..20 slowly (with delays between each), while
// we consume from out. This creates genuine timing pressure.
func TestSeamDuringBackfillPersist(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ctx := context.Background()

	// Pre-persist a moderately large backlog: 1..30 (to extend backfill time).
	for i := 1; i <= 30; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Start seam at since=4 (will backfill 5..30). Use a modest bufSize to
	// apply backpressure and extend the backfill window.
	out, cleanup2, err := server.StreamFrom(ctx, p, h, 4, 8)
	require.NoError(t, err)
	defer cleanup2()

	// Spawn a goroutine that persists 31..50 with tiny delays between each,
	// to ensure some land while backfill is still draining (before lastBackfill
	// has reached 30).
	persisterDone := make(chan struct{})
	go func() {
		defer close(persisterDone)
		for i := int32(31); i <= 50; i++ {
			// Tiny delay to spread persists across the backfill window.
			time.Sleep(5 * time.Millisecond)
			seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
			require.NoError(t, err)
			require.Equal(t, int64(i), seq)
		}
	}()

	// Consume all expected 5..50 from out (25 total events).
	// Use a small consume delay to avoid draining too fast, keeping the
	// persister goroutine in-flight for longer.
	const total = 46 // 5..50
	seqs := make([]int64, 0, total)
	for len(seqs) < total {
		select {
		case e, ok := <-out:
			require.True(t, ok, "out should not close before all events")
			seqs = append(seqs, e.RelaySeq)
			// Tiny delay to slow consumption, letting persister overlap backfill.
			time.Sleep(1 * time.Millisecond)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for events; got %d, expected %d", len(seqs), total)
		}
	}

	// Wait for persister to finish.
	<-persisterDone

	// Verify strictly ascending 5..50, no gaps, no dups.
	expected := make([]int64, 0, total)
	for i := int64(5); i <= 50; i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, seqs,
		"events persisted during backfill must be delivered exactly once at the seam boundary")

	// Verify no duplicates in seqs.
	seen := make(map[int64]bool)
	for _, seq := range seqs {
		require.False(t, seen[seq], "seq %d appears more than once", seq)
		seen[seq] = true
	}
}

// TestSeamAfterBackfillPersist verifies the no-dup boundary when events
// persist AFTER backfill completes but are already in the live buffer.
// This is the complement to TestSeamDuringBackfillPersist.
func TestSeamAfterBackfillPersist(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ctx := context.Background()

	// Pre-persist 1..10.
	for i := 1; i <= 10; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Start seam. Subscribe to live BEFORE backfill drains.
	out, cleanup2, err := server.StreamFrom(ctx, p, h, 4, 32)
	require.NoError(t, err)
	defer cleanup2()

	// Let backfill drain fully (it's fast on small backlog).
	// Then persist 11..15 — these will only hit the live path.
	time.Sleep(50 * time.Millisecond)
	for i := 11; i <= 15; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Consume all 5..15 (11 total).
	const total = 11
	seqs := make([]int64, 0, total)
	for len(seqs) < total {
		select {
		case e, ok := <-out:
			require.True(t, ok, "out should not close before all events")
			seqs = append(seqs, e.RelaySeq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for events; got %d, expected %d", len(seqs), total)
		}
	}

	// Verify strictly ascending 5..15.
	expected := make([]int64, 0, total)
	for i := int64(5); i <= 15; i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, seqs)

	// Verify no duplicates.
	seen := make(map[int64]bool)
	for _, seq := range seqs {
		require.False(t, seen[seq], "seq %d appears more than once", seq)
		seen[seq] = true
	}
}

// TestSeamReconnectGap verifies AC9.2: consume up to seq K, cancel; then
// re-StreamFrom(since=K); assert first delivered is K+1 with no gap.
func TestSeamReconnectGap(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ctx := context.Background()

	// Pre-persist 1..20.
	for i := 1; i <= 20; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// First stream: StreamFrom(since=0), consume up to seq=10, then cancel.
	out1, cleanup1, err := server.StreamFrom(ctx, p, h, 0, 32)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		<-out1
	}

	lastConsumed := int64(10)
	cleanup1() // cancel first stream

	// Wait a bit for cleanup.
	time.Sleep(10 * time.Millisecond)

	// Second stream: StreamFrom(since=lastConsumed=10), should yield 11..20.
	out2, cleanup2, err := server.StreamFrom(ctx, p, h, lastConsumed, 32)
	require.NoError(t, err)
	defer cleanup2()

	// Consume all remaining 11..20.
	const total = 10
	seqs := make([]int64, 0, total)
	for len(seqs) < total {
		select {
		case e, ok := <-out2:
			require.True(t, ok, "out should not close before all events")
			seqs = append(seqs, e.RelaySeq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for events; got %d, expected %d", len(seqs), total)
		}
	}

	// Verify first is exactly 11 (no gap from 10).
	require.Equal(t, int64(11), seqs[0], "first event after reconnect must be lastConsumed+1")

	// Verify strictly ascending 11..20.
	expected := make([]int64, 0, total)
	for i := int64(11); i <= 20; i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, seqs)
}

// TestSeamCtxCancellation verifies that when ctx is cancelled, the seam
// closes out and stops draining.
func TestSeamCtxCancellation(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	// Create a cancellable context.
	ctx, cancel := context.WithCancel(context.Background())

	// Pre-persist 1..10.
	for i := 1; i <= 10; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Start seam.
	out, cleanup2, err := server.StreamFrom(ctx, p, h, 0, 32)
	require.NoError(t, err)
	defer cleanup2()

	// Consume a few events.
	<-out
	<-out
	<-out

	// Cancel context.
	cancel()

	// out should close after a brief moment.
	for {
		select {
		case v, ok := <-out:
			if !ok {
				return // closed, good
			}
			_ = v // ignore value
			// still open
		case <-time.After(500 * time.Millisecond):
			t.Fatal("out channel did not close after ctx cancellation")
		}
	}
}
