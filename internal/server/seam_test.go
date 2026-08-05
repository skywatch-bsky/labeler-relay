package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
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
// persisted DURING backfill (present in both the live buffer AND the DB
// for PlaybackFrames) is delivered EXACTLY ONCE.
//
// Deterministic mechanism — no timing/sleep races:
//   Pre-persist 1..80 (a large backlog). StreamFrom(since=0, bufSize=16).
//   The seam goroutine starts PlaybackFrames and forwards events to out
//   (capacity 16). After 16 events fill out, the callback blocks on "out <- e"
//   because the test hasn't drained yet. Backfill is provably mid-drain.
//   While blocked, PersistIngest 81..83. These broadcast to the Hub's live
//   channel (capacity 16, plenty of room since it's not full — only newly
//   persisted events go there) AND are now in the DB (visible to the still-
//   running PlaybackFrames query under WAL). Then drain out; backfill resumes
//   (17..80), switches to live, dedup boundary drops any live events with
//   RelaySeq <= lastBackfill (80), and forwards 81..83. Assert 1..83 exactly
//   once, ascending.
func TestSeamDuringBackfillPersist(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ctx := context.Background()

	// Pre-persist a large backlog: 1..80.
	for i := 1; i <= 80; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// bufSize=16: the out channel holds 16 events. The seam goroutine fills
	// it with backfill events 1..16, then blocks trying to send 17. The Hub's
	// live subscription also has capacity 16 — large enough to absorb the 3
	// events we persist while backfill is blocked (the Hub only queues events
	// that happen AFTER subscribe, so the 80 pre-existing events don't fill it).
	out, cleanup2, err := server.StreamFrom(ctx, p, h, 0, 16)
	require.NoError(t, err)
	defer cleanup2()

	// Read one event from out (seq 1). This proves the goroutine has started
	// backfill. With 80 total and capacity 16, the goroutine is now blocked
	// trying to send the 17th (out has 15 events buffered + we consumed 1).
	// Actually: we consumed 1, so out has room for 1 more → goroutine unblocks
	// and sends up to 17, blocks on 18. Either way, backfill is mid-drain.
	var first store.LiveEvent
	select {
	case e := <-out:
		first = e
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first backfill event")
	}
	require.Equal(t, int64(1), first.RelaySeq)

	// WHILE BACKFILL IS BLOCKED (out is full, goroutine is waiting on send):
	// persist 81..83. These broadcast to the Hub's live buffer AND are in the DB.
	for i := 81; i <= 83; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Now drain out fully. Backfill resumes and delivers 2..80, then the seam
	// switches to live. The dedup boundary drops any live event with
	// RelaySeq <= 80 (lastBackfill). Events 81..83 pass through.
	const total = 83 // seqs 1..83; we already consumed 1
	seqs := []int64{first.RelaySeq}
	for len(seqs) < total {
		select {
		case e, ok := <-out:
			require.True(t, ok, "out closed before all events delivered")
			seqs = append(seqs, e.RelaySeq)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout: got %d events, expected %d; last seq: %d",
				len(seqs), total, seqs[len(seqs)-1])
		}
	}

	// Assert exactly 1..83, once each, strictly ascending.
	expected := make([]int64, 0, total)
	for i := int64(1); i <= 83; i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, seqs,
		"events persisted during backfill must be delivered exactly once at the seam boundary")
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

	// Drain all 6 backfill events (5..10) condition-based, proving backfill
	// completed before we persist anything new.
	backfill := make([]int64, 0, 6)
	for len(backfill) < 6 {
		select {
		case e, ok := <-out:
			require.True(t, ok, "out closed during backfill drain")
			backfill = append(backfill, e.RelaySeq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout draining backfill; got %d of 6", len(backfill))
		}
	}
	require.Equal(t, []int64{5, 6, 7, 8, 9, 10}, backfill)

	// Now persist 11..15 — these hit ONLY the live path (backfill is done).
	for i := 11; i <= 15; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i), seq)
	}

	// Consume the 5 live events.
	live := make([]int64, 0, 5)
	for len(live) < 5 {
		select {
		case e, ok := <-out:
			require.True(t, ok, "out closed during live drain")
			live = append(live, e.RelaySeq)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout draining live; got %d of 5", len(live))
		}
	}
	require.Equal(t, []int64{11, 12, 13, 14, 15}, live)
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

	// Drain out1 to confirm it closed (cleanup complete). No sleep needed.
	for range out1 {
	}

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

// TestSeamChunkedBackfillSurvivesSustainedIngest verifies issue #11: a consumer
// resuming from the retention floor with a large backlog can catch up to live
// under sustained ingest without being dropped.
//
// Mechanism: pre-persist a backlog much larger than bufSize, then start
// StreamFrom with a small bufSize. Concurrently persist new events during the
// backfill. The drainer runs throughout all DB-reading phases, discarding live
// events (which are read from the DB instead). After the DB is drained
// (including a straggler read), the drainer stops and the stream switches to
// live with dedup. This prevents the live buffer from overflowing.
func TestSeamChunkedBackfillSurvivesSustainedIngest(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ctx := context.Background()

	const backlog = 200
	const bufSize = 16
	const liveEvents = 30

	for i := 1; i <= backlog; i++ {
		_, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i%256)))
		require.NoError(t, err)
	}

	out, cleanup2, err := server.StreamFrom(ctx, p, h, 0, bufSize)
	require.NoError(t, err)
	defer cleanup2()

	// Sustained ingest: persist new events while backfill is draining.
	// The drainer discards these from the live channel; they are read from
	// the DB instead. After DB drain + straggler read, the stream switches
	// to live with dedup.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := backlog + 1; i <= backlog+liveEvents; i++ {
			_, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i%256)))
			if err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	total := backlog + liveEvents
	seqs := make([]int64, 0, total)
	for len(seqs) < total {
		select {
		case e, ok := <-out:
			require.True(t, ok, "channel closed prematurely at %d events (ConsumerTooSlow); expected %d", len(seqs), total)
			seqs = append(seqs, e.RelaySeq)
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout: got %d events, expected %d", len(seqs), total)
		}
	}

	wg.Wait()

	expected := make([]int64, 0, total)
	for i := int64(1); i <= int64(total); i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, seqs,
		"chunked backfill must deliver all events in order without dropping the consumer")
}

// TestSeamChunkedBackfillDedup verifies that events persisted during the final
// seam chunk are still deduplicated correctly (no duplicates at the boundary).
func TestSeamChunkedBackfillDedup(t *testing.T) {
	p, cleanup := testPersist(t)
	defer cleanup()

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	ctx := context.Background()

	const backlog = 100
	const bufSize = 16

	for i := 1; i <= backlog; i++ {
		_, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i%256)))
		require.NoError(t, err)
	}

	out, cleanup2, err := server.StreamFrom(ctx, p, h, 0, bufSize)
	require.NoError(t, err)
	defer cleanup2()

	// Consume a few events to prove chunked backfill started.
	var first store.LiveEvent
	select {
	case e := <-out:
		first = e
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first event")
	}
	require.Equal(t, int64(1), first.RelaySeq)

	// Persist events while backfill is in progress. These will appear in both
	// the DB (visible to remaining chunks) and the live buffer. The drainer
	// discards them from live; they're read from the DB. The straggler read
	// after drainer stop catches any that arrived between the last chunk and
	// drainer stop. The dedup boundary then drops any remaining live dups.
	for i := backlog + 1; i <= backlog+5; i++ {
		_, err := p.PersistIngest(ctx, labelsEvent("did:plc:labeler", byte(i%256)))
		require.NoError(t, err)
	}

	total := backlog + 5
	seqs := []int64{first.RelaySeq}
	for len(seqs) < total {
		select {
		case e, ok := <-out:
			require.True(t, ok, "channel closed prematurely")
			seqs = append(seqs, e.RelaySeq)
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout: got %d events, expected %d", len(seqs), total)
		}
	}

	expected := make([]int64, 0, total)
	for i := int64(1); i <= int64(total); i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, seqs,
		"chunked backfill must deliver exactly-once across seam boundary")
}
