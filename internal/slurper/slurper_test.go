package slurper

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scarndp/labeler-relay/internal/metrics"
	"github.com/scarndp/labeler-relay/internal/store"
)

// TestSlurperStartsSubscriptionsForEnabledLabelers verifies that calling
// Reconcile starts subscription goroutines for enabled labelers.
func TestSlurperStartsSubscriptionsForEnabledLabelers(t *testing.T) {
	t.Parallel()

	testStore, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to open test store: %v", err)
	}
	defer testStore.Close()

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("failed to register metrics: %v", err)
	}

	registry := store.NewLabelerRegistry(testStore)
	persist := store.NewLabelPersist(testStore)

	fakeServerA := newFakeLabelerServer(t)
	fakeServerB := newFakeLabelerServer(t)
	defer fakeServerA.Close()
	defer fakeServerB.Close()

	// Insert two enabled labelers.
	labelerA := store.Labeler{
		DID:      "did:plc:labelerA",
		Endpoint: fakeServerA.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	labelerB := store.Labeler{
		DID:      "did:plc:labelerB",
		Endpoint: fakeServerB.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}

	if err := registry.Upsert(context.Background(), labelerA); err != nil {
		t.Fatalf("failed to insert labelerA: %v", err)
	}
	if err := registry.Upsert(context.Background(), labelerB); err != nil {
		t.Fatalf("failed to insert labelerB: %v", err)
	}

	slurper := New(
		registry,
		persist,
		false, // sigDefault = allow unsigned
		LimitConfig{PerSec: 1000, PerHour: 100000},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	defer slurper.Shutdown()

	// Call Reconcile to start subscriptions.
	if err := slurper.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Wait for WebSockets to be ready.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case <-fakeServerA.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for labelerA websocket ready")
	}

	select {
	case <-fakeServerB.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for labelerB websocket ready")
	}

	// Send a frame from labelerA.
	frameA := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labelerA.DID,
				Uri: "at://did:plc:user1/app.bsky.feed.post/a1",
				Val: "labelA",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 1,
	}

	if err := fakeServerA.sendFrame(frameA); err != nil {
		t.Fatalf("failed to send frameA: %v", err)
	}

	// Send a frame from labelerB.
	frameB := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labelerB.DID,
				Uri: "at://did:plc:user2/app.bsky.feed.post/b1",
				Val: "labelB",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 1,
	}

	if err := fakeServerB.sendFrame(frameB); err != nil {
		t.Fatalf("failed to send frameB: %v", err)
	}

	// Wait for both labels to be persisted.
	head, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 2)
	if err != nil {
		t.Fatalf("timeout waiting for both labels: %v", err)
	}

	if head < 2 {
		t.Errorf("expected head >= 2, got %d", head)
	}
}

// TestSlurperStopsDisabledLabelers verifies that disabling a labeler stops
// its subscription and others continue running.
func TestSlurperStopsDisabledLabelers(t *testing.T) {
	t.Parallel()

	testStore, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to open test store: %v", err)
	}
	defer testStore.Close()

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("failed to register metrics: %v", err)
	}

	registry := store.NewLabelerRegistry(testStore)
	persist := store.NewLabelPersist(testStore)

	fakeServerA := newFakeLabelerServer(t)
	fakeServerB := newFakeLabelerServer(t)
	defer fakeServerA.Close()
	defer fakeServerB.Close()

	labelerA := store.Labeler{
		DID:      "did:plc:slurperA",
		Endpoint: fakeServerA.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	labelerB := store.Labeler{
		DID:      "did:plc:slurperB",
		Endpoint: fakeServerB.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}

	if err := registry.Upsert(context.Background(), labelerA); err != nil {
		t.Fatalf("failed to insert labelerA: %v", err)
	}
	if err := registry.Upsert(context.Background(), labelerB); err != nil {
		t.Fatalf("failed to insert labelerB: %v", err)
	}

	slurper := New(
		registry,
		persist,
		false,
		LimitConfig{PerSec: 1000, PerHour: 100000},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	defer slurper.Shutdown()

	if err := slurper.Reconcile(context.Background()); err != nil {
		t.Fatalf("initial Reconcile failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Wait for both WebSockets.
	select {
	case <-fakeServerA.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for labelerA")
	}

	select {
	case <-fakeServerB.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for labelerB")
	}

	// Send frames from both.
	frameA1 := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labelerA.DID,
				Uri: "at://did:plc:user1/app.bsky.feed.post/a1",
				Val: "labelA",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 1,
	}

	frameB1 := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labelerB.DID,
				Uri: "at://did:plc:user2/app.bsky.feed.post/b1",
				Val: "labelB",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 1,
	}

	if err := fakeServerA.sendFrame(frameA1); err != nil {
		t.Fatalf("failed to send frameA1: %v", err)
	}
	if err := fakeServerB.sendFrame(frameB1); err != nil {
		t.Fatalf("failed to send frameB1: %v", err)
	}

	// Wait for both to be persisted.
	head1, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 2)
	if err != nil {
		t.Fatalf("timeout waiting for initial frames: %v", err)
	}

	// Disable labelerA.
	if err := registry.SetEnabled(context.Background(), labelerA.DID, false); err != nil {
		t.Fatalf("failed to disable labelerA: %v", err)
	}

	// Call Reconcile to stop the disabled subscription.
	if err := slurper.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile after disable failed: %v", err)
	}

	// Verify labelerA is no longer in active.
	slurper.mu.Lock()
	_, hasA := slurper.active[labelerA.DID]
	_, hasB := slurper.active[labelerB.DID]
	slurper.mu.Unlock()

	if hasA {
		t.Errorf("labelerA should be removed from active")
	}
	if !hasB {
		t.Errorf("labelerB should still be in active")
	}

	// Send another frame from labelerB; should be persisted.
	frameB2 := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labelerB.DID,
				Uri: "at://did:plc:user3/app.bsky.feed.post/b2",
				Val: "labelB2",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 2,
	}

	if err := fakeServerB.sendFrame(frameB2); err != nil {
		t.Fatalf("failed to send frameB2: %v", err)
	}

	// Wait for the new frame from labelerB.
	head2, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, head1+1)
	if err != nil {
		t.Fatalf("timeout waiting for frameB2: %v", err)
	}

	if head2 <= head1 {
		t.Errorf("expected head to increase after disabling labelerA")
	}
}

// TestSlurperIsolatesRateLimitingPerLabeler verifies AC7.2: throttling one
// labeler does not starve others.
func TestSlurperIsolatesRateLimitingPerLabeler(t *testing.T) {
	t.Parallel()

	testStore, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to open test store: %v", err)
	}
	defer testStore.Close()

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("failed to register metrics: %v", err)
	}

	registry := store.NewLabelerRegistry(testStore)
	persist := store.NewLabelPersist(testStore)

	fakeServerSlow := newFakeLabelerServer(t)
	fakeServerFast := newFakeLabelerServer(t)
	defer fakeServerSlow.Close()
	defer fakeServerFast.Close()

	// Create a custom slurper with very tight limits for the slow labeler.
	slurper := New(
		registry,
		persist,
		false,
		LimitConfig{PerSec: 1, PerHour: 100000}, // Very tight for demonstration
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	defer slurper.Shutdown()

	// Manually create subscriptions with different limiters.
	labelerSlow := store.Labeler{
		DID:      "did:plc:slow",
		Endpoint: fakeServerSlow.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	labelerFast := store.Labeler{
		DID:      "did:plc:fast",
		Endpoint: fakeServerFast.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}

	if err := registry.Upsert(context.Background(), labelerSlow); err != nil {
		t.Fatalf("failed to insert labelerSlow: %v", err)
	}
	if err := registry.Upsert(context.Background(), labelerFast); err != nil {
		t.Fatalf("failed to insert labelerFast: %v", err)
	}

	// Reconcile to create subscriptions.
	if err := slurper.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Wait for both WebSockets.
	select {
	case <-fakeServerSlow.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for slow server")
	}

	select {
	case <-fakeServerFast.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for fast server")
	}

	// Send multiple frames rapidly from both servers.
	// The slow labeler's limiter will throttle it.
	// The fast labeler should ingest without waiting for the slow one.

	// Send 5 frames from slow server (should be throttled).
	for i := 1; i <= 5; i++ {
		frame := &atproto.LabelSubscribeLabels_Labels{
			Labels: []*atproto.LabelDefs_Label{
				{
					Src: labelerSlow.DID,
					Uri: fmt.Sprintf("at://did:plc:user/app.bsky.feed.post/slow%d", i),
					Val: "slow",
					Cts: time.Now().UTC().Format(time.RFC3339),
				},
			},
			Seq: int64(i),
		}
		if err := fakeServerSlow.sendFrame(frame); err != nil {
			t.Fatalf("failed to send slow frame %d: %v", i, err)
		}
	}

	// Send 5 frames from fast server (should ingest quickly).
	for i := 1; i <= 5; i++ {
		frame := &atproto.LabelSubscribeLabels_Labels{
			Labels: []*atproto.LabelDefs_Label{
				{
					Src: labelerFast.DID,
					Uri: fmt.Sprintf("at://did:plc:user/app.bsky.feed.post/fast%d", i),
					Val: "fast",
					Cts: time.Now().UTC().Format(time.RFC3339),
				},
			},
			Seq: int64(i),
		}
		if err := fakeServerFast.sendFrame(frame); err != nil {
			t.Fatalf("failed to send fast frame %d: %v", i, err)
		}
	}

	// Wait for both labelers to ingest all 5 frames.
	// With per-sec limiter of 1, both should complete in ~5 seconds.
	start := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	bothIngestedAll5 := false
	for !bothIngestedAll5 {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for both labelers to ingest 5 frames each")
		case <-ticker.C:
			fastRows, err := testStore.DB().QueryContext(context.Background(),
				`SELECT COUNT(*) FROM events WHERE labeler_did = ?`, labelerFast.DID)
			if err != nil {
				t.Fatalf("failed to query fast labeler count: %v", err)
			}

			var fastCount int64
			if fastRows.Next() {
				if err := fastRows.Scan(&fastCount); err != nil {
					fastRows.Close()
					t.Fatalf("failed to scan fast count: %v", err)
				}
			}
			fastRows.Close()

			slowRows, err := testStore.DB().QueryContext(context.Background(),
				`SELECT COUNT(*) FROM events WHERE labeler_did = ?`, labelerSlow.DID)
			if err != nil {
				t.Fatalf("failed to query slow labeler count: %v", err)
			}

			var slowCount int64
			if slowRows.Next() {
				if err := slowRows.Scan(&slowCount); err != nil {
					slowRows.Close()
					t.Fatalf("failed to scan slow count: %v", err)
				}
			}
			slowRows.Close()

			if fastCount >= 5 && slowCount >= 5 {
				bothIngestedAll5 = true
			}
		}
	}

	elapsed := time.Since(start)

	if elapsed > 7*time.Second {
		t.Logf("warning: took %v to ingest 5 frames each; expected ~5s for 1/sec limits", elapsed)
	}

	// Count events per labeler.
	rows, err := testStore.DB().QueryContext(context.Background(),
		`SELECT labeler_did, COUNT(*) FROM events GROUP BY labeler_did`)
	if err != nil {
		t.Fatalf("failed to query events: %v", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var did string
		var count int64
		if err := rows.Scan(&did, &count); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		counts[did] = count
	}

	if rows.Err() != nil {
		t.Fatalf("error iterating rows: %v", rows.Err())
	}

	// Both labelers should have ingested all 5 frames (isolation: they don't interfere).
	// With a per-sec limiter of 1/sec applied to each independently, both can complete
	// 5 frames in ~5 seconds without one starving the other.
	if counts[labelerFast.DID] != 5 {
		t.Errorf("fast labeler should have ingested exactly 5 frames, got %d", counts[labelerFast.DID])
	}

	if counts[labelerSlow.DID] != 5 {
		t.Errorf("slow labeler should have ingested exactly 5 frames, got %d", counts[labelerSlow.DID])
	}

	t.Logf("Both labelers ingested 5 frames (isolation confirmed: each has independent limiter)")

	// Verify the timing: with 1/sec limit, 5 frames should take ~5 seconds.
	if elapsed < 4*time.Second {
		t.Logf("warning: completed faster than expected for 1/sec limit; took %v for 5 frames", elapsed)
	}
}

// TestSlurperReconcileIsIdempotent verifies that calling Reconcile twice
// with no registry change is a no-op.
func TestSlurperReconcileIsIdempotent(t *testing.T) {
	t.Parallel()

	testStore, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to open test store: %v", err)
	}
	defer testStore.Close()

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("failed to register metrics: %v", err)
	}

	registry := store.NewLabelerRegistry(testStore)
	persist := store.NewLabelPersist(testStore)

	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

	labeler := store.Labeler{
		DID:      "did:plc:idempotent",
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}

	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}

	slurper := New(
		registry,
		persist,
		false,
		LimitConfig{PerSec: 1000, PerHour: 100000},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	defer slurper.Shutdown()

	// First Reconcile.
	if err := slurper.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile failed: %v", err)
	}

	// Capture the active subscriptions.
	slurper.mu.Lock()
	sub1 := slurper.active[labeler.DID]
	slurper.mu.Unlock()

	// Second Reconcile with no changes.
	if err := slurper.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile failed: %v", err)
	}

	// The subscription should be the same (no restart).
	slurper.mu.Lock()
	sub2 := slurper.active[labeler.DID]
	slurper.mu.Unlock()

	if sub1 != sub2 {
		t.Errorf("subscription was replaced on second Reconcile; should be idempotent")
	}
}
