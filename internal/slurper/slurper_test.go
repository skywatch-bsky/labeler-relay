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

// countEvents returns the number of stored events for a given labeler DID.
func countEvents(t *testing.T, s *store.Store, did string) int64 {
	t.Helper()
	row := s.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM events WHERE labeler_did = ?`, did)
	var n int64
	if err := row.Scan(&n); err != nil {
		t.Fatalf("failed to count events for %s: %v", did, err)
	}
	return n
}

// TestSlurperIsolatesRateLimitingPerLabeler verifies AC7.2: throttling one
// upstream does not starve or block ingest from another. Each subscription
// owns its own Limiter, so a saturated labeler blocks only its own goroutine.
//
// The two subscriptions are built directly with distinct limiters (rather than
// via Reconcile, which applies one shared LimitConfig to every subscription).
// The free labeler must finish all 5 frames while the saturated labeler is
// still throttled below 5 at that same instant — proving the free goroutine
// never waited on the saturated one.
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

	fakeServerSaturated := newFakeLabelerServer(t)
	fakeServerFree := newFakeLabelerServer(t)
	defer fakeServerSaturated.Close()
	defer fakeServerFree.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	labelerSaturated := store.Labeler{
		DID:      "did:plc:saturated",
		Endpoint: fakeServerSaturated.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	labelerFree := store.Labeler{
		DID:      "did:plc:free",
		Endpoint: fakeServerFree.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	if err := registry.Upsert(context.Background(), labelerSaturated); err != nil {
		t.Fatalf("failed to insert saturated labeler: %v", err)
	}
	if err := registry.Upsert(context.Background(), labelerFree); err != nil {
		t.Fatalf("failed to insert free labeler: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Build two subscriptions directly with DISTINCT limiters. The saturated
	// labeler gets 1 token/sec (so 5 frames take ~4s); the free labeler gets a
	// high limit (effectively unthrottled).
	subSaturated := &subscription{
		labeler:    labelerSaturated,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1, 100000),
		sigDefault: false,
		log:        log,
	}
	subFree := &subscription{
		labeler:    labelerFree,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: false,
		log:        log,
	}

	go func() { _ = subSaturated.run(ctx) }()
	go func() { _ = subFree.run(ctx) }()

	// Wait for both upstream connections.
	select {
	case <-fakeServerSaturated.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for saturated server")
	}
	select {
	case <-fakeServerFree.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for free server")
	}

	sendFrames := func(srv *fakeLabelerServer, did, tag string) {
		for i := 1; i <= 5; i++ {
			frame := &atproto.LabelSubscribeLabels_Labels{
				Labels: []*atproto.LabelDefs_Label{
					{
						Src: did,
						Uri: fmt.Sprintf("at://did:plc:user/app.bsky.feed.post/%s%d", tag, i),
						Val: tag,
						Cts: time.Now().UTC().Format(time.RFC3339),
					},
				},
				Seq: int64(i),
			}
			if err := srv.sendFrame(frame); err != nil {
				t.Errorf("failed to send %s frame %d: %v", tag, i, err)
				return
			}
		}
	}

	// Saturate first so its goroutine is busy waiting on its own limiter while
	// the free labeler streams.
	sendFrames(fakeServerSaturated, labelerSaturated.DID, "sat")
	sendFrames(fakeServerFree, labelerFree.DID, "free")

	// Wait until the FREE labeler has ingested all 5 frames. It must not be
	// blocked behind the saturated labeler's throttle.
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for countEvents(t, testStore, labelerFree.DID) < 5 {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout: free labeler did not ingest 5 frames (got %d); isolation broken",
				countEvents(t, testStore, labelerFree.DID))
		case <-ticker.C:
		}
	}

	// At the instant the free labeler finished, the saturated labeler (1/sec)
	// cannot have delivered all 5 — it is still throttled in its own goroutine.
	// This is the isolation proof: throttling one upstream did not block the other.
	satCount := countEvents(t, testStore, labelerSaturated.DID)
	if satCount >= 5 {
		t.Errorf("saturated labeler should still be throttled (<5) when free labeler finished, got %d", satCount)
	}

	t.Logf("isolation confirmed: free labeler ingested 5 while saturated was at %d", satCount)
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

// TestSlurperReconcileIsolatesRateLimitingPerLabeler verifies AC7.2 through
// the real Reconcile wiring: flooding one labeler does not starve another.
// Unlike TestSlurperIsolatesRateLimitingPerLabeler (which builds subscriptions
// directly), this test goes through New(...) and Reconcile, proving that each
// subscription gets its own Limiter at slurper.go:92.
func TestSlurperReconcileIsolatesRateLimitingPerLabeler(t *testing.T) {
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
		DID:      "did:plc:reconcile-saturated",
		Endpoint: fakeServerA.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	labelerB := store.Labeler{
		DID:      "did:plc:reconcile-free",
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

	// Create slurper with a tiny PerSec for labelerA to throttle it;
	// labelerB will be independent because each subscription gets its own Limiter.
	slurper := New(
		registry,
		persist,
		false,
		LimitConfig{PerSec: 2, PerHour: 100000},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	defer slurper.Shutdown()

	// Reconcile to start both subscriptions.
	if err := slurper.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Wait for both upstream connections.
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

	// Send 10 frames from A (saturating at 2/sec).
	for i := 1; i <= 10; i++ {
		frame := &atproto.LabelSubscribeLabels_Labels{
			Labels: []*atproto.LabelDefs_Label{
				{
					Src: labelerA.DID,
					Uri: fmt.Sprintf("at://did:plc:user/app.bsky.feed.post/a%d", i),
					Val: "labelA",
					Cts: time.Now().UTC().Format(time.RFC3339),
				},
			},
			Seq: int64(i),
		}
		if err := fakeServerA.sendFrame(frame); err != nil {
			t.Fatalf("failed to send frameA %d: %v", i, err)
		}
	}

	// Send 3 frames from B (unthrottled at 2/sec with high limit).
	for i := 1; i <= 3; i++ {
		frame := &atproto.LabelSubscribeLabels_Labels{
			Labels: []*atproto.LabelDefs_Label{
				{
					Src: labelerB.DID,
					Uri: fmt.Sprintf("at://did:plc:user/app.bsky.feed.post/b%d", i),
					Val: "labelB",
					Cts: time.Now().UTC().Format(time.RFC3339),
				},
			},
			Seq: int64(i),
		}
		if err := fakeServerB.sendFrame(frame); err != nil {
			t.Fatalf("failed to send frameB %d: %v", i, err)
		}
	}

	// Wait for B to finish all 3 frames while A is throttled.
	// Use condition-based polling (no fixed sleep) to detect when B reaches 3.
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		countB := countEvents(t, testStore, labelerB.DID)
		if countB >= 3 {
			// B finished; now verify A is still behind (isolation proof).
			countA := countEvents(t, testStore, labelerA.DID)
			if countA >= 10 {
				t.Errorf("isolation broken: A should be throttled (<10) when B finishes, but A=%d", countA)
			}
			t.Logf("isolation confirmed through Reconcile: B ingested 3 while A was at %d", countA)
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timeout: B did not ingest 3 frames (got %d); isolation broken",
				countEvents(t, testStore, labelerB.DID))
		case <-ticker.C:
		}
	}
}
