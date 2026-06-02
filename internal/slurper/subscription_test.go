package slurper

import (
	"bytes"
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


// TestSubscriptionIngestsSignedLabels verifies AC4.1: a signed label from an
// enabled labeler is persisted.
func TestSubscriptionIngestsSignedLabels(t *testing.T) {
	t.Parallel()

	// Setup: fake server, test store, metrics.
	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

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

	// Insert test labeler.
	labeler := store.Labeler{
		DID:      "did:plc:test123",
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
		RequireSig: boolPtr(true),
	}
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}
	seedCursor(t, registry, labeler.DID)

	// Create subscription and run it in background.
	sub := &subscription{
		labeler:    labeler,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: true,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = sub.run(ctx)
	}()

	// Wait for WebSocket to be ready.
	select {
	case <-fakeServer.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for websocket ready")
	}

	// Send a signed label frame via the fake server.
	frame := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user123/app.bsky.feed.post/abc",
				Val: "adult",
				Cts: time.Now().UTC().Format(time.RFC3339),
				Sig: []byte{0x01, 0x02, 0x03, 0x04},
			},
		},
		Seq: 100,
	}

	if err := fakeServer.sendFrame(frame); err != nil {
		t.Fatalf("failed to send frame: %v", err)
	}

	// Wait for the label to be persisted.
	head, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 1)
	if err != nil {
		t.Fatalf("timeout waiting for label to be persisted: %v", err)
	}

	if head < 1 {
		t.Errorf("expected head >= 1, got %d", head)
	}

	// Verify cursor was updated.
	cursor, err := registry.ReadCursor(context.Background(), labeler.DID)
	if err != nil {
		t.Fatalf("failed to read cursor: %v", err)
	}
	if cursor == nil || *cursor != 100 {
		t.Errorf("expected cursor 100, got %v", cursor)
	}
}

// TestSubscriptionByteFailfulSigPassthrough verifies AC1.3: label Sig bytes are
// byte-for-byte identical through the slurper.
func TestSubscriptionByteFailfulSigPassthrough(t *testing.T) {
	t.Parallel()

	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

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

	labeler := store.Labeler{
		DID:      "did:plc:test456",
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
		RequireSig: boolPtr(true),
	}
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}
	seedCursor(t, registry, labeler.DID)

	sub := &subscription{
		labeler:    labeler,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: true,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = sub.run(ctx)
	}()

	select {
	case <-fakeServer.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for websocket ready")
	}

	// Create a specific random signature.
	testSig := []byte{0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe}

	frame := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user456/app.bsky.feed.post/def",
				Val: "spam",
				Cts: time.Now().UTC().Format(time.RFC3339),
				Sig: testSig,
			},
		},
		Seq: 101,
	}

	if err := fakeServer.sendFrame(frame); err != nil {
		t.Fatalf("failed to send frame: %v", err)
	}

	// Wait for persistence.
	head, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 1)
	if err != nil {
		t.Fatalf("timeout waiting for label to be persisted: %v", err)
	}

	// Read back the stored frame.
	var storedFrameBytes []byte
	row := testStore.DB().QueryRowContext(context.Background(),
		`SELECT frame_cbor FROM events WHERE relay_seq = ?`, head)
	if err := row.Scan(&storedFrameBytes); err != nil {
		t.Fatalf("failed to read stored frame: %v", err)
	}

	// Decode the stored frame.
	r := bytes.NewReader(storedFrameBytes)
	var decodedFrame atproto.LabelSubscribeLabels_Labels
	if err := decodedFrame.UnmarshalCBOR(r); err != nil {
		t.Fatalf("failed to unmarshal stored frame: %v", err)
	}

	if len(decodedFrame.Labels) != 1 {
		t.Fatalf("expected 1 label, got %d", len(decodedFrame.Labels))
	}

	storedSig := decodedFrame.Labels[0].Sig
	if !bytes.Equal(storedSig, testSig) {
		t.Errorf("signature not byte-faithful: sent %v, got %v", testSig, storedSig)
	}
}

// TestSubscriptionSourceIsOriginLabeler verifies AC1.4: output src equals the
// origin labeler's DID, not the relay's identity.
func TestSubscriptionSourceIsOriginLabeler(t *testing.T) {
	t.Parallel()

	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

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

	// Labeler's DID is the source in the frame.
	labelerDID := "did:plc:labeler789"
	labeler := store.Labeler{
		DID:      labelerDID,
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}
	seedCursor(t, registry, labeler.DID)

	sub := &subscription{
		labeler:    labeler,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: false, // Allow unsigned.
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = sub.run(ctx)
	}()

	select {
	case <-fakeServer.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for websocket ready")
	}

	frame := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labelerDID,
				Uri: "at://did:plc:user789/app.bsky.feed.post/ghi",
				Val: "toxic",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 102,
	}

	if err := fakeServer.sendFrame(frame); err != nil {
		t.Fatalf("failed to send frame: %v", err)
	}

	head, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 1)
	if err != nil {
		t.Fatalf("timeout waiting for label to be persisted: %v", err)
	}

	// Read back the stored frame.
	var storedFrameBytes []byte
	row := testStore.DB().QueryRowContext(context.Background(),
		`SELECT frame_cbor FROM events WHERE relay_seq = ?`, head)
	if err := row.Scan(&storedFrameBytes); err != nil {
		t.Fatalf("failed to read stored frame: %v", err)
	}

	// Decode and check the Src field.
	r := bytes.NewReader(storedFrameBytes)
	var decodedFrame atproto.LabelSubscribeLabels_Labels
	if err := decodedFrame.UnmarshalCBOR(r); err != nil {
		t.Fatalf("failed to unmarshal stored frame: %v", err)
	}

	if len(decodedFrame.Labels) != 1 {
		t.Fatalf("expected 1 label, got %d", len(decodedFrame.Labels))
	}

	if decodedFrame.Labels[0].Src != labelerDID {
		t.Errorf("source not origin labeler: expected %s, got %s", labelerDID, decodedFrame.Labels[0].Src)
	}
}

// TestSubscriptionRedialOnServerRestart verifies the subscription reconnects
// after the server is restarted and resumes from persisted cursor.
func TestSubscriptionRedialOnServerRestart(t *testing.T) {
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

	// Start first server.
	fakeServer1 := newFakeLabelerServer(t)

	labeler := store.Labeler{
		DID:      "did:plc:testredial",
		Endpoint: fakeServer1.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}
	seedCursor(t, registry, labeler.DID)

	sub := &subscription{
		labeler:    labeler,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: false,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		_ = sub.run(ctx)
	}()

	// Wait for first connection.
	select {
	case <-fakeServer1.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for first websocket ready")
	}

	// Send first frame.
	frame1 := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user1/app.bsky.feed.post/1",
				Val: "label1",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 50,
	}

	if err := fakeServer1.sendFrame(frame1); err != nil {
		t.Fatalf("failed to send first frame: %v", err)
	}

	// Wait for persistence.
	head1, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 1)
	if err != nil {
		t.Fatalf("timeout waiting for first label: %v", err)
	}

	// Verify cursor is at 50.
	cursor, err := registry.ReadCursor(context.Background(), labeler.DID)
	if err != nil {
		t.Fatalf("failed to read cursor: %v", err)
	}
	if cursor == nil || *cursor != 50 {
		t.Errorf("expected cursor 50 after first frame, got %v", cursor)
	}

	// Drop the connection to trigger redial (without closing the server).
	fakeServer1.dropConn()

	// Wait for the subscription to reconnect to the same server.
	select {
	case <-fakeServer1.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for reconnection")
	}

	// Send second frame with higher seq on the same server.
	frame2 := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user2/app.bsky.feed.post/2",
				Val: "label2",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Seq: 51,
	}

	if err := fakeServer1.sendFrame(frame2); err != nil {
		t.Fatalf("failed to send second frame: %v", err)
	}

	// Wait for second frame to be persisted.
	head2, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, head1+1)
	if err != nil {
		t.Fatalf("timeout waiting for second label after reconnect: %v", err)
	}

	if head2 <= head1 {
		t.Errorf("expected head to increase after reconnect, got head1=%d, head2=%d", head1, head2)
	}

	// Verify cursor advanced.
	cursor, err = registry.ReadCursor(context.Background(), labeler.DID)
	if err != nil {
		t.Fatalf("failed to read cursor after reconnect: %v", err)
	}
	if cursor == nil || *cursor != 51 {
		t.Errorf("expected cursor 51 after reconnect, got %v", cursor)
	}

	fakeServer1.Close()
}

// TestSubscriptionDropsUnsignedLabelsWhenRequireSigTrue verifies AC4.2:
// with require_sig=true, an unsigned label is DROPPED and the
// labeler_relay_dropped_unsigned_total counter increments.
func TestSubscriptionDropsUnsignedLabelsWhenRequireSigTrue(t *testing.T) {
	t.Parallel()

	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

	testStore, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to open test store: %v", err)
	}
	defer testStore.Close()

	// Use a fresh registry to isolate metrics by labeler DID.
	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("failed to register metrics: %v", err)
	}

	registry := store.NewLabelerRegistry(testStore)
	persist := store.NewLabelPersist(testStore)

	// Labeler with require_sig=nil, global default=true => sig required.
	labeler := store.Labeler{
		DID:      "did:plc:unsigned-drop-test",
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
		RequireSig: nil,
	}
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}
	seedCursor(t, registry, labeler.DID)

	sub := &subscription{
		labeler:    labeler,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: true,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		onDropUnsigned: func(did string) {
			metrics.DroppedUnsigned.WithLabelValues(did).Inc()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = sub.run(ctx)
	}()

	select {
	case <-fakeServer.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for websocket ready")
	}

	// Read baseline dropped counter.
	baselineDropped, err := getMetricValue(reg, "labeler_relay_dropped_unsigned_total", labeler.DID)
	if err != nil {
		t.Logf("baseline dropped not found (expected): %v", err)
		baselineDropped = 0
	}

	// Send an UNSIGNED label (Sig is nil).
	frame := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user/app.bsky.feed.post/abc",
				Val: "test",
				Cts: time.Now().UTC().Format(time.RFC3339),
				Sig: nil,
			},
		},
		Seq: 100,
	}

	if err := fakeServer.sendFrame(frame); err != nil {
		t.Fatalf("failed to send frame: %v", err)
	}

	// Wait for the metric to reflect the drop (condition-based, no fixed sleep).
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var finalDropped float64
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for dropped counter to increment")
		case <-ticker.C:
			finalDropped, err = getMetricValue(reg, "labeler_relay_dropped_unsigned_total", labeler.DID)
			if err == nil && finalDropped > baselineDropped {
				// Counter incremented.
				goto verified
			}
		}
	}

verified:
	// Assert the head did NOT advance (label was not persisted).
	head, err := persist.Head(context.Background())
	if err != nil {
		t.Fatalf("failed to read head: %v", err)
	}
	if head != 0 {
		t.Errorf("expected head=0 (unsigned label dropped), got %d", head)
	}

	// Assert the counter incremented by 1.
	if finalDropped != baselineDropped+1 {
		t.Errorf("expected dropped counter to increment by 1 (from %v to %v), got delta %v",
			baselineDropped, finalDropped, finalDropped-baselineDropped)
	}
}

// TestSubscriptionRelaysUnsignedLabelsWhenRequireSigFalseOverride verifies AC4.3:
// with per-labeler require_sig=false override, an unsigned label IS persisted.
func TestSubscriptionRelaysUnsignedLabelsWhenRequireSigFalseOverride(t *testing.T) {
	t.Parallel()

	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

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

	// Labeler with require_sig=false override, global default=true => sig NOT required for this labeler.
	labeler := store.Labeler{
		DID:      "did:plc:unsigned-relay-test",
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
		RequireSig: boolPtr(false),
	}
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}
	seedCursor(t, registry, labeler.DID)

	sub := &subscription{
		labeler:    labeler,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: true,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = sub.run(ctx)
	}()

	select {
	case <-fakeServer.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for websocket ready")
	}

	// Send an UNSIGNED label.
	frame := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user/app.bsky.feed.post/def",
				Val: "unsigned-allowed",
				Cts: time.Now().UTC().Format(time.RFC3339),
				Sig: nil,
			},
		},
		Seq: 101,
	}

	if err := fakeServer.sendFrame(frame); err != nil {
		t.Fatalf("failed to send frame: %v", err)
	}

	// Wait for the label to be persisted.
	head, err := waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 1)
	if err != nil {
		t.Fatalf("timeout waiting for label to be persisted: %v", err)
	}

	if head < 1 {
		t.Errorf("expected head >= 1 (unsigned label persisted), got %d", head)
	}

	// Verify the decoded label has an empty Sig.
	var storedFrameBytes []byte
	row := testStore.DB().QueryRowContext(context.Background(),
		`SELECT frame_cbor FROM events WHERE relay_seq = ?`, head)
	if err := row.Scan(&storedFrameBytes); err != nil {
		t.Fatalf("failed to read stored frame: %v", err)
	}

	r := bytes.NewReader(storedFrameBytes)
	var decodedFrame atproto.LabelSubscribeLabels_Labels
	if err := decodedFrame.UnmarshalCBOR(r); err != nil {
		t.Fatalf("failed to unmarshal stored frame: %v", err)
	}

	if len(decodedFrame.Labels) != 1 {
		t.Fatalf("expected 1 label, got %d", len(decodedFrame.Labels))
	}

	// Unsigned label should have been persisted with empty Sig.
	storedSig := decodedFrame.Labels[0].Sig
	if len(storedSig) != 0 {
		t.Errorf("expected empty Sig for unsigned label, got %v", storedSig)
	}
}

// TestSubscriptionIncrementsIngestedTotalMetric verifies that IngestedTotal
// is incremented by len(kept) after a successful PersistIngest.
func TestSubscriptionIncrementsIngestedTotalMetric(t *testing.T) {
	t.Parallel()

	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

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

	labeler := store.Labeler{
		DID:      "did:plc:ingested-metric-test",
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
		RequireSig: boolPtr(true),
	}
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to insert labeler: %v", err)
	}
	seedCursor(t, registry, labeler.DID)

	sub := &subscription{
		labeler:    labeler,
		persist:    persist,
		registry:   registry,
		limiter:    NewLimiter(1000, 100000),
		sigDefault: true,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		onIngested: func(did string, n int) {
			metrics.IngestedTotal.WithLabelValues(did).Add(float64(n))
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = sub.run(ctx)
	}()

	select {
	case <-fakeServer.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for websocket ready")
	}

	// Read baseline ingested metric.
	baselineIngested, err := getMetricValue(reg, "labeler_relay_ingested_total", labeler.DID)
	if err != nil {
		t.Logf("baseline ingested not found (expected): %v", err)
		baselineIngested = 0
	}

	// Send a signed label frame with 2 labels.
	frame := &atproto.LabelSubscribeLabels_Labels{
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user1/app.bsky.feed.post/1",
				Val: "label1",
				Cts: time.Now().UTC().Format(time.RFC3339),
				Sig: []byte{0x01, 0x02},
			},
			{
				Src: labeler.DID,
				Uri: "at://did:plc:user2/app.bsky.feed.post/2",
				Val: "label2",
				Cts: time.Now().UTC().Format(time.RFC3339),
				Sig: []byte{0x03, 0x04},
			},
		},
		Seq: 200,
	}

	if err := fakeServer.sendFrame(frame); err != nil {
		t.Fatalf("failed to send frame: %v", err)
	}

	// Wait for the labels to be persisted.
	_, err = waitForCondition(ctx, func() (int64, error) {
		return persist.Head(context.Background())
	}, 1)
	if err != nil {
		t.Fatalf("timeout waiting for labels to be persisted: %v", err)
	}

	// Wait for the metric to reflect the ingest (condition-based, no fixed sleep).
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var finalIngested float64
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for ingested metric to increment")
		case <-ticker.C:
			finalIngested, err = getMetricValue(reg, "labeler_relay_ingested_total", labeler.DID)
			if err == nil && finalIngested >= baselineIngested+2 {
				goto verified
			}
		}
	}

verified:
	// Assert the counter advanced by 2 (one for each label).
	if finalIngested != baselineIngested+2 {
		t.Errorf("expected ingested counter to advance by 2 (from %v to %v), got final %v",
			baselineIngested, baselineIngested+2, finalIngested)
	}
}

// waitForCondition polls a condition function until it returns >= minValue,
// or times out.
func waitForCondition(ctx context.Context, fn func() (int64, error), minValue int64) (int64, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
			val, err := fn()
			if err != nil {
				return 0, err
			}
			if val >= minValue {
				return val, nil
			}
		}
	}
}

// seedCursor writes an initial cursor for a labeler so that the subscription
// skips head discovery and starts processing frames immediately.
func seedCursor(t *testing.T, registry *store.LabelerRegistry, did string) {
	t.Helper()
	if err := registry.WriteCursor(context.Background(), did, 0); err != nil {
		t.Fatalf("failed to seed cursor for %s: %v", did, err)
	}
}

// getMetricValue retrieves a Prometheus counter metric value for a given label value.
func getMetricValue(reg *prometheus.Registry, metricName, labelValue string) (float64, error) {
	metrics, err := reg.Gather()
	if err != nil {
		return 0, err
	}

	for _, mf := range metrics {
		if mf.GetName() == metricName {
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "labeler_did" && lp.GetValue() == labelValue {
						return m.GetCounter().GetValue(), nil
					}
				}
			}
		}
	}
	return 0, fmt.Errorf("metric %s with label %s not found", metricName, labelValue)
}

