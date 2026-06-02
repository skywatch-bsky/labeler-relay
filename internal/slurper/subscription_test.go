package slurper

import (
	"bytes"
	"context"
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

	// Close first server to trigger redial.
	fakeServer1.Close()

	// Wait a bit for the subscription to detect the disconnect.
	time.Sleep(500 * time.Millisecond)

	// Start second server with same endpoint.
	fakeServer2 := newFakeLabelerServer(t)
	labeler.Endpoint = fakeServer2.server.URL + "/xrpc/com.atproto.label.subscribeLabels"
	if err := registry.Upsert(context.Background(), labeler); err != nil {
		t.Fatalf("failed to update labeler endpoint: %v", err)
	}

	// Update subscription endpoint.
	sub.labeler.Endpoint = labeler.Endpoint

	// Wait for second connection (reconnect should happen within a few seconds).
	select {
	case <-fakeServer2.wsReady:
	case <-ctx.Done():
		t.Fatalf("timeout waiting for reconnection")
	}

	// Send second frame with higher seq.
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

	if err := fakeServer2.sendFrame(frame2); err != nil {
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

	fakeServer2.Close()
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

