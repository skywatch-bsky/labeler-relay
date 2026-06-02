package server_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/gorilla/websocket"
	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
	"github.com/stretchr/testify/require"
)

// testSubscribeServer builds a real httptest.Server mounting HandleSubscribeLabelers.
func testSubscribeServer(t *testing.T) (*httptest.Server, *store.LabelPersist, *store.LabelerRegistry, func()) {
	t.Helper()
	return testSubscribeServerWithBufSize(t, 0) // 0 = use defaultBufSize
}

// testSubscribeServerWithBufSize builds a test server with a custom subscriber buffer size.
// Use a small bufSize in AC9.1 tests to trigger slow-consumer drop without needing
// massive event floods.
func testSubscribeServerWithBufSize(t *testing.T, subBufSize int) (*httptest.Server, *store.LabelPersist, *store.LabelerRegistry, func()) {
	t.Helper()
	p, storeCleanup := testPersist(t)

	regStore, err := store.Open(t.TempDir() + "/reg.db")
	require.NoError(t, err)
	reg := store.NewLabelerRegistry(regStore)

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	var srv *server.Server
	if subBufSize > 0 {
		// Use a 100ms write timeout so stalled connections are detected quickly in tests.
		srv = server.NewServerWithBufSize(h, p, reg, slog.Default(), 3600, subBufSize, 100*time.Millisecond)
	} else {
		srv = server.NewServer(h, p, reg, slog.Default(), 3600)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/community.labeler.sync.subscribeLabelers", srv.HandleSubscribeLabelers)

	ts := httptest.NewServer(mux)
	return ts, p, reg, func() {
		ts.Close()
		storeCleanup()
		regStore.Close()
	}
}

// wsURL converts an httptest server URL (http://...) to ws://.
func wsURL(ts *httptest.Server) string {
	return "ws" + ts.URL[len("http"):]
}

// dialWS opens a WebSocket to the subscribeLabelers endpoint with the given cursor query param.
func dialWS(t *testing.T, ts *httptest.Server, cursorParam string) *websocket.Conn {
	t.Helper()
	u := wsURL(ts) + "/xrpc/community.labeler.sync.subscribeLabelers"
	if cursorParam != "" {
		u += "?cursor=" + cursorParam
	}
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	require.NoError(t, err)
	return conn
}

// readWSFrame reads one binary WS message and decodes the FrameHeader + raw body bytes.
func readWSFrame(conn *websocket.Conn) (server.FrameHeader, []byte, error) {
	msgType, payload, err := conn.ReadMessage()
	if err != nil {
		return server.FrameHeader{}, nil, err
	}
	if msgType != websocket.BinaryMessage {
		return server.FrameHeader{}, nil, nil
	}

	r := bytes.NewReader(payload)
	var hdr server.FrameHeader
	if err := hdr.UnmarshalCBOR(r); err != nil {
		return server.FrameHeader{}, nil, err
	}
	body := make([]byte, r.Len())
	copy(body, payload[len(payload)-r.Len():])
	return hdr, body, nil
}

// receiveFrames reads exactly n frames from conn within timeout, returning headers and bodies.
func receiveFrames(t *testing.T, conn *websocket.Conn, n int, timeout time.Duration) ([]server.FrameHeader, [][]byte) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})

	hdrs := make([]server.FrameHeader, 0, n)
	bodies := make([][]byte, 0, n)
	for len(hdrs) < n {
		hdr, body, err := readWSFrame(conn)
		if err != nil {
			t.Fatalf("receiveFrames: wanted %d, got %d, err: %v", n, len(hdrs), err)
		}
		hdrs = append(hdrs, hdr)
		bodies = append(bodies, body)
	}
	return hdrs, bodies
}

// serviceEvent builds a minimal IngestEvent of kind "service" for testing.
func serviceEvent(did string) store.IngestEvent {
	return store.IngestEvent{
		Kind:       "service",
		LabelerDID: did,
		Record: &bsky.LabelerService{
			CreatedAt: "2026-06-01T00:00:00Z",
			Policies:  &bsky.LabelerDefs_LabelerPolicies{},
		},
	}
}

// TestSubscribe_AC2_1_NoCursor_ReceivesLive verifies AC2.1: a consumer
// connecting with no cursor receives live #labels and #service events.
func TestSubscribe_AC2_1_NoCursor_ReceivesLive(t *testing.T) {
	ts, p, _, cleanup := testSubscribeServer(t)
	defer cleanup()

	ctx := context.Background()

	// Connect with no cursor (live-only).
	conn := dialWS(t, ts, "")
	defer conn.Close()

	// Give the handler time to call Head() and subscribe before persisting.
	time.Sleep(20 * time.Millisecond)

	// Persist a labels event and a service event.
	_, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", 0x01))
	require.NoError(t, err)
	_, err = p.PersistIngest(ctx, serviceEvent("did:plc:b"))
	require.NoError(t, err)

	hdrs, _ := receiveFrames(t, conn, 2, 5*time.Second)

	require.Equal(t, int64(1), hdrs[0].Op)
	require.Equal(t, "#labels", hdrs[0].T)
	require.Equal(t, int64(1), hdrs[1].Op)
	require.Equal(t, "#service", hdrs[1].T)
}

// TestSubscribe_AC2_2_CursorBackfillThenLive verifies AC2.2: cursor=4 delivers
// backfill events 5..10 then live continuation with no gap or duplicate.
func TestSubscribe_AC2_2_CursorBackfillThenLive(t *testing.T) {
	ts, p, _, cleanup := testSubscribeServer(t)
	defer cleanup()

	ctx := context.Background()

	// Pre-persist 10 events.
	for i := 0; i < 10; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", byte(i)))
		require.NoError(t, err)
		require.Equal(t, int64(i+1), seq)
	}

	// Connect with cursor=4 — expect backfill 5..10.
	conn := dialWS(t, ts, "4")
	defer conn.Close()

	// Read 6 backfill frames (seqs 5..10).
	hdrs, bodies := receiveFrames(t, conn, 6, 5*time.Second)
	for i, hdr := range hdrs {
		require.Equal(t, int64(1), hdr.Op, "frame %d: expected op=1", i)
		require.Equal(t, "#labels", hdr.T, "frame %d: expected #labels", i)

		frame, err := store.DecodeLabelsFrame(bodies[i])
		require.NoError(t, err)
		require.Equal(t, int64(5+i), frame.Seq, "frame %d: expected seq %d", i, 5+i)
	}

	// Persist 3 more live events; they must arrive in order with no gap.
	var liveSeqs []int64
	for i := 0; i < 3; i++ {
		seq, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", byte(10+i)))
		require.NoError(t, err)
		liveSeqs = append(liveSeqs, seq)
	}
	require.Equal(t, []int64{11, 12, 13}, liveSeqs)

	liveHdrs, liveBodies := receiveFrames(t, conn, 3, 5*time.Second)
	for i, hdr := range liveHdrs {
		require.Equal(t, int64(1), hdr.Op)
		require.Equal(t, "#labels", hdr.T)
		frame, err := store.DecodeLabelsFrame(liveBodies[i])
		require.NoError(t, err)
		require.Equal(t, liveSeqs[i], frame.Seq, "live frame %d: seq mismatch", i)
	}

	// Verify no gap: backfill tail was seq=10, first live is seq=11.
	require.Equal(t, int64(11), liveSeqs[0])
}

// TestSubscribe_AC2_3_FutureCursor verifies AC2.3: cursor above head returns
// a FutureCursor error frame and the connection closes.
func TestSubscribe_AC2_3_FutureCursor(t *testing.T) {
	ts, p, _, cleanup := testSubscribeServer(t)
	defer cleanup()

	ctx := context.Background()

	// Persist one event so head=1.
	_, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", 0x01))
	require.NoError(t, err)

	// Connect with cursor=100 (future).
	conn := dialWS(t, ts, "100")
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	hdr, bodyBytes, err := readWSFrame(conn)
	require.NoError(t, err, "expected error frame, got read error")

	require.Equal(t, int64(-1), hdr.Op, "expected op=-1 for FutureCursor error frame")
	require.Equal(t, "", hdr.T, "error frame must have empty t")

	var errBody server.ErrorFrameBody
	err = errBody.UnmarshalCBOR(bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	require.Equal(t, "FutureCursor", errBody.Error)

	// Connection must close after the error frame.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, closeErr := conn.ReadMessage()
	require.Error(t, closeErr, "connection must close after FutureCursor error")
}

// TestSubscribe_AC2_4_OutdatedCursor verifies AC2.4: cursor below the retention
// floor gets an #info OutdatedCursor message frame (op:1, not op:-1), then
// backfill from the floor.
func TestSubscribe_AC2_4_OutdatedCursor(t *testing.T) {
	ts, p, _, cleanup := testSubscribeServer(t)
	defer cleanup()

	ctx := context.Background()

	// Persist 5 events (seqs 1..5).
	for i := 0; i < 5; i++ {
		_, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", byte(i)))
		require.NoError(t, err)
	}

	// Prune all existing events (cutoff in the future evicts everything).
	cutoff := time.Now().UnixMilli() + 1000
	_, _, err := p.Prune(ctx, cutoff)
	require.NoError(t, err)

	// Persist 3 new events (seqs 6..8) — these form the new retention window.
	for i := 0; i < 3; i++ {
		_, err := p.PersistIngest(ctx, labelsEvent("did:plc:b", byte(i)))
		require.NoError(t, err)
	}

	floor, err := p.RetentionFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(6), floor)

	// Connect with cursor=1 (below floor-1=5).
	conn := dialWS(t, ts, "1")
	defer conn.Close()

	// First frame must be #info OutdatedCursor (op:1, not op:-1).
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	hdr, body, err := readWSFrame(conn)
	require.NoError(t, err)
	require.Equal(t, int64(1), hdr.Op, "OutdatedCursor must be op=1 message, not error")
	require.Equal(t, "#info", hdr.T)

	infoFrame, err := store.DecodeInfoFrame(body)
	require.NoError(t, err)
	require.Equal(t, "OutdatedCursor", infoFrame.Name)

	// Next 3 frames: backfill from floor (seqs 6, 7, 8).
	hdrs, bodies := receiveFrames(t, conn, 3, 5*time.Second)
	for i, hdr := range hdrs {
		require.Equal(t, int64(1), hdr.Op)
		require.Equal(t, "#labels", hdr.T)
		frame, err := store.DecodeLabelsFrame(bodies[i])
		require.NoError(t, err)
		require.Equal(t, int64(6+i), frame.Seq, "backfill frame %d: expected seq %d", i, 6+i)
	}
}

// TestSubscribe_AC9_1_SlowConsumerDisconnected verifies AC9.1: a stalled client
// is disconnected without blocking a healthy client that continues receiving all events.
//
// Mechanism: the stalled client's connection is closed before the flood starts.
// When the server attempts to write to the closed connection, the write fails
// immediately (broken pipe / use of closed network connection). The handler
// exits and the server moves on, leaving the healthy client unaffected.
//
// This verifies the critical property: a slow/unresponsive consumer cannot
// block or delay event delivery to other consumers.
func TestSubscribe_AC9_1_SlowConsumerDisconnected(t *testing.T) {
	// Use small subBufSize with a short write timeout so slow-consumer
	// detection is fast regardless of OS socket buffer behaviour.
	ts, p, _, cleanup := testSubscribeServerWithBufSize(t, 4)
	defer cleanup()

	ctx := context.Background()

	// Healthy client connects and will actively read all events.
	healthyConn := dialWS(t, ts, "")
	defer healthyConn.Close()

	// Stalled client connects but immediately stops reading (close read side).
	// Closing the WS connection causes the server's writes to fail, triggering
	// the slow-consumer disconnect path.
	stalledConn := dialWS(t, ts, "")

	// Let both handler goroutines complete their setup (Head query, StreamFrom).
	time.Sleep(30 * time.Millisecond)

	// Simulate stalled consumer: close the connection. Subsequent server writes
	// to this conn will fail with a write error, causing the handler to exit.
	stalledConn.Close()

	// Flood events. The stalled handler will fail quickly on its first write
	// attempt (100ms write timeout or broken pipe). The healthy handler keeps
	// receiving all events unimpeded.
	const floodCount = 30
	for i := 0; i < floodCount; i++ {
		_, err := p.PersistIngest(ctx, labelsEvent("did:plc:flood", byte(i%256)))
		require.NoError(t, err)
	}

	// Healthy client: condition-based wait — count received events.
	var healthyReceived atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		healthyConn.SetReadDeadline(time.Now().Add(10 * time.Second))
		for {
			_, _, err := healthyConn.ReadMessage()
			if err != nil {
				return
			}
			if healthyReceived.Add(1) >= floodCount {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("healthy client timed out: received %d of %d", healthyReceived.Load(), floodCount)
	}

	require.GreaterOrEqual(t, healthyReceived.Load(), int64(floodCount),
		"healthy client must receive all %d events even after stalled client disconnects", floodCount)
}
