// Package integration contains cross-package tests that verify observable
// end-to-end behaviour across multiple internal packages.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/scarndp/labeler-relay/internal/firehose"
	"github.com/scarndp/labeler-relay/internal/slurper"
	"github.com/scarndp/labeler-relay/internal/store"
)

// ---- fake subscribeLabels server (mirrors slurper/testhelpers_test.go) ----

type fakeLabelerServer struct {
	server      *httptest.Server
	mu          sync.Mutex
	wsReady     chan struct{}
	wsConn      *websocket.Conn
	dialCursors []string
	sendFrame   func(*atproto.LabelSubscribeLabels_Labels) error
}

func newFakeLabelerServer(t *testing.T) *fakeLabelerServer {
	t.Helper()
	fls := &fakeLabelerServer{
		wsReady: make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/com.atproto.label.subscribeLabels", func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")

		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		fls.mu.Lock()
		fls.wsConn = ws
		fls.dialCursors = append(fls.dialCursors, cursor)
		wsReady := fls.wsReady
		fls.mu.Unlock()

		select {
		case <-wsReady:
		default:
			close(wsReady)
		}

		for {
			_, _, err := ws.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	fls.server = httptest.NewServer(mux)

	fls.sendFrame = func(frame *atproto.LabelSubscribeLabels_Labels) error {
		fls.mu.Lock()
		defer fls.mu.Unlock()
		if fls.wsConn == nil {
			return fmt.Errorf("websocket not connected")
		}
		var buf bytes.Buffer
		cw := cbg.NewCborWriter(&buf)
		hdr := stream.EventHeader{Op: stream.EvtKindMessage, MsgType: "#labels"}
		if err := hdr.MarshalCBOR(cw); err != nil {
			return fmt.Errorf("marshal header: %w", err)
		}
		if err := frame.MarshalCBOR(cw); err != nil {
			return fmt.Errorf("marshal frame: %w", err)
		}
		return fls.wsConn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	}

	return fls
}

func (fls *fakeLabelerServer) lastDialCursor() string {
	fls.mu.Lock()
	defer fls.mu.Unlock()
	if len(fls.dialCursors) == 0 {
		return ""
	}
	return fls.dialCursors[len(fls.dialCursors)-1]
}

func (fls *fakeLabelerServer) dialCount() int {
	fls.mu.Lock()
	defer fls.mu.Unlock()
	return len(fls.dialCursors)
}

// resetForNextDial drops the current conn and replaces wsReady so the test can
// wait for the subsequent connection.
func (fls *fakeLabelerServer) resetForNextDial() {
	fls.mu.Lock()
	defer fls.mu.Unlock()
	if fls.wsConn != nil {
		fls.wsConn.Close()
		fls.wsConn = nil
	}
	fls.wsReady = make(chan struct{})
}

func (fls *fakeLabelerServer) wsReadyCh() chan struct{} {
	fls.mu.Lock()
	defer fls.mu.Unlock()
	return fls.wsReady
}

func (fls *fakeLabelerServer) Close() {
	fls.mu.Lock()
	if fls.wsConn != nil {
		fls.wsConn.Close()
	}
	fls.mu.Unlock()
	fls.server.Close()
}

// ---- fake subscribeRepos server (firehose watcher) ----
//
// On each connection it records the cursor query param, then sends a minimal
// #commit frame (no ops) so the watcher advances and persists its cursor.

type fakeSubscribeReposServer struct {
	server      *httptest.Server
	mu          sync.Mutex
	wsReady     chan struct{}
	dialCursors []string
	// commitSeqToSend is the Seq to embed in the auto-sent #commit frame.
	// Set to 0 to skip auto-send.
	commitSeqToSend int64
}

func newFakeSubscribeReposServer(t *testing.T, commitSeq int64) *fakeSubscribeReposServer {
	t.Helper()
	fs := &fakeSubscribeReposServer{
		wsReady:         make(chan struct{}),
		commitSeqToSend: commitSeq,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/com.atproto.sync.subscribeRepos", func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")

		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		fs.mu.Lock()
		fs.dialCursors = append(fs.dialCursors, cursor)
		wsReady := fs.wsReady
		seq := fs.commitSeqToSend
		fs.mu.Unlock()

		select {
		case <-wsReady:
		default:
			close(wsReady)
		}

		// Send a minimal #commit frame so the watcher processes it and persists the cursor.
		if seq > 0 {
			if err := sendMinimalCommit(ws, seq); err != nil {
				return
			}
		}

		// Keep alive until client disconnects.
		for {
			_, _, err := ws.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	fs.server = httptest.NewServer(mux)
	return fs
}

// sendMinimalCommit sends a #commit frame with no ops — enough for the watcher
// to persist the cursor (ExtractLabelerServiceOps returns nil on empty ops).
// A valid CID is required for the Commit field; we generate a synthetic one.
func sendMinimalCommit(ws *websocket.Conn, seq int64) error {
	pref := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256)
	fakeCID, err := pref.Sum([]byte("fake-commit"))
	if err != nil {
		return fmt.Errorf("compute CID: %w", err)
	}
	lexCID := lexutil.LexLink(fakeCID)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   "did:plc:unused",
		Seq:    seq,
		Rev:    "aaaaaaaaa",
		Commit: lexCID,
		// Ops and Blocks intentionally empty — no labeler ops to extract.
	}

	var buf bytes.Buffer
	cw := cbg.NewCborWriter(&buf)
	hdr := stream.EventHeader{Op: stream.EvtKindMessage, MsgType: "#commit"}
	if err := hdr.MarshalCBOR(cw); err != nil {
		return err
	}
	if err := commit.MarshalCBOR(cw); err != nil {
		return err
	}
	return ws.WriteMessage(websocket.BinaryMessage, buf.Bytes())
}

func (fs *fakeSubscribeReposServer) lastDialCursor() string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.dialCursors) == 0 {
		return ""
	}
	return fs.dialCursors[len(fs.dialCursors)-1]
}

func (fs *fakeSubscribeReposServer) dialCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.dialCursors)
}

func (fs *fakeSubscribeReposServer) wsReadyCh() chan struct{} {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.wsReady
}

func (fs *fakeSubscribeReposServer) Close() {
	fs.server.Close()
}

// ---- helpers ----

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func openTempStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir() + "/relay.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// waitForCond polls cond (every 20ms up to timeoutMs) until it returns true.
func waitForCond(t *testing.T, timeoutMs int, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout after %dms waiting for: %s", timeoutMs, msg)
}

// fakeDIDResolver for the firehose watcher.
type fakeDIDResolver struct{ endpoint string }

func (f *fakeDIDResolver) LabelerEndpoint(_ context.Context, _ string) (string, error) {
	return f.endpoint, nil
}

// ---- tests ----

// TestResumeSlurper verifies AC10.1: after a slurper is stopped, a new slurper
// over the same store dials the upstream with ?cursor=<persisted_seq>.
func TestResumeSlurper(t *testing.T) {
	t.Parallel()

	const (
		did      = "did:plc:resume-slurper"
		firstSeq = int64(42)
	)

	fakeServer := newFakeLabelerServer(t)
	defer fakeServer.Close()

	s := openTempStore(t)
	registry := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	if err := registry.Upsert(context.Background(), store.Labeler{
		DID:      did,
		Endpoint: fakeServer.server.URL + "/xrpc/com.atproto.label.subscribeLabels",
		Source:   "test",
		Enabled:  true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// --- Phase 1: run slurper, ingest up to upstream_seq=42 ---

	slurper1 := slurper.New(
		registry, persist,
		false,
		slurper.LimitConfig{PerSec: 1000, PerHour: 100000},
		discardLog(),
	)

	ctx1, cancel1 := context.WithCancel(context.Background())

	if err := slurper1.Reconcile(ctx1); err != nil {
		cancel1()
		t.Fatalf("Reconcile: %v", err)
	}

	// Wait for first connection.
	waitForCond(t, 5000, func() bool {
		select {
		case <-fakeServer.wsReadyCh():
			return true
		default:
			return false
		}
	}, "first slurper connection")

	// Drain the wsReady signal so resetForNextDial starts fresh.
	select {
	case <-fakeServer.wsReadyCh():
	default:
	}

	// Send one label at seq=42.
	if err := fakeServer.sendFrame(&atproto.LabelSubscribeLabels_Labels{
		Seq: firstSeq,
		Labels: []*atproto.LabelDefs_Label{
			{
				Src: did,
				Uri: "at://did:plc:user/app.bsky.feed.post/1",
				Val: "test",
				Cts: time.Now().UTC().Format(time.RFC3339),
			},
		},
	}); err != nil {
		cancel1()
		t.Fatalf("sendFrame: %v", err)
	}

	// Wait for the cursor to be persisted (at-least-once: we check it equals firstSeq).
	waitForCond(t, 5000, func() bool {
		c, err := registry.ReadCursor(context.Background(), did)
		return err == nil && c != nil && *c == firstSeq
	}, fmt.Sprintf("cursor persisted at %d", firstSeq))

	// Simulate crash: cancel + shutdown.
	cancel1()
	slurper1.Shutdown()

	// Reset server for next dial detection.
	fakeServer.resetForNextDial()
	prevDialCount := fakeServer.dialCount()

	// --- Phase 2: new slurper over same store, same registry ---

	slurper2 := slurper.New(
		registry, persist,
		false,
		slurper.LimitConfig{PerSec: 1000, PerHour: 100000},
		discardLog(),
	)
	defer slurper2.Shutdown()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := slurper2.Reconcile(ctx2); err != nil {
		t.Fatalf("Reconcile2: %v", err)
	}
	go func() { _ = slurper2.Run(ctx2) }()

	// Wait for new connection.
	waitForCond(t, 8000, func() bool {
		select {
		case <-fakeServer.wsReadyCh():
			return true
		default:
			return false
		}
	}, "second slurper connection")

	waitForCond(t, 3000, func() bool {
		return fakeServer.dialCount() > prevDialCount
	}, "dial count advance after reconnect")

	got := fakeServer.lastDialCursor()
	want := fmt.Sprintf("%d", firstSeq)
	if got != want {
		t.Errorf("AC10.1: second slurper dialled cursor=%q, want %q (no gap after restart)", got, want)
	}
}

// TestResumeFirehose verifies AC10.2: after a watcher is stopped, a new watcher
// over the same store dials subscribeRepos with the persisted cursor.
func TestResumeFirehose(t *testing.T) {
	t.Parallel()

	const firehoseSeq = int64(77)

	s := openTempStore(t)
	registry := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	resolver := &fakeDIDResolver{
		endpoint: "https://labeler.example.com/xrpc/com.atproto.label.subscribeLabels",
	}

	// --- Phase 1: stand up a fake subscribeRepos server, run watcher, wait for
	// the #commit (seq=77) to be processed and the cursor to be persisted. ---

	fakeServer1 := newFakeSubscribeReposServer(t, firehoseSeq)
	defer fakeServer1.Close()

	watcher1 := firehose.NewFirehoseWatcher(
		"ws://"+fakeServer1.server.Listener.Addr().String()+"/xrpc/com.atproto.sync.subscribeRepos",
		registry, persist, resolver, s,
		func() {},
		discardLog(),
	)

	ctx1, cancel1 := context.WithCancel(context.Background())

	var watcher1Err atomic.Value
	go func() {
		if err := watcher1.Run(ctx1); err != nil && err != context.Canceled {
			watcher1Err.Store(err)
		}
	}()

	// Wait for fakeServer1 to see the connection.
	waitForCond(t, 5000, func() bool {
		select {
		case <-fakeServer1.wsReadyCh():
			return true
		default:
			return false
		}
	}, "watcher1 connects to fakeServer1")

	// Wait for cursor to be persisted (watcher processes the auto-sent #commit).
	waitForCond(t, 5000, func() bool {
		cursorStr, found, err := s.GetMeta(context.Background(), "firehose_cursor")
		return err == nil && found && cursorStr == fmt.Sprintf("%d", firehoseSeq)
	}, "firehose cursor persisted")

	// Simulate crash: cancel watcher1.
	cancel1()

	// --- Phase 2: new watcher over same store, new fake subscribeRepos server ---

	fakeServer2 := newFakeSubscribeReposServer(t, 0) // no auto-send; we just check the dial cursor
	defer fakeServer2.Close()

	watcher2 := firehose.NewFirehoseWatcher(
		"ws://"+fakeServer2.server.Listener.Addr().String()+"/xrpc/com.atproto.sync.subscribeRepos",
		store.NewLabelerRegistry(s), store.NewLabelPersist(s), resolver, s,
		func() {},
		discardLog(),
	)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	var watcher2Err atomic.Value
	go func() {
		if err := watcher2.Run(ctx2); err != nil && err != context.Canceled {
			watcher2Err.Store(err)
		}
	}()

	// Wait for watcher2 to dial fakeServer2.
	waitForCond(t, 5000, func() bool {
		select {
		case <-fakeServer2.wsReadyCh():
			return true
		default:
			return false
		}
	}, "watcher2 connects to fakeServer2")

	waitForCond(t, 3000, func() bool {
		return fakeServer2.dialCount() > 0
	}, "fakeServer2 dial count > 0")

	got := fakeServer2.lastDialCursor()
	want := fmt.Sprintf("%d", firehoseSeq)
	if got != want {
		t.Errorf("AC10.2: watcher2 dialled cursor=%q, want %q (no gap after restart)", got, want)
	}

	if v := watcher1Err.Load(); v != nil {
		t.Errorf("watcher1 unexpected error: %v", v)
	}
	if v := watcher2Err.Load(); v != nil {
		t.Errorf("watcher2 unexpected error: %v", v)
	}
}
