package slurper

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/gorilla/websocket"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// fakeLabelerServer is a test fixture that emits subscribeLabels frames via WebSocket.
type fakeLabelerServer struct {
	server    *httptest.Server
	mu        sync.Mutex
	wsReady   chan struct{}
	wsConn    *websocket.Conn
	emitErrs  chan error
	sendFrame func(*atproto.LabelSubscribeLabels_Labels) error
}

func newFakeLabelerServer(t *testing.T) *fakeLabelerServer {
	fls := &fakeLabelerServer{
		wsReady:  make(chan struct{}),
		emitErrs: make(chan error, 10),
	}

	// HTTP server with WebSocket handler.
	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/com.atproto.label.subscribeLabels", func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fls.emitErrs <- fmt.Errorf("upgrade failed: %w", err)
			return
		}
		fls.mu.Lock()
		fls.wsConn = ws
		wsReady := fls.wsReady
		fls.mu.Unlock()

		// Close wsReady once using select pattern to avoid panic.
		select {
		case <-wsReady:
			// Already closed, no-op
		default:
			close(wsReady)
		}

		// Keep the connection open until it's closed.
		// Read loop to detect client disconnect.
		for {
			_, _, err := ws.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					fls.emitErrs <- fmt.Errorf("websocket error: %w", err)
				}
				return
			}
		}
	})
	fls.server = httptest.NewServer(mux)

	// Helper to send frames.
	fls.sendFrame = func(frame *atproto.LabelSubscribeLabels_Labels) error {
		fls.mu.Lock()
		defer fls.mu.Unlock()
		if fls.wsConn == nil {
			return fmt.Errorf("websocket not connected")
		}

		// Emit the two-part DAG-CBOR stream frame indigo's HandleRepoStream
		// expects in a single binary message: an EventHeader{op,t} object
		// followed by the body object, both written to the same CBOR writer.
		// indigo's own XRPCStreamEvent.Serialize omits a #labels case, so the
		// header is written explicitly here rather than via Serialize.
		var buf bytes.Buffer
		cw := cbg.NewCborWriter(&buf)
		header := stream.EventHeader{Op: stream.EvtKindMessage, MsgType: "#labels"}
		if err := header.MarshalCBOR(cw); err != nil {
			return fmt.Errorf("failed to marshal event header: %w", err)
		}
		if err := frame.MarshalCBOR(cw); err != nil {
			return fmt.Errorf("failed to marshal label frame: %w", err)
		}

		// Send as binary message.
		return fls.wsConn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	}

	return fls
}

func (fls *fakeLabelerServer) dropConn() {
	fls.mu.Lock()
	defer fls.mu.Unlock()
	if fls.wsConn != nil {
		fls.wsConn.Close()
	}
	fls.wsConn = nil
	// Reset wsReady for the reconnect.
	select {
	case <-fls.wsReady:
		// Already closed, create a new one.
		fls.wsReady = make(chan struct{})
	default:
		// Not closed yet, just close it.
		close(fls.wsReady)
		fls.wsReady = make(chan struct{})
	}
}

func (fls *fakeLabelerServer) Close() {
	fls.mu.Lock()
	if fls.wsConn != nil {
		fls.wsConn.Close()
	}
	fls.mu.Unlock()
	fls.server.Close()
}
