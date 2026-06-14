// pattern: Imperative Shell
// subscribe.go implements the community.labeler.sync.subscribeLabelers
// WebSocket handler. It composes cursor validation, StreamFrom (backfill+live
// seam), and frame writing into a single HTTP handler.

package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/scarndp/labeler-relay/internal/store"
)

const (
	// defaultBufSize is the per-subscriber live-event buffer. It must be
	// large enough to absorb live events arriving during a typical backfill
	// drain, but small enough to shed genuinely stuck consumers quickly.
	// 512 events at ~200 bytes each ≈ 100 KiB peak per subscriber.
	defaultBufSize = 512

	// wsWriteTimeout bounds how long a single WS write may take. If a client
	// stalls long enough for its socket buffer to fill, writes fail after this
	// deadline rather than blocking the handler goroutine indefinitely.
	wsWriteTimeout = 5 * time.Second

	// wsPingInterval is how often the server sends a ping to each consumer.
	// Must be less than wsPongDeadline + wsPingInterval to avoid spurious
	// timeout: the read deadline is reset to now+pongDeadline on every pong.
	wsPingInterval = 30 * time.Second

	// wsPongDeadline is how long the server waits for a pong reply before
	// declaring the consumer dead. The read pump sets a read deadline of
	// now+pongDeadline; if no pong (or any message) arrives before it
	// expires, ReadMessage returns an error and the read pump cancels ctx.
	wsPongDeadline = 10 * time.Second
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server holds the dependencies needed by the subscription and health handlers.
type Server struct {
	hub                    *Hub
	persist                *store.LabelPersist
	registry               *store.LabelerRegistry
	log                    *slog.Logger
	retentionWindowSeconds int64
	// subBufSize is the per-subscriber event buffer size passed to StreamFrom.
	// Zero means use defaultBufSize. Set to a small value in tests to exercise
	// slow-consumer drop under low event counts.
	subBufSize int
	// writeTimeout overrides wsWriteTimeout for tests. Zero uses wsWriteTimeout.
	writeTimeout time.Duration
	// pingInterval overrides wsPingInterval for tests. Zero uses wsPingInterval.
	pingInterval time.Duration
	// pongDeadline overrides wsPongDeadline for tests. Zero uses wsPongDeadline.
	pongDeadline time.Duration
	// onConnect is called when a WebSocket connection is established.
	// Used to increment consumer counters without importing metrics (FCIS).
	onConnect func()
	// onDisconnect is called when a WebSocket connection is closed.
	// Used to decrement consumer counters without importing metrics (FCIS).
	onDisconnect func()
}

// NewServer constructs a Server with defaultBufSize for subscribers.
func NewServer(hub *Hub, persist *store.LabelPersist, registry *store.LabelerRegistry, log *slog.Logger, retentionWindowSeconds int64) *Server {
	return &Server{
		hub:                    hub,
		persist:                persist,
		registry:               registry,
		log:                    log,
		retentionWindowSeconds: retentionWindowSeconds,
	}
}

// NewServerWithBufSize constructs a Server with custom subscriber buffer size,
// write timeout, and ping/pong intervals. Used in tests. Pass zero for
// pingInterval/pongDeadline to use production defaults.
func NewServerWithBufSize(hub *Hub, persist *store.LabelPersist, registry *store.LabelerRegistry, log *slog.Logger, retentionWindowSeconds int64, subBufSize int, writeTimeout, pingInterval, pongDeadline time.Duration) *Server {
	return &Server{
		hub:                    hub,
		persist:                persist,
		registry:               registry,
		log:                    log,
		retentionWindowSeconds: retentionWindowSeconds,
		subBufSize:             subBufSize,
		writeTimeout:           writeTimeout,
		pingInterval:           pingInterval,
		pongDeadline:           pongDeadline,
	}
}

func (s *Server) effectiveBufSize() int {
	if s.subBufSize > 0 {
		return s.subBufSize
	}
	return defaultBufSize
}

func (s *Server) effectiveWriteTimeout() time.Duration {
	if s.writeTimeout > 0 {
		return s.writeTimeout
	}
	return wsWriteTimeout
}

func (s *Server) effectivePingInterval() time.Duration {
	if s.pingInterval > 0 {
		return s.pingInterval
	}
	return wsPingInterval
}

func (s *Server) effectivePongDeadline() time.Duration {
	if s.pongDeadline > 0 {
		return s.pongDeadline
	}
	return wsPongDeadline
}

// SetConnectCallback registers a function called when a WebSocket connection
// is successfully established. Used to increment consumer counters without
// importing metrics from server (FCIS).
func (s *Server) SetConnectCallback(fn func()) {
	s.onConnect = fn
}

// SetDisconnectCallback registers a function called when a WebSocket connection
// is closed. Used to decrement consumer counters without importing metrics
// from server (FCIS).
func (s *Server) SetDisconnectCallback(fn func()) {
	s.onDisconnect = fn
}

// readPump discards all inbound WebSocket messages and cancels ctx when the
// read fails (close frame, broken pipe, pong deadline exceeded). It also
// resets the read deadline on every pong so the connection stays alive as
// long as the client is responsive.
func (s *Server) readPump(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()

	pongWait := s.effectivePongDeadline()
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// pingLoop sends periodic pings to the client. It exits when ctx is
// cancelled (read pump detected disconnect) or a ping write fails.
func (s *Server) pingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(s.effectivePingInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(s.effectiveWriteTimeout()))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// HandleSubscribeLabelers upgrades the connection to WebSocket and serves the
// unified community.labeler.sync.subscribeLabelers stream.
//
// Flow:
//  1. Parse optional ?cursor= query param.
//  2. Evaluate cursor state (absent → live from head; present → CursorStatus).
//  3. Upgrade to WebSocket.
//  4. Track the number of active consumers for observability (AC10.3).
//  5. Start read pump + ping loop for dead-consumer detection.
//  6. StreamFrom the resolved since value.
//  7. Range over the stream channel writing one frame per event.
//  8. On channel close (slow-consumer drop), send ConsumerTooSlow and exit.
func (s *Server) HandleSubscribeLabelers(w http.ResponseWriter, r *http.Request) {
	cursorParam := r.URL.Query().Get("cursor")

	// Upgrade to WebSocket.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Error("websocket upgrade failed", "err", err)
		return
	}
	defer conn.Close()

	// Derive a context that the read pump cancels on client disconnect.
	// r.Context() is inert after hijack, so this replaces it entirely.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.readPump(ctx, cancel, conn)
	go s.pingLoop(ctx, conn)

	// Track the number of active consumers for observability (AC10.3).
	// Only increment after successful upgrade.
	if s.onConnect != nil {
		s.onConnect()
	}
	defer func() {
		if s.onDisconnect != nil {
			s.onDisconnect()
		}
	}()

	var since int64

	if cursorParam == "" {
		// No cursor: subscribe live from current head. No backfill.
		head, err := s.persist.Head(ctx)
		if err != nil {
			s.log.Error("failed to read head for live subscribe", "err", err)
			return
		}
		since = head
	} else {
		cursor, err := strconv.ParseInt(cursorParam, 10, 64)
		if err != nil {
			writeErr := writeWSFrame(conn, s.effectiveWriteTimeout(), func(w *frameWriter) error {
				return WriteError(w, "InvalidRequest", fmt.Sprintf("invalid cursor: %v", err))
			})
			if writeErr != nil {
				s.log.Debug("failed to write InvalidRequest error frame", "err", writeErr)
			}
			return
		}

		state, err := s.persist.CursorStatus(ctx, cursor)
		if err != nil {
			s.log.Error("failed to evaluate cursor status", "err", err)
			return
		}

		switch state {
		case store.CursorFuture:
			writeErr := writeWSFrame(conn, s.effectiveWriteTimeout(), func(w *frameWriter) error {
				return WriteError(w, "FutureCursor", "cursor is ahead of the current stream head")
			})
			if writeErr != nil {
				s.log.Debug("failed to write FutureCursor error frame", "err", writeErr)
			}
			return

		case store.CursorOutdated:
			floor, err := s.persist.RetentionFloor(ctx)
			if err != nil {
				s.log.Error("failed to read retention floor", "err", err)
				return
			}
			msg := "cursor is below the retention floor; resuming from the oldest available event"
			infoBody, err := store.EncodeInfoFrame("OutdatedCursor", &msg)
			if err != nil {
				s.log.Error("failed to encode OutdatedCursor info frame", "err", err)
				return
			}
			writeErr := writeWSFrame(conn, s.effectiveWriteTimeout(), func(w *frameWriter) error {
				return WriteMessage(w, "#info", infoBody)
			})
			if writeErr != nil {
				s.log.Debug("failed to write OutdatedCursor info frame", "err", writeErr)
				return
			}
			since = floor - 1

		case store.CursorOK:
			since = cursor
		}
	}

	// Subscribe and stream.
	ch, cleanup, err := StreamFrom(ctx, s.persist, s.hub, since, s.effectiveBufSize())
	if err != nil {
		s.log.Error("failed to start stream", "err", err)
		return
	}
	defer cleanup()

	for e := range ch {
		t := kindToMsgType(e.Kind)
		writeErr := writeWSFrame(conn, s.effectiveWriteTimeout(), func(w *frameWriter) error {
			return WriteMessage(w, t, e.FrameCBOR)
		})
		if writeErr != nil {
			s.log.Debug("write failed, closing subscriber", "err", writeErr)
			return
		}
	}

	// Channel closed: either slow-consumer drop from Hub, or context cancelled.
	// Only send ConsumerTooSlow if the context was NOT cancelled (i.e. this was
	// a genuine slow-consumer drop, not a client disconnect).
	if ctx.Err() == nil {
		writeErr := writeWSFrame(conn, s.effectiveWriteTimeout(), func(w *frameWriter) error {
			return WriteError(w, "ConsumerTooSlow", "consumer is too slow to keep up with the stream")
		})
		if writeErr != nil {
			s.log.Debug("failed to write ConsumerTooSlow error frame", "err", writeErr)
		}
	}
}

// kindToMsgType converts a store event Kind to the XRPC frame message type.
func kindToMsgType(kind string) string {
	switch kind {
	case "labels":
		return "#labels"
	case "service":
		return "#service"
	default:
		return "#" + kind
	}
}

// frameWriter wraps io.Writer for use with WriteMessage/WriteError.
type frameWriter struct {
	buf []byte
}

func (f *frameWriter) Write(p []byte) (int, error) {
	f.buf = append(f.buf, p...)
	return len(p), nil
}

// writeWSFrame collects the output of fn and writes it as a single WS binary
// message with the given write deadline applied so a stalled client cannot
// block the handler goroutine indefinitely once the OS socket buffer is full.
func writeWSFrame(conn *websocket.Conn, writeTimeout time.Duration, fn func(w *frameWriter) error) error {
	fw := &frameWriter{}
	if err := fn(fw); err != nil {
		return err
	}
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return conn.WriteMessage(websocket.BinaryMessage, fw.buf)
}
