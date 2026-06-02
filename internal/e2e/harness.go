// pattern: Imperative Shell
// harness.go provides an in-process test harness that boots the full relay
// stack on a free port with a temporary database, exposes methods to register
// labelers via the admin API, and decodes frames from the relay's own
// community.labeler.sync.subscribeLabelers WebSocket output.

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/scarndp/labeler-relay/api/community"
	"github.com/scarndp/labeler-relay/internal/admin"
	"github.com/scarndp/labeler-relay/internal/firehose"
	"github.com/scarndp/labeler-relay/internal/metrics"
	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/slurper"
	"github.com/scarndp/labeler-relay/internal/store"
)

const (
	testAdminToken = "e2e-harness-token"

	// wsDialTimeout bounds how long we wait for the initial WebSocket dial.
	wsDialTimeout = 5 * time.Second

	// collectTimeout is the default bounded timeout for Collect/CollectAny.
	collectTimeout = 30 * time.Second
)

// CollectedLabel is a single #labels entry collected from the relay output.
type CollectedLabel struct {
	// RelaySeq is the relay-minted sequence number from the #labels frame.
	RelaySeq int64
	// OutputSrc is the frame-level src field (the labeler DID the relay
	// attributes this batch to).
	OutputSrc string
	// Label is the individual label from the batch.
	Label *comatproto.LabelDefs_Label
}

// Frame is a single decoded frame from the relay output stream.
type Frame struct {
	// Op is the frame operation: 1 for message, -1 for error.
	Op int64
	// T is the message type for op=1 frames: "#labels", "#service", "#info".
	T string
	// Labels is non-nil when T == "#labels".
	Labels *community.LabelerSyncSubscribeLabelers_Labels
	// Service is non-nil when T == "#service".
	Service *community.LabelerSyncSubscribeLabelers_Service
	// Info is non-nil when T == "#info".
	Info *community.LabelerSyncSubscribeLabelers_Info
	// ErrorName and ErrorMessage are set when Op == -1.
	ErrorName    string
	ErrorMessage string
}

// Harness holds a running relay and provides test helpers.
type Harness struct {
	addr      string
	cancel    context.CancelFunc
	done      chan error
	closeOnce sync.Once
	t         *testing.T
}

// NewHarness boots a full relay stack on a free port with a temporary
// SQLite database and returns a ready-to-use Harness. The relay's firehose
// watcher is pointed at a non-existent address so it never dials the real
// network — labelers are registered exclusively via the admin API.
//
// Call h.Close() (or use t.Cleanup) to shut down the relay.
func NewHarness(t *testing.T) (*Harness, error) {
	t.Helper()

	addr, err := freeLocalAddr()
	if err != nil {
		return nil, fmt.Errorf("failed to find free port: %w", err)
	}

	dbPath := t.TempDir() + "/e2e.db"

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runRelay(ctx, addr, dbPath, testAdminToken)
	}()

	h := &Harness{
		addr:   addr,
		cancel: cancel,
		done:   done,
		t:      t,
	}

	// Condition-based wait: poll /_health until 200 or deadline.
	if err := h.waitReady(5 * time.Second); err != nil {
		cancel()
		return nil, fmt.Errorf("relay did not become ready: %w", err)
	}

	t.Cleanup(h.Close)
	return h, nil
}

// Addr returns the "host:port" the relay is listening on.
func (h *Harness) Addr() string {
	return h.addr
}

// AdminToken returns the admin API bearer token for this harness.
func (h *Harness) AdminToken() string {
	return testAdminToken
}

// Close shuts down the relay and waits for it to stop. It is idempotent and
// safe to call multiple times (e.g. from both defer and t.Cleanup).
func (h *Harness) Close() {
	h.closeOnce.Do(func() {
		h.cancel()
		select {
		case <-h.done:
		case <-time.After(10 * time.Second):
			h.t.Log("warning: relay did not shut down within 10 seconds")
		}
	})
}

// RegisterLabeler adds a labeler to the relay via the admin API. endpoint is
// the full URL of the labeler's subscribeLabels endpoint
// (e.g. "https://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels").
// The admin API resolves the labeler endpoint from the DID; pass endpoint=""
// to use a resolver-backed DID, or wire a mock resolver by controlling the
// relay's DID resolver (not yet exposed — see RegisterLabelerWithEndpoint
// if you need to override endpoint resolution in tests).
func (h *Harness) RegisterLabeler(did string) error {
	body, err := json.Marshal(map[string]string{"did": did})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost,
		"http://"+h.addr+"/admin/labelers",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testAdminToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("admin POST /admin/labelers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var errBody map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("register labeler %s: HTTP %d: %v", did, resp.StatusCode, errBody)
	}
	return nil
}

// Collect waits until at least n CollectedLabels have arrived on the relay
// output, then returns them. It uses condition-based waiting with a bounded
// timeout; if fewer than n labels arrive before the deadline, it returns an
// error with a clear message. The WebSocket connection is established fresh
// on each call (cursor=0 to receive all stored events plus live events).
func (h *Harness) Collect(ctx context.Context, n int) ([]CollectedLabel, error) {
	return h.CollectWithCursor(ctx, n, 0)
}

// CollectWithCursor is like Collect but with an explicit cursor value.
func (h *Harness) CollectWithCursor(ctx context.Context, n int, cursor int64) ([]CollectedLabel, error) {
	wsURL := fmt.Sprintf("ws://%s/xrpc/community.labeler.sync.subscribeLabelers?cursor=%d",
		h.addr, cursor)

	dialCtx, dialCancel := context.WithTimeout(ctx, wsDialTimeout)
	defer dialCancel()

	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial relay WebSocket: %w", err)
	}
	defer conn.Close()

	var collected []CollectedLabel
	timeoutAt := time.Now().Add(collectTimeout)

	for len(collected) < n {
		if time.Now().After(timeoutAt) {
			return nil, fmt.Errorf("collect: timeout waiting for %d labels: got %d after %v",
				n, len(collected), collectTimeout)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if isTimeout(err) {
				continue
			}
			return nil, fmt.Errorf("collect: WebSocket read error: %w", err)
		}

		frame, err := decodeFrame(msg)
		if err != nil {
			return nil, fmt.Errorf("collect: frame decode error: %w", err)
		}

		if frame.T == "#labels" && frame.Labels != nil {
			for _, lbl := range frame.Labels.Labels {
				collected = append(collected, CollectedLabel{
					RelaySeq:  frame.Labels.Seq,
					OutputSrc: frame.Labels.Src,
					Label:     lbl,
				})
			}
		}
	}

	return collected, nil
}

// CollectAny collects n frames of any type from the relay output stream.
// It uses condition-based waiting with a bounded timeout. Like Collect, it
// opens a fresh WebSocket connection from cursor=0.
func (h *Harness) CollectAny(ctx context.Context, n int) ([]Frame, error) {
	wsURL := fmt.Sprintf("ws://%s/xrpc/community.labeler.sync.subscribeLabelers?cursor=0",
		h.addr)

	dialCtx, dialCancel := context.WithTimeout(ctx, wsDialTimeout)
	defer dialCancel()

	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial relay WebSocket: %w", err)
	}
	defer conn.Close()

	var frames []Frame
	timeoutAt := time.Now().Add(collectTimeout)

	for len(frames) < n {
		if time.Now().After(timeoutAt) {
			return nil, fmt.Errorf("collect-any: timeout waiting for %d frames: got %d after %v",
				n, len(frames), collectTimeout)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if isTimeout(err) {
				continue
			}
			return nil, fmt.Errorf("collect-any: WebSocket read error: %w", err)
		}

		frame, err := decodeFrame(msg)
		if err != nil {
			return nil, fmt.Errorf("collect-any: frame decode error: %w", err)
		}

		frames = append(frames, *frame)
	}

	return frames, nil
}

// decodeFrame decodes a single WebSocket binary message into a Frame.
// The wire format is: CBOR-encoded FrameHeader concatenated with a
// CBOR-encoded body. The header's T field discriminates the body type;
// op=-1 indicates an error frame.
func decodeFrame(msg []byte) (*Frame, error) {
	r := bytes.NewReader(msg)

	var hdr server.FrameHeader
	if err := hdr.UnmarshalCBOR(r); err != nil {
		return nil, fmt.Errorf("decode frame header: %w", err)
	}

	f := &Frame{Op: hdr.Op, T: hdr.T}

	if hdr.Op == -1 {
		var errBody server.ErrorFrameBody
		if err := errBody.UnmarshalCBOR(r); err != nil {
			return nil, fmt.Errorf("decode error frame body: %w", err)
		}
		f.ErrorName = errBody.Error
		f.ErrorMessage = errBody.Message
		return f, nil
	}

	switch hdr.T {
	case "#labels":
		body := &community.LabelerSyncSubscribeLabelers_Labels{}
		if err := body.UnmarshalCBOR(r); err != nil {
			return nil, fmt.Errorf("decode #labels body: %w", err)
		}
		f.Labels = body
	case "#service":
		body := &community.LabelerSyncSubscribeLabelers_Service{}
		if err := body.UnmarshalCBOR(r); err != nil {
			return nil, fmt.Errorf("decode #service body: %w", err)
		}
		f.Service = body
	case "#info":
		body := &community.LabelerSyncSubscribeLabelers_Info{}
		if err := body.UnmarshalCBOR(r); err != nil {
			return nil, fmt.Errorf("decode #info body: %w", err)
		}
		f.Info = body
	default:
		// Unknown frame type — still valid, just no typed body.
	}

	return f, nil
}

// waitReady polls /_health until it returns 200 or the deadline elapses.
func (h *Harness) waitReady(timeout time.Duration) error {
	healthURL := "http://" + h.addr + "/_health"
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("/_health did not respond 200 within %v", timeout)
}

// runRelay wires and starts the full relay stack on addr with the given db and
// admin token. It mirrors cmd/labeler-relay/main.go's run() exactly, but
// accepts explicit configuration to keep the test environment deterministic.
// The firehose watcher is pointed at an unreachable address so it never
// connects to the real network.
func runRelay(ctx context.Context, addr, dbPath, adminToken string) error {
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	st, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer st.Close()

	persist := store.NewLabelPersist(st)
	registry := store.NewLabelerRegistry(st)

	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		return fmt.Errorf("failed to register metrics: %w", err)
	}

	hub := server.NewHub()
	persist.SetBroadcaster(hub.Broadcast)

	sl := slurper.New(
		registry,
		persist,
		false,
		slurper.LimitConfig{PerSec: 1000, PerHour: 100_000},
		log,
	)

	poke := func() {
		go func() {
			if err := sl.Reconcile(ctx); err != nil && err != context.Canceled {
				log.Debug("poke reconcile failed", "err", err)
			}
		}()
	}

	// Use the real indigo identity resolver for DID resolution when labelers
	// are registered via the admin API. The e2e harness doesn't hit the
	// firehose, so DID resolution only happens during RegisterLabeler calls.
	baseDir := &identity.BaseDirectory{}
	resolver := firehose.NewIndigoResolver(baseDir)

	// Point the firehose watcher at an unreachable address — it will retry in
	// the background but never succeed, keeping the test bounded to labelers
	// registered via the admin API.
	fw := firehose.NewFirehoseWatcher(
		"ws://127.0.0.1:1/nope",
		registry,
		persist,
		resolver,
		st,
		poke,
		log,
	)

	adminAPI := admin.NewAPI(registry, resolver, poke, adminToken)

	srv := server.NewServer(hub, persist, registry, log, int64((336 * time.Hour).Seconds()))

	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/community.labeler.sync.subscribeLabelers", srv.HandleSubscribeLabelers)
	mux.HandleFunc("/_health", srv.HandleHealth)
	mux.Handle("/metrics", server.MetricsHandler(reg))
	mux.Handle("/admin/", adminAPI.Routes())

	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	pruneJob := store.NewPruneJob(
		persist,
		336*time.Hour,
		10*time.Minute,
		nil,
		log,
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutCtx)
	})

	g.Go(func() error {
		if err := sl.Run(gctx); err != nil && err != context.Canceled {
			return fmt.Errorf("slurper: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := fw.Run(gctx); err != nil && err != context.Canceled {
			return fmt.Errorf("firehose watcher: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := pruneJob.Run(gctx); err != nil && err != context.Canceled {
			return fmt.Errorf("prune job: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := sl.Reconcile(gctx); err != nil && err != context.Canceled {
			log.Debug("initial reconcile failed", "err", err)
		}
		return nil
	})

	return g.Wait()
}

// freeLocalAddr returns a "127.0.0.1:PORT" string for an available TCP port.
func freeLocalAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	l.Close()
	return addr, nil
}

// isTimeout reports whether err is a network timeout.
func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "timeout") ||
		strings.Contains(err.Error(), "deadline exceeded")
}
