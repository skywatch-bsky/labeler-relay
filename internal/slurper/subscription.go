// pattern: Imperative Shell

package slurper

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/url"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/scarndp/labeler-relay/internal/store"
)

// subscription owns one labeler's upstream connection lifecycle and frame handling.
type subscription struct {
	labeler         store.Labeler
	persist         *store.LabelPersist
	registry        *store.LabelerRegistry
	limiter         *Limiter
	sigDefault      bool
	log             *slog.Logger
	onDropUnsigned  func(did string)       // called when a label is dropped for missing signature; may be nil
	onIngested      func(did string, n int) // called after a successful PersistIngest with the count kept; may be nil
}

// run starts the subscription loop with redial + backoff. It blocks until ctx is cancelled.
func (s *subscription) run(ctx context.Context) error {
	backoffMs := 100
	const maxBackoffMs = 30000

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := s.dial(ctx)
		if err == context.Canceled || err == context.DeadlineExceeded {
			return err
		}

		if err != nil {
			s.log.Error("subscription dial failed", "labeler", s.labeler.DID, "err", err)
		}

		// Compute next backoff with jitter.
		maxJitter := int(float64(backoffMs) * 0.1)
		if maxJitter < 1 {
			maxJitter = 1
		}
		jitter := rand.Intn(maxJitter)
		sleepMs := backoffMs + jitter
		if sleepMs > maxBackoffMs {
			sleepMs = maxBackoffMs
		}

		// Sleep with context awareness.
		select {
		case <-time.After(time.Duration(sleepMs) * time.Millisecond):
			// Exponential backoff.
			backoffMs = int(math.Min(float64(backoffMs)*2, float64(maxBackoffMs)))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// dial establishes a WebSocket connection and processes frames via indigo's HandleRepoStream.
func (s *subscription) dial(ctx context.Context) error {
	// Read the current cursor.
	lastSeq, err := s.registry.ReadCursor(ctx, s.labeler.DID)
	if err != nil {
		return fmt.Errorf("failed to read cursor: %w", err)
	}

	// No stored cursor: this is a first-time connection. Skip historical
	// backfill by connecting live-only to discover the current head seq,
	// then persisting it as the cursor so subsequent dials resume from here.
	if lastSeq == nil {
		headSeq, err := s.discoverHead(ctx)
		if err != nil {
			return fmt.Errorf("failed to discover head: %w", err)
		}
		if err := s.registry.WriteCursor(ctx, s.labeler.DID, headSeq); err != nil {
			return fmt.Errorf("failed to write initial cursor: %w", err)
		}
		s.log.Info("skipped backfill, starting live", "labeler", s.labeler.DID, "cursor", headSeq)
		lastSeq = &headSeq
	}

	return s.dialFrom(ctx, lastSeq)
}

// discoverHead connects briefly to the upstream labeler with no cursor,
// reads one frame to learn the current seq, and disconnects. This avoids
// replaying the labeler's entire history on first subscription.
func (s *subscription) discoverHead(ctx context.Context) (int64, error) {
	u, err := url.Parse(s.labeler.Endpoint)
	if err != nil {
		return 0, fmt.Errorf("invalid endpoint URL: %w", err)
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else if u.Scheme == "https" {
		u.Scheme = "wss"
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return 0, fmt.Errorf("failed to dial for head discovery: %w", err)
	}
	defer conn.Close()

	var headSeq int64
	found := make(chan struct{})

	sched := sequential.NewScheduler(s.labeler.DID, func(ctx context.Context, evt *stream.XRPCStreamEvent) error {
		cb := &stream.RepoStreamCallbacks{
			LabelLabels: func(evt *atproto.LabelSubscribeLabels_Labels) error {
				headSeq = evt.Seq
				close(found)
				return fmt.Errorf("head discovered")
			},
			LabelInfo: func(evt *atproto.LabelSubscribeLabels_Info) error {
				return nil
			},
			Error: func(evt *stream.ErrorFrame) error {
				return fmt.Errorf("error during head discovery: %s", evt.Error)
			},
		}
		return cb.EventHandler(ctx, evt)
	})
	defer sched.Shutdown()

	go func() {
		_ = stream.HandleRepoStream(ctx, conn, sched, s.log)
	}()

	select {
	case <-found:
		return headSeq, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(30 * time.Second):
		return 0, fmt.Errorf("timeout waiting for first frame from %s", s.labeler.DID)
	}
}

// dialFrom connects to the upstream labeler from a known cursor position.
func (s *subscription) dialFrom(ctx context.Context, lastSeq *int64) error {
	u, err := url.Parse(s.labeler.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid endpoint URL: %w", err)
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else if u.Scheme == "https" {
		u.Scheme = "wss"
	}

	if lastSeq != nil {
		q := u.Query()
		q.Set("cursor", fmt.Sprintf("%d", *lastSeq))
		u.RawQuery = q.Encode()
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer conn.Close()

	sched := sequential.NewScheduler(s.labeler.DID, s.handleEvent)
	defer sched.Shutdown()

	return stream.HandleRepoStream(ctx, conn, sched, s.log)
}

// handleEvent is called by the sequential scheduler for each frame event.
func (s *subscription) handleEvent(ctx context.Context, evt *stream.XRPCStreamEvent) error {
	cb := &stream.RepoStreamCallbacks{
		LabelLabels: func(evt *atproto.LabelSubscribeLabels_Labels) error { return s.handleLabelLabels(ctx, evt) },
		LabelInfo:   s.handleLabelInfo,
		Error:       s.handleError,
	}
	return cb.EventHandler(ctx, evt)
}

// handleLabelLabels processes a #labels frame.
func (s *subscription) handleLabelLabels(ctx context.Context, evt *atproto.LabelSubscribeLabels_Labels) error {
	// Process each label in the frame.
	var kept []*atproto.LabelDefs_Label

	sigRequired := SigRequired(s.labeler.RequireSig, s.sigDefault)

	for _, label := range evt.Labels {
		if err := s.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("rate limiter failed: %w", err)
		}

		// Check if we should keep this label.
		if !KeepLabel(label, sigRequired) {
			if s.onDropUnsigned != nil {
				s.onDropUnsigned(s.labeler.DID)
			}
			continue
		}

		// Label is kept as-is (unmodified passthrough).
		kept = append(kept, label)
	}

	if len(kept) == 0 {
		return nil
	}

	// Persist the kept labels.
	_, err := s.persist.PersistIngest(ctx, store.IngestEvent{
		Kind:        "labels",
		LabelerDID:  s.labeler.DID,
		UpstreamSeq: &evt.Seq,
		Labels:      kept,
	})
	if err != nil {
		return fmt.Errorf("failed to persist labels: %w", err)
	}

	if s.onIngested != nil {
		s.onIngested(s.labeler.DID, len(kept))
	}

	// Write cursor after successful persist (crash-safe ordering).
	if err := s.registry.WriteCursor(ctx, s.labeler.DID, evt.Seq); err != nil {
		return fmt.Errorf("failed to flush cursor: %w", err)
	}

	return nil
}

// handleLabelInfo processes a #info frame (control signal, not persisted).
func (s *subscription) handleLabelInfo(evt *atproto.LabelSubscribeLabels_Info) error {
	// Log the info frame but do not persist it.
	s.log.Debug("received upstream #info frame", "labeler", s.labeler.DID, "message", evt.Message)
	return nil
}

// handleError processes an error frame.
func (s *subscription) handleError(evt *stream.ErrorFrame) error {
	s.log.Error("received error frame", "labeler", s.labeler.DID, "error", evt.Error)
	// Break to trigger redial.
	return fmt.Errorf("upstream error: %s", evt.Error)
}
