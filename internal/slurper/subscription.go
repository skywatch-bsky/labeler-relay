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

	// Build the subscription URL with cursor parameter.
	endpoint := s.labeler.Endpoint

	// Convert HTTP/HTTPS to WS/WSS scheme.
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid endpoint URL: %w", err)
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else if u.Scheme == "https" {
		u.Scheme = "wss"
	}

	// Add cursor parameter if we have a resume point.
	if lastSeq != nil {
		q := u.Query()
		q.Set("cursor", fmt.Sprintf("%d", *lastSeq))
		u.RawQuery = q.Encode()
	}

	endpoint = u.String()

	// Dial the WebSocket.
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, _, err := dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer conn.Close()

	// Build the frame handler callbacks.
	sched := sequential.NewScheduler(s.labeler.DID, s.handleEvent)

	// Wrap the scheduler's Shutdown in a deferred call to ensure cleanup.
	defer sched.Shutdown()

	// Call HandleRepoStream to consume frames.
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
		// Nothing to persist, but the frame was fully consumed: advance the
		// cursor so redials resume from here instead of replaying the backlog.
		// Safe without a persist -- there is nothing to lose on crash.
		if err := s.registry.WriteCursor(ctx, s.labeler.DID, evt.Seq); err != nil {
			return fmt.Errorf("failed to flush cursor: %w", err)
		}
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
