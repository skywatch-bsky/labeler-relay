// pattern: Imperative Shell

package firehose

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/url"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/scarndp/labeler-relay/internal/store"
)

// FirehoseWatcher consumes com.atproto.sync.subscribeRepos, discovers labelers,
// and emits #service events into the output stream.
type FirehoseWatcher struct {
	url      string
	registry *store.LabelerRegistry
	persist  *store.LabelPersist
	resolver DIDResolver
	store    *store.Store // for meta cursor
	poke     func()        // notify slurper to reconcile
	log      *slog.Logger
}

// NewFirehoseWatcher creates a new FirehoseWatcher.
func NewFirehoseWatcher(
	url string,
	registry *store.LabelerRegistry,
	persist *store.LabelPersist,
	resolver DIDResolver,
	store *store.Store,
	poke func(),
	log *slog.Logger,
) *FirehoseWatcher {
	return &FirehoseWatcher{
		url:      url,
		registry: registry,
		persist:  persist,
		resolver: resolver,
		store:    store,
		poke:     poke,
		log:      log,
	}
}

// Run starts the redial loop, consuming firehose commits.
// Blocks until ctx is cancelled.
func (w *FirehoseWatcher) Run(ctx context.Context) error {
	backoffMs := 100
	const maxBackoffMs = 30000

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := w.dial(ctx)
		if err == context.Canceled || err == context.DeadlineExceeded {
			return err
		}

		if err != nil {
			w.log.Error("firehose dial failed", "err", err)
		}

		// Exponential backoff with jitter.
		maxJitter := int(float64(backoffMs) * 0.1)
		if maxJitter < 1 {
			maxJitter = 1
		}
		jitter := rand.Intn(maxJitter)
		sleepMs := backoffMs + jitter
		if sleepMs > maxBackoffMs {
			sleepMs = maxBackoffMs
		}

		select {
		case <-time.After(time.Duration(sleepMs) * time.Millisecond):
			backoffMs = int(math.Min(float64(backoffMs)*2, float64(maxBackoffMs)))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// dial establishes a WebSocket connection and processes commits.
func (w *FirehoseWatcher) dial(ctx context.Context) error {
	// Build the subscription URL.
	u, err := url.Parse(w.url)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Read persisted firehose cursor if present.
	if cursorStr, found, err := w.store.GetMeta(ctx, "firehose_cursor"); err == nil && found {
		q := u.Query()
		q.Set("cursor", cursorStr)
		u.RawQuery = q.Encode()
	} else if err != nil {
		w.log.Error("failed to read firehose cursor", "err", err)
		// Continue without cursor on error; next dial will attempt again.
	}

	// Dial the WebSocket.
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer conn.Close()

	// Build the scheduler to handle events.
	sched := sequential.NewScheduler("firehose", w.handleEvent)
	defer sched.Shutdown()

	// Consume the firehose.
	return stream.HandleRepoStream(ctx, conn, sched, w.log)
}

// handleEvent is called by the scheduler for each stream event.
func (w *FirehoseWatcher) handleEvent(ctx context.Context, evt *stream.XRPCStreamEvent) error {
	cb := &stream.RepoStreamCallbacks{
		RepoCommit: func(commit *comatproto.SyncSubscribeRepos_Commit) error {
			return w.handleCommit(ctx, commit)
		},
		Error: func(evt *stream.ErrorFrame) error {
			return w.handleError(evt)
		},
	}
	return cb.EventHandler(ctx, evt)
}

// handleCommit processes a commit event from the firehose.
func (w *FirehoseWatcher) handleCommit(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit) error {
	// Extract labeler.service ops from the commit.
	ops, err := ExtractLabelerServiceOps(ctx, commit)
	if err != nil {
		w.log.Error("extract failed", "err", err)
		// Record the error but don't crash.
		return nil
	}

	// Process each labeler op.
	for _, op := range ops {
		w.log.Debug("labeler op", "did", op.RepoDID, "action", op.Action, "rkey", op.Rkey)

		if op.Action == "create" || op.Action == "update" {
			// Resolve the endpoint.
			endpoint, err := w.resolver.LabelerEndpoint(ctx, op.RepoDID)
			if err != nil {
				if err == ErrNoLabelerEndpoint {
					w.log.Debug("no labeler endpoint", "did", op.RepoDID)
					// Record the error in the registry and continue.
					_ = w.registry.RecordError(ctx, op.RepoDID, "no atproto_labeler service endpoint")
				} else {
					w.log.Error("resolve endpoint failed", "did", op.RepoDID, "err", err)
					_ = w.registry.RecordError(ctx, op.RepoDID, err.Error())
				}
				continue
			}

			// Upsert the labeler with firehose source.
			// The Phase-2 upsert rule preserves manual labeler entries.
			err = w.registry.Upsert(ctx, store.Labeler{
				DID:       op.RepoDID,
				Endpoint:  endpoint,
				Source:    "firehose",
				Enabled:   true,
				UpdatedAt: time.Now().Unix(),
			})
			if err != nil {
				w.log.Error("upsert failed", "did", op.RepoDID, "err", err)
				continue
			}

			// Persist the #service event.
			_, err = w.persist.PersistIngest(ctx, store.IngestEvent{
				Kind:       "service",
				LabelerDID: op.RepoDID,
				Record:     op.Record,
			})
			if err != nil {
				w.log.Error("persist failed", "did", op.RepoDID, "err", err)
				continue
			}

			// Notify the slurper to reconcile.
			w.poke()
		} else if op.Action == "delete" {
			// Delete ops: only disable firehose-sourced labelers, never manual.
			// (Manual stickiness is enforced at the registry level.)
			// For now, we skip this—only enable on discovery, don't disable.
		}
	}

	// Persist the firehose cursor for recovery on restart.
	if err := w.store.SetMeta(ctx, "firehose_cursor", fmt.Sprint(commit.Seq)); err != nil {
		w.log.Error("failed to persist firehose cursor", "seq", commit.Seq, "err", err)
		// Don't fail the entire commit handler on cursor persistence error.
	}

	return nil
}

// handleError handles error frames from the firehose.
func (w *FirehoseWatcher) handleError(evt *stream.ErrorFrame) error {
	w.log.Error("firehose error", "error", evt.Error, "message", evt.Message)
	return nil
}
