// pattern: Imperative Shell

package slurper

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/scarndp/labeler-relay/internal/store"
)

// LimitConfig specifies default rate limits per labeler.
type LimitConfig struct {
	PerSec  int
	PerHour int
}

// subscriptionContext holds a subscription and its cancellation function.
type subscriptionContext struct {
	sub    *subscription
	cancel context.CancelFunc
}

// LabelSlurper manages the set of subscription goroutines, one per enabled labeler.
type LabelSlurper struct {
	registry              *store.LabelerRegistry
	persist               *store.LabelPersist
	sigDefault            bool
	limits                LimitConfig
	log                   *slog.Logger
	onUpstreamsReconciled func(float64)        // called after Reconcile with the new upstream count
	onThrottled           func(string)         // called when a labeler is throttled, passed the labeler DID
	onDropUnsigned        func(string)         // called when a label is dropped for missing signature, passed the labeler DID
	onIngested            func(string, int)    // called after a successful PersistIngest with labeler DID and count

	pokeCh chan struct{} // capacity-1 signal; multiple Poke() calls collapse into one Reconcile
	mu     sync.Mutex
	active map[string]*subscriptionContext // keyed by labeler DID
	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new LabelSlurper.
func New(
	registry *store.LabelerRegistry,
	persist *store.LabelPersist,
	sigDefault bool,
	limits LimitConfig,
	log *slog.Logger,
) *LabelSlurper {
	ctx, cancel := context.WithCancel(context.Background())
	return &LabelSlurper{
		registry:   registry,
		persist:    persist,
		sigDefault: sigDefault,
		limits:     limits,
		log:        log,
		pokeCh:     make(chan struct{}, 1),
		active:     make(map[string]*subscriptionContext),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Reconcile syncs the active subscriptions with the enabled labelers in the registry.
// It starts new subscriptions for enabled labelers not yet active, restarts
// subscriptions whose endpoint or sig policy changed in the registry, and
// cancels subscriptions for labelers that are no longer enabled.
// Reconcile is idempotent: calling twice with no registry change is a no-op.
func (s *LabelSlurper) Reconcile(ctx context.Context) error {
	// Fetch enabled labelers from registry.
	enabled, err := s.registry.ListEnabled(ctx)
	if err != nil {
		return fmt.Errorf("failed to list enabled labelers: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Build a set of enabled DIDs for efficient lookup.
	enabledSet := make(map[string]struct{})
	for _, labeler := range enabled {
		enabledSet[labeler.DID] = struct{}{}
	}

	// Restart subscriptions whose registry config changed (e.g. the labeler
	// moved hosts): cancel the stale subscription so the loop below starts a
	// fresh one against the current endpoint and sig policy.
	for _, labeler := range enabled {
		sc, exists := s.active[labeler.DID]
		if !exists || !SubscriptionConfigChanged(sc.sub.labeler, labeler) {
			continue
		}
		sc.cancel()
		sc.sub.limiter.Close()
		delete(s.active, labeler.DID)
		s.log.Info("restarting subscription for config change",
			"labeler", labeler.DID,
			"old_endpoint", sc.sub.labeler.Endpoint,
			"new_endpoint", labeler.Endpoint)
	}

	// Start subscriptions for newly enabled labelers.
	for _, labeler := range enabled {
		if _, exists := s.active[labeler.DID]; !exists {
			// New labeler: start a subscription.
			subCtx, subCancel := context.WithCancel(s.ctx)

			limiter := NewLimiter(s.limits.PerSec, s.limits.PerHour)
			did := labeler.DID
			if s.onThrottled != nil {
				limiter.SetThrottledCallback(func() {
					s.onThrottled(did)
				})
			}

			sub := &subscription{
				labeler:        labeler,
				persist:        s.persist,
				registry:       s.registry,
				limiter:        limiter,
				sigDefault:     s.sigDefault,
				log:            s.log,
				onDropUnsigned: s.onDropUnsigned,
				onIngested:     s.onIngested,
			}

			sc := &subscriptionContext{
				sub:    sub,
				cancel: subCancel,
			}
			s.active[labeler.DID] = sc

			// Run the subscription in a background goroutine.
			go func(sc *subscriptionContext) {
				err := sc.sub.run(subCtx)
				if err != nil && err != context.Canceled {
					s.log.Error("subscription failed", "labeler", sc.sub.labeler.DID, "err", err)
				}
			}(sc)

			s.log.Info("started subscription", "labeler", labeler.DID)
		}
	}

	// Cancel subscriptions for disabled labelers.
	for did, sc := range s.active {
		if _, isEnabled := enabledSet[did]; !isEnabled {
			// Labeler is no longer enabled: cancel its subscription and stop the
			// rate-limiter goroutines to avoid a goroutine leak.
			sc.cancel()
			sc.sub.limiter.Close()
			delete(s.active, did)
			s.log.Info("stopped subscription", "labeler", did)
		}
	}

	// Notify observer of updated upstream count (e.g. Prometheus gauge).
	if s.onUpstreamsReconciled != nil {
		s.onUpstreamsReconciled(float64(len(s.active)))
	}

	return nil
}

// SetUpstreamsCallback registers a function called after each Reconcile with
// the current number of active upstream subscriptions. Used to update
// Prometheus gauges without importing metrics from slurper (FCIS).
func (s *LabelSlurper) SetUpstreamsCallback(fn func(float64)) {
	s.onUpstreamsReconciled = fn
}

// SetThrottledCallback registers a function called when a labeler is throttled
// by its rate limiter. The callback is passed the labeler DID. Used to increment
// Prometheus counters without importing metrics from slurper (FCIS).
func (s *LabelSlurper) SetThrottledCallback(fn func(string)) {
	s.onThrottled = fn
}

// SetDropUnsignedCallback registers a function called when a label is dropped
// because it lacks a signature and the labeler requires one. The callback
// receives the labeler DID. Used to increment Prometheus counters without
// importing metrics from slurper (FCIS).
func (s *LabelSlurper) SetDropUnsignedCallback(fn func(string)) {
	s.onDropUnsigned = fn
}

// SetIngestedCallback registers a function called after each successful
// PersistIngest. The callback receives the labeler DID and the number of
// labels ingested. Used to increment Prometheus counters without importing
// metrics from slurper (FCIS).
func (s *LabelSlurper) SetIngestedCallback(fn func(string, int)) {
	s.onIngested = fn
}

// Poke requests an immediate Reconcile on the next Run loop iteration.
// Non-blocking: if a poke is already pending it is collapsed into one reconcile.
func (s *LabelSlurper) Poke() {
	select {
	case s.pokeCh <- struct{}{}:
	default:
	}
}

// Run periodically calls Reconcile on a ticker until the context is cancelled.
// It also selects on pokeCh so that callers (firehose watcher, admin API) can
// trigger an immediate reconcile without spawning unbounded goroutines.
func (s *LabelSlurper) Run(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Reconcile(ctx); err != nil {
				s.log.Error("reconcile failed", "err", err)
			}
		case <-s.pokeCh:
			if err := s.Reconcile(ctx); err != nil {
				s.log.Error("poke reconcile failed", "err", err)
			}
		}
	}
}

// Shutdown stops the slurper and cancels all active subscriptions.
func (s *LabelSlurper) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for did, sc := range s.active {
		sc.cancel()
		s.log.Info("cancelled subscription", "labeler", did)
	}
	s.active = make(map[string]*subscriptionContext)
	s.cancel()
}
