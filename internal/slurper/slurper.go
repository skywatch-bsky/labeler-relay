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
	registry   *store.LabelerRegistry
	persist    *store.LabelPersist
	sigDefault bool
	limits     LimitConfig
	log        *slog.Logger

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
		active:     make(map[string]*subscriptionContext),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Reconcile syncs the active subscriptions with the enabled labelers in the registry.
// It starts new subscriptions for enabled labelers not yet active, and cancels
// subscriptions for labelers that are no longer enabled.
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

	// Start subscriptions for newly enabled labelers.
	for _, labeler := range enabled {
		if _, exists := s.active[labeler.DID]; !exists {
			// New labeler: start a subscription.
			subCtx, subCancel := context.WithCancel(s.ctx)

			sub := &subscription{
				labeler:    labeler,
				persist:    s.persist,
				registry:   s.registry,
				limiter:    NewLimiter(s.limits.PerSec, s.limits.PerHour),
				sigDefault: s.sigDefault,
				log:        s.log,
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
			// Labeler is no longer enabled: cancel its subscription.
			sc.cancel()
			delete(s.active, did)
			s.log.Info("stopped subscription", "labeler", did)
		}
	}

	return nil
}

// Run periodically calls Reconcile on a ticker until the context is cancelled.
// It allows the caller to reconcile on demand via channels in future phases.
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
