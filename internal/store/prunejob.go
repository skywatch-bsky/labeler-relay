// pattern: Imperative Shell

package store

import (
	"context"
	"log/slog"
	"time"
)

// PruneJob runs LabelPersist.Prune on a ticker, deleting events older than the
// retention window and advancing the floor. Returns when ctx is cancelled.
type PruneJob struct {
	persist         *LabelPersist
	window          time.Duration
	interval        time.Duration
	nowMs           func() int64 // injectable clock for tests
	onFloorAdvanced func(float64) // called after each tick with the new floor value; may be nil
	log             *slog.Logger
}

// NewPruneJob creates a PruneJob. Pass nil for nowMs to use the real clock.
// Pass nil for onFloorAdvanced if no gauge update is needed (e.g. in tests).
func NewPruneJob(
	persist *LabelPersist,
	window time.Duration,
	interval time.Duration,
	nowMs func() int64,
	log *slog.Logger,
) *PruneJob {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &PruneJob{
		persist:  persist,
		window:   window,
		interval: interval,
		nowMs:    nowMs,
		log:      log,
	}
}

// SetFloorCallback registers a function to be called after each prune tick
// with the new retention floor value. Used to update Prometheus metrics
// without importing the metrics package from store (FCIS: callback injection).
func (j *PruneJob) SetFloorCallback(fn func(float64)) {
	j.onFloorAdvanced = fn
}

// Run blocks until ctx is cancelled, running one prune tick per interval.
func (j *PruneJob) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			j.tick(ctx)
		}
	}
}

// tick executes a single prune pass.
func (j *PruneJob) tick(ctx context.Context) {
	cutoff := j.nowMs() - j.window.Milliseconds()
	deleted, floor, err := j.persist.Prune(ctx, cutoff)
	if err != nil {
		j.log.Error("prune failed", "err", err)
		return
	}
	if deleted > 0 {
		j.log.Info("pruned events", "deleted", deleted, "new_floor", floor)
	}
	if j.onFloorAdvanced != nil {
		j.onFloorAdvanced(float64(floor))
	}
}
