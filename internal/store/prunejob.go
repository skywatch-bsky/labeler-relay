// pattern: Imperative Shell

package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var retentionFloorGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "labeler_relay_retention_floor",
	Help: "Lowest relay_seq still present after the last prune.",
})

// PruneJob runs LabelPersist.Prune on a ticker, deleting events older than the
// retention window and advancing the floor. Returns when ctx is cancelled.
type PruneJob struct {
	persist  *LabelPersist
	window   time.Duration
	interval time.Duration
	nowMs    func() int64 // injectable clock for tests
	log      *slog.Logger
}

// NewPruneJob creates a PruneJob. Pass nil for nowMs to use the real clock.
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
	retentionFloorGauge.Set(float64(floor))
}
