// pattern: Imperative Shell

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// DroppedUnsigned tracks the count of labels dropped due to missing signature,
	// partitioned by labeler DID.
	DroppedUnsigned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "labeler_relay_dropped_unsigned_total",
			Help: "Labels dropped due to missing signature, by labeler.",
		},
		[]string{"labeler_did"},
	)

	// IngestedTotal tracks the count of labels successfully ingested from upstream,
	// partitioned by labeler DID.
	IngestedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "labeler_relay_ingested_total",
			Help: "Labels successfully ingested from upstream, by labeler.",
		},
		[]string{"labeler_did"},
	)

	// ConnectedUpstreams is the current number of active labeler subscriptions.
	// Set by the slurper after each Reconcile.
	ConnectedUpstreams = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "labeler_relay_connected_upstreams",
		Help: "Current number of active labeler upstream subscriptions.",
	})

	// ConsumerCount is the current number of active subscribeLabelers WebSocket
	// consumers. Incremented on connect, decremented on disconnect.
	ConsumerCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "labeler_relay_consumer_count",
		Help: "Current number of active subscribeLabelers WebSocket consumers.",
	})

	// HeadSeq is the relay_seq of the most recently persisted event.
	// Updated after each successful PersistIngest via callback.
	HeadSeq = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "labeler_relay_head_seq",
		Help: "Relay sequence number of the most recently persisted event.",
	})

	// RetentionFloorGauge is the lowest relay_seq still present after the last prune.
	// Updated by PruneJob after each tick via callback.
	RetentionFloorGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "labeler_relay_retention_floor",
		Help: "Lowest relay_seq still present after the last prune.",
	})

	// Throttled counts the number of times a labeler's ingest was delayed by its
	// rate limiter, partitioned by labeler DID.
	Throttled = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "labeler_relay_throttled_total",
			Help: "Times a labeler's ingest was throttled by its rate limiter.",
		},
		[]string{"labeler_did"},
	)
)

// Register registers all metrics with the given Prometheus registry.
// This function should be called once at startup to avoid cross-test pollution
// in unit tests. Pass a fresh registry for each test.
func Register(reg *prometheus.Registry) error {
	collectors := []prometheus.Collector{
		DroppedUnsigned,
		IngestedTotal,
		ConnectedUpstreams,
		ConsumerCount,
		HeadSeq,
		RetentionFloorGauge,
		Throttled,
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}
