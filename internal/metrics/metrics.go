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
)

// Register registers all metrics with the given Prometheus registry.
// This function should be called once at startup to avoid cross-test pollution
// in unit tests. Pass a fresh registry for each test.
func Register(reg *prometheus.Registry) error {
	if err := reg.Register(DroppedUnsigned); err != nil {
		return err
	}
	if err := reg.Register(IngestedTotal); err != nil {
		return err
	}
	return nil
}
