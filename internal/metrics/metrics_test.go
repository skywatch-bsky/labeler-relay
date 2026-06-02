package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetrics(t *testing.T) {
	// Create a fresh registry for testing
	reg := prometheus.NewRegistry()

	// Register metrics
	err := Register(reg)
	require.NoError(t, err, "Register should not error on fresh registry")

	// Increment the counter and verify it works
	DroppedUnsigned.WithLabelValues("did:plc:test").Inc()
	DroppedUnsigned.WithLabelValues("did:plc:other").Add(5)

	// Verify IngestedTotal counter exists and works
	IngestedTotal.WithLabelValues("did:plc:test").Inc()
	IngestedTotal.WithLabelValues("did:plc:test").Add(10)

	// Verify we can gather without error and have metrics
	gathered, err := reg.Gather()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(gathered), 2, "should have at least 2 metrics after incrementing")
}

func TestRegisterIdempotence(t *testing.T) {
	// Registering the same metric twice should error (Prometheus behavior)
	reg1 := prometheus.NewRegistry()
	err1 := Register(reg1)
	require.NoError(t, err1)

	// Try to register again to the same registry - should error
	err2 := Register(reg1)
	require.Error(t, err2, "registering same metrics twice should error")
}
