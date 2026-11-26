package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	err := Register(reg)
	require.NoError(t, err, "Register should not error on fresh registry")

	DroppedUnsigned.WithLabelValues("did:plc:test").Inc()
	DroppedUnsigned.WithLabelValues("did:plc:other").Add(5)

	IngestedTotal.WithLabelValues("did:plc:test").Inc()
	IngestedTotal.WithLabelValues("did:plc:test").Add(10)

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

// TestQueryRateLimitedRegistered verifies the query rate-limited counter is
// registered by Register with its endpoint label (AC.6).
func TestQueryRateLimitedRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	require.NoError(t, Register(reg))

	QueryRateLimitedTotal.WithLabelValues("getLabelers").Inc()

	families, err := reg.Gather()
	require.NoError(t, err)

	var found *dto.MetricFamily
	for _, mf := range families {
		if mf.GetName() == "labeler_relay_query_rate_limited_total" {
			found = mf
			break
		}
	}
	require.NotNil(t, found, "labeler_relay_query_rate_limited_total must be registered")
	require.Len(t, found.GetMetric(), 1)
	labels := found.GetMetric()[0].GetLabel()
	require.Len(t, labels, 1)
	require.Equal(t, "endpoint", labels[0].GetName())
	require.Equal(t, "getLabelers", labels[0].GetValue())
	require.Equal(t, float64(1), found.GetMetric()[0].GetCounter().GetValue())
}
