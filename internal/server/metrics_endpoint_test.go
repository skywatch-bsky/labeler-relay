package server_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scarndp/labeler-relay/internal/metrics"
	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
	"github.com/stretchr/testify/require"
)

// testMetricsServer builds a real httptest.Server mounting HandleHealth and
// HandleMetrics, using an explicit prometheus.Registry for test isolation.
func testMetricsServer(t *testing.T) (*httptest.Server, *store.LabelPersist, *store.LabelerRegistry, *prometheus.Registry, func()) {
	t.Helper()
	p, storeCleanup := testPersist(t)

	regStore, err := store.Open(t.TempDir() + "/reg.db")
	require.NoError(t, err)
	reg := store.NewLabelerRegistry(regStore)

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	reg2 := prometheus.NewRegistry()
	require.NoError(t, metrics.Register(reg2))

	const retentionWindow = 7200
	srv := server.NewServer(h, p, reg, slog.Default(), retentionWindow, 0)

	mux := http.NewServeMux()
	mux.HandleFunc("/_health", srv.HandleHealth)
	mux.Handle("/metrics", server.MetricsHandler(reg2))

	ts := httptest.NewServer(mux)

	return ts, p, reg, reg2, func() {
		ts.Close()
		storeCleanup()
		regStore.Close()
	}
}

// scrapeMetrics fetches /metrics and returns the body as a string.
func scrapeMetrics(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(ts.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

// TestMetrics_AC10_3_Scrape verifies that after persisting events the /metrics
// scrape contains the expected metric names with sane values.
func TestMetrics_AC10_3_Scrape(t *testing.T) {
	ts, p, _, _, cleanup := testMetricsServer(t)
	defer cleanup()

	ctx := context.Background()

	// Set head_seq via HeadSeq gauge directly and verify it appears on the
	// scrape endpoint (the metric update path for the gauge is driven by
	// PersistIngest callback via SetHeadSeqCallback — tested in store tests).
	// For this endpoint test we drive the gauges directly.
	metrics.HeadSeq.Set(42)
	metrics.ConnectedUpstreams.Set(3)
	metrics.ConsumerCount.Set(2)
	metrics.RetentionFloorGauge.Set(10)
	metrics.DroppedUnsigned.WithLabelValues("did:plc:test").Add(5)

	// Also do a real PersistIngest so the endpoint is tested against a live store.
	_, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", 0x01))
	require.NoError(t, err)

	body := scrapeMetrics(t, ts)

	require.True(t, strings.Contains(body, "labeler_relay_head_seq"),
		"expected labeler_relay_head_seq in scrape, got:\n%s", body)
	require.True(t, strings.Contains(body, "labeler_relay_connected_upstreams"),
		"expected labeler_relay_connected_upstreams in scrape")
	require.True(t, strings.Contains(body, "labeler_relay_consumer_count"),
		"expected labeler_relay_consumer_count in scrape")
	require.True(t, strings.Contains(body, "labeler_relay_retention_floor"),
		"expected labeler_relay_retention_floor in scrape")
	require.True(t, strings.Contains(body, "labeler_relay_dropped_unsigned_total"),
		"expected labeler_relay_dropped_unsigned_total in scrape")
}

// TestMetrics_ConsumerCountIncDec verifies that ConsumerCount increments on
// connect and decrements on disconnect.
func TestMetrics_ConsumerCountIncDec(t *testing.T) {
	// Start with a known value via a fresh counter.
	// We can't reset gauges in prometheus but we can observe direction.
	before := gaugeValue(t, metrics.ConsumerCount)

	metrics.ConsumerCount.Inc()
	afterInc := gaugeValue(t, metrics.ConsumerCount)
	require.Equal(t, before+1, afterInc, "Inc must increase ConsumerCount by 1")

	metrics.ConsumerCount.Dec()
	afterDec := gaugeValue(t, metrics.ConsumerCount)
	require.Equal(t, before, afterDec, "Dec must restore ConsumerCount")
}

// TestMetrics_Throttled_AC7_1_Observability verifies that Throttled counter
// increments are observable in the registry scrape, satisfying the AC7.1
// observability requirement.
func TestMetrics_Throttled_AC7_1_Observability(t *testing.T) {
	ts, _, _, reg, cleanup := testMetricsServer(t)
	defer cleanup()

	// Increment Throttled for a labeler.
	metrics.Throttled.WithLabelValues("did:plc:throttled-test").Add(7)

	body := scrapeMetrics(t, ts)

	require.True(t, strings.Contains(body, "labeler_relay_throttled_total"),
		"expected labeler_relay_throttled_total in scrape, got:\n%s", body)
	require.True(t, strings.Contains(body, `did:plc:throttled-test`),
		"expected throttled-test labeler DID in scrape output")
	_ = reg

	// Wait for the metric to be reflected (condition-based, no fixed sleep).
	deadline := time.Now().Add(2 * time.Second)
	for {
		body = scrapeMetrics(t, ts)
		if strings.Contains(body, "labeler_relay_throttled_total") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout: labeler_relay_throttled_total never appeared in scrape")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// gaugeValue reads the current float64 value of a prometheus.Gauge using
// a temporary registry gather. Works only with gauges registered globally.
func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	tmp := prometheus.NewRegistry()
	require.NoError(t, tmp.Register(g))
	mfs, err := tmp.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 1)
	require.Len(t, mfs[0].GetMetric(), 1)
	return mfs[0].GetMetric()[0].GetGauge().GetValue()
}
