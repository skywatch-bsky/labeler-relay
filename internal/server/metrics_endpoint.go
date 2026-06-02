// pattern: Imperative Shell

package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsHandler returns an HTTP handler that serves Prometheus metrics from
// the given registry. The handler MUST use the same registry that
// metrics.Register() populated, not the global default, to avoid cross-test
// pollution and to keep metrics isolated per process.
func MetricsHandler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
