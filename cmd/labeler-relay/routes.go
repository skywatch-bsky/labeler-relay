// pattern: Imperative Shell
// routes.go builds the HTTP mux for the labeler-relay process. Extracted from
// run() so tests can mount the production routes without booting the full
// process.

package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/scarndp/labeler-relay/internal/admin"
	"github.com/scarndp/labeler-relay/internal/metrics"
	"github.com/scarndp/labeler-relay/internal/server"
)

// buildMux mounts all HTTP routes: the subscribeLabelers WebSocket, /_health,
// /metrics, the admin API, and the public query endpoints behind the per-IP
// rate limiter.
//
// The query endpoints use GET-qualified ServeMux patterns, so HEAD is also
// served and any other method receives ServeMux's automatic 405 with an
// Allow header.
func buildMux(srv *server.Server, queryPerSec, queryPerHour int, reg *prometheus.Registry, adminAPI *admin.API) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/community.labeler.sync.subscribeLabelers", srv.HandleSubscribeLabelers)
	mux.HandleFunc("/_health", srv.HandleHealth)
	mux.Handle("/metrics", server.MetricsHandler(reg))

	mux.Handle("/admin/", adminAPI.Routes())

	ql := server.NewQueryRateLimiter(queryPerSec, queryPerHour)
	ql.SetRateLimitedCallback(func(endpoint string) {
		metrics.QueryRateLimitedTotal.WithLabelValues(endpoint).Inc()
	})
	mux.Handle("GET /xrpc/community.labeler.sync.getLabelers", ql.Middleware("getLabelers", http.HandlerFunc(srv.HandleGetLabelers)))
	mux.Handle("GET /xrpc/community.labeler.sync.getLabeler", ql.Middleware("getLabeler", http.HandlerFunc(srv.HandleGetLabeler)))

	return mux
}
