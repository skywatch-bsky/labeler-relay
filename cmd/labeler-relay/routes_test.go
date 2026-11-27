// pattern: Imperative Shell
// Tests for the production route mounting via buildMux: public query
// endpoints reachable without auth, method handling, and the metric callback
// wiring.

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/scarndp/labeler-relay/internal/admin"
	"github.com/scarndp/labeler-relay/internal/firehose"
	"github.com/scarndp/labeler-relay/internal/metrics"
	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
)

// loopbackTrusted is the loopback trust set, mirroring the config default.
var loopbackTrusted = []netip.Prefix{
	netip.MustParsePrefix("127.0.0.1/32"),
	netip.MustParsePrefix("::1/128"),
}

// newRoutesEnv builds the production mux through buildMux over a real store.
// queryPerSec/queryPerHour feed the rate limiter exactly as config would;
// trusted is passed to buildMux so tests can exercise proxy keying.
func newRoutesEnv(t *testing.T, queryPerSec, queryPerHour int, trusted []netip.Prefix) (*httptest.Server, *store.LabelerRegistry) {
	t.Helper()

	s, err := store.Open(t.TempDir() + "/routes.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	persist := store.NewLabelPersist(s)
	registry := store.NewLabelerRegistry(s)

	hub := server.NewHub()
	persist.SetBroadcaster(hub.Broadcast)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := server.NewServer(hub, persist, registry, log, 7200, 0)

	// identity.BaseDirectory{} is the zero value and is directly usable as a
	// Resolver; queries stay offline for these tests.
	resolver := firehose.NewIndigoResolver(&identity.BaseDirectory{})
	adminAPI := admin.NewAPI(registry, resolver, func() {}, "test-token")

	mux := buildMux(srv, queryPerSec, queryPerHour, trusted, prometheus.NewRegistry(), adminAPI)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, registry
}

// TestRoutes_PublicQueryEndpoints verifies AC.9: the query endpoints are
// served under their /xrpc/ paths without auth, other methods get 405 with
// an Allow header naming exactly GET and HEAD.
func TestRoutes_PublicQueryEndpoints(t *testing.T) {
	ts, reg := newRoutesEnv(t, 1000, 100000, loopbackTrusted)
	ctx := context.Background()

	require.NoError(t, reg.Upsert(ctx, store.Labeler{
		DID:       "did:plc:abc",
		Endpoint:  "abc.example.com",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: 1,
	}))

	resp, err := http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabelers")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	resp, err = http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:abc")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Unknown DID is reachable too: a 400, not a 404/401.
	resp, err = http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:zzz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// POST is rejected by the GET-qualified pattern with 405 + Allow.
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/xrpc/community.labeler.sync.getLabelers", nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	allow := resp.Header.Get("Allow")
	methods := strings.Split(strings.ReplaceAll(allow, " ", ""), ",")
	require.ElementsMatch(t, []string{"GET", "HEAD"}, methods, "Allow header: %q", allow)
}

// TestRoutes_RejectedRequestIncrementsMetric verifies AC.5's 429 shape through
// the production middleware and AC.6's callback wiring: an accepted request
// leaves the counter untouched, a rejected one increments it.
func TestRoutes_RejectedRequestIncrementsMetric(t *testing.T) {
	ts, _ := newRoutesEnv(t, 1, 100000, loopbackTrusted)

	counter := metrics.QueryRateLimitedTotal.WithLabelValues("getLabelers")
	before := testutil.ToFloat64(counter)

	resp, err := http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabelers")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, before, testutil.ToFloat64(counter),
		"accepted request must not increment the counter")

	resp, err = http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabelers")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	require.Equal(t, "1", resp.Header.Get("Retry-After"))

	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "RateLimitExceeded", body.Error)
	require.Equal(t, before+1, testutil.ToFloat64(counter),
		"rejected request must increment the counter via the wired callback")
}

// TestRoutes_ReadmeExamplesMatchResponses verifies AC.10's concrete check:
// the README's documented request/response shapes match what the production
// mux actually returns — a numeric observed_seq cursor when more pages exist,
// and a last_upstream_seq key that is always present (null when never
// ingested).
func TestRoutes_ReadmeExamplesMatchResponses(t *testing.T) {
	ts, reg := newRoutesEnv(t, 1000, 100000, loopbackTrusted)
	ctx := context.Background()

	did := "did:plc:ar7c4by46qjdydhdevvrndac"
	require.NoError(t, reg.Upsert(ctx, store.Labeler{
		DID:       did,
		Endpoint:  "bsky.app",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: 1,
	}))
	require.NoError(t, reg.WriteCursor(ctx, did, 35916330))

	// A second, never-ingested labeler so a second page exists.
	neverDid := "did:plc:k2vjpxvpxbnfyjzkb66cg7ju"
	require.NoError(t, reg.Upsert(ctx, store.Labeler{
		DID:       neverDid,
		Endpoint:  "",
		Source:    "firehose",
		Enabled:   false,
		UpdatedAt: 1,
	}))

	// GET .../getLabelers?limit=1 — documented example response: a JSON
	// object with labelers (each carrying did, endpoint, source, enabled,
	// last_upstream_seq) and a cursor when more pages exist.
	resp, err := http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabelers?limit=1")
	require.NoError(t, err)
	var list map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, list, "labelers")
	items, ok := list["labelers"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	first, _ := items[0].(map[string]any)
	require.Equal(t, did, first["did"])
	require.Equal(t, "bsky.app", first["endpoint"])
	require.Equal(t, "manual", first["source"])
	require.Equal(t, true, first["enabled"])
	require.Equal(t, float64(35916330), first["last_upstream_seq"])

	// The cursor is a JSON number — the observed_seq of the page's last row —
	// not a quoted string.
	require.Equal(t, float64(1), list["cursor"])

	// The next page carries the never-ingested labeler: last_upstream_seq
	// present with JSON null, and no cursor on the final page.
	resp, err = http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabelers?limit=1&cursor=1")
	require.NoError(t, err)
	var page2 map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page2))
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, page2, "cursor", "final page must omit cursor")
	items, ok = page2["labelers"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	second, _ := items[0].(map[string]any)
	require.Equal(t, neverDid, second["did"])
	require.Contains(t, second, "last_upstream_seq")
	require.Nil(t, second["last_upstream_seq"])

	// GET .../getLabeler?did=... — documented single-lookup response.
	resp, err = http.Get(ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=" + did)
	require.NoError(t, err)
	var single map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&single))
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, did, single["did"])
}

// TestDocs_NewSurfaceDocumented verifies AC.10's drift guard: the README
// mentions both new env vars and both endpoints, and the lexicon files exist.
func TestDocs_NewSurfaceDocumented(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	require.NoError(t, err)
	text := string(readme)
	require.Contains(t, text, "LABELER_RELAY_QUERY_RATE_LIMIT_PER_SEC")
	require.Contains(t, text, "LABELER_RELAY_QUERY_RATE_LIMIT_PER_HOUR")
	require.Contains(t, text, "LABELER_RELAY_TRUSTED_PROXIES")
	require.Contains(t, text, "community.labeler.sync.getLabelers")
	require.Contains(t, text, "community.labeler.sync.getLabeler")

	for _, path := range []string{
		"../../lexicons/community/labeler/sync/defs.json",
		"../../lexicons/community/labeler/sync/getLabelers.json",
		"../../lexicons/community/labeler/sync/getLabeler.json",
	} {
		_, err := os.Stat(path)
		require.NoError(t, err, path)
	}

	// labelerView is the shared response object: both query lexicons must
	// reference the defs document, not carry or point at their own copy.
	defs, err := os.ReadFile("../../lexicons/community/labeler/sync/defs.json")
	require.NoError(t, err)
	require.Contains(t, string(defs), `"id": "community.labeler.sync.defs"`)
	require.Contains(t, string(defs), `"labelerView"`)
	for _, path := range []string{
		"../../lexicons/community/labeler/sync/getLabelers.json",
		"../../lexicons/community/labeler/sync/getLabeler.json",
	} {
		body, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Contains(t, string(body), "community.labeler.sync.defs#labelerView", path)
		require.NotContains(t, string(body), "community.labeler.sync.getLabelers#labelerView", path)
	}
}

// TestRoutes_ProxyIsolation covers AC.1 through the production mux: the
// httptest peer is loopback and therefore trusted by the default list, so
// requests carrying different X-Forwarded-For client IPs draw from
// independent budgets while the same client IP exhausts its own.
func TestRoutes_ProxyIsolation(t *testing.T) {
	ts, _ := newRoutesEnv(t, 1, 100000, loopbackTrusted)

	get := func(xff string) int {
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/xrpc/community.labeler.sync.getLabelers", nil)
		require.NoError(t, err)
		req.Header.Set("X-Forwarded-For", xff)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp.StatusCode
	}

	require.Equal(t, http.StatusOK, get("9.9.9.9"))
	require.Equal(t, http.StatusTooManyRequests, get("9.9.9.9"),
		"the same client IP must exhaust its budget behind the proxy")
	require.Equal(t, http.StatusOK, get("8.8.8.8"),
		"a second client behind the same proxy must have an independent budget")
}

// TestDocs_TrustedProxiesDocumented is the drift guard for the trusted-proxy
// feature: the variable and its keying contract must appear in the README,
// both package CLAUDE.md files, and llms.txt.
func TestDocs_TrustedProxiesDocumented(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	require.NoError(t, err)
	require.Contains(t, string(readme), "LABELER_RELAY_TRUSTED_PROXIES")
	require.Contains(t, string(readme), "127.0.0.1/32,::1/128",
		"README must document the loopback default")

	configDoc, err := os.ReadFile("../../internal/config/CLAUDE.md")
	require.NoError(t, err)
	require.Contains(t, string(configDoc), "LABELER_RELAY_TRUSTED_PROXIES")

	serverDoc, err := os.ReadFile("../../internal/server/CLAUDE.md")
	require.NoError(t, err)
	require.Contains(t, string(serverDoc), "LABELER_RELAY_TRUSTED_PROXIES")
	require.Contains(t, string(serverDoc), "X-Forwarded-For")
	require.Contains(t, string(serverDoc), "clientip.go")

	llms, err := os.ReadFile("../../llms.txt")
	require.NoError(t, err)
	require.Contains(t, string(llms), "LABELER_RELAY_TRUSTED_PROXIES")
}

// TestDocs_LlmsEnvListComplete pins the llms.txt deployment guide to the full
// configurable surface: every environment variable the loader reads and both
// public query endpoints must appear, so the guide cannot silently fall
// behind the configuration again.
func TestDocs_LlmsEnvListComplete(t *testing.T) {
	llms, err := os.ReadFile("../../llms.txt")
	require.NoError(t, err)
	text := string(llms)

	for _, name := range []string{
		"LABELER_RELAY_DB_PATH",
		"LABELER_RELAY_LISTEN_ADDR",
		"LABELER_RELAY_FIREHOSE_URL",
		"LABELER_RELAY_ADMIN_TOKEN",
		"LABELER_RELAY_RETENTION_WINDOW",
		"LABELER_RELAY_REQUIRE_SIG",
		"LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED",
		"LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC",
		"LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR",
		"LABELER_RELAY_QUERY_RATE_LIMIT_PER_SEC",
		"LABELER_RELAY_QUERY_RATE_LIMIT_PER_HOUR",
		"LABELER_RELAY_TRUSTED_PROXIES",
		"LABELER_RELAY_SUBSCRIBER_BUF_SIZE",
	} {
		require.Contains(t, text, name, "llms.txt env list must document %s", name)
	}

	require.Contains(t, text, "`/xrpc/community.labeler.sync.getLabelers`")
	require.Contains(t, text, "`/xrpc/community.labeler.sync.getLabeler`")
}
