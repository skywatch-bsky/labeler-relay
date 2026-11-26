// pattern: Imperative Shell
// Tests for the per-IP query rate limiter. This file lives in the internal
// test package so it can drive newQueryRateLimiter's explicit map cap and
// the allowAt timestamp seam.

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// loopbackTrusted is the loopback trust set, mirroring the config default.
var loopbackTrusted = []netip.Prefix{
	netip.MustParsePrefix("127.0.0.1/32"),
	netip.MustParsePrefix("::1/128"),
}

// rlBase is the fixed timestamp the limiter tests advance from.
var rlBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestRateLimiter_AllowAndReject(t *testing.T) {
	q := newQueryRateLimiter(2, 1000, 4, nil)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.1", rlBase))
	require.True(t, q.allowAt("10.0.0.1", rlBase.Add(time.Millisecond)))
	require.False(t, q.allowAt("10.0.0.1", rlBase.Add(2*time.Millisecond)),
		"third request within the per-second budget must be rejected")
}

func TestRateLimiter_HourlyBudget(t *testing.T) {
	// Generous per-second budget, tiny hourly budget: the rejection must
	// come from the hourly tier only.
	q := newQueryRateLimiter(100, 3, 4, nil)
	defer q.Close()

	for i := 0; i < 3; i++ {
		require.True(t, q.allowAt("10.0.0.2", rlBase.Add(time.Duration(i)*time.Millisecond)))
	}
	require.False(t, q.allowAt("10.0.0.2", rlBase.Add(3*time.Millisecond)),
		"fourth request within the hourly budget must be rejected")
}

func TestRateLimiter_WindowRecovery(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4, nil)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.3", rlBase))
	require.False(t, q.allowAt("10.0.0.3", rlBase.Add(time.Millisecond)))

	// Past the per-second window the budget resets.
	require.True(t, q.allowAt("10.0.0.3", rlBase.Add(2*time.Second)))
}

func TestRateLimiter_HourlyWindowRecovery(t *testing.T) {
	q := newQueryRateLimiter(1000, 2, 4, nil)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.4", rlBase))
	require.True(t, q.allowAt("10.0.0.4", rlBase.Add(time.Millisecond)))
	require.False(t, q.allowAt("10.0.0.4", rlBase.Add(2*time.Millisecond)),
		"hourly budget exhausted")

	// The hourly window expires an hour after the first take.
	require.True(t, q.allowAt("10.0.0.4", rlBase.Add(time.Hour+time.Second)))
}

func TestRateLimiter_PerIPIsolation(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4, nil)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.5", rlBase))
	require.False(t, q.allowAt("10.0.0.5", rlBase))
	require.True(t, q.allowAt("10.0.0.6", rlBase), "a second IP must have its own budget")
}

func TestRateLimiter_CapEviction(t *testing.T) {
	// Cap 4: filling the map and adding one more must evict the least
	// recently seen entry, keeping the map bounded.
	q := newQueryRateLimiter(10, 1000, 4, nil)
	defer q.Close()

	for i := 0; i < 4; i++ {
		require.True(t, q.allowAt(fmt.Sprintf("10.0.1.%d", i), rlBase.Add(time.Duration(i)*time.Second)))
	}
	require.Len(t, q.windows, 4)

	require.True(t, q.allowAt("10.0.1.4", rlBase.Add(5*time.Second)))
	require.Len(t, q.windows, 4, "map must stay at the cap")
	require.NotContains(t, q.windows, "10.0.1.0", "oldest entry must be evicted")
}

func TestRateLimiter_IdleEviction(t *testing.T) {
	q := newQueryRateLimiter(10, 1000, 4, nil)
	defer q.Close()

	for i := 0; i < 4; i++ {
		require.True(t, q.allowAt(fmt.Sprintf("10.0.2.%d", i), rlBase))
	}
	require.Len(t, q.windows, 4)

	// Every tracked entry is idle an hour later. The least recently seen —
	// an idle entry by construction — is evicted to make room and the map
	// stays bounded at the cap.
	require.True(t, q.allowAt("10.0.2.9", rlBase.Add(time.Hour+time.Minute)))
	require.Len(t, q.windows, 4, "map must stay at the cap")
	require.Contains(t, q.windows, "10.0.2.9")
	require.NotContains(t, q.windows, "10.0.2.0", "least recently seen entry must be evicted first")
}

func TestRateLimiter_TouchedIPNotEvicted(t *testing.T) {
	// An IP touched after admission must not be evicted as "least recently
	// seen": its recency moves to the front on every touch. This pins the
	// LRU eviction order.
	q := newQueryRateLimiter(10, 1000, 2, nil)
	defer q.Close()

	require.True(t, q.allowAt("10.9.0.1", rlBase))
	require.True(t, q.allowAt("10.9.0.2", rlBase.Add(1*time.Second)))
	require.True(t, q.allowAt("10.9.0.1", rlBase.Add(2*time.Second)), "ip1 touched again: its recency moves to the front")

	// A third IP hits the cap. The least recently seen IP (10.9.0.2, seen at
	// +1s) must be evicted, not 10.9.0.1 last seen at +2s.
	require.True(t, q.allowAt("10.9.0.3", rlBase.Add(3*time.Second)))
	require.Contains(t, q.windows, "10.9.0.1")
	require.Contains(t, q.windows, "10.9.0.3")
	require.NotContains(t, q.windows, "10.9.0.2")
}

func TestRateLimiter_LRUOrderAcrossTouchedIPs(t *testing.T) {
	// With multiple IPs touched after insertion, eviction must pick the true
	// least-recently-seen entry, not insertion order: here insertion order
	// would evict 10.10.0.1, but its touch at +3s makes 10.10.0.2 the LRU.
	q := newQueryRateLimiter(10, 1000, 3, nil)
	defer q.Close()

	require.True(t, q.allowAt("10.10.0.1", rlBase))                    // seen t0
	require.True(t, q.allowAt("10.10.0.2", rlBase.Add(1*time.Second))) // seen t1
	require.True(t, q.allowAt("10.10.0.2", rlBase.Add(2*time.Second))) // touched t2
	require.True(t, q.allowAt("10.10.0.1", rlBase.Add(3*time.Second))) // touched t3
	require.True(t, q.allowAt("10.10.0.3", rlBase.Add(4*time.Second))) // seen t4, fills the cap

	// Cap hit: 10.10.0.2 (t2) is the least recently seen and must go.
	require.True(t, q.allowAt("10.10.0.4", rlBase.Add(5*time.Second)))
	require.Contains(t, q.windows, "10.10.0.1")
	require.Contains(t, q.windows, "10.10.0.3")
	require.Contains(t, q.windows, "10.10.0.4")
	require.NotContains(t, q.windows, "10.10.0.2")
}

func TestQuery_RateLimited_Response(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4, nil)
	defer q.Close()

	srv := httptest.NewServer(q.Middleware("getLabelers", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	resp, err = http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	require.Equal(t, "1", resp.Header.Get("Retry-After"))
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "RateLimitExceeded", body.Error)
	require.Equal(t, "rate limit exceeded for this endpoint", body.Message)
}

func TestRateLimiter_CallbackFires(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4, nil)
	defer q.Close()

	var got []string
	q.SetRateLimitedCallback(func(endpoint string) { got = append(got, endpoint) })

	handler := q.Middleware("getLabeler", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// httptest.NewRequest sets a RemoteAddr host, so requests share one IP.
	req := httptest.NewRequest(http.MethodGet, "/xrpc/community.labeler.sync.getLabeler", nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, got, "accepted request must not fire the callback")

	handler.ServeHTTP(httptest.NewRecorder(), req)
	require.Equal(t, []string{"getLabeler"}, got, "rejected request must fire the callback with the endpoint name")
}

// TestRateLimiter_ProxyIsolation checks that clients behind a trusted proxy
// have independent budgets.
func TestRateLimiter_ProxyIsolation(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4, loopbackTrusted)
	defer q.Close()

	handler := q.Middleware("getLabelers", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	request := func(xff string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "127.0.0.1:55555"
		req.Header.Set("X-Forwarded-For", xff)
		return req
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request("9.9.9.9"))
	require.Equal(t, http.StatusOK, rec.Code, "9.9.9.9's first request must pass")

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request("9.9.9.9"))
	require.Equal(t, http.StatusTooManyRequests, rec.Code, "9.9.9.9's second request must be rejected")

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request("8.8.8.8"))
	require.Equal(t, http.StatusOK, rec.Code, "8.8.8.8 must have an independent budget")
}

// TestRateLimiter_UntrustedPeerIgnoresXFF checks that an untrusted peer
// cannot use X-Forwarded-For to open a second budget.
func TestRateLimiter_UntrustedPeerIgnoresXFF(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4, nil)
	defer q.Close()

	handler := q.Middleware("getLabelers", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	request := func(xff string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "203.0.113.9:4444"
		req.Header.Set("X-Forwarded-For", xff)
		return req
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request("9.9.9.9"))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request("8.8.8.8"))
	require.Equal(t, http.StatusTooManyRequests, rec.Code,
		"a different XFF value must not open a second budget for an untrusted peer")
}

// TestRateLimiter_MappedSpellingSharesBucket checks bucket sharing across
// IPv4-mapped spellings and mapped-peer trust matching.
func TestRateLimiter_MappedSpellingSharesBucket(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4, loopbackTrusted)
	defer q.Close()

	handler := q.Middleware("getLabelers", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	request := func(remoteAddr, xff string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = remoteAddr
		req.Header.Set("X-Forwarded-For", xff)
		return req
	}

	// The mapped loopback peer is trusted, so the mapped non-trusted hop
	// keys as its IPv4 form.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request("[::ffff:127.0.0.1]:55555", "::ffff:1.2.3.4"))
	require.Equal(t, http.StatusOK, rec.Code, "the mapped loopback peer must still be trusted")

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request("127.0.0.1:55555", "1.2.3.4"))
	require.Equal(t, http.StatusTooManyRequests, rec.Code,
		"::ffff:1.2.3.4 and 1.2.3.4 must share one bucket")
}
