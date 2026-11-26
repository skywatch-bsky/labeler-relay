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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rlBase is the fixed timestamp the limiter tests advance from.
var rlBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestRateLimiter_AllowAndReject(t *testing.T) {
	q := newQueryRateLimiter(2, 1000, 4)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.1", rlBase))
	require.True(t, q.allowAt("10.0.0.1", rlBase.Add(time.Millisecond)))
	require.False(t, q.allowAt("10.0.0.1", rlBase.Add(2*time.Millisecond)),
		"third request within the per-second budget must be rejected")
}

func TestRateLimiter_HourlyBudget(t *testing.T) {
	// Generous per-second budget, tiny hourly budget: the rejection must
	// come from the hourly tier only.
	q := newQueryRateLimiter(100, 3, 4)
	defer q.Close()

	for i := 0; i < 3; i++ {
		require.True(t, q.allowAt("10.0.0.2", rlBase.Add(time.Duration(i)*time.Millisecond)))
	}
	require.False(t, q.allowAt("10.0.0.2", rlBase.Add(3*time.Millisecond)),
		"fourth request within the hourly budget must be rejected")
}

func TestRateLimiter_WindowRecovery(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.3", rlBase))
	require.False(t, q.allowAt("10.0.0.3", rlBase.Add(time.Millisecond)))

	// Past the per-second window the budget resets.
	require.True(t, q.allowAt("10.0.0.3", rlBase.Add(2*time.Second)))
}

func TestRateLimiter_HourlyWindowRecovery(t *testing.T) {
	q := newQueryRateLimiter(1000, 2, 4)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.4", rlBase))
	require.True(t, q.allowAt("10.0.0.4", rlBase.Add(time.Millisecond)))
	require.False(t, q.allowAt("10.0.0.4", rlBase.Add(2*time.Millisecond)),
		"hourly budget exhausted")

	// The hourly window expires an hour after the first take.
	require.True(t, q.allowAt("10.0.0.4", rlBase.Add(time.Hour+time.Second)))
}

func TestRateLimiter_PerIPIsolation(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4)
	defer q.Close()

	require.True(t, q.allowAt("10.0.0.5", rlBase))
	require.False(t, q.allowAt("10.0.0.5", rlBase))
	require.True(t, q.allowAt("10.0.0.6", rlBase), "a second IP must have its own budget")
}

func TestRateLimiter_CapEviction(t *testing.T) {
	// Cap 4: filling the map and adding one more must evict the least
	// recently seen entry, keeping the map bounded.
	q := newQueryRateLimiter(10, 1000, 4)
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
	q := newQueryRateLimiter(10, 1000, 4)
	defer q.Close()

	for i := 0; i < 4; i++ {
		require.True(t, q.allowAt(fmt.Sprintf("10.0.2.%d", i), rlBase))
	}
	require.Len(t, q.windows, 4)

	// Every tracked entry is idle beyond the horizon; all are evicted and
	// only the new IP remains.
	require.True(t, q.allowAt("10.0.2.9", rlBase.Add(ipIdleEviction+time.Minute)))
	require.Len(t, q.windows, 1)
}

func TestQuery_RateLimited_Response(t *testing.T) {
	q := newQueryRateLimiter(1, 1000, 4)
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
	q := newQueryRateLimiter(1, 1000, 4)
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
