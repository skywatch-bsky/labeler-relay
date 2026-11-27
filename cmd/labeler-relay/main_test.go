package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// freePort returns a random available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// waitForHTTP polls addr until it returns a 200 or the deadline elapses.
// Uses condition-based waiting rather than a fixed sleep.
func waitForHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s to be ready", url)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSmoke_BootHealthShutdown starts run() with a temp DB and free port,
// verifies /_health returns 200, then cancels the context and asserts clean
// shutdown within a bounded time (condition-based, no fixed sleep).
func TestSmoke_BootHealthShutdown(t *testing.T) {
	port := freePort(t)
	dbPath := t.TempDir() + "/smoke.db"
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)

	t.Setenv("LABELER_RELAY_DB_PATH", dbPath)
	t.Setenv("LABELER_RELAY_LISTEN_ADDR", listenAddr)
	t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "smoke-test-token")
	// Point firehose at a non-existent address; the watcher will retry in the
	// background but never succeed, which is fine for this lifecycle test.
	t.Setenv("LABELER_RELAY_FIREHOSE_URL", "ws://127.0.0.1:1/nope")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- run(ctx)
	}()

	// Condition-based wait: poll /_health until it responds 200 OK.
	healthURL := fmt.Sprintf("http://%s/_health", listenAddr)
	waitForHTTP(t, healthURL, 5*time.Second)

	// Verify the health response is valid JSON with the expected shape.
	// The body is fully read and closed BEFORE cancel: an unread response
	// body holds the keep-alive connection open against
	// httpServer.Shutdown.
	resp, err := http.Get(healthURL)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var healthBody map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&healthBody))
	require.NoError(t, resp.Body.Close())

	// Cancel context to trigger graceful shutdown.
	cancel()

	// Condition-based wait: run() must return within a bounded time. The
	// budget is 8s, deliberately above run()'s own 5s httpServer.Shutdown
	// deadline: net/http's Shutdown only reaps connections still in
	// StateNew after 5 seconds (golang/go#22682), so a connection accepted
	// without a request keeps Shutdown busy until its deadline — and a 5s
	// test budget raced that exact threshold. 8s still catches real hangs
	// while tolerating the documented reap rule.
	select {
	case err := <-done:
		// Acceptable outcomes: nil or context.Canceled (normal graceful
		// shutdown), or context.DeadlineExceeded — run()'s own
		// httpServer.Shutdown deadline (5s) expiring while net/http reaps a
		// connection still in StateNew, which it only closes after 5 seconds
		// (golang/go#22682). All three mean the process stopped serving and
		// exited within its bounded shutdown window.
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Fatalf("run() returned unexpected error: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("timeout: run() did not shut down within 8 seconds after context cancel")
	}
}
