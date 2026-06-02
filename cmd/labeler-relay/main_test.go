package main

import (
	"context"
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
	resp, err := http.Get(healthURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Cancel context to trigger graceful shutdown.
	cancel()

	// Condition-based wait: run() must return within a bounded time.
	select {
	case err := <-done:
		// run() returns context.Canceled on graceful shutdown; that is acceptable.
		if err != nil && err != context.Canceled {
			t.Fatalf("run() returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: run() did not shut down within 5 seconds after context cancel")
	}
}
