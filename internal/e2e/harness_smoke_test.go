// pattern: Imperative Shell (test)

package e2e_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scarndp/labeler-relay/internal/e2e"
)

// TestHarnessBootAndHealth boots the harness with no labelers registered,
// asserts /_health returns 200, then shuts down cleanly.
// This is an offline smoke test that exercises the harness lifecycle.
func TestHarnessBootAndHealth(t *testing.T) {
	h, err := e2e.NewHarness(t)
	require.NoError(t, err)
	defer h.Close()

	healthURL := "http://" + h.Addr() + "/_health"

	// Condition-based wait: poll /_health until 200 or timeout.
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				lastErr = nil
				break
			}
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, lastErr, "/_health did not return 200 within timeout")

	resp, err := http.Get(healthURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
