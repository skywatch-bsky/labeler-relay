package config_test

import (
	"testing"
	"time"

	"github.com/scarndp/labeler-relay/internal/config"
	"github.com/stretchr/testify/require"
)

// TestLoad_Defaults verifies that Load returns sensible defaults when no
// environment variables are set.
func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)

	// AdminToken must be set for Load to succeed (required field).
	t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "test-token")

	cfg, err := config.Load()
	require.NoError(t, err)

	require.Equal(t, "labeler-relay.db", cfg.DBPath)
	require.Equal(t, ":8080", cfg.ListenAddr)
	require.Equal(t, "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos", cfg.FirehoseURL)
	require.Equal(t, 14*24*time.Hour, cfg.RetentionWindow)
	require.True(t, cfg.AutoSubscribeDiscovered, "auto_subscribe_discovered should default to true")
	require.True(t, cfg.RequireSig, "require_sig should default to true")
	require.Equal(t, 50, cfg.UpstreamRateLimit.PerSec)
	require.Equal(t, 10000, cfg.UpstreamRateLimit.PerHour)
}

// TestLoad_EnvOverrides verifies that all fields can be overridden via env vars.
func TestLoad_EnvOverrides(t *testing.T) {
	clearEnv(t)

	t.Setenv("LABELER_RELAY_DB_PATH", "/tmp/custom.db")
	t.Setenv("LABELER_RELAY_LISTEN_ADDR", ":9090")
	t.Setenv("LABELER_RELAY_FIREHOSE_URL", "wss://firehose.example.com/sub")
	t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "secret-token")
	t.Setenv("LABELER_RELAY_RETENTION_WINDOW", "7d")
	t.Setenv("LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED", "false")
	t.Setenv("LABELER_RELAY_REQUIRE_SIG", "false")
	t.Setenv("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC", "100")
	t.Setenv("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR", "50000")

	cfg, err := config.Load()
	require.NoError(t, err)

	require.Equal(t, "/tmp/custom.db", cfg.DBPath)
	require.Equal(t, ":9090", cfg.ListenAddr)
	require.Equal(t, "wss://firehose.example.com/sub", cfg.FirehoseURL)
	require.Equal(t, "secret-token", cfg.AdminToken)
	require.Equal(t, 7*24*time.Hour, cfg.RetentionWindow)
	require.False(t, cfg.AutoSubscribeDiscovered)
	require.False(t, cfg.RequireSig)
	require.Equal(t, 100, cfg.UpstreamRateLimit.PerSec)
	require.Equal(t, 50000, cfg.UpstreamRateLimit.PerHour)
}

// TestLoad_ValidationErrors verifies that invalid configs produce descriptive errors.
func TestLoad_ValidationErrors(t *testing.T) {
	t.Run("empty admin token", func(t *testing.T) {
		clearEnv(t)
		// Do not set LABELER_RELAY_ADMIN_TOKEN.
		_, err := config.Load()
		require.Error(t, err)
		require.Contains(t, err.Error(), "admin_token")
	})

	t.Run("zero retention window", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "tok")
		t.Setenv("LABELER_RELAY_RETENTION_WINDOW", "0s")
		_, err := config.Load()
		require.Error(t, err)
		require.Contains(t, err.Error(), "retention_window")
	})

	t.Run("negative per_sec rate limit", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "tok")
		t.Setenv("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC", "-1")
		_, err := config.Load()
		require.Error(t, err)
		require.Contains(t, err.Error(), "per_sec")
	})

	t.Run("zero per_hour rate limit", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "tok")
		t.Setenv("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR", "0")
		_, err := config.Load()
		require.Error(t, err)
		require.Contains(t, err.Error(), "per_hour")
	})

	t.Run("invalid retention window string", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "tok")
		t.Setenv("LABELER_RELAY_RETENTION_WINDOW", "not-a-duration")
		_, err := config.Load()
		require.Error(t, err)
	})
}

// TestLoad_RetentionWindowFormats verifies that various duration formats parse correctly.
func TestLoad_RetentionWindowFormats(t *testing.T) {
	cases := []struct {
		input    string
		expected time.Duration
	}{
		{"24h", 24 * time.Hour},
		{"168h", 7 * 24 * time.Hour},
		{"336h0m0s", 14 * 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"14d", 14 * 24 * time.Hour},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "tok")
			t.Setenv("LABELER_RELAY_RETENTION_WINDOW", tc.input)
			cfg, err := config.Load()
			require.NoError(t, err, "input %q should parse", tc.input)
			require.Equal(t, tc.expected, cfg.RetentionWindow, "input %q", tc.input)
		})
	}
}

// clearEnv unsets all LABELER_RELAY_* env vars for the duration of the test
// by setting them to empty strings. t.Setenv restores on cleanup; Load treats
// empty strings as "not set" and applies defaults.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"LABELER_RELAY_DB_PATH",
		"LABELER_RELAY_LISTEN_ADDR",
		"LABELER_RELAY_FIREHOSE_URL",
		"LABELER_RELAY_ADMIN_TOKEN",
		"LABELER_RELAY_RETENTION_WINDOW",
		"LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED",
		"LABELER_RELAY_REQUIRE_SIG",
		"LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC",
		"LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR",
	} {
		t.Setenv(v, "")
	}
}
