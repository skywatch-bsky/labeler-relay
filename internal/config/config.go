// pattern: Imperative Shell

package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// RateLimit specifies per-labeler upstream rate limits.
type RateLimit struct {
	PerSec  int
	PerHour int
}

// Config holds all tunables for the labeler-relay process.
// Load reads these from environment variables; validation returns
// lowercase-fragment errors for composability.
type Config struct {
	DBPath                  string
	ListenAddr              string
	FirehoseURL             string
	AdminToken              string
	RetentionWindow         time.Duration
	AutoSubscribeDiscovered bool
	RequireSig              bool
	UpstreamRateLimit       RateLimit
}

// Load reads configuration from environment variables and validates it.
// Environment variable names follow the LABELER_RELAY_<FIELD> convention.
// Returns a validated Config or a descriptive error.
//
// Env reading lives only at the app entry point / config loader — NOT in
// libraries — per house style.
func Load() (Config, error) {
	cfg := Config{
		DBPath:                  getEnvOr("LABELER_RELAY_DB_PATH", "labeler-relay.db"),
		ListenAddr:              getEnvOr("LABELER_RELAY_LISTEN_ADDR", ":8080"),
		FirehoseURL:             getEnvOr("LABELER_RELAY_FIREHOSE_URL", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"),
		AdminToken:              os.Getenv("LABELER_RELAY_ADMIN_TOKEN"),
		AutoSubscribeDiscovered: parseBoolOr("LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED", true),
		RequireSig:              parseBoolOr("LABELER_RELAY_REQUIRE_SIG", true),
	}

	var err error

	// Parse retention window.
	retentionStr := getEnvOr("LABELER_RELAY_RETENTION_WINDOW", "336h") // 14 days
	cfg.RetentionWindow, err = parseDuration(retentionStr)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: invalid retention_window %q: %w", retentionStr, err)
	}

	// Parse rate limits.
	cfg.UpstreamRateLimit = RateLimit{
		PerSec:  parseIntOr("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC", 500),
		PerHour: parseIntOr("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR", 100000),
	}

	if err := validate(cfg); err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	return cfg, nil
}

// validate checks all invariants on a populated Config.
// Returns lowercase-fragment errors so they compose naturally.
func validate(cfg Config) error {
	if cfg.AdminToken == "" {
		return fmt.Errorf("admin_token is required")
	}
	if cfg.RetentionWindow <= 0 {
		return fmt.Errorf("retention_window must be positive, got %v", cfg.RetentionWindow)
	}
	if cfg.UpstreamRateLimit.PerSec <= 0 {
		return fmt.Errorf("upstream_rate_limit.per_sec must be positive, got %d", cfg.UpstreamRateLimit.PerSec)
	}
	if cfg.UpstreamRateLimit.PerHour <= 0 {
		return fmt.Errorf("upstream_rate_limit.per_hour must be positive, got %d", cfg.UpstreamRateLimit.PerHour)
	}
	return nil
}

// getEnvOr returns the env var value or the fallback if unset or empty.
func getEnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseBoolOr parses a boolean env var, returning the fallback if unset or unparseable.
func parseBoolOr(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

// parseIntOr parses an integer env var, returning the fallback if unset or unparseable.
func parseIntOr(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// parseDuration parses a duration string, supporting Go's time.ParseDuration
// format plus a shorthand "Nd" for N days (e.g. "7d" = 168h).
func parseDuration(s string) (time.Duration, error) {
	// Support "Nd" shorthand for N days.
	if strings.HasSuffix(s, "d") {
		daysStr := strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(daysStr)
		if err != nil {
			return 0, fmt.Errorf("invalid day count in %q: %w", s, err)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
