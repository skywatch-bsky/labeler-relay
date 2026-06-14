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
	SubscriberBufSize       int
}

// Load reads configuration from environment variables and validates it.
// Environment variable names follow the LABELER_RELAY_<FIELD> convention.
// Returns a validated Config or a descriptive error.
//
// Env reading lives only at the app entry point / config loader — NOT in
// libraries — per house style.
func Load() (Config, error) {
	cfg := Config{
		DBPath:      getEnvOr("LABELER_RELAY_DB_PATH", "labeler-relay.db"),
		ListenAddr:  getEnvOr("LABELER_RELAY_LISTEN_ADDR", ":8080"),
		FirehoseURL: getEnvOr("LABELER_RELAY_FIREHOSE_URL", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"),
		AdminToken:  os.Getenv("LABELER_RELAY_ADMIN_TOKEN"),
	}

	var err error

	cfg.AutoSubscribeDiscovered, err = parseBoolEnv("LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED", "auto_subscribe_discovered", true)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	cfg.RequireSig, err = parseBoolEnv("LABELER_RELAY_REQUIRE_SIG", "require_sig", true)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	// Parse retention window.
	retentionStr := getEnvOr("LABELER_RELAY_RETENTION_WINDOW", "336h") // 14 days
	cfg.RetentionWindow, err = parseDuration(retentionStr)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: invalid retention_window %q: %w", retentionStr, err)
	}

	// Parse rate limits.
	cfg.UpstreamRateLimit.PerSec, err = parseIntEnv("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC", "upstream_rate_limit.per_sec", 500)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	cfg.UpstreamRateLimit.PerHour, err = parseIntEnv("LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR", "upstream_rate_limit.per_hour", 100000)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	cfg.SubscriberBufSize, err = parseIntEnv("LABELER_RELAY_SUBSCRIBER_BUF_SIZE", "subscriber_buf_size", 512)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
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
	if cfg.SubscriberBufSize <= 0 {
		return fmt.Errorf("subscriber_buf_size must be positive, got %d", cfg.SubscriberBufSize)
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

// parseBoolEnv parses a boolean env var, returning the fallback if unset and
// an error (using the lowercase field name) if the value is unparseable.
func parseBoolEnv(key, field string, fallback bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: %w", field, v, err)
	}
	return b, nil
}

// parseIntEnv parses an integer env var, returning the fallback if unset and
// an error (using the lowercase field name) if the value is unparseable.
func parseIntEnv(key, field string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, v, err)
	}
	return n, nil
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
