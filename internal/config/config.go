// pattern: Imperative Shell

package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
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
	// QueryRateLimit bounds public per-client-IP query endpoint requests
	// (two-tier sliding window: per-second and per-hour).
	QueryRateLimit    RateLimit
	SubscriberBufSize int
	// TrustedProxies lists proxy CIDRs allowed to set X-Forwarded-For for
	// query rate-limit keys. Other peers key on their connection address.
	TrustedProxies []netip.Prefix
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

	retentionStr := getEnvOr("LABELER_RELAY_RETENTION_WINDOW", "336h") // 14 days
	cfg.RetentionWindow, err = parseDuration(retentionStr)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: invalid retention_window %q: %w", retentionStr, err)
	}

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

	cfg.QueryRateLimit.PerSec, err = parseIntEnv("LABELER_RELAY_QUERY_RATE_LIMIT_PER_SEC", "query_rate_limit.per_sec", 10)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	cfg.QueryRateLimit.PerHour, err = parseIntEnv("LABELER_RELAY_QUERY_RATE_LIMIT_PER_HOUR", "query_rate_limit.per_hour", 1200)
	if err != nil {
		return Config{}, fmt.Errorf("failed to load config: %w", err)
	}

	cfg.TrustedProxies, err = parseTrustedProxies("LABELER_RELAY_TRUSTED_PROXIES")
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
	if cfg.SubscriberBufSize > 65536 {
		return fmt.Errorf("subscriber_buf_size must be at most 65536, got %d", cfg.SubscriberBufSize)
	}
	if cfg.QueryRateLimit.PerSec <= 0 {
		return fmt.Errorf("query_rate_limit.per_sec must be positive, got %d", cfg.QueryRateLimit.PerSec)
	}
	if cfg.QueryRateLimit.PerHour <= 0 {
		return fmt.Errorf("query_rate_limit.per_hour must be positive, got %d", cfg.QueryRateLimit.PerHour)
	}
	// The watcher dials this URL as given, so anything it cannot dial must
	// fail here rather than on every dial after startup.
	u, err := url.Parse(cfg.FirehoseURL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" || u.User != nil {
		return fmt.Errorf("firehose_url must be a ws:// or wss:// URL with a hostname and no userinfo")
	}
	authority := u.Host
	if u.Port() == "" && !strings.HasSuffix(authority, ":") {
		authority += ":80"
	}
	if _, _, err := net.SplitHostPort(authority); err != nil {
		return fmt.Errorf("firehose_url must have a valid hostname or bracketed IPv6 address and optional port")
	}
	if strings.HasPrefix(u.Host, "[") {
		addr, err := netip.ParseAddr(u.Hostname())
		if err != nil || !addr.Is6() {
			return fmt.Errorf("firehose_url bracketed hostname must be an IPv6 address")
		}
	}
	if strings.HasSuffix(u.Host, ":") {
		return fmt.Errorf("firehose_url port must be an integer between 1 and 65535")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return fmt.Errorf("firehose_url port must be an integer between 1 and 65535")
		}
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

// parseTrustedProxies reads comma-separated CIDRs. Unset uses loopback
// defaults; empty or "none" disables proxy trust. Invalid CIDRs and
// IPv4-mapped IPv6 prefixes fail startup to avoid address-family mismatches.
func parseTrustedProxies(key string) ([]netip.Prefix, error) {
	raw, set := os.LookupEnv(key)
	if !set {
		return []netip.Prefix{
			netip.MustParsePrefix("127.0.0.1/32"),
			netip.MustParsePrefix("::1/128"),
		}, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "none" {
		return nil, nil
	}
	var trusted []netip.Prefix
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted_proxies entry %q: %w", entry, err)
		}
		if prefix.Addr().Is4In6() {
			return nil, fmt.Errorf("invalid trusted_proxies entry %q: trusted_proxies entries must not be IPv4-mapped IPv6; write the IPv4 CIDR directly", entry)
		}
		trusted = append(trusted, prefix)
	}
	return trusted, nil
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
