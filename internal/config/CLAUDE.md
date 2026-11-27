# Config

Last verified: 2026-11-27

## Purpose
Centralizes environment-variable-based configuration loading and validation. Ensures env reading happens only at the app boundary, never in library packages.

## Contracts
- **Exposes**: `Config` struct, `Load() (Config, error)`
- **Guarantees**:
  - All env vars prefixed `LABELER_RELAY_`
  - Validation rejects empty admin token, non-positive retention/rate-limit/buf-size values (including both query rate limits), buf-size > 65536, and a `LABELER_RELAY_FIREHOSE_URL` that is not a `ws://` or `wss://` URL with a host (error contains `firehose_url`)
  - `Config.TrustedProxies` is a `[]netip.Prefix`: unset `LABELER_RELAY_TRUSTED_PROXIES` defaults to loopback; explicitly empty or literal `none` yields no trusted proxies. Invalid CIDRs and IPv4-mapped IPv6 prefixes fail `Load` with an error containing `trusted_proxies`
  - Duration parsing supports Go's format plus "Nd" shorthand for days
  - Sensible defaults: `:8080`, `labeler-relay.db`, 14d retention, 500/s + 100k/h upstream rate limits, 10/s + 1200/h query rate limits, sig required
- **Expects**: `LABELER_RELAY_ADMIN_TOKEN` must be set (no default)

## Environment Variables
| Variable | Default | Notes |
|----------|---------|-------|
| LABELER_RELAY_DB_PATH | labeler-relay.db | SQLite database path |
| LABELER_RELAY_LISTEN_ADDR | :8080 | HTTP listen address |
| LABELER_RELAY_FIREHOSE_URL | wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos | Must be a `ws://` or `wss://` URL; the watcher dials it as given |
| LABELER_RELAY_ADMIN_TOKEN | (required) | Bearer token for /admin/ |
| LABELER_RELAY_RETENTION_WINDOW | 336h (14d) | Supports "Nd" shorthand |
| LABELER_RELAY_REQUIRE_SIG | true | Global default sig policy |
| LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED | true | Enable newly discovered labelers |
| LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC | 500 | Per-labeler |
| LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR | 100000 | Per-labeler |
| LABELER_RELAY_QUERY_RATE_LIMIT_PER_SEC | 10 | Per-client-IP, public query endpoints |
| LABELER_RELAY_QUERY_RATE_LIMIT_PER_HOUR | 1200 | Per-client-IP, public query endpoints |
| LABELER_RELAY_TRUSTED_PROXIES | 127.0.0.1/32,::1/128 | Proxy CIDRs allowed to set X-Forwarded-For for query rate-limit keying; set-but-empty or "none" disables proxy trust; IPv4-mapped IPv6 prefixes are rejected |
| LABELER_RELAY_SUBSCRIBER_BUF_SIZE | 512 | Per-subscriber event buffer (max 65536) |

## Dependencies
- **Uses**: Standard library only, including `os.LookupEnv` to distinguish unset from explicitly empty proxy trust
- **Used by**: main.go, which passes `TrustedProxies` through buildMux to the query rate limiter

## Key Files
- `config.go` - Config struct, Load, validate, env helpers, parseTrustedProxies
- `config_test.go` - Defaults, overrides, validation, and trusted-proxy parsing contracts

## Gotchas
- Proxy CIDRs are comma-separated; surrounding whitespace and empty entries are ignored. `none` is case-sensitive and applies to the whole value.
- The loopback trust default belongs to `Load`, not the server limiter constructor. Use IPv4 CIDRs directly rather than IPv4-mapped IPv6 prefixes.
