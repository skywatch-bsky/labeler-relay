# Config

Last verified: 2026-06-13

## Purpose
Centralizes environment-variable-based configuration loading and validation. Ensures env reading happens only at the app boundary, never in library packages.

## Contracts
- **Exposes**: `Config` struct, `Load() (Config, error)`
- **Guarantees**:
  - All env vars prefixed `LABELER_RELAY_`
  - Validation rejects empty admin token, non-positive retention/rate-limit values
  - Duration parsing supports Go's format plus "Nd" shorthand for days
  - Sensible defaults: `:8080`, `labeler-relay.db`, 14d retention, 500/s + 100k/h rate limits, sig required
- **Expects**: `LABELER_RELAY_ADMIN_TOKEN` must be set (no default)

## Environment Variables
| Variable | Default | Notes |
|----------|---------|-------|
| LABELER_RELAY_DB_PATH | labeler-relay.db | SQLite database path |
| LABELER_RELAY_LISTEN_ADDR | :8080 | HTTP listen address |
| LABELER_RELAY_FIREHOSE_URL | wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos | |
| LABELER_RELAY_ADMIN_TOKEN | (required) | Bearer token for /admin/ |
| LABELER_RELAY_RETENTION_WINDOW | 336h (14d) | Supports "Nd" shorthand |
| LABELER_RELAY_REQUIRE_SIG | true | Global default sig policy |
| LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_SEC | 500 | Per-labeler |
| LABELER_RELAY_UPSTREAM_RATE_LIMIT_PER_HOUR | 100000 | Per-labeler |
| LABELER_RELAY_SUBSCRIBER_BUF_SIZE | 512 | Per-subscriber event buffer |

## Key Files
- `config.go` - Config struct, Load, validate, env helpers
