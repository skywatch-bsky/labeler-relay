# Human Test Plan: labeler-relay

**Generated:** 2026-06-02
**Coverage:** All 30 acceptance criteria (AC1.1-AC11.2) verified by automated tests. This plan covers manual verification for operational scenarios not fully testable offline.

## Prerequisites

- Go 1.22+ toolchain
- `go test ./... -race` passes (all offline tests green)
- Network access to `mod.bsky.app` and `ozone.skywatch.blue` for E2E tests
- `LABELER_RELAY_ADMIN_TOKEN` env var set for admin API testing

## Phase 1: Offline Suite Smoke

| Step | Action | Expected |
|------|--------|----------|
| 1 | `go test ./... -race -count=1 -timeout 120s` | All 12 packages pass, zero failures, no DATA RACE |
| 2 | `go test ./internal/store/... -run TestPersistIngestConcurrencySeqUniqueness -count=5 -race` | All 5 runs pass |

## Phase 2: Network E2E

| Step | Action | Expected |
|------|--------|----------|
| 1 | `go test -tags integration ./internal/e2e/ -run TestN2 -v -timeout 5m` | Keys resolved for both DIDs, labels from both origins, crypto passes, provenance clean, relay_seq monotonic |

## Phase 3: Admin API

| Step | Action | Expected |
|------|--------|----------|
| 1 | Start relay: `go run ./cmd/labeler-relay` | Listens on configured port |
| 2 | `GET /_health` | 200 JSON with head_seq, labeler_count, retention_floor |
| 3 | `POST /admin/labelers` with valid bearer token + DID | 201 Created |
| 4 | Same POST without Authorization header | 401 Unauthorized |
| 5 | `DELETE /admin/labelers/{did}` with token | 204 No Content |

## Phase 4: Live Consumer Stream

| Step | Action | Expected |
|------|--------|----------|
| 1 | Connect WS client to `/xrpc/community.labeler.sync.subscribeLabelers` (no cursor) | Binary CBOR frames stream: op:1 with t:#labels or t:#service |
| 2 | Disconnect, reconnect with `?cursor=<last_seen_seq>` | First frame is last_seen_seq+1, no gap |
| 3 | Connect with `?cursor=999999999` | FutureCursor error frame (op:-1), conn closes |
| 4 | Connect with `?cursor=0` after prune | OutdatedCursor #info frame, then backfill from floor |

## Phase 5: Crash Resume

| Step | Action | Expected |
|------|--------|----------|
| 1 | Run relay 30+ seconds, note head_seq | Labels accumulating |
| 2 | `kill -9 <pid>` | Process terminates |
| 3 | Restart with same DB | Upstreams resume from persisted cursors; head_seq continues, no reset |
| 4 | Connect consumer with pre-crash cursor | Backfill spans the crash boundary with no gap |

## Phase 6: Metrics

| Step | Action | Expected |
|------|--------|----------|
| 1 | `GET /metrics` | Prometheus format with head_seq, connected_upstreams, consumer_count, retention_floor, dropped_unsigned_total, throttled_total, ingested_total |
| 2 | Connect/disconnect a WS consumer | consumer_count increments then decrements |

## Human Verification Required (not automatable)

| Area | Why | Steps |
|------|-----|-------|
| Throughput under load | Tests use small event counts | Monitor head_seq growth + throttle counters over 10+ minutes |
| Upstream failure resilience | Tests use cooperative fakes | Kill one labeler endpoint, verify the other continues |
| Third-party client interop | Tests use same encode/decode lib | Connect a different CBOR WS client, verify frames parse |
| Long-running resource usage | Short-lived tests can't catch leaks | Run 24+ hours, monitor RSS/goroutines/FDs/WAL size |
| TLS/reverse proxy | Tests use plain HTTP | Deploy behind nginx/caddy with TLS, verify WSS |

## AC Traceability

All 30 ACs (AC1.1-AC11.2) have automated test coverage. See `docs/implementation-plans/2026-06-01-labeler-relay/test-requirements.md` for the full mapping from ACs to test files.
