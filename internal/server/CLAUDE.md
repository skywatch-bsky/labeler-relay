# Server

Last verified: 2026-11-26

## Purpose
Serves the `community.labeler.sync.subscribeLabelers` output stream over WebSocket and the public XRPC query endpoints over the labeler registry. Handles cursor validation, backfill-to-live seam stitching, XRPC frame encoding, and per-client-IP rate limiting for queries.

## Contracts
- **Exposes**: `Server` (HandleSubscribeLabelers, HandleGetLabelers, HandleGetLabeler, HandleHealth), `Hub` (Broadcast, Subscribe), `StreamFrom` (backfill+live compositor), `QueryRateLimiter` (NewQueryRateLimiter, Middleware, SetRateLimitedCallback, Close), `MetricsHandler`, XRPC frame writers (WriteMessage, WriteError)
- **Guarantees**:
  - Backfill-to-live seam delivers exactly-once: live events with seq <= last backfill seq are dropped
  - Slow consumers are dropped (channel closed) rather than blocking the write path
  - FutureCursor returns an error frame; OutdatedCursor sends #info then resumes from floor
  - Hub.Broadcast is non-blocking: called under persist mutex, must return immediately
  - XRPC frames follow header+body CBOR concatenation (op:1 for messages, op:-1 for errors)
  - Query responses are JSON projections limited to did, endpoint, source, enabled, last_upstream_seq (always present; JSON null when the labeler has never ingested); last_error, require_sig, and updated_at never appear
  - Query pagination is keyset over the observed_seq-ordered list (cursor = the observed_seq of the last row of the previous page); limit outside 1-100, a non-integer or negative cursor, and nonempty enabled values other than true/false are rejected with 400, not clamped; absent or empty enabled applies no filter; a cursor past the current maximum yields HTTP 200 with an empty labelers array and no cursor
  - The query rate limiter keys on the connection peer address (RemoteAddr) unless the peer is inside the trusted proxy CIDRs (LABELER_RELAY_TRUSTED_PROXIES, loopback default): a trusted peer keys on the rightmost X-Forwarded-For hop outside the trusted set. An absent header, an all-trusted chain, or a malformed/zoned entry encountered during the walk falls back to the normalized peer; an unparseable RemoteAddr falls back to its raw string without consulting X-Forwarded-For. The tracked-IP map is bounded at 65536 entries with O(1) LRU eviction (the evicted entry is an idle one whenever any entry is idle)
- **Expects**: Hub.Broadcast wired as LabelPersist's broadcaster. Persist provides valid playback and head/floor. Registry provides observed_seq-ordered lists.

## Dependencies
- **Uses**: store (LabelPersist, LabelerRegistry, Labeler, LiveEvent, CursorState), gorilla/websocket, RussellLuo/slidingwindow, indigo atproto/syntax (ParseDID), prometheus (MetricsHandler only)
- **Used by**: main.go (HTTP handler registration)
- **Boundary**: Must not import slurper, firehose, admin, config, or metrics

## Key Decisions
- Chunked backfill in StreamFrom: live subscription starts synchronously (before StreamFrom returns) to guarantee no events are missed. Large backlogs are drained in bufSize-sized chunks from the durable store; a background drainer discards live events during bulk backfill (Phase 1) and the final seam (Phase 2) to prevent hub buffer overflow. After the DB is drained (returns 0 rows), the drainer stops and a straggler read loop (bounded chunks without drainer) catches events drained from live but not yet read from DB (Phase 3). If the consumer can't keep up, the Hub drops the subscription (ConsumerTooSlow). Then the stream switches to pure live with dedup (Phase 4). An internal cancellable context (streamCtx) ensures cleanup can terminate all loops.
- Per-subscriber buffer (default 512, configurable via LABELER_RELAY_SUBSCRIBER_BUF_SIZE): bounds memory per consumer. Overflow triggers drop, not backpressure.
- Write timeout (5s): prevents a stalled client from blocking the handler goroutine.
- Query pagination and the enabled filter happen in the handler over one registry List call (no new store methods): fine at registry scale (hundreds of rows) and consistent with /_health.
- Query rate limiter reuses the slurper's two-tier sliding window pattern (per-second + per-hour) with an AllowN now-seam so tests drive windows without sleeping. Recency is maintained incrementally (O(1) move-to-front per request; eviction takes the list tail), so capped admission never scans the tracked set. Local-window StopFuncs are no-ops; Close exists for hygiene if the window implementation changes.
- Trusted-proxy keying (`clientip.go`): pure `resolveClientIP` walks X-Forwarded-For right to left across header lines and comma-separated entries, skipping trusted hops and stopping at the first untrusted address. Entries left of that address are not inspected. `Unmap().WithZone("")` normalizes peer addresses for trust checks and bucket keys; zoned XFF entries encountered during traversal are rejected rather than normalized. Config rejects IPv4-mapped IPv6 CIDRs to avoid address-family mismatches.
- `NewQueryRateLimiter(perSec, perHour, trusted)` receives trust explicitly; nil/empty means trust nobody. Loopback defaults are supplied by config and passed through buildMux, not installed by the server package. Both query endpoints share one limiter and therefore share each client's budgets.

## Invariants
- StreamFrom's dedup boundary relies on relay_seq monotonicity: if persist ever minted non-monotonic seqs, dedup would break
- StreamFrom's cleanup closes the live channel (via liveCancel) to unblock the goroutine, then drains out. The live subscription MUST be synchronous (before StreamFrom returns) — moving it into the goroutine causes a race where events are lost between return and subscribe.
- Hub subscriber IDs are monotonically increasing integers (never reused within a process lifetime)
- Frame body bytes are written verbatim from store -- never re-encoded at the server layer
- /_health returns 200 with JSON (head_seq, labeler_count, retention_floor, retention_window_seconds) on success; 500 if a store read fails
- Query handlers return XRPC JSON error bodies for validation and store failures (400 InvalidRequest, 429 RateLimitExceeded from the limiter, 500 InternalServerError); detailed causes are logged, never returned. Method rejection (405 + Allow) is ServeMux's, outside the XRPC writer

## Key Files
- `subscribe.go` - HandleSubscribeLabelers: cursor validation, WS upgrade, stream loop
- `seam.go` - StreamFrom: backfill + live stitching with dedup boundary
- `hub.go` - Hub: fan-out with slow-consumer drop
- `frames.go` - XRPC frame encoding (FrameHeader, WriteMessage, WriteError)
- `health.go` - /_health JSON endpoint
- `metrics_endpoint.go` - /metrics Prometheus handler
- `query.go` - HandleGetLabelers/HandleGetLabeler: public projection, keyset pagination over observed_seq, writeXRPCError
- `ratelimit.go` - QueryRateLimiter: per-IP two-tier sliding windows, Middleware, eviction
- `clientip.go` - resolveClientIP: trusted-proxy-aware client-IP resolver for rate-limit keying

## Gotchas
- `kindToMsgType` prepends "#" to the stored kind string. If a new kind is added, it must be handled here or it defaults to "#<kind>".
- The WS upgrader accepts all origins (`CheckOrigin` returns true). Appropriate for a relay, but would need restricting for user-facing endpoints.
- X-Forwarded-For is honored only from trusted TCP peers. Configure only actual proxy ranges, and ensure those proxies append the observed upstream address or overwrite XFF with a verified chain; a trusted proxy that forwards an arbitrary client-supplied header unchanged permits spoofed bucket keys. Each intervening proxy must be trusted to recover the original client; otherwise the nearest untrusted hop is the key. Missing headers, fully trusted chains, and invalid entries encountered during traversal share the peer's bucket.
- `clientip_test.go` covers trust boundaries, multi-line/multi-hop chains, fallbacks, and address normalization; `ratelimit_test.go` covers middleware keying, including malformed RemoteAddr. Route tests cover separate client budgets behind a trusted proxy.
- Query error bodies use the XRPC shape `{"error", "message"}`, which differs from the admin package's single-key `{"error"}` shape. Do not unify them blindly; XRPC clients expect both keys.
- Query responses are snake_case (`last_upstream_seq`) like /_health, and the getLabelers pagination cursor is a plain integer (the row's `observed_seq`) rather than atproto's opaque string — both deliberate deviations from atproto conventions, matching the issue's field list. Stream frames remain camelCase.
