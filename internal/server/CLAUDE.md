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
  - Query responses are JSON projections limited to did, endpoint, source, enabled, last_upstream_seq (omitted when null); last_error, require_sig, and updated_at never appear
  - Query pagination is keyset over the DID-ordered list (cursor = last did of the previous page); limit outside 1-100 and nonempty enabled values other than true/false are rejected with 400, not clamped; absent or empty enabled applies no filter
  - The query rate limiter keys on the connection peer address (RemoteAddr), never X-Forwarded-For, and bounds its tracked-IP map at 65536 entries with idle eviction
- **Expects**: Hub.Broadcast wired as LabelPersist's broadcaster. Persist provides valid playback and head/floor. Registry provides DID-ordered lists.

## Dependencies
- **Uses**: store (LabelPersist, LabelerRegistry, Labeler, LiveEvent, CursorState), gorilla/websocket, RussellLuo/slidingwindow, indigo atproto/syntax (ParseDID), prometheus (MetricsHandler only)
- **Used by**: main.go (HTTP handler registration)
- **Boundary**: Must not import slurper, firehose, admin, config, or metrics

## Key Decisions
- Chunked backfill in StreamFrom: live subscription starts synchronously (before StreamFrom returns) to guarantee no events are missed. Large backlogs are drained in bufSize-sized chunks from the durable store; a background drainer discards live events during bulk backfill (Phase 1) and the final seam (Phase 2) to prevent hub buffer overflow. After the DB is drained (returns 0 rows), the drainer stops and a straggler read loop (bounded chunks without drainer) catches events drained from live but not yet read from DB (Phase 3). If the consumer can't keep up, the Hub drops the subscription (ConsumerTooSlow). Then the stream switches to pure live with dedup (Phase 4). An internal cancellable context (streamCtx) ensures cleanup can terminate all loops.
- Per-subscriber buffer (default 512, configurable via LABELER_RELAY_SUBSCRIBER_BUF_SIZE): bounds memory per consumer. Overflow triggers drop, not backpressure.
- Write timeout (5s): prevents a stalled client from blocking the handler goroutine.
- Query pagination and the enabled filter happen in the handler over one registry List call (no new store methods): fine at registry scale (hundreds of rows) and consistent with /_health.
- Query rate limiter reuses the slurper's two-tier sliding window pattern (per-second + per-hour) with an AllowN now-seam so tests drive windows without sleeping. Local-window StopFuncs are no-ops; Close exists for hygiene if the window implementation changes.

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
- `query.go` - HandleGetLabelers/HandleGetLabeler: public projection, keyset pagination, writeXRPCError
- `ratelimit.go` - QueryRateLimiter: per-IP two-tier sliding windows, Middleware, eviction

## Gotchas
- `kindToMsgType` prepends "#" to the stored kind string. If a new kind is added, it must be handled here or it defaults to "#<kind>".
- The WS upgrader accepts all origins (`CheckOrigin` returns true). Appropriate for a relay, but would need restricting for user-facing endpoints.
- The query rate limiter keys on RemoteAddr. Behind a single reverse proxy all clients share one bucket; X-Forwarded-For is deliberately ignored because any client can set it.
- Query error bodies use the XRPC shape `{"error", "message"}`, which differs from the admin package's single-key `{"error"}` shape. Do not unify them blindly; XRPC clients expect both keys.
- Query responses are snake_case (`last_upstream_seq`) like /_health — a deliberate deviation from atproto's camelCase lexicon convention, matching the issue's field list. Stream frames remain camelCase.
