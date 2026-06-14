# Server

Last verified: 2026-06-13

## Purpose
Serves the `community.labeler.sync.subscribeLabelers` output stream over WebSocket. Handles cursor validation, backfill-to-live seam stitching, and XRPC frame encoding.

## Contracts
- **Exposes**: `Server` (HandleSubscribeLabelers, HandleHealth), `Hub` (Broadcast, Subscribe), `StreamFrom` (backfill+live compositor), `MetricsHandler`, XRPC frame writers (WriteMessage, WriteError)
- **Guarantees**:
  - Backfill-to-live seam delivers exactly-once: live events with seq <= last backfill seq are dropped
  - Slow consumers are dropped (channel closed) rather than blocking the write path
  - Dead consumers (vanished peers, no close frame) are detected within pingInterval + pongDeadline (default 30s + 10s) via read pump + server-initiated pings
  - Client close frames are processed promptly by the read pump — no need to wait for a write failure
  - FutureCursor returns an error frame; OutdatedCursor sends #info then resumes from floor
  - Hub.Broadcast is non-blocking: called under persist mutex, must return immediately
  - XRPC frames follow header+body CBOR concatenation (op:1 for messages, op:-1 for errors)
- **Expects**: Hub.Broadcast wired as LabelPersist's broadcaster. Persist provides valid playback and head/floor.

## Dependencies
- **Uses**: store (LabelPersist, LabelerRegistry, LiveEvent, CursorState), gorilla/websocket, prometheus (MetricsHandler only)
- **Used by**: main.go (HTTP handler registration)
- **Boundary**: Must not import slurper, firehose, admin, or config

## Key Decisions
- Chunked backfill in StreamFrom: live subscription starts synchronously (before StreamFrom returns) to guarantee no events are missed. Large backlogs are drained in bufSize-sized chunks from the durable store; a background drainer discards live events during this phase to prevent hub buffer overflow. Once within bufSize of head, the drainer stops and the final seam reads remaining backfill then switches to live with dedup.
- Per-subscriber buffer (default 512, configurable via LABELER_RELAY_SUBSCRIBER_BUF_SIZE): bounds memory per consumer. Overflow triggers drop, not backpressure.
- Write timeout (5s): prevents a stalled client from blocking the handler goroutine.
- Read pump + ping/pong per connection: read pump goroutine discards inbound messages and cancels a derived context on read error. Ping loop sends periodic pings; pong handler resets the read deadline. r.Context() is inert after WS hijack, so the derived context is the sole cancellation signal.

## Invariants
- StreamFrom's dedup boundary relies on relay_seq monotonicity: if persist ever minted non-monotonic seqs, dedup would break
- StreamFrom's cleanup closes the live channel (via liveCancel) to unblock the goroutine, then drains out. The live subscription MUST be synchronous (before StreamFrom returns) — moving it into the goroutine causes a race where events are lost between return and subscribe.
- Hub subscriber IDs are monotonically increasing integers (never reused within a process lifetime)
- Frame body bytes are written verbatim from store -- never re-encoded at the server layer
- /_health returns 200 with JSON (head_seq, labeler_count, retention_floor, retention_window_seconds) on success; 500 if a store read fails

## Key Files
- `subscribe.go` - HandleSubscribeLabelers: cursor validation, WS upgrade, read pump, ping loop, stream loop
- `seam.go` - StreamFrom: backfill + live stitching with dedup boundary
- `hub.go` - Hub: fan-out with slow-consumer drop
- `frames.go` - XRPC frame encoding (FrameHeader, WriteMessage, WriteError)
- `health.go` - /_health JSON endpoint
- `metrics_endpoint.go` - /metrics Prometheus handler

## Gotchas
- `kindToMsgType` prepends "#" to the stored kind string. If a new kind is added, it must be handled here or it defaults to "#<kind>".
- The WS upgrader accepts all origins (`CheckOrigin` returns true). Appropriate for a relay, but would need restricting for user-facing endpoints.
- Ping and pong share the WebSocket connection with the write loop. The ping loop sets its own write deadline before each ping; the write loop sets its own deadline before each data frame. gorilla/websocket serializes concurrent writes internally, but interleaving deadline-sets with writes from different goroutines is safe because each caller sets its deadline immediately before its own write.
