# Server

Last verified: 2026-06-13

## Purpose
Serves the `community.labeler.sync.subscribeLabelers` output stream over WebSocket. Handles cursor validation, backfill-to-live seam stitching, and XRPC frame encoding.

## Contracts
- **Exposes**: `Server` (HandleSubscribeLabelers, HandleHealth), `Hub` (Broadcast, Subscribe), `StreamFrom` (backfill+live compositor), `MetricsHandler`, XRPC frame writers (WriteMessage, WriteError)
- **Guarantees**:
  - Backfill-to-live seam delivers exactly-once: live events with seq <= last backfill seq are dropped
  - Slow consumers are dropped (channel closed) rather than blocking the write path
  - FutureCursor returns an error frame; OutdatedCursor sends #info then resumes from floor
  - Hub.Broadcast is non-blocking: called under persist mutex, must return immediately
  - XRPC frames follow header+body CBOR concatenation (op:1 for messages, op:-1 for errors)
- **Expects**: Hub.Broadcast wired as LabelPersist's broadcaster. Persist provides valid playback and head/floor.

## Dependencies
- **Uses**: store (LabelPersist, LabelerRegistry, LiveEvent, CursorState), gorilla/websocket, prometheus (MetricsHandler only)
- **Used by**: main.go (HTTP handler registration)
- **Boundary**: Must not import slurper, firehose, admin, or config

## Key Decisions
- Chunked backfill in StreamFrom: live subscription starts synchronously (before StreamFrom returns) to guarantee no events are missed. Large backlogs are drained in bufSize-sized chunks from the durable store; a background drainer discards live events during bulk backfill (Phase 1) and the final seam (Phase 2) to prevent hub buffer overflow. After the DB is drained (returns 0 rows), the drainer stops and a final straggler read loop (bounded chunks without drainer) catches events drained from live but not yet read from DB. An internal cancellable context (streamCtx) ensures cleanup can terminate all loops. Then the stream switches to live with dedup.
- Per-subscriber buffer (default 512, configurable via LABELER_RELAY_SUBSCRIBER_BUF_SIZE): bounds memory per consumer. Overflow triggers drop, not backpressure.
- Write timeout (5s): prevents a stalled client from blocking the handler goroutine.

## Invariants
- StreamFrom's dedup boundary relies on relay_seq monotonicity: if persist ever minted non-monotonic seqs, dedup would break
- StreamFrom's cleanup closes the live channel (via liveCancel) to unblock the goroutine, then drains out. The live subscription MUST be synchronous (before StreamFrom returns) — moving it into the goroutine causes a race where events are lost between return and subscribe.
- Hub subscriber IDs are monotonically increasing integers (never reused within a process lifetime)
- Frame body bytes are written verbatim from store -- never re-encoded at the server layer
- /_health returns 200 with JSON (head_seq, labeler_count, retention_floor, retention_window_seconds) on success; 500 if a store read fails

## Key Files
- `subscribe.go` - HandleSubscribeLabelers: cursor validation, WS upgrade, stream loop
- `seam.go` - StreamFrom: backfill + live stitching with dedup boundary
- `hub.go` - Hub: fan-out with slow-consumer drop
- `frames.go` - XRPC frame encoding (FrameHeader, WriteMessage, WriteError)
- `health.go` - /_health JSON endpoint
- `metrics_endpoint.go` - /metrics Prometheus handler

## Gotchas
- `kindToMsgType` prepends "#" to the stored kind string. If a new kind is added, it must be handled here or it defaults to "#<kind>".
- The WS upgrader accepts all origins (`CheckOrigin` returns true). Appropriate for a relay, but would need restricting for user-facing endpoints.
