# Labeler Relay Implementation Plan — Phase 5

**Goal:** Serve the unified `community.labeler.sync.subscribeLabelers` stream with backfill→live handoff, slow-consumer disconnect, and a health endpoint.

**Architecture:** An XRPC/WebSocket handler serves the unified stream. The handler parses `cursor`, validates it via `LabelPersist.CursorStatus` (Phase 2), then runs **our own backfill→live seam** (operator-confirmed): a `Hub` registers the subscriber for live `LiveEvent`s from `LabelPersist`'s broadcaster (buffering from the moment of subscribe), drains `LabelPersist.Playback(since)` in ascending relay_seq, then switches to the buffered live feed, **suppressing any live event whose `RelaySeq <= lastBackfillSeq`** — giving no gap and no duplicate at the seam, keyed on OUR relay seq. Each delivered event is framed as `{op,t}` header + stored `frame_cbor` body and written to the WebSocket. Slow consumers are dropped by the Hub (bounded per-subscriber buffer; overflow → `ConsumerTooSlow` + disconnect).

**Why we do NOT use `EventManager.Subscribe(since)`:** code review confirmed `XRPCStreamEvent.Sequence()` returns `-1` for label events, so indigo's catch-up boundary never terminates and produces duplicates for our payloads. We own the relay seq, so we own the seam. We MAY still reuse `EventManager` purely as the live broadcaster/fan-out primitive, but the simplest correct design is a small local `Hub` (≈100 lines) that fans out `LiveEvent`s and enforces the slow-consumer policy. This phase builds the `Hub`; it does not fork or depend on EventManager's sequencing.

**Tech Stack:** Go, indigo (`cmd/relay/stream/eventmgr`, `cmd/relay/stream`), `github.com/gorilla/websocket`, `net/http`.

**Scope:** 7 phases. This is Phase 5 of 7.

**Codebase verified:** 2026-06-01. indigo @ `5368f55344e0b5e203ab4d9501e1ddebd9983ad7`. Phases 1–4 produced output types, store/persist (with Playback + CursorStatus + Head + RetentionFloor), slurper, and discovery.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### labeler-relay.AC2: Unified output stream & cursors
- **labeler-relay.AC2.1 Success:** A consumer connecting with no cursor receives live `#labels` and `#service` messages on `community.labeler.sync.subscribeLabelers`.
- **labeler-relay.AC2.2 Success:** A consumer connecting with `cursor=N` receives backfill from `relay_seq > N` in order, then continues live with no gap or duplicate at the seam. *(Completes the live-seam portion begun in Phase 2.)*
- **labeler-relay.AC2.3 Failure:** A `cursor` above the current head returns a `FutureCursor` error frame.
- **labeler-relay.AC2.4 Edge:** A `cursor` below the retention floor returns `#info OutdatedCursor` and resumes from the floor.

### labeler-relay.AC9: Consumer lifecycle
- **labeler-relay.AC9.1 Success:** A consumer too slow to drain is disconnected rather than blocking the broadcast.
- **labeler-relay.AC9.2 Success:** A reconnecting consumer resumes from its last cursor and backfills the gap.

### labeler-relay.AC10 (partial)
- **labeler-relay.AC10.3 Success:** A health/metrics endpoint reports head seq, labeler count, and retention window. *(Health endpoint built here; metrics surface completed in Phase 6.)*

---

## Key research findings (ground truth from indigo @ 5368f553)

- **`XRPCStreamEvent.Sequence()` returns `-1` for `LabelLabels`/`LabelInfo`** (no case in the switch). This is WHY `EventManager.Subscribe(since)`'s seam cannot be used for our payloads: `lastSeq` never advances past `since` and the `ErrCaughtUp` boundary (`seq > SequenceForEvent(first)` → `-1 > -1` = false) never trips → duplicate replay + non-termination. **Confirmed by code review against indigo source** (`events.go:155-174`, `event_manager.go:148-210`).
- **Wire contract (pinned):** union discrimination is via the `{op,t}` frame header (the `subscribeRepos` precedent), NOT a body `$type`. `t` is `"#labels"`, `"#service"`, or `"#info"`. The stored `frame_cbor` is the body; consumers read `t` to pick the body type. Phase 1's lexicon `union` of refs describes the logical message set; on the wire, the header `t` is the discriminator.
- **Our live path:** `LabelPersist.PersistIngest` (Phase 2) calls the registered broadcaster with a `LiveEvent{RelaySeq, Kind, LabelerDID, FrameCBOR}` after each commit. The `Hub` (Task 1) is that broadcaster's fan-out. The handler writes each `LiveEvent.FrameCBOR` with a `{op:1, t}` header (`t` from `Kind`).
- If `EventManager` is reused for live fan-out instead of a local `Hub`, it must be fed events whose body is our `frame_cbor` and the handler must ignore indigo's `Serialize()` — but the local `Hub` is simpler and avoids the `-1`-Sequence trap entirely. **Default to the local Hub.**

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Live fan-out Hub with slow-consumer disconnect

**Verifies:** labeler-relay.AC2.1 (live path), labeler-relay.AC9.1

**Files:**
- Create: `internal/server/hub.go`
- Test: `internal/server/hub_test.go` (integration)

**Implementation:**

A small in-process fan-out for live `LiveEvent`s. It is registered as `LabelPersist`'s broadcaster (Phase 2 `SetBroadcaster`).

```go
type Hub struct {
    mu   sync.Mutex
    subs map[int]*hubSub // id → subscriber
    next int
}

type hubSub struct {
    ch     chan store.LiveEvent
    cancel func() // marks the sub for removal
}

func NewHub() *Hub

// Broadcast is registered via LabelPersist.SetBroadcaster. Non-blocking per
// subscriber: if a sub's buffer is full, the sub is marked slow, its channel is
// closed, and it is removed — the broadcast never blocks on a slow consumer.
func (h *Hub) Broadcast(e store.LiveEvent)

// Subscribe returns a buffered channel of live events and a cleanup func.
// bufSize bounds the per-subscriber backlog; overflow ⇒ slow-consumer drop.
func (h *Hub) Subscribe(bufSize int) (<-chan store.LiveEvent, func())
```

`Broadcast` iterates subscribers; for each, `select { case sub.ch <- e: default: /* full → drop sub */ }`. Dropping closes the channel so the handler's range loop ends and it can send a `ConsumerTooSlow` error frame before closing the WS.

**Testing:**
Integration (no network):
- AC2.1 (live, single): wire `Hub` as broadcaster on a real `LabelPersist`; `Subscribe`; `PersistIngest` a `labels` then a `service` event; assert both arrive on the channel as `LiveEvent`s carrying the minted relay seq, in order. `waitFor` on the channel, not a sleep.
- AC2.1 (live, **concurrent ordering**): subscribe a draining consumer; fire N goroutines × M `PersistIngest` calls concurrently; assert the consumer observes `RelaySeq` values **strictly ascending** with no reorder. This is the live-path counterpart to the Phase-2 broadcast-ordering test and only passes if broadcast runs under the persist `mu` (Phase 2 Task 4). A single-goroutine "in order" test is insufficient — it cannot catch a reorder.
- AC9.1: subscribe with a tiny `bufSize`; flood `Broadcast` past the buffer WITHOUT draining; assert the slow sub's channel is closed (dropped) AND that a second, draining subscriber keeps receiving every event (slow one didn't block the broadcast). Condition-based on the healthy sub's received count.

**Non-blocking contract:** `Hub.Broadcast` is invoked by `LabelPersist` while it holds the persist mutex (Phase 2 Task 4), so `Broadcast` MUST be non-blocking per subscriber (`select { case ch <- e: default: drop }`). Never add a blocking send here — it would stall the entire write path under the mutex.

**Verification:**
Run: `go test ./internal/server/ -run TestHub -race -v`
Expected: pass under `-race`.

**Commit:** `feat: add live fan-out hub with slow-consumer disconnect`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Backfill→live seam (our own, over Playback + Hub)

**Verifies:** labeler-relay.AC2.2, labeler-relay.AC9.2

**Files:**
- Create: `internal/server/seam.go`
- Test: `internal/server/seam_test.go` (integration)

**Implementation:**

The seam composes Phase-2 `Playback` with the Task-1 `Hub`, keyed on our relay seq. **Order matters: register for live BEFORE draining backfill**, so no event is missed in the window between the two.

```go
// StreamFrom delivers events with relay_seq > since in ascending order with no
// gap and no duplicate, then continues live. It returns a channel of LiveEvent
// and a cleanup func. The caller frames + writes each event.
func StreamFrom(ctx context.Context, p *store.LabelPersist, hub *Hub, since int64, bufSize int) (<-chan store.LiveEvent, func(), error)
```

Algorithm:
1. `live, cancel := hub.Subscribe(bufSize)` — start buffering live events NOW (from current head onward).
2. Spawn a goroutine that:
   a. `lastBackfill := since; p.Playback(ctx, since, func(e) { out <- e; lastBackfill = e.RelaySeq; return nil })` — drain the durable backlog in ascending order.
   b. Then range over `live`: **drop any `e.RelaySeq <= lastBackfill`** (these were already sent during backfill — this is the dedup boundary), forward the rest to `out`.
   c. On ctx done or `live` closed (slow-consumer drop), close `out` and `cancel()`.
3. Return `out` and a cleanup that cancels + drains.

Because both backfill and live are ordered by the SAME monotonic relay seq — AND live events are broadcast in seq order (guaranteed by Phase 2 Task 4 broadcasting under the persist `mu`) — the `> lastBackfill` filter guarantees exactly-one delivery across the seam in ascending order (AC2.2: no gap, no dup, ascending). A consumer reconnecting with `since = lastReceived` gets the next seq with no gap (AC9.2).

**Backfill backpressure (intended behaviour):** during stage 2a the goroutine forwards potentially many backlog rows to `out` while live events accumulate in the Hub's bounded buffer (`bufSize`). For a large backlog behind a slow WS client, the Hub buffer can overflow DURING backfill and the subscriber is dropped as slow before reaching live. This is intended — a consumer that cannot keep up with backfill IS a slow consumer. Size `bufSize` deliberately (large enough to absorb normal live arrival during a typical backfill drain; small enough to shed genuinely stuck clients). Document the chosen default and rationale at the call site.

**Testing:**
- AC2.2: `PersistIngest` events 1..10. Call `StreamFrom(since=4)`. Concurrently `PersistIngest` 11..13 shortly after subscribing (timed to land during or after backfill — both cases must work). Assert `out` yields seqs 5,6,...,13 strictly ascending, no gaps, no duplicates. The critical case: an event persisted DURING backfill (so it appears in both the live buffer and possibly the Playback set) must be delivered exactly once — assert no dup at the boundary specifically.
- AC9.2: consume up to seq K, cancel; re-`StreamFrom(since=K)`; assert first delivered is K+1 with no gap.
- Condition-based waits throughout; no fixed sleeps for assertions.

**Verification:**
Run: `go test ./internal/server/ -run TestSeam -race -v`
Expected: pass under `-race`.

**Commit:** `feat: add relay-seq backfill-to-live seam over Playback and hub`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-5) -->
<!-- START_TASK_3 -->
### Task 3: WebSocket frame writer ({op,t} header + body)

**Verifies:** labeler-relay.AC2.1 (wire framing), labeler-relay.AC2.3, labeler-relay.AC2.4

**Files:**
- Create: `internal/server/frames.go`
- Test: `internal/server/frames_test.go` (unit + property-based)

**Implementation:**

Pure-ish helpers that write XRPC subscription frames to a WebSocket. An XRPC stream frame is `header CBOR` then `body CBOR` in one binary message. The header is `{op: 1, t: "#labels"}` for messages and `{op: -1}` for errors.

```go
type frameHeader struct {
    Op int64  `cborgen:"op"`
    T  string `cborgen:"t,omitempty"`
}

// WriteMessage writes a {op:1, t:type} header followed by the stored body bytes.
func WriteMessage(w io.Writer, t string, body []byte) error

// WriteError writes a {op:-1} header followed by {error, message} body.
func WriteError(w io.Writer, errName, message string) error
```

`t` is `"#labels"`, `"#service"`, or `"#info"`. For `FutureCursor` (AC2.3) write an error frame (`op:-1`, body `{error:"FutureCursor"}`). For `OutdatedCursor` (AC2.4) write an `#info` MESSAGE frame (op:1, t:"#info", body name=OutdatedCursor) — per protocol, OutdatedCursor is informational, not an error, and the stream then resumes from the floor.

The header struct needs CBOR marshalling — either add it to `gen/main.go` (Phase 1 driver) or hand-encode the tiny fixed map. Prefer adding to `gen/main.go` for consistency.

**Testing:**
- Property-based roundtrip: `decode(WriteMessage(t, body))` recovers the same `t` and the exact `body` bytes (byte-faithful framing — the body is the stored `frame_cbor`, untouched).
- Unit: `WriteError("FutureCursor", "")` decodes to `op:-1, error:"FutureCursor"`.
- Unit: `WriteMessage("#info", outdatedBody)` decodes to `op:1, t:"#info"`.

**Verification:**
Run: `go test ./internal/server/ -run TestFrames -v`
Expected: pass.

**Commit:** `feat: add XRPC subscription frame writers`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: subscribeLabelers WebSocket handler

**Verifies:** labeler-relay.AC2.1, labeler-relay.AC2.2, labeler-relay.AC2.3, labeler-relay.AC2.4, labeler-relay.AC9.1

**Files:**
- Create: `internal/server/subscribe.go`
- Test: `internal/server/subscribe_test.go` (integration via httptest WS)

**Implementation:**

```go
type Server struct {
    hub     *Hub
    persist *store.LabelPersist
    log     *slog.Logger
}

// HandleSubscribeLabelers upgrades to WS and serves the unified stream.
func (s *Server) HandleSubscribeLabelers(w http.ResponseWriter, r *http.Request)
```

Flow:
1. Parse `cursor` query param. Absent → live only: `since = head` (subscribe to live from now; no backfill).
2. If present, `persist.CursorStatus(ctx, cursor)`:
   - `CursorFuture` → `WriteError("FutureCursor", ...)`, close. (AC2.3)
   - `CursorOutdated` → `WriteMessage("#info", OutdatedCursor body)`, then set `since = floor-1` so backfill resumes from the floor. (AC2.4)
   - `CursorOK` → `since = cursor`.
3. Upgrade to WebSocket (`gorilla/websocket.Upgrader`).
4. `ch, cleanup, err := StreamFrom(ctx, s.persist, s.hub, since, bufSize)` (Task 2); `defer cleanup()`.
5. Range over `ch`: for each `LiveEvent` compute `t` from its `Kind` (`"#labels"`/`"#service"`) and `WriteMessage(conn, t, e.FrameCBOR)`. If a write fails (client gone) or `ch` is closed (slow-consumer drop from the Hub), send a `ConsumerTooSlow` error frame if possible, then exit and clean up. (AC9.1)

The body bytes are `e.FrameCBOR` — the stored, relay-seq'd frame body from Phase 2. Each WS binary message = one frame from Task 3.

**Testing:**
Integration: httptest server mounting the handler; dial it with a real `gorilla/websocket` client.
- AC2.1: no cursor → persist a `labels` then a `service` event → client receives both as `#labels`/`#service` frames with correct `t`.
- AC2.2: pre-persist 10 events; connect with `cursor=4`; assert client reads seqs 5..10 then live continuation with no gap/dup.
- AC2.3: connect with `cursor=head+5` → client receives a `FutureCursor` error frame and the conn closes.
- AC2.4: prune below floor (Phase 2 `Prune`), connect with a below-floor cursor → client receives `#info OutdatedCursor` then backfill from the floor.
- AC9.1: subscribe a deliberately stalled client (don't read), flood persists past the buffer; assert the server disconnects that client (its conn closes / it gets `ConsumerTooSlow` via the Hub drop) and that a SECOND well-behaved client keeps receiving events — i.e. the slow one didn't block the broadcast. Condition-based assertions on the healthy client's received count.

**Verification:**
Run: `go test ./internal/server/ -run TestSubscribe -race -v`
Expected: pass under `-race`.

**Commit:** `feat: serve community.labeler.sync.subscribeLabelers with cursor handling`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Health endpoint

**Verifies:** labeler-relay.AC10.3

**Files:**
- Create: `internal/server/health.go`
- Test: `internal/server/health_test.go` (integration via httptest)

**Implementation:**

```go
// HandleHealth reports head seq, labeler count, and retention window.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request)
```

Returns JSON: `{ "head_seq": <int>, "labeler_count": <int>, "retention_floor": <int>, "retention_window_seconds": <int> }`. `head_seq` from `persist.Head`, `labeler_count` from `registry.List` length, floor from `persist.RetentionFloor`, window from config (Phase 6 supplies the value; pass it in via the `Server` struct now with a sensible default).

**Testing:**
- AC10.3: persist some events + register labelers; `GET /_health` → 200 JSON with `head_seq` == current head, `labeler_count` == registered count, `retention_floor` correct.

**Verification:**
Run: `go test ./internal/server/ -run TestHealth -v`
Expected: pass.

**Commit:** `feat: add health endpoint reporting head seq and labeler count`
<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->

---

## Phase 5 Done When

All tests pass under `-race`:
- No-cursor consumer receives live `#labels` and `#service`. — AC2.1
- Backfill from a mid-stream cursor replays in `relay_seq` order then continues live with no gap/dup at the seam. — AC2.2
- `FutureCursor` above head; `#info OutdatedCursor` below floor (then resume from floor). — AC2.3, AC2.4
- Slow consumer disconnected without blocking the broadcast; reconnecting consumer resumes from last cursor. — AC9.1, AC9.2
- Health endpoint reports head seq, labeler count, retention window. — AC10.3

Run: `go test ./internal/server/... -race` → all green.

**Executor notes:**
- **AC2.1 passing does NOT imply AC2.2/AC9.2 passing.** The live-only path (Task 1 Hub) is much simpler than the seam (Task 2) and will go green first. Do not declare the output server "done" until the Task 2 seam test (no gap/no dup across backfill→live, including the during-backfill-persist case) passes. Verify Task 2 before Task 4's full handler is considered complete.
- The seam is OURS (Task 2), built over `LabelPersist.Playback` + the Hub, keyed on the relay seq we control. We deliberately do NOT use `EventManager.Subscribe(since)` because `XRPCStreamEvent.Sequence()` returns -1 for label events and breaks its catch-up boundary. If a gap/dup appears, the bug is in the `> lastBackfill` dedup boundary or the subscribe-before-drain ordering in `StreamFrom` — fix it there. Do not reach for EventManager's seam.
