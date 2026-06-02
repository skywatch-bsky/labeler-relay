# Labeler Relay Implementation Plan — Phase 3

**Goal:** Connect to upstream labelers, decode `subscribeLabels` frames, drop unsigned labels, and funnel verified frames into persistence.

**Architecture:** `LabelSlurper` mirrors indigo's Slurper pattern: one redial-looping goroutine per enabled labeler. We REUSE indigo's `stream.HandleRepoStream` with a `RepoStreamCallbacks` that wires `LabelLabels`/`LabelInfo` (operator-confirmed), so we don't hand-roll WebSocket/CBOR decode. We own the goroutine lifecycle, redial-with-backoff, per-upstream rate limiting, the unsigned-drop policy, the single ingest channel into `LabelPersist.PersistIngest`, and the registry-driven reconcile loop.

**Tech Stack:** Go, `github.com/bluesky-social/indigo` (`cmd/relay/stream`, `api/atproto`), `github.com/gorilla/websocket`, `github.com/RussellLuo/slidingwindow` (the limiter indigo itself uses).

**Scope:** 7 phases. This is Phase 3 of 7.

**Codebase verified:** 2026-06-01. indigo @ `5368f55344e0b5e203ab4d9501e1ddebd9983ad7`. Phases 1–2 produced output types, store, and `LabelPersist`.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### labeler-relay.AC1: Faithful label passthrough
- **labeler-relay.AC1.3 Success:** A label's `sig` bytes and all signed fields are byte-for-byte identical in the relay's output to what arrived from upstream.
- **labeler-relay.AC1.4 Edge:** A label's `src` in the output equals its origin labeler's DID (no rewrite to the relay's identity).

*(AC1.1 and AC1.2 — full cryptographic verification against the two real labelers' keys — are proven end-to-end in Phase 7. This phase proves the byte-faithful mechanics that make them possible.)*

### labeler-relay.AC4: Ingest & unsigned policy
- **labeler-relay.AC4.1 Success:** A signed label from an enabled labeler is persisted and emitted.
- **labeler-relay.AC4.2 Failure:** With `require_sig=true`, an unsigned label is dropped and `dropped_unsigned{labeler_did}` increments.
- **labeler-relay.AC4.3 Success:** With a per-labeler `require_sig=false` override, that labeler's unsigned labels are relayed.

### labeler-relay.AC7: Rate limiting
- **labeler-relay.AC7.1 Success:** A labeler exceeding its per-second/per-hour limit is throttled at its own goroutine.
- **labeler-relay.AC7.2 Edge:** Throttling one upstream does not starve or block ingest from other labelers.

---

## Key research findings (ground truth from indigo @ 5368f553)

- **`stream.RepoStreamCallbacks`** (`cmd/relay/stream/consumer.go`) already includes label callbacks:
  ```go
  type RepoStreamCallbacks struct {
      RepoCommit   func(evt *comatproto.SyncSubscribeRepos_Commit) error
      // ...
      LabelLabels  func(evt *comatproto.LabelSubscribeLabels_Labels) error
      LabelInfo    func(evt *comatproto.LabelSubscribeLabels_Info) error
      Error        func(evt *ErrorFrame) error
  }
  ```
- **Decode entrypoint:** `HandleRepoStream(ctx, conn, scheduler, logger)` consumes the WebSocket and dispatches frames to the callbacks. It checks `header.MsgType == "#labels"` and unmarshals into `LabelSubscribeLabels_Labels`. We pass a `*gorilla/websocket.Conn` we dialed ourselves.
- **`LabelDefs_Label`** (`api/atproto/labeldefs.go`): `Sig` is `lexutil.LexBytes` (raw `[]byte`); fields round-trip without reordering. `Src`, `Uri`, `Val`, `Cts`, `Ver`, `Cid`, `Neg`, `Exp` present.
- **`LabelSubscribeLabels_Labels`**: `{ Labels []*LabelDefs_Label; Seq int64 }` — `Seq` here is the **upstream** seq (our resume bookmark), distinct from relay seq.
- **Rate limiting:** indigo uses `github.com/RussellLuo/slidingwindow` (`PerSecond`/`PerHour`/`PerDay` limiters), enforced before dispatch. We reuse the same library for per-upstream limiting.
- **Scheduler:** indigo dispatches via a `parallel.Scheduler` from `cmd/relay/stream/schedulers/`. For a single-stream-per-goroutine slurper a sequential scheduler is appropriate; the executor should use indigo's `sequential.Scheduler` (one ident = one labeler) so ordering per upstream is preserved.

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Unsigned-drop policy (pure) and metrics counter

**Verifies:** labeler-relay.AC4.2 (drop logic), labeler-relay.AC4.3 (override logic)

**Files:**
- Create: `internal/slurper/policy.go`
- Create: `internal/metrics/metrics.go`
- Test: `internal/slurper/policy_test.go` (unit)

**Implementation:**

`policy.go` — pure decision function (Functional Core, no I/O):

```go
// SigRequired returns whether a labeler must have signed labels, resolving the
// per-labeler override against the global default. nil override => use default.
func SigRequired(override *bool, globalDefault bool) bool

// KeepLabel reports whether a label should be relayed given the sig policy.
// A label is kept if it has a non-empty Sig, OR sig is not required.
func KeepLabel(label *comatproto.LabelDefs_Label, sigRequired bool) bool
```

`metrics.go` — define the counters used across phases. Use `github.com/prometheus/client_golang/prometheus`:
```go
var DroppedUnsigned = prometheus.NewCounterVec(
    prometheus.CounterOpts{Name: "labeler_relay_dropped_unsigned_total",
        Help: "Labels dropped due to missing signature, by labeler."},
    []string{"labeler_did"},
)
```
Register it (and an `IngestedTotal` CounterVec) in an `init()` or an exported `Register(*prometheus.Registry)` (prefer the explicit registry function for testability — avoids global-registry pollution across tests).

**Testing:**
- `SigRequired`: override nil → default; override true/false → that value.
- `KeepLabel`: signed label kept under required=true; unsigned dropped under required=true; unsigned kept under required=false. Use a label with `Sig: nil` and one with `Sig: []byte{...}`.

**Verification:**
Run: `go test ./internal/slurper/ -run TestPolicy -v`
Expected: pass.

**Commit:** `feat: add unsigned-drop policy and prometheus metrics`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Per-upstream token-bucket rate limiter

**Verifies:** labeler-relay.AC7.1, labeler-relay.AC7.2

**Files:**
- Create: `internal/slurper/ratelimit.go`
- Test: `internal/slurper/ratelimit_test.go` (unit/integration)

**Implementation:**

Wrap `slidingwindow` into a small per-labeler limiter struct:

```go
type Limiter struct {
    perSecond *slidingwindow.Limiter
    perHour   *slidingwindow.Limiter
}

func NewLimiter(perSec, perHour int) *Limiter

// Wait blocks until a token is available across both windows or ctx is done.
// It must block ONLY this labeler's goroutine, never a shared resource.
func (l *Limiter) Wait(ctx context.Context) error
```

`Wait` polls the limiters (condition-based, with a short backoff) until both `Allow()` return true or `ctx` is cancelled. Critically, each labeler owns its own `Limiter` instance → throttling is isolated per goroutine (AC7.2).

**Testing:**
- AC7.1: a `Limiter` with `perSec=5`; fire 20 `Wait` calls; assert that the first 5 pass quickly and the rest are paced (measure that total elapsed reflects throttling — this is a real timing behaviour, so a bounded `time` assertion with an explanatory comment is acceptable per testing house style).
- AC7.2 (isolation): two independent `Limiter`s, one saturated and one idle; assert calls on the idle limiter return immediately while the saturated one is blocked. Run the saturated waiter in a goroutine; use condition-based `waitFor` to confirm the idle limiter served N calls while the other is still blocked. No fixed sleeps for the isolation assertion.

**Verification:**
Run: `go test ./internal/slurper/ -run TestRateLimit -race -v`
Expected: pass.

**Commit:** `feat: add per-upstream sliding-window rate limiter`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-4) -->
<!-- START_TASK_3 -->
### Task 3: Per-labeler subscription goroutine with redial and frame handling

**Verifies:** labeler-relay.AC1.3, labeler-relay.AC1.4, labeler-relay.AC4.1

**Files:**
- Create: `internal/slurper/subscription.go`
- Test: `internal/slurper/subscription_test.go` (integration against a local fake subscribeLabels WS server)

**Implementation:**

`subscription` owns one labeler's upstream connection:

```go
type subscription struct {
    labeler   store.Labeler
    persist   *store.LabelPersist
    registry  *store.LabelerRegistry
    limiter   *Limiter
    sigDefault bool
    log       *slog.Logger
    cancel    context.CancelFunc
}
```

`run(ctx)`:
1. Redial loop with exponential backoff + jitter (cap ~30s, mirroring indigo's `sleepForBackoff`). On each iteration, dial the labeler's `subscribeLabels` endpoint, appending `?cursor=<last_upstream_seq>` from `registry.ReadCursor`.
2. Build `*stream.RepoStreamCallbacks`:
   - `LabelLabels`: for each `*LabelDefs_Label` in `evt.Labels`:
     - `limiter.Wait(ctx)` (rate limit per upstream).
     - Apply `KeepLabel` with resolved `SigRequired`; if dropped, `metrics.DroppedUnsigned.WithLabelValues(did).Inc()` and continue.
     - Call `persist.PersistIngest(ctx, IngestEvent{Kind:"labels", LabelerDID: labeler.DID, UpstreamSeq:&evt.Seq, Labels: kept})`. **`src` on the output is set to `labeler.DID` and the label's own `Src`; never the relay's identity (AC1.4).** Labels are passed through unmodified — `Sig` bytes untouched (AC1.3).
     - **Persist-before-bookmark ordering (crash safety):** only after `PersistIngest` succeeds, record `evt.Seq` for the batched bookmark flush.
   - `LabelInfo`: **do NOT persist upstream `#info` into `events`.** Upstream `#info` frames (e.g. an upstream's own OutdatedCursor) are control signals for OUR consumption of that upstream, not content to relay. Log them and, if useful, surface via a metric. They must not consume a relay seq. (The relay's OWN `#info OutdatedCursor` to downstream consumers is synthesized at the wire in Phase 5 Task 3 — it is a control frame, not a stored event.) The one-cursor union space persisted in `events` is exactly `#labels` + `#service`.
   - `Error`: log and break to trigger redial.
3. Consume via `stream.HandleRepoStream(ctx, conn, sequentialScheduler, log)`.
4. Batched bookmark flush: every ~N labels or ~T seconds, `registry.WriteCursor(ctx, did, lastSeq)`. Persist always precedes bookmark advance.

**Testing:**
Integration with a **local fake** `subscribeLabels` WebSocket server (managed test fixture, not a mock of indigo) that emits a known sequence of `#labels` frames using indigo's own marshalling, so the wire bytes are real.
- AC4.1: fake emits one signed label from an enabled labeler → assert it lands in `events` and `PersistIngest` returned a seq.
- AC1.3: the fake emits a label with a specific random `Sig []byte`; read it back via `store` Playback/decode and assert the output `Sig` is byte-for-byte identical to what the fake sent (this is the passthrough property end-to-end through the slurper).
- AC1.4: assert the persisted/emitted `src` equals the fake labeler's DID, and a stored `LabelDefs_Label.Src` is unchanged.
- Redial: kill the fake server mid-stream, bring it back; assert the subscription reconnects and resumes from the persisted cursor (use `waitFor` on the reconnect, not a sleep).

**Verification:**
Run: `go test ./internal/slurper/ -run TestSubscription -race -v`
Expected: pass.

**Commit:** `feat: add per-labeler subscription with redial and byte-faithful ingest`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: LabelSlurper reconcile loop (start/stop goroutines from registry)

**Verifies:** labeler-relay.AC4.1 (multi-labeler), labeler-relay.AC7.2 (isolation across goroutines)

**Files:**
- Create: `internal/slurper/slurper.go`
- Test: `internal/slurper/slurper_test.go` (integration)

**Implementation:**

`LabelSlurper` manages the set of `subscription` goroutines:

```go
type LabelSlurper struct {
    registry   *store.LabelerRegistry
    persist    *store.LabelPersist
    sigDefault bool
    limits     LimitConfig // perSec, perHour defaults
    mu         sync.Mutex
    active     map[string]*subscription // keyed by DID
    log        *slog.Logger
}

func New(...) *LabelSlurper
func (s *LabelSlurper) Reconcile(ctx context.Context) error // diff registry vs active
func (s *LabelSlurper) Run(ctx context.Context) error       // periodic Reconcile loop
func (s *LabelSlurper) Shutdown()
```

`Reconcile`:
- `registry.ListEnabled` → desired set.
- Start a `subscription` goroutine (each with its OWN `Limiter`) for any enabled DID not in `active`.
- Cancel + remove any `active` DID no longer enabled.
- Idempotent: calling twice with no registry change is a no-op.

`Run` calls `Reconcile` on a ticker (and can be poked via a channel when the registry changes — wire the poke in Phase 4).

**Testing:**
Integration with two local fake labeler servers.
- Enable labeler A in the registry → `Reconcile` → assert A's goroutine starts and ingests. Enable B → `Reconcile` → both ingest. Disable A → `Reconcile` → A's goroutine stops, B continues (use `waitFor` on goroutine state via observable effects: counts of ingested events per DID).
- AC7.2 across real goroutines: saturate A's limiter (tiny perSec) while B streams freely; assert B keeps ingesting (its event count rises) while A is throttled. Condition-based, no fixed sleeps for the assertion.

**Verification:**
Run: `go test ./internal/slurper/ -run TestSlurper -race -v`
Expected: pass under `-race`.

**Commit:** `feat: add registry-driven slurper reconcile loop`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_B -->

---

## Phase 3 Done When

All tests pass under `-race`:
- Frame decode round-trip with **sig bytes byte-for-byte preserved** through the slurper. — AC1.3
- Output `src` equals origin labeler DID, never the relay. — AC1.4
- Signed label from enabled labeler is persisted and emitted. — AC4.1
- Unsigned labels dropped + `dropped_unsigned{labeler_did}` increments under `require_sig=true`. — AC4.2
- Per-labeler `require_sig=false` override relays unsigned labels. — AC4.3
- Rate limiter throttles a flooding upstream at its own goroutine without starving others. — AC7.1, AC7.2

Run: `go test ./internal/slurper/... -race` → all green.

**Executor note:** Use a real local WebSocket fake that emits frames via indigo's own marshalling — do NOT mock indigo's decode. The byte-faithfulness ACs are only meaningful if real wire bytes flow through the real decode path.
