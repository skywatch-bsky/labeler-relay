# Labeler Relay Implementation Plan — Phase 6

**Goal:** Periodic prune job, crash-restart resume, metrics surface, and config plumbing — meeting the hardened Definition of Done.

**Architecture:** A background prune job advances the retention floor on a ticker. Crash-restart resume is achieved by having every upstream (each labeler + the firehose) dial with its last durable bookmark on startup, replaying from there (at-least-once). A Prometheus metrics surface exposes ingest rates, dropped-unsigned, connected upstreams, consumer count, and head seq. Config plumbing wires all tunables into `cmd/labeler-relay/main.go`, replacing the Phase-1 stub with the full wiring.

**Tech Stack:** Go, `github.com/prometheus/client_golang`, `net/http`, all internal packages from Phases 2–5.

**Scope:** 7 phases. This is Phase 6 of 7.

**Codebase verified:** 2026-06-01. indigo @ `5368f55344e0b5e203ab4d9501e1ddebd9983ad7`. Phases 1–5 produced the full pipeline minus hardening; cursors are already persisted (slurper `last_upstream_seq`, firehose `meta` cursor).

---

## Acceptance Criteria Coverage

This phase implements and tests:

### labeler-relay.AC6: Retention window
- **labeler-relay.AC6.1 Success:** Events older than the configured window are pruned and the retention floor advances. *(Prune logic built in Phase 2; here it runs on a schedule and is validated as a live job.)*
- **labeler-relay.AC6.2 Edge:** Both `#labels` and `#service` events are subject to the same window.

### labeler-relay.AC7: Rate limiting
- **labeler-relay.AC7.1 Success:** A labeler exceeding its per-second/per-hour limit is throttled at its own goroutine. *(Validated under simulated load here, on top of Phase 3's unit-level proof.)*
- **labeler-relay.AC7.2 Edge:** Throttling one upstream does not starve or block ingest from other labelers.

### labeler-relay.AC10: Crash resume & observability
- **labeler-relay.AC10.1 Success:** After a crash and restart, the relay resumes each upstream from its last durable bookmark (at-least-once).
- **labeler-relay.AC10.2 Success:** The firehose consumer resumes from its persisted cursor after restart.
- **labeler-relay.AC10.3 Success:** A health/metrics endpoint reports head seq, labeler count, and retention window. *(Health built in Phase 5; metrics endpoint completed here.)*

---

## Key research findings

- Crash-resume mechanics already exist as data: Phase 3 writes `last_upstream_seq` per labeler (persist-before-bookmark ordering), Phase 4 writes the firehose cursor into `meta`. This phase proves that a fresh process picks those up and resumes — no new persistence, just startup wiring + tests.
- At-least-once is intentional and safe: labels are idempotent by `(src, uri, val, cts)`. Re-emission after restart is harmless (design "Additional Considerations").
- Metrics: Phase 3 defined `DroppedUnsigned` and `IngestedTotal`. Add gauges for connected upstreams, consumer count, and head seq, exposed via `promhttp.Handler()`.

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Scheduled prune job

**Verifies:** labeler-relay.AC6.1, labeler-relay.AC6.2

**Files:**
- Create: `internal/store/prunejob.go`
- Test: `internal/store/prunejob_test.go` (integration)

**Implementation:**

```go
// PruneJob runs LabelPersist.Prune on a ticker, deleting events older than the
// retention window and advancing the floor. Returns when ctx is cancelled.
type PruneJob struct {
    persist  *LabelPersist
    window   time.Duration
    interval time.Duration
    nowMs    func() int64 // injectable clock for tests (defense-in-depth + testability)
    log      *slog.Logger
}

func (j *PruneJob) Run(ctx context.Context) error
```

On each tick: `cutoff := j.nowMs() - j.window.Milliseconds()`; `deleted, floor, err := j.persist.Prune(ctx, cutoff)`; log deleted count + new floor; update a `retention_floor` gauge. Inject `nowMs` so tests can control time without sleeping real durations.

**Testing:**
- AC6.1: insert events with `ingest_ts` spanning before/after the window; run one tick (call the tick function directly, or use a tiny `interval` with `nowMs` set so the cutoff lands mid-range); assert old events deleted and floor advanced to the oldest survivor.
- AC6.2: include both a `labels` and a `service` event in the "old" set; assert both pruned.
- Use the injected clock; do NOT `time.Sleep` for the window.

**Verification:**
Run: `go test ./internal/store/ -run TestPruneJob -v`
Expected: pass.

**Commit:** `feat: add scheduled retention prune job`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Crash-restart resume verification

**Verifies:** labeler-relay.AC10.1, labeler-relay.AC10.2

**Files:**
- Create: `internal/integration/resume_test.go` (integration; uses real store + slurper + firehose watcher + local fakes)
- Modify: (only if a gap is found) the relevant startup wiring in slurper/firehose

**Implementation:**

This is primarily a verification task — the resume data already exists. If the test reveals that a fresh slurper/watcher does NOT dial with the persisted bookmark, fix the startup path (read cursor before first dial). Confirm:
- `subscription.run` reads `registry.ReadCursor(did)` and appends `?cursor=` on EVERY dial including the first after process start.
- `FirehoseWatcher.Run` reads the `meta` firehose cursor before its first dial.

**Testing:**
Integration with local fake upstream servers that record the `cursor` query param they were dialed with:
- AC10.1: run a slurper against a fake labeler; ingest up to upstream_seq=K (assert `last_upstream_seq` persisted). Shut down the slurper (simulate crash). Start a NEW slurper instance over the SAME store. Assert the fake server's next connection carries `cursor=K` (resumes from the durable bookmark). Assert no label below K is required to be re-fetched beyond the at-least-once boundary (re-delivery of K itself is acceptable and expected).
- AC10.2: same shape for the firehose watcher — second process dials `subscribeRepos` with the persisted `meta` cursor.
- Assert at-least-once, not exactly-once: a duplicate at the seam is allowed; a GAP is not.

**Verification:**
Run: `go test ./internal/integration/ -run TestResume -race -v`
Expected: pass.

**Commit:** `test: verify crash-restart resume from durable bookmarks`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-5) -->
<!-- START_TASK_3 -->
### Task 3: Metrics surface and endpoint

**Verifies:** labeler-relay.AC10.3, labeler-relay.AC7.1 (observability of throttling)

**Files:**
- Modify: `internal/metrics/metrics.go`
- Create: `internal/server/metrics_endpoint.go`
- Test: `internal/server/metrics_endpoint_test.go` (integration)

**Implementation:**

Extend `metrics.go` with:
```go
var (
    ConnectedUpstreams = prometheus.NewGauge(...)              // set by slurper Reconcile
    ConsumerCount      = prometheus.NewGauge(...)              // inc/dec by server subscribe handler
    HeadSeq            = prometheus.NewGauge(...)              // updated on each persist
    RetentionFloor     = prometheus.NewGauge(...)              // updated by prune job
    IngestRate         = prometheus.NewCounterVec(..., []string{"labeler_did"}) // per-labeler ingest
    Throttled          = prometheus.NewCounterVec(             // per-labeler rate-limit waits
        prometheus.CounterOpts{Name: "labeler_relay_throttled_total",
            Help: "Times a labeler's ingest was throttled by its rate limiter."},
        []string{"labeler_did"})
)
```
Wire each at its source: slurper sets `ConnectedUpstreams` after `Reconcile`; server subscribe handler inc/decs `ConsumerCount`; `PersistIngest` sets `HeadSeq`; prune job sets `RetentionFloor`; slurper ingest path increments `IngestRate`; the per-upstream `Limiter.Wait` (Phase 3 Task 2) increments `Throttled{labeler_did}` whenever it has to block for a token.

`metrics_endpoint.go`: mount `promhttp.HandlerFor(registry, ...)` at `/metrics`.

**Testing:**
- AC10.3: after persisting events and connecting a consumer, scrape `/metrics`; assert `labeler_relay_head_seq`, `labeler_relay_connected_upstreams`, `labeler_relay_consumer_count`, and `labeler_relay_dropped_unsigned_total` are present with sane values. Use the explicit test registry (not the global) to avoid cross-test pollution.
- AC7.1 observability: drive a throttled upstream (saturate its `Limiter`); scrape `/metrics` and assert `labeler_relay_throttled_total{labeler_did="..."}` is non-zero for that labeler. (The throttling behaviour itself is proven in Phase 3 `ratelimit_test.go`; this leg only asserts it is observable.)

**Verification:**
Run: `go test ./internal/server/ -run TestMetrics -v`
Expected: pass.

**Commit:** `feat: add prometheus metrics surface and /metrics endpoint`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Config

**Verifies:** None directly (infrastructure for the tunables AC6/AC7 exercise).

**Files:**
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go` (unit)

**Implementation:**

```go
type Config struct {
    DBPath                 string        // db_path
    ListenAddr             string        // listen_addr
    FirehoseURL            string        // firehose_url
    AdminToken             string        // admin_token (required if admin enabled)
    RetentionWindow        time.Duration // retention_window (default ~14d)
    AutoSubscribeDiscovered bool          // auto_subscribe_discovered (default true)
    RequireSig             bool          // require_sig (default true)
    UpstreamRateLimit      RateLimit     // upstream_rate_limit {per_sec, per_hour}
}

// Load reads config from env vars (app entry point may read env; libraries must not).
func Load() (Config, error)
```

Per house style, env reading lives only at the app entry point / config loader — NOT in libraries. `Load` validates: `AdminToken` non-empty when admin is served, `RetentionWindow > 0`, sane rate limits; returns lowercase-fragment errors ("failed to load config: admin_token is required"). Provide defaults matching the design (retention ~14d, require_sig=true, auto_subscribe=true).

**Testing:**
- Defaults applied when env unset.
- Validation: empty admin token (with admin enabled) → error; zero/negative window → error.
- Env override parsing for each field.

**Verification:**
Run: `go test ./internal/config/ -v`
Expected: pass.

**Commit:** `feat: add config loading with validation and defaults`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Full main.go wiring

**Verifies:** None directly (operational; exercised end-to-end in Phase 7).

**Files:**
- Modify: `cmd/labeler-relay/main.go` (replace the Phase-1 stub)
- Test: `cmd/labeler-relay/main_test.go` (smoke: process boots, /_health responds, graceful shutdown)

**Implementation:**

`run()` now wires the whole graph:
1. `config.Load()`.
2. `store.Open(cfg.DBPath)`; build `LabelPersist`, `LabelerRegistry`.
3. `Hub` (Phase 5 Task 1); register it as `LabelPersist`'s broadcaster via `SetBroadcaster`.
4. `LabelSlurper` (Phase 3) with reconcile poke.
5. `FirehoseWatcher` (Phase 4) with poke → slurper reconcile.
6. `admin.API` (Phase 4) mounted behind bearer auth.
7. `server.Server` (Phase 5): subscribeLabelers handler, `/_health`, `/metrics`.
8. `PruneJob` (Task 1).
9. Start all background goroutines under an `errgroup`/context; install signal handling for graceful shutdown (`SIGINT`/`SIGTERM` → cancel ctx → `persist.Flush`/`Shutdown`).

Use `context.Context` threaded everywhere; no globals beyond the metrics registry.

**Testing:**
- Smoke test: start `run()` in a goroutine with a temp DB and a free port and NO firehose (or a fake), `GET /_health` returns 200, then cancel ctx and assert clean shutdown within a bounded time (condition-based wait on the goroutine returning, not a fixed sleep).

**Verification:**
Run: `go test ./cmd/labeler-relay/ -race -v`
Expected: pass.

Run: `go build ./... && go vet ./...`
Expected: clean.

**Commit:** `feat: wire full labeler-relay process with graceful shutdown`
<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->

---

## Phase 6 Done When

All tests pass under `-race`:
- Prune advances floor (labels + service) on a schedule; below-floor cursors get `OutdatedCursor` (cross-checked with Phase 5). — AC6.1, AC6.2
- Crash+restart resumes both each labeler and the firehose without loss (at-least-once, idempotent; duplicate-at-seam OK, gap not). — AC10.1, AC10.2
- Rate limiting verified under simulated load without starving other upstreams. — AC7.1, AC7.2
- `/metrics` and `/_health` live and reporting head seq, labeler count, retention window, dropped-unsigned, connected upstreams, consumer count. — AC10.3
- `go build ./... && go vet ./...` clean; process boots and shuts down gracefully.

Run: `go test ./... -race` → all green.
