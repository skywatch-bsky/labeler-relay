# Labeler Relay Implementation Plan — Phase 2

**Goal:** SQLite-backed registry and the `EventPersistence` implementation (`LabelPersist`) that mints the global `relay_seq`.

**Architecture:** A serialized single-writer persistence layer over SQLite (WAL mode). `LabelPersist` mints the relay seq under a single mutex, stores the relay-seq'd output frame body, and exposes `Playback`, `Head`, `RetentionFloor`, and `CursorStatus` — the primitives the Phase-5 output server uses to build the backfill→live seam **itself** (we own the seq end-to-end). `LabelerRegistry` shares the same SQLite database for the tracked-labelers table.

**IMPORTANT — we do NOT delegate the cursor seam to indigo's `EventManager`.** Code review against indigo confirmed `XRPCStreamEvent.Sequence()` returns `-1` for label events (it has no case for `LabelLabels`/`LabelInfo`). That makes `EventManager.Subscribe(since)`'s catch-up boundary logic non-functional for our payloads (it would never advance `lastSeq` past `since` and never hit `ErrCaughtUp`, producing duplicates and a non-terminating replay). Therefore the relay seq lives in OUR storage and OUR output frames, and Phase 5 builds the seam over `LabelPersist.Playback` + a live tap (operator-confirmed). `EventManager` is reused ONLY for live fan-out and slow-consumer disconnect (or replaced by a thin local fan-out if that proves simpler — Phase 5 decides). `LabelPersist` does still satisfy the `persist.EventPersistence` interface signature so it can be handed to `NewEventManager` for the live-fan-out broadcaster wiring, but its `Persist`/`Playback` are driven by us, not by EventManager's seam.

**Tech Stack:** Go, `github.com/bluesky-social/indigo` (`cmd/relay/stream`, `cmd/relay/stream/persist`), `modernc.org/sqlite` (pure-Go, no cgo) or `github.com/mattn/go-sqlite3` (cgo).

**Scope:** 7 phases from original design. This is Phase 2 of 7.

**Codebase verified:** 2026-06-01. indigo inspected at `5368f55344e0b5e203ab4d9501e1ddebd9983ad7`. Phase 1 produced `api/community` output types and the module skeleton.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### labeler-relay.AC2: Unified output stream & cursors
- **labeler-relay.AC2.2 Success:** A consumer connecting with `cursor=N` receives backfill from `relay_seq > N` in order, then continues live with no gap or duplicate at the seam. *(Playback ordering portion; the live-seam portion completes in Phase 5.)*
- **labeler-relay.AC2.3 Failure:** A `cursor` above the current head returns a `FutureCursor` error frame. *(Detection + error signalled from persistence; wire framing completes in Phase 5.)*
- **labeler-relay.AC2.4 Edge:** A `cursor` below the retention floor returns `#info OutdatedCursor` and resumes from the floor. *(Floor detection + OutdatedCursor signalling from persistence; wire framing completes in Phase 5.)*

### labeler-relay.AC3: Global sequencing
- **labeler-relay.AC3.1 Success:** Interleaved events from multiple labelers receive a single strictly-increasing `relay_seq`.
- **labeler-relay.AC3.2 Success:** `relay_seq` is monotonic under concurrent ingest from all upstreams.
- **labeler-relay.AC3.3 Edge:** `relay_seq` never rewinds or reuses a value across crash, restart, and prune.

### labeler-relay.AC6: Retention window
- **labeler-relay.AC6.1 Success:** Events older than the configured window are pruned and the retention floor advances.
- **labeler-relay.AC6.2 Edge:** Both `#labels` and `#service` events are subject to the same window.

*(Registry CRUD is also built here but has no AC of its own at this phase; AC8 admin behaviour is tested in Phase 4. AC5 discovery is tested in Phase 4.)*

---

## Key research findings (ground truth from indigo @ 5368f553)

- **`persist.EventPersistence` interface** (`cmd/relay/stream/persist/persist.go`):
  ```go
  type EventPersistence interface {
      Persist(ctx context.Context, e *stream.XRPCStreamEvent) error
      Playback(ctx context.Context, since int64, cb func(*stream.XRPCStreamEvent) error) error
      TakeDownRepo(ctx context.Context, uid uint64) error
      Flush(context.Context) error
      Shutdown(context.Context) error
      SetEventBroadcaster(func(*stream.XRPCStreamEvent))
  }
  ```
- **`XRPCStreamEvent`** (`cmd/relay/stream/events.go`) has **no Seq field on label variants**. It carries `LabelLabels *comatproto.LabelSubscribeLabels_Labels`, `LabelInfo *comatproto.LabelSubscribeLabels_Info`, and a `Preserialized []byte` cache field.
- **Seq is minted under a single writer** in `DiskPersistence.doPersist`: `seq := dp.curSeq; dp.curSeq++`. We replicate this pattern with a mutex + a SQLite `AUTOINCREMENT` PK as the durable source of truth.
- **`DiskPersistence.Persist()` is reported to silently drop labels** (no seq, no store for `LabelLabels`/`LabelInfo`). **Executor: re-confirm this at the pinned commit** before relying on the exact behaviour — but the conclusion holds regardless: we write our own persister because we need our own seq space and storage, and `DiskPersistence` is repo-stream oriented.
- `Playback(ctx, since, cb)` streams events with `seq > since`. **The cutover/`ErrCaughtUp` handling in indigo's `EventManager` does NOT work for label events** (see Architecture note: `Sequence() == -1`). We build the seam ourselves in Phase 5 over our `Playback`. This phase provides `Playback` + `Head` + `RetentionFloor` + `CursorStatus`; Phase 5 composes them into the seam.

**Design carry-over (operator-confirmed):** `LabelPersist` stores the relay-seq'd **output frame body bytes** (built from the byte-faithful upstream label `Sig` via Phase-1 `community` types). The relay seq is OUR value, stored in `events.relay_seq` (the PK) and embedded in `events.frame_cbor`. Playback reads those stored frames in `relay_seq` order — it does not depend on `XRPCStreamEvent.Sequence()`. For live fan-out, after a commit `PersistIngest` calls a broadcaster with both the relay seq and the stored frame body so the Phase-5 live tap can key on the relay seq directly. No reliance on indigo's seq accounting anywhere.

---

## SQLite driver decision

Use **`modernc.org/sqlite`** (pure Go, no cgo) to keep the build toolchain simple and cross-compilable. If the executor finds a concrete blocker (e.g. a needed pragma unsupported), `github.com/mattn/go-sqlite3` is the fallback — surface the switch to the operator with the specific reason. Both expose `database/sql`; the schema and queries below are driver-agnostic.

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: SQLite schema and connection bootstrap

**Files:**
- Create: `internal/store/schema.sql`
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go` (integration, real SQLite file)

**Implementation:**

`schema.sql` defines three tables. `events.relay_seq` is the durable monotonic sequence (`INTEGER PRIMARY KEY AUTOINCREMENT` — AUTOINCREMENT guarantees no reuse even after deletes/prune, which is exactly AC3.3).

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS events (
    relay_seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT    NOT NULL,            -- 'labels' | 'service' | 'info'
    labeler_did  TEXT    NOT NULL,
    upstream_seq INTEGER,                     -- nullable: #service/#info may lack one
    frame_cbor   BLOB    NOT NULL,            -- raw output frame body bytes (byte-faithful)
    ingest_ts    INTEGER NOT NULL             -- unix millis, for retention window
);
CREATE INDEX IF NOT EXISTS idx_events_ingest_ts ON events (ingest_ts);

CREATE TABLE IF NOT EXISTS labelers (
    did              TEXT PRIMARY KEY,
    endpoint         TEXT,
    source           TEXT NOT NULL,           -- 'firehose' | 'manual'
    enabled          INTEGER NOT NULL DEFAULT 1,
    require_sig      INTEGER,                  -- nullable: NULL = inherit global default
    last_upstream_seq INTEGER,                 -- per-labeler resume bookmark
    last_error       TEXT,
    updated_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

`store.go` opens the DB, applies WAL + busy_timeout pragmas, and runs the schema. Provide:
- `Open(path string) (*Store, error)` — opens, sets pragmas (`journal_mode=WAL`, `busy_timeout=5000`, `synchronous=NORMAL`), executes `schema.sql` (embedded via `//go:embed`).
- `(*Store) Close() error`.
- `(*Store) DB() *sql.DB` accessor for the persistence/registry layers.

Follow FCIS: `store.go` is the imperative shell (I/O). Keep pure helpers (e.g. window-floor math) in separate files later.

**Testing:**
Integration test against a real temp-file SQLite DB (managed dependency → use the real thing, per testing house style; do NOT mock SQLite). Use `t.TempDir()` for isolation.
- Opening a fresh path creates all three tables and reports `journal_mode = wal`.
- Re-opening an existing DB is idempotent (schema `IF NOT EXISTS`).

**Verification:**
Run: `go test ./internal/store/ -run TestStoreOpen -v`
Expected: pass.

**Commit:** `feat: add sqlite store with WAL schema for events, labelers, meta`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: LabelerRegistry accessors

**Files:**
- Create: `internal/store/registry.go`
- Test: `internal/store/registry_test.go` (integration)

**Implementation:**

`LabelerRegistry` wraps `*Store` and exposes CRUD over the `labelers` table:

```go
type Labeler struct {
    DID             string
    Endpoint        string
    Source          string // "firehose" | "manual"
    Enabled         bool
    RequireSig      *bool  // nil = inherit global default
    LastUpstreamSeq *int64
    LastError       string
    UpdatedAt       int64
}
```

Methods (each uses a parameterized query; wrap multi-statement writes in a transaction):
- `Upsert(ctx, Labeler) error` — insert or update by DID. **Stickiness rule (AC8.4):** when upserting from discovery (`source='firehose'`), if a row already exists with `source='manual'`, do NOT downgrade its source and do NOT flip `enabled`. Implement as: `INSERT ... ON CONFLICT(did) DO UPDATE SET endpoint=excluded.endpoint, updated_at=excluded.updated_at` — and crucially leave `source` and `enabled` untouched on conflict. Manual upserts may set `source='manual'` and `enabled`.
- `SetEnabled(ctx, did string, enabled bool) error`.
- `Get(ctx, did string) (Labeler, bool, error)`.
- `List(ctx) ([]Labeler, error)`.
- `ListEnabled(ctx) ([]Labeler, error)`.
- `ReadCursor(ctx, did string) (*int64, error)` and `WriteCursor(ctx, did string, seq int64) error` — per-labeler resume bookmark (`last_upstream_seq`).

**Testing:**
Integration, real SQLite (`t.TempDir()`).
- Upsert then Get round-trips all fields.
- List / ListEnabled return correct subsets after SetEnabled.
- WriteCursor then ReadCursor returns the written seq.
- **Stickiness:** insert manual labeler (enabled), then upsert same DID from firehose with `enabled=1` semantics — assert `source` stays `'manual'` and a prior manual `enabled=0` is NOT flipped back on. *(Full AC8.4 assertion through the admin/firehose paths happens in Phase 4; this is the storage-level guarantee.)*

**Verification:**
Run: `go test ./internal/store/ -run TestRegistry -v`
Expected: pass.

**Commit:** `feat: add labeler registry CRUD with manual stickiness`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-6) -->
<!-- START_TASK_3 -->
### Task 3: Output frame encoding helpers (pure)

**Files:**
- Create: `internal/store/frame.go`
- Test: `internal/store/frame_test.go` (unit + property-based)

**Implementation:**

Pure functions (Functional Core — no I/O) that build and CBOR-encode the output frame bodies from Phase 1's `community` types. These are stored in `events.frame_cbor`.

```go
// EncodeLabelsFrame builds a #labels output body carrying the relay seq and
// byte-faithful upstream labels, and returns its CBOR bytes.
func EncodeLabelsFrame(seq int64, src string, labels []*comatproto.LabelDefs_Label) ([]byte, error)

// EncodeServiceFrame builds a #service output body and returns its CBOR bytes.
func EncodeServiceFrame(seq int64, src string, rec *bsky.LabelerService) ([]byte, error)

// EncodeInfoFrame builds an #info output body (e.g. OutdatedCursor).
func EncodeInfoFrame(name string, message *string) ([]byte, error)
```

Each constructs the corresponding `community.LabelerSyncSubscribeLabelers_*` struct and calls its generated `MarshalCBOR`. Decoders (`DecodeLabelsFrame`, etc.) are also provided for tests and Playback.

**Testing:**
- **Property-based (roundtrip + byte-faithfulness):** for arbitrary label inputs, `DecodeLabelsFrame(EncodeLabelsFrame(seq, src, labels))` yields labels whose `Sig` bytes are byte-for-byte identical to the input (`labeler-relay.AC1.3` foundation — full passthrough proven in Phase 3/7). Use a generator producing labels with random `Sig []byte`. This is a serialization pair → PBT roundtrip property is the right tool.
- Unit: encoding a known label and decoding it preserves `seq`, `src`, `val`, `cts`, `sig`.

**Verification:**
Run: `go test ./internal/store/ -run TestFrame -v`
Expected: pass.

**Commit:** `feat: add byte-faithful CBOR frame encode/decode helpers`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: LabelPersist — Persist() mints the global seq

**Verifies:** labeler-relay.AC3.1, labeler-relay.AC3.2

**Files:**
- Create: `internal/store/persist.go`
- Test: `internal/store/persist_test.go` (integration, real SQLite)

**Implementation:**

`LabelPersist` is the serialized single writer. Its **primary entrypoint is `PersistIngest`**, which mints the relay seq, stores the relay-seq'd frame, and broadcasts to live subscribers. It also satisfies the `persist.EventPersistence` interface signature so it can be passed to `NewEventManager` for broadcaster wiring (Phase 5), but our code never relies on EventManager driving `Persist`.

```go
// IngestEvent is what the slurper (Phase 3) and firehose watcher (Phase 4)
// hand to PersistIngest. Exported: it is the cross-package ingest entrypoint.
type IngestEvent struct {
    Kind        string // "labels" | "service"
    LabelerDID  string
    UpstreamSeq *int64
    Labels      []*comatproto.LabelDefs_Label // for kind="labels"
    Record      *bsky.LabelerService          // for kind="service"
}

// LiveEvent is what the broadcaster delivers to live subscribers. It carries
// the relay seq explicitly so the Phase-5 seam keys on it directly — we never
// rely on indigo's XRPCStreamEvent.Sequence() (which returns -1 for labels).
type LiveEvent struct {
    RelaySeq   int64
    Kind       string
    LabelerDID string
    FrameCBOR  []byte // the stored output frame body (relay seq already embedded)
}

type LabelPersist struct {
    store       *Store
    mu          sync.Mutex // serializes seq minting; single-writer like DiskPersistence
    broadcaster func(LiveEvent)
}

func NewLabelPersist(s *Store) (*LabelPersist, error)

func (p *LabelPersist) SetBroadcaster(fn func(LiveEvent)) // our own live tap registration
func (p *LabelPersist) PersistIngest(ctx context.Context, e IngestEvent) (relaySeq int64, err error)
```

`PersistIngest(ctx, e)` — **the entire body runs under `mu`**, including the broadcast:
1. Acquire `mu` (held through step 4). Open a transaction.
2. **Mint the seq before encoding the frame** (the frame embeds the seq, so seq must exist first): `INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts) VALUES (?, ?, ?, x'', ?)` with an empty placeholder `frame_cbor`; read `relaySeq = last_insert_rowid()`.
3. Encode the output frame with that seq (Task 3 helpers: `EncodeLabelsFrame(relaySeq, e.LabelerDID, e.Labels)` or `EncodeServiceFrame`), then `UPDATE events SET frame_cbor=? WHERE relay_seq=?` in the **same transaction**. Commit. This guarantees the frame's embedded seq equals its PK.
4. **Still holding `mu`** (after COMMIT), if `broadcaster != nil` call it with `LiveEvent{RelaySeq: relaySeq, Kind: e.Kind, LabelerDID: e.LabelerDID, FrameCBOR: frameBytes}`. Update the `HeadSeq` metric gauge. **This MUST be inside `mu`** so that **broadcast order == commit order == seq order**: with N concurrent slurper goroutines (Phase 3) calling `PersistIngest`, broadcasting after releasing `mu` would let a later commit (seq 6) broadcast before an earlier one (seq 5), delivering live events out of seq order and breaking the Phase-5 seam's ascending guarantee (AC2.1/AC2.2). Holding `mu` across the broadcast serializes it with minting.
5. Release `mu`. Return `relaySeq`.

**Broadcaster contract (load-bearing):** because the broadcaster runs under the persist mutex, it **MUST be non-blocking** — it may not stall the write path. The Phase-5 `Hub.Broadcast` satisfies this (per-subscriber `select { case ch <- e: default: /* drop slow sub */ }`). Any broadcaster wired here must honour the same non-blocking contract. Document this where `SetBroadcaster` is defined.

The `persist.EventPersistence` interface methods are satisfied minimally for the `NewEventManager` handshake: `SetEventBroadcaster` may be a no-op (we use our own `SetBroadcaster`), `Persist`/`Playback` adapt or document non-use by EventManager, and `Flush`/`Shutdown`/`TakeDownRepo` return nil (`TakeDownRepo` is repo-specific and irrelevant — comment it). **Whether to bother passing `LabelPersist` to `NewEventManager` at all is a Phase-5 decision** (Phase 5 may use a thin local fan-out instead); this phase just makes the broadcaster path work.

**Testing:**
- AC3.1: persist a mix of `labels` and `service` ingest events from two different `labeler_did`s; assert returned seqs are `1,2,3,...` strictly increasing with no gaps for a single-threaded run.
- AC3.2 (concurrency, seq uniqueness): spawn N goroutines each calling `PersistIngest` M times concurrently; collect all returned seqs; assert they form exactly the set `{1..N*M}` with no duplicates and no missing values (monotonic + unique under concurrency). Wait on a `sync.WaitGroup`, not a sleep.
- AC3.2 (concurrency, **broadcast ordering** — guards the AC2.1/AC2.2 seam): register a broadcaster that appends each `LiveEvent.RelaySeq` to a mutex-protected slice. Spawn N goroutines each calling `PersistIngest` M times concurrently. After all complete, assert the captured broadcast sequence is **strictly ascending** (`1,2,3,…,N*M` in order). This deterministically fails if the broadcast is moved outside `mu`. This test is the regression guard for "broadcast order == commit order == seq order."

**Verification:**
Run: `go test ./internal/store/ -run TestPersist -race -v`
Expected: pass under `-race`.

**Commit:** `feat: add LabelPersist with serialized global seq minting`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Playback with boundary behaviour

**Verifies:** labeler-relay.AC2.2, labeler-relay.AC2.3, labeler-relay.AC2.4

**Files:**
- Modify: `internal/store/persist.go`
- Test: `internal/store/playback_test.go` (integration)

**Implementation:**

Add to `LabelPersist`:
- `Playback(ctx, since int64, cb func(LiveEvent) error) error` — query `SELECT relay_seq, kind, labeler_did, frame_cbor FROM events WHERE relay_seq > ? ORDER BY relay_seq ASC`, build a `LiveEvent` per row (the stored `frame_cbor` already embeds the seq — no re-encode, byte-faithful), invoke `cb`. Stop early if `cb` returns an error. (WAL lets this read run alongside the write path.) The range scan `WHERE relay_seq > ? ORDER BY relay_seq ASC` uses the `relay_seq` PRIMARY KEY index directly — no additional index needed. Note: this signature uses our `LiveEvent`, NOT `*stream.XRPCStreamEvent`, precisely so the Phase-5 seam keys on the relay seq we control.
- `Head(ctx) (int64, error)` — `SELECT COALESCE(MAX(relay_seq), 0) FROM events`.
- `RetentionFloor(ctx) (int64, error)` — `SELECT COALESCE(MIN(relay_seq), 0) FROM events` (the oldest surviving event after prune defines the floor).
- `CursorStatus(ctx, cursor int64) (CursorState, error)` returning an enum: `CursorOK` (floor-1 <= cursor <= head), `CursorFuture` (cursor > head → AC2.3 `FutureCursor`), `CursorOutdated` (cursor < floor-1 → AC2.4 `OutdatedCursor`). This pure-ish decision is what Phase 5's server turns into wire frames.

**Testing:**
- AC2.2: persist 10 events; `Playback(since=4)` yields exactly seqs 5..10 in ascending order.
- AC2.3: `CursorStatus(head+5)` == `CursorFuture`.
- AC2.4: after deleting (simulating prune) events 1..5, `CursorStatus(2)` == `CursorOutdated`, and floor == 6.
- Empty DB: `Playback(since=0)` yields nothing; `Head` == 0.

**Verification:**
Run: `go test ./internal/store/ -run TestPlayback -v`
Expected: pass.

**Commit:** `feat: add Playback and cursor boundary detection`
<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Retention prune and seq-never-rewinds guarantee

**Verifies:** labeler-relay.AC6.1, labeler-relay.AC6.2, labeler-relay.AC3.3

**Files:**
- Modify: `internal/store/persist.go`
- Test: `internal/store/retention_test.go` (integration)

**Implementation:**

Add:
- `Prune(ctx, olderThanMillis int64) (deleted int64, newFloor int64, err error)` — `DELETE FROM events WHERE ingest_ts < ?`; return rows affected and the new `RetentionFloor`. Applies to ALL kinds (`labels` AND `service`) since there is no kind filter (AC6.2).

Seq-never-rewinds (AC3.3) is structurally guaranteed by `INTEGER PRIMARY KEY AUTOINCREMENT` (SQLite never reuses an AUTOINCREMENT rowid even after the max row is deleted, because it tracks the high-water mark in `sqlite_sequence`). The test proves it.

**Testing:**
- AC6.1: insert events with controlled `ingest_ts`; prune older-than cutoff; assert old rows gone and `RetentionFloor` advanced to the oldest survivor.
- AC6.2: insert both a `labels` and a `service` event older than the cutoff; assert both are pruned.
- AC3.3 (critical): persist events 1..5; prune all of them; persist a new event; assert its seq is **6, not 1** (no reuse). Then simulate restart by closing and re-`Open`ing the same DB file and persisting again; assert seq continues from the high-water mark, never rewinds.

**Verification:**
Run: `go test ./internal/store/ -run TestRetention -v`
Expected: pass.

**Commit:** `feat: add retention prune with no-seq-reuse guarantee`
<!-- END_TASK_6 -->
<!-- END_SUBCOMPONENT_B -->

---

## Phase 2 Done When

All tests pass:
- Seq monotonicity under concurrent ingest (`-race`). — AC3.1, AC3.2
- Seq never rewinds across simulated crash+restart+prune. — AC3.3
- Playback boundary behaviour: ascending replay, FutureCursor, OutdatedCursor/floor. — AC2.2, AC2.3, AC2.4
- Retention prune advances floor for both labels and service. — AC6.1, AC6.2
- Registry CRUD + cursor read/write + manual stickiness (storage level).

Run: `go test ./internal/store/... -race` → all green.

**Executor note:** The "INSERT placeholder → read `last_insert_rowid()` → UPDATE `frame_cbor` within one transaction" pattern is the load-bearing technique for making the embedded seq equal the PK — do not skip it. The relay seq lives entirely in our storage (`events.relay_seq`) and our output frames; we do NOT route sequencing through indigo's `XRPCStreamEvent.Sequence()` (which returns -1 for label events). Playback and the live broadcaster both carry our `LiveEvent{RelaySeq, ...}` so Phase 5 can build the backfill→live seam on our own seq. Do not modify indigo.
