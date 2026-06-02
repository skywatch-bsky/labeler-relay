# Labeler Relay Implementation Plan — Phase 4

**Goal:** Both discovery paths (firehose auto-discovery + authenticated admin API) populate the registry; firehose service-record changes also emit `#service` into the output stream.

**Architecture:** `FirehoseWatcher` consumes `com.atproto.sync.subscribeRepos`, filters commit ops touching `app.bsky.labeler.service`, resolves the labeler's DID doc for its `subscribeLabels` endpoint, upserts a `source='firehose'` registry row, and emits a `#service` `IngestEvent` (carrying the full record) into `LabelPersist`. The Admin HTTP API offers authenticated CRUD over the registry as the manual path. Both pokes the slurper's reconcile loop (Phase 3).

**Tech Stack:** Go, indigo (`cmd/relay/stream`, `api/atproto`, `api/bsky`, `atproto/identity`, `atproto/repo`), `net/http`.

**Scope:** 7 phases. This is Phase 4 of 7.

**Codebase verified:** 2026-06-01. indigo @ `5368f55344e0b5e203ab4d9501e1ddebd9983ad7`. Phases 1–3 produced output types, store/persist, and the slurper.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### labeler-relay.AC5: Firehose discovery & #service
- **labeler-relay.AC5.1 Success:** A `subscribeRepos` commit creating an `app.bsky.labeler.service` record registers the labeler with `source='firehose'`, `enabled=1`.
- **labeler-relay.AC5.2 Success:** That same commit emits a `#service` message carrying the full `app.bsky.labeler.service` record.
- **labeler-relay.AC5.3 Success:** A labeler updating its service record produces a new `#service` message downstream.

### labeler-relay.AC8: Admin API
- **labeler-relay.AC8.1 Success:** `POST /admin/labelers {did}` with a valid token registers a labeler (`source='manual'`).
- **labeler-relay.AC8.2 Success:** `DELETE /admin/labelers/{did}` removes/disables a labeler.
- **labeler-relay.AC8.3 Failure:** A request without a valid bearer token is rejected (401).
- **labeler-relay.AC8.4 Edge:** A manually-added labeler is not auto-disabled by discovery churn.

---

## Key research findings (ground truth from indigo @ 5368f553)

- **`SyncSubscribeRepos_Commit`** (`api/atproto/syncsubscribeRepos.go`): `{ Blocks lexutil.LexBytes (CAR); Ops []*SyncSubscribeRepos_RepoOp; Repo string (DID); Rev string; Seq int64; Time string; ... }`.
- **`SyncSubscribeRepos_RepoOp`**: `{ Action string ("create"|"update"|"delete"); Cid *lexutil.LexLink; Path string }`. **`Path` is `"<collection>/<rkey>"`** — split on `/` to match `app.bsky.labeler.service`.
- **CAR decode:** `atproto/repo` exposes `LoadRepoFromCAR()` (from `evt.Blocks`); the record bytes for an op CID are read from the loaded repo, then unmarshalled into the typed record.
- **`bsky.LabelerService`** (`api/bsky/labelerservice.go`): `LexiconTypeID = "app.bsky.labeler.service"`, fields `CreatedAt`, `Policies *LabelerDefs_LabelerPolicies`, `Labels`, etc. Has generated `UnmarshalCBOR`.
- **Identity resolution** (`atproto/identity`): `Resolver` interface → `ResolveDID(ctx, syntax.DID) (*DIDDocument, error)`. `DIDDocument.Service []DocService{ID,Type,ServiceEndpoint}` — match `Type == "atproto_labeler"` (or `ID` containing `#atproto_labeler`) for the subscribeLabels endpoint. `DIDDocument.VerificationMethod` carries `#atproto_label` key (used in Phase 7).
- **Consuming the firehose:** same `stream.HandleRepoStream` + `RepoStreamCallbacks{RepoCommit: ...}` mechanism used by the slurper in Phase 3.

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Commit op filtering and record extraction (pure-ish)

**Verifies:** labeler-relay.AC5.1 (filtering portion), labeler-relay.AC5.2 (record extraction portion)

**Files:**
- Create: `internal/firehose/extract.go`
- Test: `internal/firehose/extract_test.go` (unit, with a hand-built CAR fixture)

**Implementation:**

```go
// LabelerServiceOp is a discovered create/update of an app.bsky.labeler.service record.
type LabelerServiceOp struct {
    RepoDID string
    Action  string // "create" | "update" | "delete"
    Rkey    string
    Record  *bsky.LabelerService // nil for delete
}

// ExtractLabelerServiceOps scans a commit's ops for app.bsky.labeler.service
// changes, loading record bytes from the commit CAR for create/update.
func ExtractLabelerServiceOps(commit *comatproto.SyncSubscribeRepos_Commit) ([]LabelerServiceOp, error)
```

Logic: load the repo from `commit.Blocks` via `atproto/repo`. For each op, split `Path` on `/`; if collection == `app.bsky.labeler.service`, for create/update read the record at `op.Cid` and `UnmarshalCBOR` into `*bsky.LabelerService`; for delete, emit with `Record: nil`. Ignore all other collections (this is the hot path on the full firehose — keep it allocation-light and return early when no labeler ops are present).

**Testing:**
Build a CAR fixture containing one `app.bsky.labeler.service` record (use indigo's repo/CAR helpers to construct it, so the bytes are real). Then:
- AC5.1 (filter): a commit whose ops include a `app.bsky.labeler.service` create yields exactly one `LabelerServiceOp` with the right DID/rkey; a commit with only `app.bsky.feed.post` ops yields none.
- AC5.2 (extraction): the extracted `Record` has the expected `CreatedAt`/`Policies` from the fixture.

**Verification:**
Run: `go test ./internal/firehose/ -run TestExtract -v`
Expected: pass.

**Commit:** `feat: extract app.bsky.labeler.service ops from firehose commits`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: DID-doc resolution for the labeler endpoint

**Verifies:** labeler-relay.AC5.1 (endpoint resolution portion)

**Files:**
- Create: `internal/firehose/resolve.go`
- Test: `internal/firehose/resolve_test.go` (unit; wrap the resolver so we mock OUR wrapper, not indigo)

**Implementation:**

Define a thin interface we own (so tests mock our wrapper, per testing house style — do NOT mock indigo's resolver directly):

```go
type DIDResolver interface {
    LabelerEndpoint(ctx context.Context, did string) (endpoint string, err error)
}

// indigoResolver adapts atproto/identity.Resolver.
type indigoResolver struct{ inner identity.Resolver }
func (r *indigoResolver) LabelerEndpoint(ctx, did) (string, error) // resolves DID doc, finds Service[Type=="atproto_labeler"].ServiceEndpoint
```

`LabelerEndpoint` resolves the DID document and returns the `atproto_labeler` service endpoint. If absent, return a typed error (`ErrNoLabelerEndpoint`) — the watcher records `last_error` and skips enabling rather than crashing.

**Testing:**
- Provide a fake `DIDResolver` returning a known endpoint → assert pass-through.
- A fake that returns `ErrNoLabelerEndpoint` → assert the watcher (Task 3) handles it gracefully (tested there).
- Optionally one integration test resolving a real DID doc behind a build tag `//go:build integration` so the default suite stays offline and never silently skips.

**Verification:**
Run: `go test ./internal/firehose/ -run TestResolve -v`
Expected: pass.

**Commit:** `feat: resolve labeler subscribeLabels endpoint from DID doc`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: FirehoseWatcher — consume, upsert, emit #service, persist cursor

**Verifies:** labeler-relay.AC5.1, labeler-relay.AC5.2, labeler-relay.AC5.3

**Files:**
- Create: `internal/firehose/watcher.go`
- Test: `internal/firehose/watcher_test.go` (integration against a local fake subscribeRepos WS server)

**Implementation:**

```go
type FirehoseWatcher struct {
    url      string
    registry *store.LabelerRegistry
    persist  *store.LabelPersist
    resolver DIDResolver
    store    *store.Store // for meta cursor
    poke     func()       // notify slurper to reconcile
    log      *slog.Logger
}

func (w *FirehoseWatcher) Run(ctx context.Context) error
```

`Run`: redial loop (same backoff pattern as the slurper) dialing `w.url` with `?cursor=<meta firehose cursor>`. Build `RepoStreamCallbacks{RepoCommit: handleCommit}` and consume via `stream.HandleRepoStream`.

`handleCommit(evt)`:
1. `ExtractLabelerServiceOps(evt)` (Task 1).
2. For each create/update op:
   - `resolver.LabelerEndpoint(ctx, op.RepoDID)`; on `ErrNoLabelerEndpoint`, record `last_error`, continue.
   - `registry.Upsert(Labeler{DID: op.RepoDID, Endpoint: endpoint, Source:"firehose", Enabled:true})` — the Phase-2 upsert leaves a pre-existing `manual` row's source/enabled untouched (AC8.4).
   - `persist.PersistIngest(IngestEvent{Kind:"service", LabelerDID: op.RepoDID, Record: op.Record})` → emits a `#service` output frame carrying the full record (AC5.2/AC5.3). **Fidelity note:** the record is decoded from the firehose CAR into a typed `*bsky.LabelerService` and re-encoded into the output frame. This is **field-faithful, not byte-faithful** (unlike labels, whose `Sig` bytes pass through verbatim). No AC requires service-record byte/signature fidelity; service records are advisory metadata.
   - `w.poke()` so the slurper reconciles and (if newly enabled) starts ingesting.
3. For delete ops: optionally disable the labeler if `source='firehose'` (do NOT disable manual entries).
4. After processing, periodically persist the firehose cursor (`evt.Seq`) into `meta` (batched, like the slurper bookmark).

**Testing:**
Integration with a local fake `subscribeRepos` server emitting real-CBOR commit frames (constructed via indigo's marshalling) containing a labeler.service create, then an update.
- AC5.1: after the create commit, assert a registry row exists with `source='firehose'`, `enabled=1`, and the resolved endpoint. Use a fake `DIDResolver` returning a fixed endpoint.
- AC5.2: assert a `#service` event was persisted (query `events` where `kind='service'`) carrying the record's `CreatedAt`.
- AC5.2 (field round-trip): decode the persisted `#service` frame body and assert ALL record fields present in the fixture (`CreatedAt`, `Policies`, etc.) survive the decode→re-encode→decode round trip. This is a field-fidelity check, NOT a byte/signature check (service records are not byte-faithful by design).
- AC5.3: emit an update commit → assert a SECOND `#service` event appears with a higher relay_seq.
- Cursor: assert `meta` firehose cursor advanced; restart the watcher and assert it dials with the persisted cursor (full crash-resume is Phase 6, but the cursor write is proven here).

**Verification:**
Run: `go test ./internal/firehose/ -run TestWatcher -race -v`
Expected: pass.

**Commit:** `feat: add firehose watcher with discovery and #service emission`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 4-5) -->
<!-- START_TASK_4 -->
### Task 4: Bearer-token auth middleware

**Verifies:** labeler-relay.AC8.3

**Files:**
- Create: `internal/admin/auth.go`
- Test: `internal/admin/auth_test.go` (unit via httptest)

**Implementation:**

```go
// RequireBearer wraps a handler, rejecting requests whose Authorization header
// is not exactly "Bearer <token>". Uses constant-time comparison.
func RequireBearer(token string, next http.Handler) http.Handler
```

Use `crypto/subtle.ConstantTimeCompare` for the token check (defense-in-depth: never short-circuit on length-leaking `==`). Missing/malformed/mismatched → `401` with a small JSON error body.

**Testing:**
- AC8.3: request with no `Authorization` → 401; wrong token → 401; correct `Bearer <token>` → reaches `next` (200 from a stub).

**Verification:**
Run: `go test ./internal/admin/ -run TestAuth -v`
Expected: pass.

**Commit:** `feat: add bearer-token auth middleware for admin API`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Admin CRUD handlers

**Verifies:** labeler-relay.AC8.1, labeler-relay.AC8.2, labeler-relay.AC8.4

**Files:**
- Create: `internal/admin/admin.go`
- Test: `internal/admin/admin_test.go` (integration via httptest + real store)

**Implementation:**

```go
type API struct {
    registry *store.LabelerRegistry
    resolver firehose.DIDResolver // to resolve endpoint on manual add
    poke     func()
    token    string
}

func (a *API) Routes() http.Handler // mounts the handlers under RequireBearer
```

Endpoints (all behind `RequireBearer`):
- `POST /admin/labelers` body `{"did": "..."}` → resolve endpoint, `Upsert(Labeler{DID, Endpoint, Source:"manual", Enabled:true})`, `poke()`, `201`. (AC8.1)
- `DELETE /admin/labelers/{did}` → `SetEnabled(did, false)` (disable; design says "removes/disables"), `poke()`, `204`. (AC8.2)
- `GET /admin/labelers` → `List` as JSON. (used by tests + ops)
- `POST /admin/labelers/{did}/enable` and `/disable` → toggle enabled.

**Stickiness (AC8.4):** because manual adds set `source='manual'`, and the firehose upsert (Phase 2 rule) never downgrades source or flips enabled on conflict, a manual labeler survives discovery churn. The test exercises the full path.

**Testing:**
Integration: httptest server mounting `Routes()`, real `t.TempDir()` store, fake `DIDResolver`.
- AC8.1: `POST` with valid token + did → 201; assert registry row `source='manual'`, `enabled=1`.
- AC8.2: `DELETE` → 204; assert `enabled=0`.
- AC8.4: manually add labeler X (source=manual). Then drive a firehose upsert for X via the registry path (simulating discovery churn). Assert X's `source` stays `'manual'` and its enabled state is not auto-toggled by the firehose upsert. Then run a firehose "delete" for X and assert it is NOT disabled (manual stickiness).
- AC8.3 is covered in Task 4 but add one end-to-end no-token call here → 401.

**Verification:**
Run: `go test ./internal/admin/ -race -v`
Expected: pass.

**Commit:** `feat: add authenticated admin CRUD for labeler registry`
<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->

---

## Phase 4 Done When

All tests pass under `-race`:
- Firehose commit with a `labeler.service` op → registry upsert (`source='firehose'`, `enabled=1`) + `#service` emission, including on update. — AC5.1, AC5.2, AC5.3
- Admin add/remove/list with auth enforcement. — AC8.1, AC8.2, AC8.3
- Manual entries are sticky against discovery churn and firehose deletes. — AC8.4

Run: `go test ./internal/firehose/... ./internal/admin/... -race` → all green.

**Executor note:** Both watcher and admin share the slurper's `poke()` reconcile trigger from Phase 3 — wire `LabelSlurper.Reconcile` as the poke target. Keep the firehose `RepoCommit` handler allocation-light; it runs on the full network firehose, not just labeler traffic.
