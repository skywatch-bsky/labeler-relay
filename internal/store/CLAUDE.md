# Store

Last verified: 2026-11-27

## Purpose
Single source of truth for event persistence and labeler registration. Owns the SQLite database, global relay sequence minting, labeler declaration tracking, and retention pruning.

## Contracts
- **Exposes**: `Store` (DB lifecycle, meta KV), `LabelPersist` (ingest, playback, head/floor, prune, `ApplyServiceOp`), `LabelerRegistry` (CRUD, cursor read/write, declaration reads), `PruneJob`, frame encode/decode helpers
- **Guarantees**:
  - `labelers.endpoint` holds only the host and optional port, or an empty string before resolution. Older databases migrate on open in idempotent steps. Invalid endpoints remain unchanged. The endpoint migration preserves `updated_at`.
  - `relay_seq` is strictly monotonic via AUTOINCREMENT; never reused even after prune/restart
  - `labelers.id` is strictly monotonic and never reused: minted at first insert only as MAX(id)+1; re-upserts (firehose or manual) and RecordError on an existing DID never touch it; a unique index enforces one row per value; labeler rows are never deleted, so values are never reclaimed. `List` returns rows ascending by id (first-observed first); `ListEnabled` keeps `ORDER BY did` (internal set semantics for Reconcile; order is irrelevant there)
  - `labelers.created_at` (unix seconds) is set once at first insert and never updated. `labelers.declared_at` (unix seconds) is the public labelerView.updatedAt: NULL until a declaration transition is observed, and written ONLY by `ApplyServiceOp` — never by admin upserts, RecordError, cursor writes, SetEnabled, or SetRequireSig — advancing on every applied declaration op (create, update, tracked delete). `updated_at` remains generic registry bookkeeping, bumped by Upsert/RecordError.
  - `labeler_declarations(did, collection)` tracks the declaration collections each labeler currently holds, as observed by this relay. Declaration writes happen only inside `ApplyServiceOp`; there is no public standalone add/remove, so no caller can mutate declarations non-atomically. `DeclarationsFor`/`ListDeclarations` are the read paths (always non-nil, empty slice when none). `GetWithDeclarations` and `ListWithDeclarations` return labelers and their declarations from one SQLite read snapshot.
  - `applied_service_ops` is the durable exactly-once gate for upstream declaration-op identities (`svc:<commitSeq>/<did>/<collection>/<rkey>/<op>`); it survives event pruning and records both applied ops and deliberate skips (including untracked deletes). `events.dedup_key` plus unique index `idx_events_dedup` is a redundant event-side record and defense in depth; it is NULL for labels-path events and for direct/test service ops with CommitSeq 0. CommitSeq 0 disables replay dedup; the watcher always sets the firehose commit sequence.
  - **`ApplyServiceOp` atomicity contract**: the declaration registry transition (upsert, declaration insert/delete, enable/disable, declared_at) and the #service event (seq minting + frame) commit in ONE SQLite transaction under the same mutex as `PersistIngest` (broadcast order == commit order == seq order). On any error everything rolls back: no registry mutation, no event row, no seq consumed, no declared_at change. Delete of an untracked declaration and replay of an already-seen upstream op return `Skipped`; applied_service_ops records identity even for skips, making them replay-safe and independent of event pruning. Re-enable (enabled=1) fires only on the zero-to-one declaration transition for firehose-sourced disabled rows with AutoSubscribe set; disable (enabled=0) fires only when the last tracked declaration is removed from a firehose-sourced enabled row. Manual rows are never auto-enabled or auto-disabled.
  - **Identity migration state machine** (`Store.Open`, each step its own transaction, all idempotent): ancient DB (neither `observed_seq` nor `id`) gets `id` added and backfilled in rowid order (a practical approximation of insert order); pre-change DB (`observed_seq` only) renames the column to `id` preserving every value; fresh/migrated DB (`id` only) performs no identity mutation; both-columns DB fails `Open` with an explicit error. The obsolete `observed_seq` column and its legacy index never survive an open; `idx_labelers_id` is created in migrate (kept out of schema.sql so old-shape databases don't fail on the missing column).
  - **Timestamps/declarations backfill**: `created_at` is approximated from `updated_at` for every row; `declared_at` and an `app.bsky.labeler.service` declaration row are backfilled only for successfully-discovered rows (`source='firehose' AND endpoint != ''` — a non-empty stored endpoint is the evidence of a declaration-driven resolution, deliberately not `last_error IS NULL`). The declarations backfill is meta-gated (`declarations_backfilled` key) to run exactly once, so a reopen cannot re-insert a declaration the labeler deleted. RecordError-only rows (empty endpoint) stay NULL/undeclared, so their first successful declaration is still a zero-to-one transition that can re-enable them.
  - `PersistIngest` serializes under a mutex: broadcast order == commit order == seq order. The watcher's service events go through `ApplyServiceOp`; `PersistIngest` remains the labels path and the direct event entrypoint.
  - Broadcaster is called while holding the mutex -- must be non-blocking
  - Upsert stickiness: firehose upserts only update endpoint and updated_at (source/enabled preserved); manual upserts additionally claim source and enabled. Cursor, require_sig, id, created_at, and declared_at are never touched on conflict; last_error is cleared (a successful upsert means any prior discovery error is stale).
  - Playback returns events in ascending relay_seq order
  - `PlaybackFramesChunk` supports LIMIT for chunked backfill
- **Expects**: Single process owns the database (WAL mode, single-writer)

## Dependencies
- **Uses**: modernc.org/sqlite, indigo types (LabelDefs_Label, persist.EventPersistence), api/community (frame types incl. RawRecord), labelerendpoint (Host, for the endpoint migration)
- **Used by**: slurper (ingest + cursor), firehose (ApplyServiceOp + registry + meta), server (playback + hub broadcast + registry reads), admin (registry CRUD), main.go (wiring)
- **Boundary**: Must not import metrics, slurper, server, admin, or firehose

## Key Decisions
- Single mutex for seq minting + broadcast: simplifies ordering guarantees at the cost of serialized writes. Acceptable because label throughput is low relative to repo relays.
- `persist.EventPersistence` interface: implemented for indigo compatibility but methods are no-ops. Real persistence flows through `PersistIngest`/`PlaybackFrames`.
- Frame CBOR stored as blobs: avoids re-encoding on playback. Encode-once at ingest, write-once to client. #service frames embed the record bytes verbatim via `RawRecord`.
- `ApplyServiceOp` exists because the watcher's registry transition and event must be one transaction: a crash between them would otherwise leave the registry and the stream disagreeing, and a replay would duplicate the event.
- Declaration tracking is incremental, not authoritative: the set reflects only declaration ops this relay observed.

## Invariants
- `relay_seq` never decreases and never has gaps (AUTOINCREMENT + no manual inserts)
- `ingest_ts` is milliseconds since epoch; used as the prune cutoff (not relay_seq)
- `labelers.source` is either "firehose" or "manual"; stickiness prevents firehose from overwriting manual entries
- WAL mode and busy_timeout(5000) are set via DSN pragmas on every connection

## Key Files
- `schema.sql` - DDL (events, labelers, labeler_declarations, meta tables)
- `persist.go` - LabelPersist: ingest, playback, head, floor, prune, cursor status, IngestEvent
- `serviceop.go` - ApplyServiceOp: atomic declaration ops + #service events, ServiceOp/ServiceOpResult
- `registry.go` - LabelerRegistry: upsert, get, list, enable/disable, cursor, error recording, declaration reads
- `frame.go` - CBOR encode/decode for #labels, #service, #info output frames (byte-faithful records)
- `prunejob.go` - Periodic retention pruning with floor callback
- `store.go` - Store: open, close, meta KV, idempotent identity/timestamps/declarations migrations

## Gotchas
- `CursorStatus` returns `CursorOutdated` when cursor < floor-1 (not floor). Off-by-one matters.
- `RecordError` inserts a minimal disabled row if the labeler doesn't exist yet -- intentional for discovery error tracking. Such rows carry no declaration and NULL `declared_at`; their first successful declaration re-enables them (the fix for the pre-change wart where such rows stayed disabled forever).
- The identity migration backfills `id` by rowid order. Rowids reflect original insert order in practice, but external SQLite maintenance (e.g. VACUUM) can renumber implicit rowids on tables without an INTEGER PRIMARY KEY, so backfill order approximates observation order rather than guaranteeing it.
- SQLite cannot add a NOT NULL column without a default, so the migrated `created_at` column lacks the NOT NULL flag on old databases (schema.sql keeps it for fresh ones). The backfill leaves no NULLs and every write path sets the column.
- `ALTER TABLE RENAME COLUMN` requires SQLite >= 3.25; modernc.org/sqlite embeds a current SQLite. A pre-change database's legacy index follows the renamed column, so the identity rename drops it and creates `idx_labelers_id` in the same transaction.
- `applied_service_ops` is never pruned; it grows by one small row per applied or skipped declaration op (negligible at observed op rates). Bounding it requires a max-replay-window decision.
- `DecodeServiceFrame` decodes via cbor-gen, whose generated unknown-field skip uses `ScanForLinks` — not hardened against adversarial container lengths. Never expose stored-frame decoding to untrusted input; the ingest path validates records through `internal/dagcbor`.
