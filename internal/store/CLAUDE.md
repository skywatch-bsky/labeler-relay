# Store

Last verified: 2026-06-02

## Purpose
Single source of truth for event persistence and labeler registration. Owns the SQLite database, global relay sequence minting, and retention pruning.

## Contracts
- **Exposes**: `Store` (DB lifecycle, meta KV), `LabelPersist` (ingest, playback, head/floor, prune), `LabelerRegistry` (CRUD, cursor read/write), `PruneJob`, frame encode/decode helpers
- **Guarantees**:
  - `relay_seq` is strictly monotonic via AUTOINCREMENT; never reused even after prune/restart
  - `PersistIngest` serializes under a mutex: broadcast order == commit order == seq order
  - Broadcaster is called while holding the mutex -- must be non-blocking
  - Upsert stickiness: firehose upserts only update endpoint and updated_at (source/enabled preserved); manual upserts additionally claim source and enabled. Cursor and require_sig are never touched on conflict; last_error is cleared (a successful upsert means any prior discovery error is stale).
  - Playback returns events in ascending relay_seq order
  - `PlaybackFramesChunk` supports LIMIT for chunked backfill
- **Expects**: Single process owns the database (WAL mode, single-writer)

## Dependencies
- **Uses**: modernc.org/sqlite, indigo types (LabelDefs_Label, LabelerService, persist.EventPersistence)
- **Used by**: slurper (ingest + cursor), firehose (ingest + registry + meta), server (playback + hub broadcast), admin (registry CRUD), main.go (wiring)
- **Boundary**: Must not import metrics, slurper, server, admin, or firehose

## Key Decisions
- Single mutex for seq minting + broadcast: simplifies ordering guarantees at the cost of serialized writes. Acceptable because label throughput is low relative to repo relays.
- `persist.EventPersistence` interface: implemented for indigo compatibility but methods are no-ops. Real persistence flows through `PersistIngest`/`PlaybackFrames`.
- Frame CBOR stored as blobs: avoids re-encoding on playback. Encode-once at ingest, write-once to client.

## Invariants
- `relay_seq` never decreases and never has gaps (AUTOINCREMENT + no manual inserts)
- `ingest_ts` is milliseconds since epoch; used as the prune cutoff (not relay_seq)
- `labelers.source` is either "firehose" or "manual"; stickiness prevents firehose from overwriting manual entries
- WAL mode and busy_timeout(5000) are set via DSN pragmas on every connection

## Key Files
- `schema.sql` - DDL (events, labelers, meta tables)
- `persist.go` - LabelPersist: ingest, playback, head, floor, prune, cursor status
- `registry.go` - LabelerRegistry: upsert, get, list, enable/disable, cursor, error recording
- `frame.go` - CBOR encode/decode for #labels, #service, #info output frames
- `prunejob.go` - Periodic retention pruning with floor callback
- `store.go` - Store: open, close, meta KV

## Gotchas
- `CursorStatus` returns `CursorOutdated` when cursor < floor-1 (not floor). Off-by-one matters.
- `RecordError` inserts a minimal disabled row if the labeler doesn't exist yet -- intentional for discovery error tracking.
