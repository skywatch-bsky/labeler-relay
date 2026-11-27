PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS events (
    relay_seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT    NOT NULL,
    labeler_did  TEXT    NOT NULL,
    upstream_seq INTEGER,
    frame_cbor   BLOB    NOT NULL,
    ingest_ts    INTEGER NOT NULL -- milliseconds since epoch (prune cutoff)
);
CREATE INDEX IF NOT EXISTS idx_events_ingest_ts ON events (ingest_ts);

CREATE TABLE IF NOT EXISTS labelers (
    did              TEXT PRIMARY KEY,
    endpoint         TEXT,
    source           TEXT NOT NULL,
    enabled          INTEGER NOT NULL DEFAULT 1,
    require_sig      INTEGER,
    last_upstream_seq INTEGER,
    last_error       TEXT,
    id               INTEGER, -- monotonically increasing identifier, minted at first insert and never reused (renamed from observed_seq; ancient databases get it backfilled by the migrate step in store.go)
    created_at       INTEGER NOT NULL, -- unix SECONDS: row creation, never updated (migrated rows are backfilled from updated_at by the migrate step in store.go)
    declared_at      INTEGER, -- unix SECONDS: last observed declaration transition; NULL until one is observed; written only by ApplyServiceOp
    updated_at       INTEGER NOT NULL -- unix SECONDS (unlike events.ingest_ts, which is milliseconds)
);

CREATE TABLE IF NOT EXISTS labeler_declarations (
    did        TEXT NOT NULL,
    collection TEXT NOT NULL, -- declaration collection NSID, e.g. app.bsky.labeler.service
    PRIMARY KEY (did, collection),
    FOREIGN KEY (did) REFERENCES labelers(did)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
