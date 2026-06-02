PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS events (
    relay_seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT    NOT NULL,
    labeler_did  TEXT    NOT NULL,
    upstream_seq INTEGER,
    frame_cbor   BLOB    NOT NULL,
    ingest_ts    INTEGER NOT NULL
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
    updated_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
