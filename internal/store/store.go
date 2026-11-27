// pattern: Imperative Shell

package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/scarndp/labeler-relay/internal/labelerendpoint"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	// Set pragmas via DSN so every pooled connection inherits them.
	// modernc.org/sqlite uses _pragma= query params in the DSN.
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Run the schema (schema.sql also contains PRAGMA statements for new databases)
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to execute schema: %w", err)
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to migrate schema: %w", err)
	}

	return &Store{db: db}, nil
}

// idxLabelersID is the unique index backing the pagination cursor. It lives in
// migrate rather than schema.sql because on an old-shape database it would
// fail before the column exists: schema.sql runs unconditionally on every
// open, while migrate guarantees the column first (fresh databases get it
// from schema.sql, old ones from the identity migration below).
const idxLabelersID = `CREATE UNIQUE INDEX IF NOT EXISTS idx_labelers_id ON labelers(id)`

// migrate normalizes the labelers identity column, timestamps, declarations,
// and stored endpoints. Each mutation step is its own transaction and is
// idempotent, so a crash between steps leaves a database that the next Open
// completes.
//
// The identity step is a state machine over the three historical schema
// shapes, and it is the recovery contract for the id column:
//
//   - ancient (neither observed_seq nor id): id is added and backfilled in
//     rowid order, minting 1..N;
//   - pre-change (observed_seq only): observed_seq is renamed to id,
//     preserving every stored value;
//   - fresh or already-migrated (id only): no identity mutation;
//   - both columns present: Open fails — an undefined state that no released
//     migration can produce.
//
// The database is either fully migrated or untouched for each step. A crash
// (or a mid-backfill failure) rolls the whole step back, so no mixed state is
// reachable and the next Open simply reruns it.
func migrate(db *sql.DB) error {
	// Which identity/timestamp columns does labelers carry?
	var hasObservedSeq, hasID, hasCreatedAt, hasDeclaredAt bool
	rows, err := db.Query("PRAGMA table_info(labelers)")
	if err != nil {
		return fmt.Errorf("failed to inspect labelers schema: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		// table_info columns: cid, name, type, notnull, dflt_value, pk.
		// dflt_value is nullable, so scan it into an interface placeholder.
		var name string
		var cid, notnull, pk int
		var colType string
		var dflt interface{}
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			return fmt.Errorf("failed to scan labelers schema: %w", err)
		}
		switch name {
		case "observed_seq":
			hasObservedSeq = true
		case "id":
			hasID = true
		case "created_at":
			hasCreatedAt = true
		case "declared_at":
			hasDeclaredAt = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to inspect labelers schema: %w", err)
	}

	switch {
	case hasObservedSeq && hasID:
		return fmt.Errorf("labelers table has both observed_seq and id columns; refusing to migrate an ambiguous schema")
	case hasObservedSeq && !hasID:
		// Pre-change database: rename in place, preserving every value.
		// SQLite >= 3.25 updates index expressions that reference the renamed
		// column, so the legacy index is dropped and replaced inside the same
		// transaction to avoid a duplicate unique index on id.
		if err := execInTx(db, "identity rename", []string{
			`ALTER TABLE labelers RENAME COLUMN observed_seq TO id`,
			`DROP INDEX IF EXISTS idx_labelers_observed_seq`,
			idxLabelersID,
		}); err != nil {
			return err
		}
	case !hasObservedSeq && !hasID:
		// Ancient database: the labelers table predates observation tracking.
		// Right after the ALTER every existing row is NULL, so numbering all
		// rows from 1 is collision-free. Rowid order reflects original insert
		// order in practice, but external maintenance (e.g. VACUUM) can change
		// implicit rowids on tables without an INTEGER PRIMARY KEY, so the
		// backfill order is a practical approximation of observation order,
		// not a guarantee. The obsolete observed_seq column is never created.
		if err := execInTx(db, "identity backfill", []string{
			`ALTER TABLE labelers ADD COLUMN id INTEGER`,
			`WITH numbered AS (SELECT did, ROW_NUMBER() OVER (ORDER BY rowid) AS rn FROM labelers)
			 UPDATE labelers SET id = (SELECT rn FROM numbered WHERE numbered.did = labelers.did)`,
			idxLabelersID,
		}); err != nil {
			return err
		}
	}
	// id only: fresh (schema.sql) or already migrated — no identity mutation.

	// Add replay deduplication identity to events on existing databases, then
	// always ensure its unique index exists. NULL keys remain allowed for
	// labels-path events and do not conflict in SQLite.
	if err := migrateEventDedup(db); err != nil {
		return err
	}

	// Durable service-op identity is independent of prunable event rows.
	if err := execInTx(db, "applied service ops", []string{
		`CREATE TABLE IF NOT EXISTS applied_service_ops (dedup_key TEXT PRIMARY KEY, applied_ts INTEGER NOT NULL)`,
	}); err != nil {
		return err
	}

	// Unconditional belt-and-braces: fresh databases reach this without any
	// identity mutation above, so the index must be created here for them
	// too, and any surviving legacy index must go.
	if _, err := db.Exec(`DROP INDEX IF EXISTS idx_labelers_observed_seq`); err != nil {
		return fmt.Errorf("failed to drop legacy idx_labelers_observed_seq: %w", err)
	}
	if _, err := db.Exec(idxLabelersID); err != nil {
		return fmt.Errorf("failed to ensure idx_labelers_id: %w", err)
	}

	// Timestamps: add created_at/declared_at to databases that predate them
	// (one transaction for all missing columns, then backfill).
	//
	// Backfill policy: created_at is approximated from updated_at for every
	// row (row-creation time was not tracked before this change). declared_at
	// — the public updatedAt — is approximated only for successfully
	// discovered rows: source='firehose' AND endpoint != ''. source alone is
	// not evidence of a registered declaration, because historical
	// RecordError inserts create source='firehose' rows with an empty
	// endpoint and no declaration ever observed. A non-empty stored endpoint
	// is the evidence of a declaration-driven resolution (endpoint is never
	// cleared once discovered, so a later error on a discovered row does not
	// exclude it — deliberately not last_error IS NULL). Manual rows and
	// RecordError-only rows stay NULL, so a RecordError-only row's first
	// successful declaration is still a zero-to-one transition that can
	// re-enable it.
	//
	// Note: SQLite cannot add a NOT NULL column without a default, so the
	// added created_at column lacks the NOT NULL flag on migrated databases
	// (schema.sql keeps it for fresh ones). The backfill leaves no NULLs and
	// every write path sets the column, so the invariant holds everywhere.
	if !hasCreatedAt || !hasDeclaredAt {
		stmts := []string{}
		if !hasCreatedAt {
			stmts = append(stmts, `ALTER TABLE labelers ADD COLUMN created_at INTEGER`)
		}
		if !hasDeclaredAt {
			stmts = append(stmts, `ALTER TABLE labelers ADD COLUMN declared_at INTEGER`)
		}
		stmts = append(stmts,
			`UPDATE labelers SET created_at = updated_at WHERE created_at IS NULL`,
			`UPDATE labelers SET declared_at = updated_at WHERE declared_at IS NULL AND source = 'firehose' AND endpoint != ''`,
		)
		if err := execInTx(db, "timestamps backfill", stmts); err != nil {
			return err
		}
	}

	// Declarations backfill, meta-gated to run exactly once: an unconditional
	// backfill would re-insert declarations after a labeler deletes them, on
	// every reopen. The predicate matches the declared_at backfill above
	// (successfully discovered rows), so RecordError-only rows are not marked
	// declared and keep their zero-to-one re-enable path.
	_, found, err := rawGetMeta(db, "declarations_backfilled")
	if err != nil {
		return fmt.Errorf("failed to read declarations_backfilled meta: %w", err)
	}
	if !found {
		if err := execInTx(db, "declarations backfill", []string{
			`INSERT OR IGNORE INTO labeler_declarations(did, collection)
			 SELECT did, 'app.bsky.labeler.service' FROM labelers WHERE source = 'firehose' AND endpoint != ''`,
			`INSERT INTO meta(key, value) VALUES ('declarations_backfilled', '1')`,
		}); err != nil {
			return err
		}
	}

	return migrateEndpoints(db)
}

// migrateEventDedup adds the optional upstream-op key and its uniqueness
// constraint to both fresh and existing databases.
func migrateEventDedup(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(events)`)
	if err != nil {
		return fmt.Errorf("failed to inspect events schema: %w", err)
	}
	hasDedupKey := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, colType string
		var dflt interface{}
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan events schema: %w", err)
		}
		if name == "dedup_key" {
			hasDedupKey = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("failed to inspect events schema: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close events schema rows: %w", err)
	}
	stmts := []string{}
	if !hasDedupKey {
		stmts = append(stmts, `ALTER TABLE events ADD COLUMN dedup_key TEXT`)
	}
	stmts = append(stmts, `CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedup ON events(dedup_key)`)
	return execInTx(db, "event dedup", stmts)
}

// execInTx runs the given statements inside one transaction (BeginTx, not
// standalone BEGIN/COMMIT Execs, because db is a pooled *sql.DB and separate
// Execs could land on different connections). On any failure the whole step
// rolls back.
func execInTx(db *sql.DB, step string, stmts []string) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin %s migration: %w", step, err)
	}
	defer tx.Rollback()
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("%s migration failed: %w", step, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit %s migration: %w", step, err)
	}
	return nil
}

// rawGetMeta reads a meta key before the Store type exists (used by migrate).
func rawGetMeta(db *sql.DB, key string) (string, bool, error) {
	var value string
	err := db.QueryRow("SELECT value FROM meta WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func migrateEndpoints(db *sql.DB) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to begin endpoint migration: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.Query("SELECT did, endpoint FROM labelers")
	if err != nil {
		return fmt.Errorf("failed to read endpoints: %w", err)
	}
	defer rows.Close()
	type endpointUpdate struct{ did, host string }
	var updates []endpointUpdate
	for rows.Next() {
		var did, endpoint string
		if err := rows.Scan(&did, &endpoint); err != nil {
			return fmt.Errorf("failed to scan endpoint: %w", err)
		}
		host, err := labelerendpoint.Host(endpoint)
		if err == nil && host != endpoint {
			updates = append(updates, endpointUpdate{did, host})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read endpoints: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close endpoints: %w", err)
	}
	for _, update := range updates {
		if _, err := tx.Exec("UPDATE labelers SET endpoint = ? WHERE did = ?", update.host, update.did); err != nil {
			return fmt.Errorf("failed to migrate endpoint: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit endpoint migration: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

// GetMeta retrieves a metadata value by key.
// Returns (value, found, error).
func (s *Store) GetMeta(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to get meta: %w", err)
	}
	return value, true, nil
}

// SetMeta stores a metadata key-value pair.
// Uses INSERT ... ON CONFLICT to handle both insert and update.
func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
		key, value)
	if err != nil {
		return fmt.Errorf("failed to set meta: %w", err)
	}
	return nil
}
