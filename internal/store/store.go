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

// idxLabelersObservedSeq is the unique index backing the pagination cursor
// sequence. It lives in migrate rather than schema.sql because on an
// old-shape database it would fail before the column exists: schema.sql runs
// unconditionally on every open, while migrate guarantees the column first
// (fresh databases get it from schema.sql, old ones from the ALTER below).
const idxLabelersObservedSeq = `CREATE UNIQUE INDEX IF NOT EXISTS idx_labelers_observed_seq ON labelers(observed_seq)`

// migrate adds missing observation sequences and normalizes stored endpoints.
// Each step is its own transaction and is idempotent, so a crash between the
// two leaves a database that the next Open completes.
//
// The observed_seq step is the recovery contract for that column. The
// database is either fully migrated or untouched. A crash (or a mid-backfill
// failure) rolls the whole step back, so no mixed NULL/non-NULL state is ever
// reachable and the next Open simply reruns it.
func migrate(db *sql.DB) error {
	// Does labelers already carry observed_seq? (Pre-migration databases do not.)
	hasObservedSeq := false
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
		if name == "observed_seq" {
			hasObservedSeq = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to inspect labelers schema: %w", err)
	}

	if !hasObservedSeq {
		// One transaction: ALTER, backfill, unique index. BeginTx (not
		// standalone BEGIN/COMMIT Execs) because db is a pooled *sql.DB and
		// separate Execs could land on different connections.
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("failed to begin observed_seq migration: %w", err)
		}
		defer tx.Rollback()

		// Right after the ALTER every existing row is NULL, so numbering all
		// rows from 1 is collision-free. Rowid order reflects original insert
		// order in practice, but external maintenance (e.g. VACUUM) can change
		// implicit rowids on tables without an INTEGER PRIMARY KEY, so the
		// backfill order is a practical approximation of observation order,
		// not a guarantee.
		for _, stmt := range []string{
			`ALTER TABLE labelers ADD COLUMN observed_seq INTEGER`,
			`WITH numbered AS (SELECT did, ROW_NUMBER() OVER (ORDER BY rowid) AS rn FROM labelers)
			 UPDATE labelers SET observed_seq = (SELECT rn FROM numbered WHERE numbered.did = labelers.did)`,
			idxLabelersObservedSeq,
		} {
			if _, err := tx.Exec(stmt); err != nil {
				return fmt.Errorf("observed_seq migration failed: %w", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit observed_seq migration: %w", err)
		}
	}

	// Unconditional: fresh databases reach this without the migration branch
	// above, so the index must be created here for them too.
	if _, err := db.Exec(idxLabelersObservedSeq); err != nil {
		return fmt.Errorf("failed to ensure idx_labelers_observed_seq: %w", err)
	}
	return migrateEndpoints(db)
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
