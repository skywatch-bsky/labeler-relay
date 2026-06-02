// pattern: Imperative Shell

package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

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

	return &Store{db: db}, nil
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
