package store_test

import (
	"path/filepath"
	"testing"

	"github.com/scarndp/labeler-relay/internal/store"
)

func TestStoreOpen(t *testing.T) {
	t.Run("opens fresh path creates all three tables", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		// Verify journal_mode is WAL
		var journalMode string
		if err := s.DB().QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
			t.Fatalf("failed to check journal_mode: %v", err)
		}
		if journalMode != "wal" {
			t.Errorf("expected journal_mode=wal, got %q", journalMode)
		}

		// Verify events table exists
		var exists int
		err = s.DB().QueryRow("SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'").Scan(&exists)
		if err != nil {
			t.Errorf("events table not found: %v", err)
		}

		// Verify labelers table exists
		err = s.DB().QueryRow("SELECT 1 FROM sqlite_master WHERE type='table' AND name='labelers'").Scan(&exists)
		if err != nil {
			t.Errorf("labelers table not found: %v", err)
		}

		// Verify meta table exists
		err = s.DB().QueryRow("SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").Scan(&exists)
		if err != nil {
			t.Errorf("meta table not found: %v", err)
		}

		// Verify foreign_keys pragma is ON
		var fkEnabled int
		if err := s.DB().QueryRow("PRAGMA foreign_keys").Scan(&fkEnabled); err != nil {
			t.Fatalf("failed to check foreign_keys: %v", err)
		}
		if fkEnabled != 1 {
			t.Errorf("expected foreign_keys=1, got %d", fkEnabled)
		}
	})

	t.Run("re-opening existing DB is idempotent", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")

		// First open
		s1, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("first Open failed: %v", err)
		}
		s1.Close()

		// Second open - should succeed without error
		s2, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("second Open failed: %v", err)
		}
		defer s2.Close()

		// Verify tables still exist
		var count int
		err = s2.DB().QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('events', 'labelers', 'meta')").Scan(&count)
		if err != nil {
			t.Fatalf("failed to count tables: %v", err)
		}
		if count != 3 {
			t.Errorf("expected 3 tables, got %d", count)
		}
	})

	t.Run("events table has correct schema", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		// Check that we can insert an event and read back an auto-incremented relay_seq
		ingestTs := int64(1234567890000)
		var relaySeq int64
		result, err := s.DB().Exec(
			"INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts) VALUES(?, ?, ?, ?, ?)",
			"labels", "did:plc:test", nil, []byte("test"), ingestTs,
		)
		if err != nil {
			t.Fatalf("failed to insert event: %v", err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatalf("failed to get last insert id: %v", err)
		}
		relaySeq = id

		if relaySeq != 1 {
			t.Errorf("expected first relay_seq=1, got %d", relaySeq)
		}

		// Insert another and verify it gets 2
		result2, err := s.DB().Exec(
			"INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts) VALUES(?, ?, ?, ?, ?)",
			"service", "did:plc:test2", nil, []byte("test2"), ingestTs,
		)
		if err != nil {
			t.Fatalf("failed to insert second event: %v", err)
		}
		id2, err := result2.LastInsertId()
		if err != nil {
			t.Fatalf("failed to get last insert id: %v", err)
		}
		if id2 != 2 {
			t.Errorf("expected second relay_seq=2, got %d", id2)
		}
	})

	t.Run("Close returns no error", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	t.Run("DB accessor returns valid *sql.DB", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		db := s.DB()
		if db == nil {
			t.Error("DB() returned nil")
		}

		// Ping to verify it's usable
		if err := db.Ping(); err != nil {
			t.Errorf("failed to ping database: %v", err)
		}
	})
}
