// pattern: Imperative Shell
// Tests for the idempotent observed_seq schema migration run by Store.Open:
// old-shape databases migrate in one Open, the migration is transactional
// (a mid-backfill failure leaves the database untouched), and reopening
// neither re-mints nor re-runs anything observable.

package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/scarndp/labeler-relay/internal/store"
)

// oldShapeDDL is the labelers table as it existed before the observed_seq
// revision: hand-written so the fixture has no dependency on the current
// schema.sql.
const oldShapeDDL = `
CREATE TABLE labelers (
    did               TEXT PRIMARY KEY,
    endpoint          TEXT,
    source            TEXT NOT NULL,
    enabled           INTEGER NOT NULL DEFAULT 1,
    require_sig       INTEGER,
    last_upstream_seq INTEGER,
    last_error        TEXT,
    updated_at        INTEGER NOT NULL
);`

// oldShapeRows are seeded in ascending rowid order (a fresh table numbers
// rowids 1, 2, 3 in insertion order).
const oldShapeRows = `INSERT INTO labelers(did, endpoint, source, enabled, updated_at) VALUES
	('did:plc:old-aaa', 'https://aaa.example.com', 'manual',   1, 100),
	('did:plc:old-bbb', 'https://bbb.example.com', 'firehose', 0, 200),
	('did:plc:old-ccc', '',                        'firehose', 0, 300)`

// createOldShapeDB writes a pre-migration database to path.
func createOldShapeDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open old-shape database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(oldShapeDDL); err != nil {
		t.Fatalf("create old-shape table: %v", err)
	}
	if _, err := db.Exec(oldShapeRows); err != nil {
		t.Fatalf("seed old-shape rows: %v", err)
	}
}

// rawExec runs one statement against path on a fresh connection.
func rawExec(t *testing.T, path, stmt string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// rawColumnExists reports whether table has the named column, inspected
// outside the store package on a fresh connection.
func rawColumnExists(t *testing.T, path, table, column string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatalf("table_info(%s): %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var cid, notnull, pk int
		var colType string
		var dflt interface{}
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info: %v", err)
	}
	return false
}

// rawIndexExists reports whether the named index exists.
func rawIndexExists(t *testing.T, path, index string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	var name string
	err = db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`, index,
	).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	return true
}

// rawObservedSeqByRowid returns the observed_seq column ordered by rowid.
// A SELECT over observed_seq fails if the column is missing, which is itself
// the assertion.
func rawObservedSeqByRowid(t *testing.T, path string) []int64 {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT observed_seq FROM labelers ORDER BY rowid`)
	if err != nil {
		t.Fatalf("select observed_seq: %v", err)
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var seq sql.NullInt64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan observed_seq: %v", err)
		}
		if !seq.Valid {
			t.Fatalf("observed_seq is NULL; backfill did not run")
		}
		seqs = append(seqs, seq.Int64)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate observed_seq: %v", err)
	}
	return seqs
}

// rawEndpointsByRowid returns the endpoint column ordered by rowid, used to
// prove original rows are untouched after a failed migration.
func rawEndpointsByRowid(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT endpoint FROM labelers ORDER BY rowid`)
	if err != nil {
		t.Fatalf("select endpoints: %v", err)
	}
	defer rows.Close()
	var endpoints []string
	for rows.Next() {
		var ep sql.NullString
		if err := rows.Scan(&ep); err != nil {
			t.Fatalf("scan endpoint: %v", err)
		}
		if !ep.Valid {
			t.Fatalf("endpoint unexpectedly NULL")
		}
		endpoints = append(endpoints, ep.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate endpoints: %v", err)
	}
	return endpoints
}

// TestMigrateAddsObservedSeqBackfillsByRowid verifies AC.2: opening a
// pre-migration database adds the column, backfills values in rowid order,
// and creates the unique index; reopening does not change any value and
// minting continues above the backfilled maximum. The fresh-database path
// (column from schema.sql, index from migrate, re-Open is a no-op) is folded
// into this test.
func TestMigrateAddsObservedSeqBackfillsByRowid(t *testing.T) {
	ctx := context.Background()

	// --- Old-shape database: one Open migrates it. ---
	dbPath := filepath.Join(t.TempDir(), "old.db")
	createOldShapeDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open on old-shape database failed: %v", err)
	}
	defer s.Close()

	// Column present (the SELECT would fail otherwise), values backfilled in
	// rowid order, unique index created.
	wantSeqs := []int64{1, 2, 3}
	gotSeqs := rawObservedSeqByRowid(t, dbPath)
	if len(gotSeqs) != len(wantSeqs) {
		t.Fatalf("expected %d rows, got %d", len(wantSeqs), len(gotSeqs))
	}
	for i, want := range wantSeqs {
		if gotSeqs[i] != want {
			t.Errorf("rowid %d: expected observed_seq=%d, got %d", i+1, want, gotSeqs[i])
		}
	}
	if !rawIndexExists(t, dbPath, "idx_labelers_observed_seq") {
		t.Error("expected idx_labelers_observed_seq to exist after migration")
	}

	// --- Reopen: values unchanged (no re-mint), minting continues at 4. ---
	if err := s.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}

	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer s2.Close()

	gotSeqs = rawObservedSeqByRowid(t, dbPath)
	for i, want := range wantSeqs {
		if gotSeqs[i] != want {
			t.Errorf("after reopen, rowid %d: expected observed_seq=%d, got %d", i+1, want, gotSeqs[i])
		}
	}

	reg := store.NewLabelerRegistry(s2)
	if err := reg.Upsert(ctx, store.Labeler{
		DID:       "did:plc:post-migration",
		Endpoint:  "https://example.com",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: 9000,
	}); err != nil {
		t.Fatalf("post-migration Upsert: %v", err)
	}
	next, exists, err := reg.Get(ctx, "did:plc:post-migration")
	if err != nil || !exists {
		t.Fatalf("Get post-migration: exists=%v err=%v", exists, err)
	}
	if next.ObservedSeq != 4 {
		t.Errorf("expected minting to continue at observed_seq=4 after backfill, got %d", next.ObservedSeq)
	}

	// --- Fresh database: column from schema.sql, index from migrate(). ---
	freshPath := filepath.Join(t.TempDir(), "fresh.db")
	s3, err := store.Open(freshPath)
	if err != nil {
		t.Fatalf("Open fresh database failed: %v", err)
	}
	defer s3.Close()
	if !rawColumnExists(t, freshPath, "labelers", "observed_seq") {
		t.Error("fresh database must carry observed_seq from schema.sql")
	}
	if !rawIndexExists(t, freshPath, "idx_labelers_observed_seq") {
		t.Error("fresh database must carry the unique index from migrate()")
	}
}

// TestMigrateObservedSeqRollsBackOnFailure verifies the rollback contract
// itself: a failure during the backfill aborts the whole migration, leaving
// the database untouched (a non-transactional implementation would leave the
// column added), and a clean retry after removing the blocker succeeds. No
// mixed NULL/non-NULL repair path exists because that state is unreachable.
func TestMigrateObservedSeqRollsBackOnFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rollback.db")
	createOldShapeDB(t, dbPath)

	// BEFORE UPDATE trigger aborts any UPDATE on labelers, so the migration's
	// backfill (an UPDATE) fails after the ALTER has already run inside the
	// transaction.
	rawExec(t, dbPath, `CREATE TRIGGER block_backfill BEFORE UPDATE ON labelers
		BEGIN
			SELECT RAISE(ABORT, 'test: update blocked');
		END;`)

	if _, err := store.Open(dbPath); err == nil {
		t.Fatal("expected Open to fail while the backfill is blocked")
	}

	// Untouched: the column was not added and the original rows are unchanged.
	if rawColumnExists(t, dbPath, "labelers", "observed_seq") {
		t.Fatal("observed_seq must not exist after a rolled-back migration")
	}
	endpoints := rawEndpointsByRowid(t, dbPath)
	wantEndpoints := []string{"https://aaa.example.com", "https://bbb.example.com", ""}
	if len(endpoints) != len(wantEndpoints) {
		t.Fatalf("expected %d rows after rollback, got %d", len(wantEndpoints), len(endpoints))
	}
	for i, want := range wantEndpoints {
		if endpoints[i] != want {
			t.Errorf("rowid %d: endpoint changed after rollback: want %q, got %q", i+1, want, endpoints[i])
		}
	}

	// Clean retry: drop the blocker and Open migrates successfully.
	rawExec(t, dbPath, "DROP TRIGGER block_backfill")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open after unblocking failed: %v", err)
	}
	defer s.Close()

	gotSeqs := rawObservedSeqByRowid(t, dbPath)
	wantSeqs := []int64{1, 2, 3}
	if len(gotSeqs) != len(wantSeqs) {
		t.Fatalf("expected %d rows after retry, got %d", len(wantSeqs), len(gotSeqs))
	}
	for i, want := range wantSeqs {
		if gotSeqs[i] != want {
			t.Errorf("rowid %d: expected observed_seq=%d after retry, got %d", i+1, want, gotSeqs[i])
		}
	}
	if !rawIndexExists(t, dbPath, "idx_labelers_observed_seq") {
		t.Error("expected idx_labelers_observed_seq to exist after retry")
	}
}
