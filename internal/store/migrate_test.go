// pattern: Imperative Shell
// Tests for the idempotent labelers schema migration run by Store.Open: the
// identity state machine (ancient / pre-change / fresh / both-columns), the
// created_at/declared_at and declarations backfills, and the rollback
// contract (a mid-backfill failure leaves the database untouched).

package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/scarndp/labeler-relay/internal/store"
)

// ancientShapeDDL is the labelers table as it existed before observation
// tracking: no identity column at all. Hand-written so the fixture has no
// dependency on the current schema.sql.
const ancientShapeDDL = `
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

// preChangeShapeDDL is the labelers table as it existed immediately before
// the id rename: observed_seq present, plus the legacy unique index the old
// migration created on every open.
const preChangeShapeDDL = `
CREATE TABLE labelers (
    did               TEXT PRIMARY KEY,
    endpoint          TEXT,
    source            TEXT NOT NULL,
    enabled           INTEGER NOT NULL DEFAULT 1,
    require_sig       INTEGER,
    last_upstream_seq INTEGER,
    last_error        TEXT,
    observed_seq      INTEGER,
    updated_at        INTEGER NOT NULL
);
CREATE UNIQUE INDEX idx_labelers_observed_seq ON labelers(observed_seq);`

// ancientRows seeds the three canonical fixture rows in rowid order:
//   - old-aaa: manual with a resolved endpoint — NOT successfully
//     discovered (source is manual), so it stays undeclared;
//   - old-bbb: firehose with a resolved endpoint and a later last_error —
//     successfully discovered despite the error (deliberately not
//     "last_error IS NULL");
//   - old-ccc: firehose with an empty endpoint — the RecordError-only shape,
//     never declared.
const ancientRows = `INSERT INTO labelers(did, endpoint, source, enabled, last_error, updated_at) VALUES
	('did:plc:old-aaa', 'https://aaa.example.com', 'manual',   1, NULL,                100),
	('did:plc:old-bbb', 'https://bbb.example.com', 'firehose', 1, 'later error',       200),
	('did:plc:old-ccc', '',                        'firehose', 0, 'resolution failed', 300)`

// preChangeRows seeds the same rows with non-sequential observed_seq values
// (5, 3, 9) so the rename's value preservation is distinguishable from any
// re-minting.
const preChangeRows = `INSERT INTO labelers(did, endpoint, source, enabled, last_error, observed_seq, updated_at) VALUES
	('did:plc:old-aaa', 'https://aaa.example.com', 'manual',   1, NULL,                5, 100),
	('did:plc:old-bbb', 'https://bbb.example.com', 'firehose', 1, 'later error',       3, 200),
	('did:plc:old-ccc', '',                        'firehose', 0, 'resolution failed', 9, 300)`

// createShapeDB writes a fixture database at path with the given DDL and
// rows.
func createShapeDB(t *testing.T, path, ddl, rows string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("create fixture table: %v", err)
	}
	if _, err := db.Exec(rows); err != nil {
		t.Fatalf("seed fixture rows: %v", err)
	}
}

func createAncientDB(t *testing.T, path string) {
	t.Helper()
	createShapeDB(t, path, ancientShapeDDL, ancientRows)
}

func createPreChangeDB(t *testing.T, path string) {
	t.Helper()
	createShapeDB(t, path, preChangeShapeDDL, preChangeRows)
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

// rawQuery runs a single-row query and scans it into dest.
func rawQueryRow(t *testing.T, path, query string, dest ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	if err := db.QueryRow(query).Scan(dest...); err != nil {
		t.Fatalf("query %q: %v", query, err)
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

// rawIDByRowid returns the id column ordered by rowid. A SELECT over id
// fails if the column is missing, which is itself the assertion.
func rawIDByRowid(t *testing.T, path string) []int64 {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT id FROM labelers ORDER BY rowid`)
	if err != nil {
		t.Fatalf("select id: %v", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id sql.NullInt64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		if !id.Valid {
			t.Fatalf("id is NULL; backfill did not run")
		}
		ids = append(ids, id.Int64)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate id: %v", err)
	}
	return ids
}

// fixtureRow is one row of a fixture database read outside the store.
type fixtureRow struct {
	did        string
	createdAt  sql.NullInt64
	declaredAt sql.NullInt64
}

// rawTimestampsByRowid returns created_at/declared_at ordered by rowid.
func rawTimestampsByRowid(t *testing.T, path string) []fixtureRow {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT did, created_at, declared_at FROM labelers ORDER BY rowid`)
	if err != nil {
		t.Fatalf("select timestamps: %v", err)
	}
	defer rows.Close()
	var out []fixtureRow
	for rows.Next() {
		var r fixtureRow
		if err := rows.Scan(&r.did, &r.createdAt, &r.declaredAt); err != nil {
			t.Fatalf("scan timestamps: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate timestamps: %v", err)
	}
	return out
}

// rawDeclarations returns the declaration collections held by did.
func rawDeclarations(t *testing.T, path, did string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	rows, err := db.Query(
		`SELECT collection FROM labeler_declarations WHERE did = ? ORDER BY collection`, did)
	if err != nil {
		t.Fatalf("select declarations: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan declaration: %v", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate declarations: %v", err)
	}
	return out
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

// TestMigrateAncientAddsIDBackfillsByRowid verifies AC.4's ancient path:
// a database with neither identity column gets id added and backfilled in
// rowid order, the unique index is created, the obsolete observed_seq column
// is never created, and reopening neither re-mints nor resets anything.
func TestMigrateAncientAddsIDBackfillsByRowid(t *testing.T) {
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "ancient.db")
	createAncientDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open on ancient database failed: %v", err)
	}

	wantIDs := []int64{1, 2, 3}
	gotIDs := rawIDByRowid(t, dbPath)
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("expected ids %v (rowid order), got %v", wantIDs, gotIDs)
	}
	if !rawIndexExists(t, dbPath, "idx_labelers_id") {
		t.Error("expected idx_labelers_id to exist after migration")
	}
	if rawIndexExists(t, dbPath, "idx_labelers_observed_seq") {
		t.Error("idx_labelers_observed_seq must not exist on an ancient database")
	}
	if rawColumnExists(t, dbPath, "labelers", "observed_seq") {
		t.Error("the obsolete observed_seq column must never be created")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer s2.Close()

	if gotIDs = rawIDByRowid(t, dbPath); fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("after reopen, expected ids %v, got %v", wantIDs, gotIDs)
	}

	// Minting continues above the backfilled maximum.
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
	if next.ID != 4 {
		t.Errorf("expected minting to continue at id=4 after backfill, got %d", next.ID)
	}
	if next.CreatedAt <= 0 {
		t.Errorf("post-migration insert must set created_at, got %d", next.CreatedAt)
	}
	if next.DeclaredAt != nil {
		t.Errorf("post-migration manual insert must leave declared_at NULL, got %d", *next.DeclaredAt)
	}
}

// TestMigrateRenamesObservedSeqToID verifies AC.4's pre-change path: the
// observed_seq column is renamed to id preserving every stored value, the
// legacy index is replaced, and the obsolete column name is gone.
func TestMigrateRenamesObservedSeqToID(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "prechange.db")
	createPreChangeDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open on pre-change database failed: %v", err)
	}
	defer s.Close()

	// Seeded values 5, 3, 9 preserved in rowid order.
	gotIDs := rawIDByRowid(t, dbPath)
	wantIDs := []int64{5, 3, 9}
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("rename must preserve values: want %v, got %v", wantIDs, gotIDs)
	}
	if rawColumnExists(t, dbPath, "labelers", "observed_seq") {
		t.Error("observed_seq must be renamed away, not kept alongside id")
	}
	if rawIndexExists(t, dbPath, "idx_labelers_observed_seq") {
		t.Error("the legacy index must be dropped")
	}
	if !rawIndexExists(t, dbPath, "idx_labelers_id") {
		t.Error("expected idx_labelers_id to exist after migration")
	}
}

// TestMigrateFreshSchemaHasID verifies AC.4's fresh path: a brand-new
// database carries id/created_at/declared_at from schema.sql, the
// declarations table, the unique index from migrate — and never the legacy
// column or index.
func TestMigrateFreshSchemaHasID(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fresh.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open fresh database failed: %v", err)
	}
	defer s.Close()

	for _, col := range []string{"id", "created_at", "declared_at"} {
		if !rawColumnExists(t, dbPath, "labelers", col) {
			t.Errorf("fresh database must carry %s from schema.sql", col)
		}
	}
	if rawColumnExists(t, dbPath, "labelers", "observed_seq") {
		t.Error("fresh database must never carry observed_seq")
	}
	if !rawIndexExists(t, dbPath, "idx_labelers_id") {
		t.Error("fresh database must carry idx_labelers_id from migrate()")
	}
	if rawIndexExists(t, dbPath, "idx_labelers_observed_seq") {
		t.Error("fresh database must never carry the legacy index")
	}
	if !rawTableExists(t, dbPath, "labeler_declarations") {
		t.Error("fresh database must carry labeler_declarations")
	}
}

// rawTableExists reports whether the named table exists.
func rawTableExists(t *testing.T, path, table string) bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	var name string
	err = db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
	).Scan(&name)
	return err == nil
}

// TestMigrateIdOnlyReopenIsNoop verifies AC.4's id-only path: a database that
// already carries id (fresh or previously migrated) reopens with no identity
// mutation and no error.
func TestMigrateIdOnlyReopenIsNoop(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idonly.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}

	reg := store.NewLabelerRegistry(s)
	ctx := context.Background()
	if err := reg.Upsert(ctx, store.Labeler{
		DID: "did:plc:idonly", Endpoint: "https://example.com",
		Source: "manual", Enabled: true, UpdatedAt: 1000,
	}); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen of an id-only database must succeed: %v", err)
	}
	defer s2.Close()

	got, exists, err := store.NewLabelerRegistry(s2).Get(ctx, "did:plc:idonly")
	if err != nil || !exists {
		t.Fatalf("Get after reopen: exists=%v err=%v", exists, err)
	}
	if got.ID != 1 {
		t.Errorf("id must survive reopen unchanged, got %d", got.ID)
	}
	if rawColumnExists(t, dbPath, "labelers", "observed_seq") {
		t.Error("reopen must never create observed_seq")
	}
}

// TestMigrateRejectsBothIdentityColumns verifies AC.4's undefined state: a
// labelers table carrying both observed_seq and id fails Open with an
// explicit error.
func TestMigrateRejectsBothIdentityColumns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "both.db")
	createPreChangeDB(t, dbPath)
	rawExec(t, dbPath, `ALTER TABLE labelers ADD COLUMN id INTEGER`)

	_, err := store.Open(dbPath)
	if err == nil {
		t.Fatal("Open must fail when both identity columns exist")
	}
	if !strings.Contains(err.Error(), "both observed_seq and id") {
		t.Fatalf("error must name the ambiguous schema, got: %v", err)
	}
}

// TestMigrateBackfillsTimestamps verifies AC.4's timestamp backfill:
// created_at is approximated from updated_at for every row; declared_at is
// backfilled only for successfully-discovered rows (source='firehose' AND
// endpoint != ”) — a discovered row with a later error still counts, while
// manual rows and RecordError-only rows stay NULL.
func TestMigrateBackfillsTimestamps(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "timestamps.db")
	createPreChangeDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	rows := rawTimestampsByRowid(t, dbPath)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// created_at = updated_at for every row.
	for i, want := range []int64{100, 200, 300} {
		got := rows[i].createdAt
		if !got.Valid || got.Int64 != want {
			t.Errorf("row %d (%s): created_at = %v, want %d (from updated_at)",
				i, rows[i].did, got, want)
		}
	}

	// declared_at: only the discovered row (bbb, updated_at=200).
	for i, want := range []bool{false, true, false} {
		got := rows[i].declaredAt
		if want {
			if !got.Valid || got.Int64 != 200 {
				t.Errorf("row %d (%s): declared_at = %v, want 200 (discovered row)", i, rows[i].did, got)
			}
		} else if got.Valid {
			t.Errorf("row %d (%s): declared_at = %v, want NULL (not successfully discovered)",
				i, rows[i].did, got)
		}
	}
}

// TestMigrateBackfillsDeclarationsOnce verifies AC.4's meta-gated
// declarations backfill: successfully-discovered rows are marked as holding
// an app.bsky.labeler.service declaration exactly once — a reopen after the
// labeler deletes that declaration must not re-insert it.
func TestMigrateBackfillsDeclarationsOnce(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "declarations.db")
	createAncientDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Only the discovered row is marked declared.
	if got := rawDeclarations(t, dbPath, "did:plc:old-bbb"); fmt.Sprint(got) != fmt.Sprint([]string{"app.bsky.labeler.service"}) {
		t.Fatalf("discovered row must be backfilled as app.bsky-declared, got %v", got)
	}
	for _, did := range []string{"did:plc:old-aaa", "did:plc:old-ccc"} {
		if got := rawDeclarations(t, dbPath, did); len(got) != 0 {
			t.Errorf("%s must not be backfilled as declared, got %v", did, got)
		}
	}

	// The meta gate is set.
	var gated string
	rawQueryRow(t, dbPath, `SELECT value FROM meta WHERE key = 'declarations_backfilled'`, &gated)
	if gated != "1" {
		t.Fatalf("declarations_backfilled meta must be set, got %q", gated)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The labeler deletes its declaration (raw delete isolates the migration
	// behavior from ApplyServiceOp), then the database reopens.
	rawExec(t, dbPath, `DELETE FROM labeler_declarations WHERE did = 'did:plc:old-bbb'`)

	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer s2.Close()

	if got := rawDeclarations(t, dbPath, "did:plc:old-bbb"); len(got) != 0 {
		t.Fatalf("reopen must not re-insert a deleted declaration, got %v", got)
	}
}

// TestMigrateRecordErrorOnlyRemainsUndeclared verifies AC.4's
// RecordError-only row: it stays NULL-declared and undeclared after
// migration, and its first successful declaration with autoSubscribe=true
// re-enables it — the zero-to-one transition the backfill deliberately
// preserves.
func TestMigrateRecordErrorOnlyRemainsUndeclared(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recorderror.db")
	createAncientDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// The RecordError-only row (empty endpoint) stays undeclared.
	if got := rawDeclarations(t, dbPath, "did:plc:old-ccc"); len(got) != 0 {
		t.Fatalf("RecordError-only row must stay undeclared, got %v", got)
	}
	var declaredAt sql.NullInt64
	rawQueryRow(t, dbPath,
		`SELECT declared_at FROM labelers WHERE did = 'did:plc:old-ccc'`, &declaredAt)
	if declaredAt.Valid {
		t.Fatalf("RecordError-only row's declared_at must stay NULL, got %d", declaredAt.Int64)
	}

	// First successful declaration re-enables the disabled row.
	persist := store.NewLabelPersist(s)
	res, err := persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op:            "create",
		DID:           "did:plc:old-ccc",
		Collection:    "town.delve.labeler.service",
		Record:        []byte{0xa1, 0x61, 0x61, 0x01},
		Endpoint:      "ccc.example.com",
		AutoSubscribe: true,
	})
	if err != nil {
		t.Fatalf("ApplyServiceOp on migrated RecordError row: %v", err)
	}
	if !res.EnabledChanged {
		t.Error("the first successful declaration must re-enable the RecordError-only row")
	}

	lab, found, err := store.NewLabelerRegistry(s).Get(ctx, "did:plc:old-ccc")
	if err != nil || !found {
		t.Fatalf("Get after re-declare: found=%v err=%v", found, err)
	}
	if !lab.Enabled {
		t.Error("RecordError-only row must be enabled after its first successful declaration")
	}
	if lab.DeclaredAt == nil {
		t.Error("declared_at must be set by the first applied declaration op")
	}
	if got := rawDeclarations(t, dbPath, "did:plc:old-ccc"); fmt.Sprint(got) != fmt.Sprint([]string{"town.delve.labeler.service"}) {
		t.Errorf("declaration must be tracked, got %v", got)
	}
}

// TestMigrateRollsBackOnFailure verifies AC.4's rollback contract: a failure
// during a migration step aborts the whole step, leaving the database
// untouched (a non-transactional implementation would leave the column
// added), and a clean retry after removing the blocker succeeds.
func TestMigrateRollsBackOnFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rollback.db")
	createAncientDB(t, dbPath)

	// BEFORE UPDATE trigger aborts any UPDATE on labelers, so the identity
	// step's backfill (an UPDATE) fails after the ALTER has already run
	// inside the transaction.
	rawExec(t, dbPath, `CREATE TRIGGER block_backfill BEFORE UPDATE ON labelers
		BEGIN
			SELECT RAISE(ABORT, 'test: update blocked');
		END;`)

	if _, err := store.Open(dbPath); err == nil {
		t.Fatal("expected Open to fail while the backfill is blocked")
	}

	// Untouched: the column was not added and the original rows are
	// unchanged.
	if rawColumnExists(t, dbPath, "labelers", "id") {
		t.Fatal("id must not exist after a rolled-back migration")
	}
	endpoints := rawEndpointsByRowid(t, dbPath)
	wantEndpoints := []string{"https://aaa.example.com", "https://bbb.example.com", ""}
	if fmt.Sprint(endpoints) != fmt.Sprint(wantEndpoints) {
		t.Fatalf("endpoints changed after rollback: want %v, got %v", wantEndpoints, endpoints)
	}

	// Clean retry: drop the blocker and Open migrates successfully.
	rawExec(t, dbPath, "DROP TRIGGER block_backfill")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open after unblocking failed: %v", err)
	}
	defer s.Close()

	if gotIDs := rawIDByRowid(t, dbPath); fmt.Sprint(gotIDs) != fmt.Sprint([]int64{1, 2, 3}) {
		t.Fatalf("expected ids [1 2 3] after retry, got %v", gotIDs)
	}
	if !rawIndexExists(t, dbPath, "idx_labelers_id") {
		t.Error("expected idx_labelers_id to exist after retry")
	}
}
