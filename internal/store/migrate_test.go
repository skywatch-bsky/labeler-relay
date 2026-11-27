// pattern: Imperative Shell

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

// Keep the schema without identity columns independent of schema.sql so schema changes cannot update the migration fixture.
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

// Include the legacy index to test its replacement when observed_seq becomes id.
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

// Only old-bbb represents a discovered declaration, despite its later error.
// A manual endpoint and a RecordError-only row must not count as declarations.
const ancientRows = `INSERT INTO labelers(did, endpoint, source, enabled, last_error, updated_at) VALUES
	('did:plc:old-aaa', 'https://aaa.example.com', 'manual',   1, NULL,                100),
	('did:plc:old-bbb', 'https://bbb.example.com', 'firehose', 1, 'later error',       200),
	('did:plc:old-ccc', '',                        'firehose', 0, 'resolution failed', 300)`

// Non-sequential observed_seq values expose accidental reminting during the rename.
const preChangeRows = `INSERT INTO labelers(did, endpoint, source, enabled, last_error, observed_seq, updated_at) VALUES
	('did:plc:old-aaa', 'https://aaa.example.com', 'manual',   1, NULL,                5, 100),
	('did:plc:old-bbb', 'https://bbb.example.com', 'firehose', 1, 'later error',       3, 200),
	('did:plc:old-ccc', '',                        'firehose', 0, 'resolution failed', 9, 300)`

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

type fixtureRow struct {
	did        string
	createdAt  sql.NullInt64
	declaredAt sql.NullInt64
}

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

// A later discovery error must not exclude a row with a resolved endpoint from declared_at backfill.
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

	for i, want := range []int64{100, 200, 300} {
		got := rows[i].createdAt
		if !got.Valid || got.Int64 != want {
			t.Errorf("row %d (%s): created_at = %v, want %d (from updated_at)",
				i, rows[i].did, got, want)
		}
	}

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

// Reopen must not restore a declaration deleted after backfill.
func TestMigrateBackfillsDeclarationsOnce(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "declarations.db")
	createAncientDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if got := rawDeclarations(t, dbPath, "did:plc:old-bbb"); fmt.Sprint(got) != fmt.Sprint([]string{"app.bsky.labeler.service"}) {
		t.Fatalf("discovered row must be backfilled as app.bsky-declared, got %v", got)
	}
	for _, did := range []string{"did:plc:old-aaa", "did:plc:old-ccc"} {
		if got := rawDeclarations(t, dbPath, did); len(got) != 0 {
			t.Errorf("%s must not be backfilled as declared, got %v", did, got)
		}
	}

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

// Backfill must preserve the first-declaration re-enable transition for RecordError-only rows.
func TestMigrateRecordErrorOnlyRemainsUndeclared(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recorderror.db")
	createAncientDB(t, dbPath)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	if got := rawDeclarations(t, dbPath, "did:plc:old-ccc"); len(got) != 0 {
		t.Fatalf("RecordError-only row must stay undeclared, got %v", got)
	}
	var declaredAt sql.NullInt64
	rawQueryRow(t, dbPath,
		`SELECT declared_at FROM labelers WHERE did = 'did:plc:old-ccc'`, &declaredAt)
	if declaredAt.Valid {
		t.Fatalf("RecordError-only row's declared_at must stay NULL, got %d", declaredAt.Int64)
	}

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

// A backfill failure must undo the preceding ALTER, not leave a partially migrated schema.
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

func TestMigrateAddsEventDedupAndAppliedOps(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "old-events.db")

	// Omit dedup_key to exercise the event migration. Let schema.sql create labelers independently.
	const oldEventsDDL = `
CREATE TABLE events (
    relay_seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT    NOT NULL,
    labeler_did  TEXT    NOT NULL,
    upstream_seq INTEGER,
    frame_cbor   BLOB    NOT NULL,
    ingest_ts    INTEGER NOT NULL
);`
	const oldEventsRows = `INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts) VALUES
	('labels', 'did:plc:old-labeler', 5, x'01', 1000),
	('service', 'did:plc:old-labeler', NULL, x'02', 2000)`
	createShapeDB(t, dbPath, oldEventsDDL, oldEventsRows)

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open on old-shape events database: %v", err)
	}

	if !rawColumnExists(t, dbPath, "events", "dedup_key") {
		t.Error("expected events.dedup_key to be added by migration")
	}
	if !rawIndexExists(t, dbPath, "idx_events_dedup") {
		t.Error("expected idx_events_dedup to exist after migration")
	}
	if !rawTableExists(t, dbPath, "applied_service_ops") {
		t.Error("expected applied_service_ops to exist after migration")
	}

	var rows, nullKeys int
	if err := s.DB().QueryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE dedup_key IS NULL) FROM events`).Scan(&rows, &nullKeys); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if rows != 2 || nullKeys != 2 {
		t.Fatalf("expected 2 surviving rows with NULL dedup keys, got rows=%d nulls=%d", rows, nullKeys)
	}

	if _, err := store.NewLabelPersist(s).ApplyServiceOp(context.Background(), store.ServiceOp{
		Op: "create", DID: "did:plc:dedup-mig", Collection: "app.bsky.labeler.service", Rkey: "self",
		Record: []byte{0xa1, 0x61, 'a', 0x01}, Endpoint: "mig.example.com", AutoSubscribe: true,
		CommitSeq: 42,
	}); err != nil {
		t.Fatalf("post-migration ApplyServiceOp: %v", err)
	}
	var identity string
	if err := s.DB().QueryRow(`SELECT dedup_key FROM applied_service_ops WHERE dedup_key LIKE 'svc:42/%'`).Scan(&identity); err != nil {
		t.Fatalf("read applied identity: %v", err)
	}
	wantKey := "svc:42/did:plc:dedup-mig/app.bsky.labeler.service/self/create"
	if identity != wantKey {
		t.Fatalf("applied identity = %q, want %q", identity, wantKey)
	}
	s.Close()

	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if !rawColumnExists(t, dbPath, "events", "dedup_key") || !rawIndexExists(t, dbPath, "idx_events_dedup") || !rawTableExists(t, dbPath, "applied_service_ops") {
		t.Fatal("expected dedup schema to survive a reopen")
	}
	var surviving int
	if err := s2.DB().QueryRow(`SELECT COUNT(*) FROM applied_service_ops`).Scan(&surviving); err != nil {
		t.Fatalf("count applied ops: %v", err)
	}
	if surviving != 1 {
		t.Fatalf("expected the applied identity to survive the reopen, got %d rows", surviving)
	}
}
