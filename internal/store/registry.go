// pattern: Imperative Shell

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Labeler struct {
	DID             string
	Endpoint        string
	Source          string
	Enabled         bool
	RequireSig      *bool
	LastUpstreamSeq *int64
	LastError       string
	UpdatedAt       int64
	// ID is the row's monotonically increasing identifier, minted at first
	// insert and never reused or changed. It is exposed publicly as
	// labelerView.id, and it backs first-observed ordering and getLabelers
	// pagination. json:"-" keeps it out of admin JSON, which encodes
	// store.Labeler directly (admin JSON exposes CreatedAt/DeclaredAt
	// instead).
	ID int64 `json:"-"`
	// CreatedAt is the row-creation timestamp in unix seconds: set once at
	// first insert, never updated. Exposed publicly as labelerView.createdAt.
	// Untagged on purpose: admin JSON carries it as CreatedAt, matching the
	// existing untagged UpdatedAt.
	CreatedAt int64
	// DeclaredAt is the timestamp of the most recently observed declaration
	// transition, in unix seconds. NULL until a declaration is observed.
	// Written only by ApplyServiceOp — never by admin upserts, RecordError,
	// cursor writes, SetEnabled, or SetRequireSig — so the public
	// labelerView.updatedAt moves exactly when a declaration op is observed.
	// Untagged on purpose: admin JSON carries it as DeclaredAt.
	DeclaredAt *int64
}

type LabelerRegistry struct {
	store *Store
}

func NewLabelerRegistry(s *Store) *LabelerRegistry {
	return &LabelerRegistry{store: s}
}

// Upsert applies the stickiness rules in its own transaction. The firehose
// watcher no longer calls this for declaration ops — it goes through
// LabelPersist.ApplyServiceOp, which applies the registry transition and the
// #service event in one transaction — but the admin API still does.
func (r *LabelerRegistry) Upsert(ctx context.Context, labeler Labeler) error {
	tx, err := r.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin upsert transaction: %w", err)
	}
	defer tx.Rollback()
	if err := upsertLabelerTx(ctx, tx, labeler); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit upsert: %w", err)
	}
	return nil
}

// upsertLabelerTx applies the Upsert write inside an existing transaction, so
// ApplyServiceOp can combine the registry transition with the event write in
// one SQLite transaction.
//
// Stickiness rule: firehose upserts only refresh endpoint and updated_at,
// preserving operator-controlled source and enabled. Manual upserts claim the
// row: they additionally take over source and enabled so an operator add
// always yields the requested state, even when the row was previously
// inserted disabled by discovery (e.g. via RecordError). Cursor and sig
// policy are preserved in both cases. A successful upsert clears last_error:
// the row resolved cleanly, so any prior discovery error is stale.
//
// id is minted at first insert only (MAX+1 over the table); created_at is set
// at first insert only and never updated. The ON CONFLICT branches never list
// either (nor declared_at), so re-upserts keep the original values and the
// identifier stays monotonic and never reused. SQLite serializes writers
// (single process, busy_timeout), so concurrent first-inserts stay monotonic.
func upsertLabelerTx(ctx context.Context, tx *sql.Tx, labeler Labeler) error {
	var requireSig interface{} = nil
	if labeler.RequireSig != nil {
		requireSig = *labeler.RequireSig
	}

	var lastUpstreamSeq interface{} = nil
	if labeler.LastUpstreamSeq != nil {
		lastUpstreamSeq = *labeler.LastUpstreamSeq
	}

	var lastError interface{} = nil
	if labeler.LastError != "" {
		lastError = labeler.LastError
	}

	query := `
		INSERT INTO labelers(did, endpoint, source, enabled, require_sig, last_upstream_seq, last_error, id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(id), 0) + 1 FROM labelers), ?, ?)
		ON CONFLICT(did) DO UPDATE SET
			endpoint = excluded.endpoint,
			last_error = NULL,
			updated_at = excluded.updated_at
	`
	if labeler.Source == "manual" {
		query = `
		INSERT INTO labelers(did, endpoint, source, enabled, require_sig, last_upstream_seq, last_error, id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(id), 0) + 1 FROM labelers), ?, ?)
		ON CONFLICT(did) DO UPDATE SET
			endpoint = excluded.endpoint,
			source = excluded.source,
			enabled = excluded.enabled,
			last_error = NULL,
			updated_at = excluded.updated_at
	`
	}

	enabled := 0
	if labeler.Enabled {
		enabled = 1
	}

	_, err := tx.ExecContext(ctx, query,
		labeler.DID,
		labeler.Endpoint,
		labeler.Source,
		enabled,
		requireSig,
		lastUpstreamSeq,
		lastError,
		time.Now().Unix(),
		labeler.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert labeler: %w", err)
	}

	return nil
}

func (r *LabelerRegistry) SetEnabled(ctx context.Context, did string, enabled bool) error {
	enabledInt := 0
	if enabled {
		enabledInt = 1
	}

	result, err := r.store.DB().ExecContext(ctx, "UPDATE labelers SET enabled = ? WHERE did = ?", enabledInt, did)
	if err != nil {
		return fmt.Errorf("failed to set enabled: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if affected == 0 {
		return fmt.Errorf("labeler not found: %s", did)
	}

	return nil
}

// SetRequireSig sets or clears the per-labeler signature-policy override.
// A nil value clears the override so the global default applies.
func (r *LabelerRegistry) SetRequireSig(ctx context.Context, did string, requireSig *bool) error {
	var val interface{}
	if requireSig != nil {
		val = *requireSig
	}

	result, err := r.store.DB().ExecContext(ctx, "UPDATE labelers SET require_sig = ? WHERE did = ?", val, did)
	if err != nil {
		return fmt.Errorf("failed to set require_sig: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if affected == 0 {
		return fmt.Errorf("labeler not found: %s", did)
	}

	return nil
}

const labelerColumns = `did, endpoint, source, enabled, require_sig, last_upstream_seq, last_error, id, created_at, declared_at, updated_at`

// scanLabeler decodes one labelers row in labelerColumns order.
func scanLabeler(s interface{ Scan(...any) error }) (Labeler, error) {
	var labeler Labeler
	var enabled int
	var requireSig *bool
	var lastUpstreamSeq *int64
	var lastError *string

	if err := s.Scan(&labeler.DID, &labeler.Endpoint, &labeler.Source, &enabled, &requireSig,
		&lastUpstreamSeq, &lastError, &labeler.ID, &labeler.CreatedAt, &labeler.DeclaredAt, &labeler.UpdatedAt); err != nil {
		return Labeler{}, err
	}

	labeler.Enabled = enabled != 0
	labeler.RequireSig = requireSig
	labeler.LastUpstreamSeq = lastUpstreamSeq
	if lastError != nil {
		labeler.LastError = *lastError
	}

	return labeler, nil
}

func (r *LabelerRegistry) Get(ctx context.Context, did string) (Labeler, bool, error) {
	labeler, err := scanLabeler(r.store.DB().QueryRowContext(ctx,
		`SELECT `+labelerColumns+` FROM labelers WHERE did = ?`, did))
	if err == sql.ErrNoRows {
		return Labeler{}, false, nil
	}
	if err != nil {
		return Labeler{}, false, fmt.Errorf("failed to get labeler: %w", err)
	}
	return labeler, true, nil
}

func (r *LabelerRegistry) List(ctx context.Context) ([]Labeler, error) {
	rows, err := r.store.DB().QueryContext(ctx,
		`SELECT `+labelerColumns+` FROM labelers ORDER BY id ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to list labelers: %w", err)
	}
	defer rows.Close()

	var labelers []Labeler
	for rows.Next() {
		labeler, err := scanLabeler(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan labeler: %w", err)
		}
		labelers = append(labelers, labeler)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating labelers: %w", err)
	}

	return labelers, nil
}

// testBetweenSnapshotReads, when non-nil, runs between the labeler and
// declaration queries of the snapshot read methods. Test-only seam for
// pinning single-snapshot semantics; nil outside tests.
var testBetweenSnapshotReads func()

// ListWithDeclarations returns the id-ordered labelers and their declarations
// from one SQLite read snapshot.
func (r *LabelerRegistry) ListWithDeclarations(ctx context.Context) ([]Labeler, map[string][]string, error) {
	tx, err := r.store.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin labeler snapshot: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+labelerColumns+` FROM labelers ORDER BY id ASC`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list labelers: %w", err)
	}
	var labelers []Labeler
	for rows.Next() {
		l, scanErr := scanLabeler(rows)
		if scanErr != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("failed to scan labeler: %w", scanErr)
		}
		labelers = append(labelers, l)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("error iterating labelers: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close labelers: %w", err)
	}
	if testBetweenSnapshotReads != nil {
		testBetweenSnapshotReads()
	}
	declRows, err := tx.QueryContext(ctx, `SELECT did, collection FROM labeler_declarations ORDER BY did ASC, collection ASC`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list declarations: %w", err)
	}
	declarations := make(map[string][]string)
	for declRows.Next() {
		var did, collection string
		if err := declRows.Scan(&did, &collection); err != nil {
			declRows.Close()
			return nil, nil, fmt.Errorf("failed to scan declaration: %w", err)
		}
		declarations[did] = append(declarations[did], collection)
	}
	if err := declRows.Err(); err != nil {
		declRows.Close()
		return nil, nil, fmt.Errorf("error iterating declarations: %w", err)
	}
	if err := declRows.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close declarations: %w", err)
	}
	return labelers, declarations, nil
}

// GetWithDeclarations returns one labeler and its declaration collections
// from one SQLite read snapshot, preserving Get's found semantics.
func (r *LabelerRegistry) GetWithDeclarations(ctx context.Context, did string) (Labeler, []string, bool, error) {
	tx, err := r.store.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Labeler{}, nil, false, fmt.Errorf("failed to begin labeler snapshot: %w", err)
	}
	defer tx.Rollback()
	labeler, err := scanLabeler(tx.QueryRowContext(ctx, `SELECT `+labelerColumns+` FROM labelers WHERE did = ?`, did))
	if err == sql.ErrNoRows {
		return Labeler{}, []string{}, false, nil
	}
	if err != nil {
		return Labeler{}, nil, false, fmt.Errorf("failed to get labeler: %w", err)
	}
	if testBetweenSnapshotReads != nil {
		testBetweenSnapshotReads()
	}
	rows, err := tx.QueryContext(ctx, `SELECT collection FROM labeler_declarations WHERE did = ? ORDER BY collection ASC`, did)
	if err != nil {
		return Labeler{}, nil, false, fmt.Errorf("failed to list declarations: %w", err)
	}
	declarations := []string{}
	for rows.Next() {
		var collection string
		if err := rows.Scan(&collection); err != nil {
			rows.Close()
			return Labeler{}, nil, false, fmt.Errorf("failed to scan declaration: %w", err)
		}
		declarations = append(declarations, collection)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Labeler{}, nil, false, fmt.Errorf("error iterating declarations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return Labeler{}, nil, false, fmt.Errorf("failed to close declarations: %w", err)
	}
	return labeler, declarations, true, nil
}

func (r *LabelerRegistry) ListEnabled(ctx context.Context) ([]Labeler, error) {
	// ORDER BY did is retained deliberately: ListEnabled feeds Reconcile's
	// internal set semantics, where order is irrelevant.
	rows, err := r.store.DB().QueryContext(ctx,
		`SELECT `+labelerColumns+` FROM labelers WHERE enabled = 1 ORDER BY did`)
	if err != nil {
		return nil, fmt.Errorf("failed to list enabled labelers: %w", err)
	}
	defer rows.Close()

	var labelers []Labeler
	for rows.Next() {
		labeler, err := scanLabeler(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan labeler: %w", err)
		}
		labelers = append(labelers, labeler)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating enabled labelers: %w", err)
	}

	return labelers, nil
}

// DeclarationsFor returns the declaration collections a labeler currently
// holds, ordered by collection. The result is non-nil: an empty slice when
// the labeler holds none (e.g. manually added labelers).
//
// Declaration rows are written only by LabelPersist.ApplyServiceOp; the set
// reflects only declaration ops this relay observed.
func (r *LabelerRegistry) DeclarationsFor(ctx context.Context, did string) ([]string, error) {
	rows, err := r.store.DB().QueryContext(ctx,
		`SELECT collection FROM labeler_declarations WHERE did = ? ORDER BY collection ASC`, did)
	if err != nil {
		return nil, fmt.Errorf("failed to list declarations: %w", err)
	}
	defer rows.Close()

	collections := []string{}
	for rows.Next() {
		var collection string
		if err := rows.Scan(&collection); err != nil {
			return nil, fmt.Errorf("failed to scan declaration: %w", err)
		}
		collections = append(collections, collection)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating declarations: %w", err)
	}

	return collections, nil
}

// ListDeclarations returns the declaration collections of every labeler that
// holds at least one, keyed by DID. The bulk read behind getLabelers.
func (r *LabelerRegistry) ListDeclarations(ctx context.Context) (map[string][]string, error) {
	rows, err := r.store.DB().QueryContext(ctx,
		`SELECT did, collection FROM labeler_declarations ORDER BY did ASC, collection ASC`)
	if err != nil {
		return nil, fmt.Errorf("failed to list all declarations: %w", err)
	}
	defer rows.Close()

	byDID := make(map[string][]string)
	for rows.Next() {
		var did, collection string
		if err := rows.Scan(&did, &collection); err != nil {
			return nil, fmt.Errorf("failed to scan declaration row: %w", err)
		}
		byDID[did] = append(byDID[did], collection)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating declaration rows: %w", err)
	}

	return byDID, nil
}

func (r *LabelerRegistry) ReadCursor(ctx context.Context, did string) (*int64, error) {
	var lastUpstreamSeq *int64

	err := r.store.DB().QueryRowContext(ctx,
		`SELECT last_upstream_seq FROM labelers WHERE did = ?`,
		did,
	).Scan(&lastUpstreamSeq)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read cursor: %w", err)
	}

	return lastUpstreamSeq, nil
}

func (r *LabelerRegistry) WriteCursor(ctx context.Context, did string, seq int64) error {
	result, err := r.store.DB().ExecContext(ctx,
		`UPDATE labelers SET last_upstream_seq = ? WHERE did = ?`,
		seq,
		did,
	)
	if err != nil {
		return fmt.Errorf("failed to write cursor: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if affected == 0 {
		return fmt.Errorf("labeler not found: %s", did)
	}

	return nil
}

// RecordError records or updates an error message for a labeler.
// For fresh labelers (not yet in the registry), inserts a minimal row with source='firehose', enabled=0,
// minting the next id like any first insert (and setting created_at).
// For existing labelers, updates only last_error and updated_at, preserving source and enabled
// (and the existing id, created_at, and declared_at, which the ON CONFLICT branch never lists).
// This ensures manual labelers remain sticky even when discovery encounters errors.
//
// RecordError never tracks a declaration: rows created here carry no
// labeler_declarations row and NULL declared_at, so their declarations view is
// empty and their first successful declaration is still a zero-to-one
// transition that can re-enable them.
func (r *LabelerRegistry) RecordError(ctx context.Context, did, msg string) error {
	query := `
		INSERT INTO labelers(did, endpoint, last_error, source, enabled, id, created_at, updated_at)
		VALUES (?, '', ?, 'firehose', 0, (SELECT COALESCE(MAX(id), 0) + 1 FROM labelers), ?, ?)
		ON CONFLICT(did) DO UPDATE SET
			last_error = excluded.last_error,
			updated_at = excluded.updated_at
	`
	now := time.Now().Unix()
	_, err := r.store.DB().ExecContext(ctx, query, did, msg, now, now)
	if err != nil {
		return fmt.Errorf("failed to record error: %w", err)
	}
	return nil
}
