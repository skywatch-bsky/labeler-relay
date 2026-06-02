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
}

type LabelerRegistry struct {
	store *Store
}

func NewLabelerRegistry(s *Store) *LabelerRegistry {
	return &LabelerRegistry{store: s}
}

func (r *LabelerRegistry) Upsert(ctx context.Context, labeler Labeler) error {
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

	// Stickiness rule: on conflict, only update endpoint and updated_at.
	// Leave source and enabled untouched to preserve manual labeler stickiness.
	query := `
		INSERT INTO labelers(did, endpoint, source, enabled, require_sig, last_upstream_seq, last_error, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(did) DO UPDATE SET
			endpoint = excluded.endpoint,
			updated_at = excluded.updated_at
	`

	enabled := 0
	if labeler.Enabled {
		enabled = 1
	}

	_, err := r.store.DB().ExecContext(ctx, query,
		labeler.DID,
		labeler.Endpoint,
		labeler.Source,
		enabled,
		requireSig,
		lastUpstreamSeq,
		lastError,
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

func (r *LabelerRegistry) Get(ctx context.Context, did string) (Labeler, bool, error) {
	var labeler Labeler
	var enabled int
	var requireSig *bool
	var lastUpstreamSeq *int64
	var lastError *string

	err := r.store.DB().QueryRowContext(ctx,
		`SELECT did, endpoint, source, enabled, require_sig, last_upstream_seq, last_error, updated_at
		 FROM labelers WHERE did = ?`,
		did,
	).Scan(&labeler.DID, &labeler.Endpoint, &labeler.Source, &enabled, &requireSig, &lastUpstreamSeq, &lastError, &labeler.UpdatedAt)

	if err == sql.ErrNoRows {
		return Labeler{}, false, nil
	}
	if err != nil {
		return Labeler{}, false, fmt.Errorf("failed to get labeler: %w", err)
	}

	labeler.Enabled = enabled != 0
	labeler.RequireSig = requireSig
	labeler.LastUpstreamSeq = lastUpstreamSeq
	if lastError != nil {
		labeler.LastError = *lastError
	}

	return labeler, true, nil
}

func (r *LabelerRegistry) List(ctx context.Context) ([]Labeler, error) {
	rows, err := r.store.DB().QueryContext(ctx,
		`SELECT did, endpoint, source, enabled, require_sig, last_upstream_seq, last_error, updated_at
		 FROM labelers ORDER BY did`)
	if err != nil {
		return nil, fmt.Errorf("failed to list labelers: %w", err)
	}
	defer rows.Close()

	var labelers []Labeler
	for rows.Next() {
		var labeler Labeler
		var enabled int
		var requireSig *bool
		var lastUpstreamSeq *int64
		var lastError *string

		if err := rows.Scan(&labeler.DID, &labeler.Endpoint, &labeler.Source, &enabled, &requireSig, &lastUpstreamSeq, &lastError, &labeler.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan labeler: %w", err)
		}

		labeler.Enabled = enabled != 0
		labeler.RequireSig = requireSig
		labeler.LastUpstreamSeq = lastUpstreamSeq
		if lastError != nil {
			labeler.LastError = *lastError
		}

		labelers = append(labelers, labeler)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating labelers: %w", err)
	}

	return labelers, nil
}

func (r *LabelerRegistry) ListEnabled(ctx context.Context) ([]Labeler, error) {
	rows, err := r.store.DB().QueryContext(ctx,
		`SELECT did, endpoint, source, enabled, require_sig, last_upstream_seq, last_error, updated_at
		 FROM labelers WHERE enabled = 1 ORDER BY did`)
	if err != nil {
		return nil, fmt.Errorf("failed to list enabled labelers: %w", err)
	}
	defer rows.Close()

	var labelers []Labeler
	for rows.Next() {
		var labeler Labeler
		var enabled int
		var requireSig *bool
		var lastUpstreamSeq *int64
		var lastError *string

		if err := rows.Scan(&labeler.DID, &labeler.Endpoint, &labeler.Source, &enabled, &requireSig, &lastUpstreamSeq, &lastError, &labeler.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan labeler: %w", err)
		}

		labeler.Enabled = enabled != 0
		labeler.RequireSig = requireSig
		labeler.LastUpstreamSeq = lastUpstreamSeq
		if lastError != nil {
			labeler.LastError = *lastError
		}

		labelers = append(labelers, labeler)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating enabled labelers: %w", err)
	}

	return labelers, nil
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
// For fresh labelers (not yet in the registry), inserts a minimal row with source='firehose', enabled=0.
// For existing labelers, updates only last_error and updated_at, preserving source and enabled.
// This ensures manual labelers remain sticky even when discovery encounters errors.
func (r *LabelerRegistry) RecordError(ctx context.Context, did, msg string) error {
	query := `
		INSERT INTO labelers(did, endpoint, last_error, source, enabled, updated_at)
		VALUES (?, '', ?, 'firehose', 0, ?)
		ON CONFLICT(did) DO UPDATE SET
			last_error = excluded.last_error,
			updated_at = excluded.updated_at
	`
	_, err := r.store.DB().ExecContext(ctx, query, did, msg, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("failed to record error: %w", err)
	}
	return nil
}
