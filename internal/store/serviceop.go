// pattern: Imperative Shell

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ServiceOp is a firehose declaration operation for LabelPersist.ApplyServiceOp.
type ServiceOp struct {
	Op            string // "create" | "update" | "delete"
	DID           string
	Collection    string // declaration collection NSID
	Record        []byte // raw DAG-CBOR record bytes; nil for delete
	Endpoint      string // resolved upstream endpoint; create/update only
	AutoSubscribe bool   // whether the zero-to-one re-enable may fire
	CommitSeq     int64  // upstream commit sequence. Zero disables replay dedup. The watcher always sets it.
	Rkey          string // upstream record key
}

// ServiceOpResult reports what an applied (or skipped) op did.
type ServiceOpResult struct {
	// Seq is the relay seq of the persisted #service event (0 when Skipped).
	Seq int64
	// Skipped reports an op that was already seen or a delete for an
	// untracked declaration: no event or registry change was written, but
	// replay identity may have been committed.
	Skipped bool
	// EnabledChanged tells callers whether to reconcile upstream subscriptions.
	EnabledChanged bool
}

type serviceOpWrites struct {
	seq            int64
	frame          []byte
	skipped        bool
	enabledChanged bool
}

// ApplyServiceOp commits the registry transition and #service event in one SQLite transaction.
// It shares PersistIngest's mutex: broadcast order == commit order == seq order.
// Only ApplyServiceOp writes declared_at, on create, update, and tracked delete.
// Any error rolls back all changes: no registry mutation, event row, consumed seq, or declared_at change.
//
// With AutoSubscribe, the first declaration re-enables a disabled firehose row.
// Removing the last declaration disables an enabled firehose row. Manual rows never change enabled state automatically.
//
// applied_service_ops retains replay identities after event pruning, including skipped deletes.
// Keys use svc:<commitSeq>/<did>/<collection>/<rkey>/<op>. CommitSeq 0 disables replay dedup.
// events.dedup_key provides a redundant event-side constraint.
func (p *LabelPersist) ApplyServiceOp(ctx context.Context, op ServiceOp) (res ServiceOpResult, err error) {
	if op.Op != "delete" && len(op.Record) == 0 {
		return res, fmt.Errorf("create/update service op requires record bytes")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now().Unix()

	tx, err := p.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	w, err := p.applyServiceOpTx(ctx, tx, op, now)
	if err != nil {
		return res, err
	}

	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("failed to commit transaction: %w", err)
	}
	if w.skipped {
		return ServiceOpResult{Skipped: true}, nil
	}

	// Hold mu through broadcast to preserve commit and seq order.
	if p.broadcaster != nil {
		p.broadcaster(LiveEvent{
			RelaySeq:   w.seq,
			Kind:       "service",
			LabelerDID: op.DID,
			FrameCBOR:  w.frame,
		})
	}
	if p.onHeadAdvanced != nil {
		p.onHeadAdvanced(float64(w.seq))
	}

	return ServiceOpResult{Seq: w.seq, EnabledChanged: w.enabledChanged}, nil
}

func (p *LabelPersist) applyServiceOpTx(ctx context.Context, tx *sql.Tx, op ServiceOp, now int64) (w serviceOpWrites, err error) {
	dedupKey := fmt.Sprintf("svc:%d/%s/%s/%s/%s", op.CommitSeq, op.DID, op.Collection, op.Rkey, op.Op)
	if op.CommitSeq == 0 {
		dedupKey = ""
	}
	if dedupKey != "" {
		result, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO applied_service_ops(dedup_key, applied_ts) VALUES (?, ?)`,
			dedupKey, now)
		if err != nil {
			return w, fmt.Errorf("failed to record service op identity: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return w, fmt.Errorf("failed to check service op identity: %w", err)
		}
		if affected == 0 {
			w.skipped = true
			return w, nil
		}
	}

	switch op.Op {
	case "create", "update":
		if err := upsertLabelerTx(ctx, tx, Labeler{
			DID:       op.DID,
			Endpoint:  op.Endpoint,
			Source:    "firehose",
			Enabled:   op.AutoSubscribe,
			UpdatedAt: now,
		}); err != nil {
			return w, err
		}

		// Count before insertion to detect the first declaration.
		var hadDeclarations int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM labeler_declarations WHERE did = ?`, op.DID).Scan(&hadDeclarations); err != nil {
			return w, fmt.Errorf("failed to count declarations: %w", err)
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO labeler_declarations(did, collection) VALUES (?, ?)`,
			op.DID, op.Collection); err != nil {
			return w, fmt.Errorf("failed to insert declaration: %w", err)
		}

		// Preserve operator-disabled rows on updates and additional declarations.
		if hadDeclarations == 0 && op.AutoSubscribe {
			result, err := tx.ExecContext(ctx,
				`UPDATE labelers SET enabled = 1 WHERE did = ? AND source = 'firehose' AND enabled = 0`,
				op.DID)
			if err != nil {
				return w, fmt.Errorf("failed to re-enable labeler: %w", err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return w, fmt.Errorf("failed to check re-enable rows: %w", err)
			}
			w.enabledChanged = affected > 0
		}

	case "delete":
		// An untracked delete must not emit an event or change the registry.
		var tracked int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM labeler_declarations WHERE did = ? AND collection = ?`,
			op.DID, op.Collection).Scan(&tracked); err != nil {
			return w, fmt.Errorf("failed to read declaration state: %w", err)
		}
		if tracked == 0 {
			w.skipped = true
			return w, nil
		}

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM labeler_declarations WHERE did = ? AND collection = ?`,
			op.DID, op.Collection); err != nil {
			return w, fmt.Errorf("failed to delete declaration: %w", err)
		}

		var remaining int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM labeler_declarations WHERE did = ?`, op.DID).Scan(&remaining); err != nil {
			return w, fmt.Errorf("failed to count remaining declarations: %w", err)
		}

		// Do not auto-disable manual rows.
		if remaining == 0 {
			result, err := tx.ExecContext(ctx,
				`UPDATE labelers SET enabled = 0 WHERE did = ? AND source = 'firehose' AND enabled = 1`,
				op.DID)
			if err != nil {
				return w, fmt.Errorf("failed to disable labeler: %w", err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return w, fmt.Errorf("failed to check disable rows: %w", err)
			}
			w.enabledChanged = affected > 0
		}

	default:
		return w, fmt.Errorf("unknown service op: %s", op.Op)
	}

	// The upsert or tracked declaration's foreign key guarantees that the labeler exists.
	if _, err := tx.ExecContext(ctx,
		`UPDATE labelers SET declared_at = ? WHERE did = ?`, now, op.DID); err != nil {
		return w, fmt.Errorf("failed to update declared_at: %w", err)
	}

	w.seq, w.frame, err = persistServiceEventTx(ctx, tx, op, dedupKey)
	return w, err
}

// persistServiceEventTx reserves relay_seq before encoding because the frame contains its own seq.
func persistServiceEventTx(ctx context.Context, tx *sql.Tx, op ServiceOp, dedupKey string) (seq int64, frame []byte, err error) {
	ingestTs := timestampMs()
	var eventDedupKey interface{}
	if dedupKey != "" {
		eventDedupKey = dedupKey
	}
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts, dedup_key)
		 VALUES ('service', ?, NULL, x'', ?, ?)
		 RETURNING relay_seq`,
		op.DID, ingestTs, eventDedupKey).Scan(&seq); err != nil {
		return 0, nil, fmt.Errorf("failed to insert placeholder: %w", err)
	}

	frame, err = EncodeServiceFrame(seq, op.DID, op.Op, op.Collection, op.Record)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to encode service frame: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE events SET frame_cbor = ? WHERE relay_seq = ?`, frame, seq); err != nil {
		return 0, nil, fmt.Errorf("failed to update frame: %w", err)
	}
	return seq, frame, nil
}
