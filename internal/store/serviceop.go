// pattern: Imperative Shell

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ServiceOp describes a single labeler service declaration operation observed
// on the firehose, applied atomically with its #service event by
// LabelPersist.ApplyServiceOp. Its replay identity is stored in
// applied_service_ops independently of event pruning, including for skipped
// ops; events.dedup_key is a redundant event-side record. CommitSeq 0 disables
// replay dedup for direct/test callers.
type ServiceOp struct {
	Op            string // "create" | "update" | "delete"
	DID           string
	Collection    string // declaration collection NSID
	Record        []byte // raw DAG-CBOR record bytes; nil for delete
	Endpoint      string // resolved upstream endpoint; create/update only
	AutoSubscribe bool   // whether the zero-to-one re-enable may fire
	CommitSeq     int64  // upstream firehose commit sequence; 0 disables replay dedup (for direct/test callers) — the watcher always sets it from the commit event
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
	// EnabledChanged reports whether the op flipped the labeler's enabled
	// state (auto-disable on last-declaration removal, or re-enable on the
	// zero-to-one declaration transition). Callers use it to decide whether
	// the slurper needs a reconcile poke.
	EnabledChanged bool
}

// serviceOpWrites carries the intermediate state of an applied op between the
// registry transition and the commit.
type serviceOpWrites struct {
	seq            int64
	frame          []byte
	skipped        bool
	enabledChanged bool
}

// ApplyServiceOp applies a declaration op — the registry transition and its
// #service event — in one SQLite transaction. It runs under the same persist
// mutex as PersistIngest, preserving broadcast order == commit order == seq
// order, and is the only writer of declared_at.
//
// Semantics:
//
//   - create/update: tx-scoped firehose upsert (stickiness rules; id and
//     created_at minted at first insert only), insert-or-ignore the
//     declaration row, advance declared_at, and re-enable (enabled=1) iff the
//     labeler held zero declarations before this one, the row is
//     firehose-sourced, the row was disabled, and AutoSubscribe is set.
//   - delete: remove the declaration row, advance declared_at, and disable
//     (enabled=0) iff no declarations remain and the row is firehose-sourced
//     and enabled. Manual rows are never auto-disabled or auto-enabled.
//
// On any error the whole transaction rolls back: no registry mutation, no
// event row, no seq consumed, no declared_at change. Exactly-once identity is
// stored in applied_service_ops independently of event pruning, including for
// skipped ops; events.dedup_key is a redundant event-side record. CommitSeq 0
// disables replay dedup for direct/test callers.
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

	// Broadcast and head-seq callback still holding mu, matching the
	// PersistIngest contract.
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

// applyServiceOpTx runs the registry transition and the #service event
// persistence for one op inside tx.
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

		// Zero-to-one transition detection: count the labeler's declaration
		// rows BEFORE inserting this one.
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

		// Re-enable only on the zero-to-one transition, only for
		// firehose-sourced rows that are currently disabled, and only when
		// auto-subscribe is on. An operator-disabled row that already holds
		// a declaration stays disabled when a second collection is added or
		// an existing declaration is updated.
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
		// Skip when the relay never registered this declaration: no event, no
		// registry change. Idempotent on replay.
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

		// Auto-disable when the last declaration is gone. The WHERE clause
		// keeps manual rows enabled regardless.
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

	// declared_at advances on every applied declaration op (create, update,
	// and tracked delete). The labelers row exists in every path that reaches
	// here: create/update just upserted it, and a tracked delete implies a
	// labeler_declarations row whose foreign key points at it.
	if _, err := tx.ExecContext(ctx,
		`UPDATE labelers SET declared_at = ? WHERE did = ?`, now, op.DID); err != nil {
		return w, fmt.Errorf("failed to update declared_at: %w", err)
	}

	// The event: placeholder insert minting relay_seq, frame encode, update —
	// the same steps PersistIngest applies.
	w.seq, w.frame, err = persistServiceEventTx(ctx, tx, op, dedupKey)
	return w, err
}

// persistServiceEventTx inserts the events placeholder row (minting
// relay_seq), encodes the #service frame, and stores it.
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
