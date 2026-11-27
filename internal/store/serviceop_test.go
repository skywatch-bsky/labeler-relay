// pattern: Imperative Shell

package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scarndp/labeler-relay/internal/store"
)

// Fail the event insert after the registry transition to test rollback across both writes.
func blockEventsInstall(t *testing.T, s *store.Store, block bool) {
	t.Helper()
	stmt := `DROP TRIGGER IF EXISTS block_events`
	if block {
		stmt = `CREATE TRIGGER block_events BEFORE INSERT ON events
			BEGIN
				SELECT RAISE(ABORT, 'test: events insert blocked');
			END;`
	}
	_, err := s.DB().Exec(stmt)
	require.NoError(t, err)
}

// An event-write failure must roll back the registry transition and sequence allocation, allowing a complete retry.
func TestApplyServiceOpAtomicRollback(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "atomic.db"))
	require.NoError(t, err)
	defer s.Close()
	reg := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	const did = "did:plc:atomic"
	const collection = "app.bsky.labeler.service"
	record := []byte{0xa1, 0x61, 'a', 0x01}

	blockEventsInstall(t, s, true)
	_, err = persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "create", DID: did, Collection: collection,
		Record: record, Endpoint: "atomic.example.com", AutoSubscribe: true,
	})
	require.Error(t, err, "the op must fail while the event write is blocked")

	_, found, err := reg.Get(ctx, did)
	require.NoError(t, err)
	require.False(t, found, "the labeler upsert must roll back with the event write")

	decs, err := reg.DeclarationsFor(ctx, did)
	require.NoError(t, err)
	require.Empty(t, decs, "the declaration insert must roll back")

	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Zero(t, head, "a rolled-back op must consume no relay seq")

	blockEventsInstall(t, s, false)
	res, err := persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "create", DID: did, Collection: collection,
		Record: record, Endpoint: "atomic.example.com", AutoSubscribe: true,
	})
	require.NoError(t, err)
	require.False(t, res.Skipped)
	require.Equal(t, int64(1), res.Seq)

	row, found, err := reg.Get(ctx, did)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, row.Enabled)
	require.Equal(t, "atomic.example.com", row.Endpoint)
	require.NotNil(t, row.DeclaredAt, "the applied create sets declared_at")
	declaredBefore := *row.DeclaredAt

	decs, err = reg.DeclarationsFor(ctx, did)
	require.NoError(t, err)
	require.Equal(t, []string{collection}, decs)

	blockEventsInstall(t, s, true)
	_, err = persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "delete", DID: did, Collection: collection,
	})
	require.Error(t, err, "the delete op must fail while the event write is blocked")

	row, found, err = reg.Get(ctx, did)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, row.Enabled, "the disable must roll back with the event write")
	require.NotNil(t, row.DeclaredAt)
	require.Equal(t, declaredBefore, *row.DeclaredAt, "declared_at must not move for a failed op")

	decs, err = reg.DeclarationsFor(ctx, did)
	require.NoError(t, err)
	require.Equal(t, []string{collection}, decs, "the declaration removal must roll back")

	head, err = persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), head, "the failed delete must consume no relay seq")

	blockEventsInstall(t, s, false)
	res, err = persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "delete", DID: did, Collection: collection,
	})
	require.NoError(t, err)
	require.False(t, res.Skipped)
	require.True(t, res.EnabledChanged, "replaying the last-declaration delete disables the labeler")
	require.Equal(t, int64(2), res.Seq)

	row, found, err = reg.Get(ctx, did)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, row.Enabled)

	decs, err = reg.DeclarationsFor(ctx, did)
	require.NoError(t, err)
	require.Empty(t, decs)

	head, err = persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), head, "exactly one event per applied op: one create, one delete")

	res, err = persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "delete", DID: did, Collection: collection,
	})
	require.NoError(t, err)
	require.True(t, res.Skipped, "deleting an untracked declaration must skip")
	head, err = persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), head, "a skipped op writes no event")
}

// Replayed upstream operations must not allocate another sequence or advance declared_at.
func TestApplyServiceOpDedupsReplay(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "dedup.db"))
	require.NoError(t, err)
	defer s.Close()
	reg := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	const did = "did:plc:dedup"
	const collection = "app.bsky.labeler.service"
	record := []byte{0xa1, 0x61, 'a', 0x01}

	op := store.ServiceOp{
		Op: "create", DID: did, Collection: collection, Rkey: "self",
		Record: record, Endpoint: "dedup.example.com", AutoSubscribe: true,
		CommitSeq: 7,
	}

	res, err := persist.ApplyServiceOp(ctx, op)
	require.NoError(t, err)
	require.False(t, res.Skipped)
	require.Equal(t, int64(1), res.Seq)

	row, found, err := reg.Get(ctx, did)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, row.DeclaredAt)
	declaredBefore := *row.DeclaredAt

	res, err = persist.ApplyServiceOp(ctx, op)
	require.NoError(t, err)
	require.True(t, res.Skipped, "a replayed upstream op must be deduplicated")
	require.Zero(t, res.Seq)

	var events int
	require.NoError(t, s.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE kind='service' AND labeler_did=?`, did).Scan(&events))
	require.Equal(t, 1, events, "the replay must not insert a second event")

	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), head, "the replay must consume no relay seq")

	row, found, err = reg.Get(ctx, did)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, row.Enabled)
	require.NotNil(t, row.DeclaredAt)
	require.Equal(t, declaredBefore, *row.DeclaredAt, "a deduplicated replay must not advance declared_at")

	// A different upstream op still applies: the dedup key includes the
	// commit sequence and the collection.
	other := op
	other.CommitSeq = 8
	other.Collection = "town.delve.labeler.service"
	res, err = persist.ApplyServiceOp(ctx, other)
	require.NoError(t, err)
	require.False(t, res.Skipped)
	require.Equal(t, int64(2), res.Seq)
}

// Reject missing record bytes before a frame-marshal failure can obscure the input error.
func TestApplyServiceOpRejectsEmptyRecord(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "empty.db"))
	require.NoError(t, err)
	defer s.Close()
	persist := store.NewLabelPersist(s)

	_, err = persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "create", DID: "did:plc:empty", Collection: "app.bsky.labeler.service",
		Record: nil, Endpoint: "empty.example.com",
	})
	require.ErrorContains(t, err, "requires record bytes")

	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Zero(t, head)
}

// Remember skipped deletes so a stale replay cannot delete a declaration created afterward.
func TestApplyServiceOpRecordsSkippedDeleteIdentity(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "skipid.db"))
	require.NoError(t, err)
	defer s.Close()
	reg := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	const did = "did:plc:skipid"
	deleteOp := store.ServiceOp{
		Op: "delete", DID: did, Collection: "app.bsky.labeler.service", Rkey: "self", CommitSeq: 11,
	}

	res, err := persist.ApplyServiceOp(ctx, deleteOp)
	require.NoError(t, err)
	require.True(t, res.Skipped)
	require.Zero(t, res.Seq)

	var ids int
	require.NoError(t, s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM applied_service_ops`).Scan(&ids))
	require.Equal(t, 1, ids, "the skipped delete must record its replay identity")

	res, err = persist.ApplyServiceOp(ctx, deleteOp)
	require.NoError(t, err)
	require.True(t, res.Skipped)
	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Zero(t, head)

	_, found, err := reg.Get(ctx, did)
	require.NoError(t, err)
	require.False(t, found, "an untracked delete must never register a labeler")
}

func TestApplyServiceOpCommitSeqZeroDisablesDedup(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "zeroseq.db"))
	require.NoError(t, err)
	defer s.Close()
	persist := store.NewLabelPersist(s)

	const did = "did:plc:zeroseq"
	op := store.ServiceOp{
		Op: "create", DID: did, Collection: "app.bsky.labeler.service", Rkey: "self",
		Record: []byte{0xa1, 0x61, 'a', 0x01}, Endpoint: "zero.example.com", AutoSubscribe: true,
	}

	res, err := persist.ApplyServiceOp(ctx, op)
	require.NoError(t, err)
	require.False(t, res.Skipped)
	require.Equal(t, int64(1), res.Seq)

	res, err = persist.ApplyServiceOp(ctx, op)
	require.NoError(t, err)
	require.False(t, res.Skipped, "CommitSeq 0 must disable replay dedup")
	require.Equal(t, int64(2), res.Seq)

	var ids int
	require.NoError(t, s.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM applied_service_ops`).Scan(&ids))
	require.Zero(t, ids, "no identity row is written for a zero CommitSeq op")
}
