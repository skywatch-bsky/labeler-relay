// pattern: Imperative Shell
// Tests for LabelPersist.ApplyServiceOp: the declaration registry transition
// and the #service event commit atomically — an injected store failure rolls
// both back, and the replayed op completes with no duplicate state.

package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scarndp/labeler-relay/internal/store"
)

// blockEventsInstall installs (or removes, when block is false) a trigger
// that aborts every INSERT into events, failing an ApplyServiceOp at the
// event-write step — after the registry transition has already run inside
// the same transaction.
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

// TestApplyServiceOpAtomicRollback verifies AC.12(a): an injected store-level
// failure leaves no registry mutation, no declaration row, no event row, no
// consumed relay_seq, and no declared_at change; the replayed op then
// completes. Both the create and the delete transition paths are exercised.
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

	// --- Create-path failure: the upsert, declaration, and declared_at all
	// roll back with the failed event write. ---
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

	// --- Replay completes: one event, row registered and enabled, the
	// declaration tracked, declared_at set. ---
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

	// --- Delete-path failure: the declaration removal, the disable, and the
	// declared_at advance all roll back with the failed event write. ---
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

	// --- Replay the delete: exactly one delete event, disabled, undeclared. ---
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

	// A replayed delete of the now-untracked declaration is a skip: no
	// event, no state change.
	res, err = persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "delete", DID: did, Collection: collection,
	})
	require.NoError(t, err)
	require.True(t, res.Skipped, "deleting an untracked declaration must skip")
	head, err = persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), head, "a skipped op writes no event")
}

// TestApplyServiceOpDedupsReplay verifies the exactly-once contract across
// replays: an op whose event already exists (same upstream commit seq, DID,
// collection, rkey, and action) is skipped with no writes at all — no second
// event, no relay seq, not even a declared_at advance — while a different
// upstream op still applies.
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

	// The identical upstream op replayed: skipped, nothing written.
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
