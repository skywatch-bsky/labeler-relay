package store

import (
	"context"
	"path/filepath"
	"testing"
)

// TestListWithDeclarationsSingleSnapshot pins the single-snapshot contract
// of the combined reads: a write that disables a labeler and removes its
// declaration, COMMITTED between the labeler query and the declaration query
// (via the testBetweenSnapshotReads seam, from a separate connection), must
// not leak into the result — both halves of one call come from one read
// snapshot. Two independent queries would observe the interleaved write
// between them and return a torn view (an enabled labeler with no
// declarations), which is exactly what this test rejects.
func TestListWithDeclarationsSingleSnapshot(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "snapshot.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()
	reg := NewLabelerRegistry(s)
	persist := NewLabelPersist(s)

	const did = "did:plc:snapshot"
	if _, err := persist.ApplyServiceOp(ctx, ServiceOp{
		Op: "create", DID: did, Collection: "app.bsky.labeler.service", Rkey: "self",
		Record: []byte{0xa1, 0x61, 'a', 0x01}, Endpoint: "snapshot.example.com", AutoSubscribe: true,
		CommitSeq: 1,
	}); err != nil {
		t.Fatalf("seed declaration op: %v", err)
	}

	// The seam fires between the two queries of each snapshot read: commit
	// a write from a separate connection that flips the labeler to disabled
	// and removes its declaration. A separate-reads implementation would
	// pick the write up between its queries and tear the view.
	testBetweenSnapshotReads = func() {
		wtx, err := s.DB().BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin interleaved write: %v", err)
		}
		defer wtx.Rollback()
		if _, err := wtx.ExecContext(ctx, `UPDATE labelers SET enabled = 0 WHERE did = ?`, did); err != nil {
			t.Fatalf("interleaved disable: %v", err)
		}
		if _, err := wtx.ExecContext(ctx, `DELETE FROM labeler_declarations WHERE did = ?`, did); err != nil {
			t.Fatalf("interleaved declaration removal: %v", err)
		}
		if err := wtx.Commit(); err != nil {
			t.Fatalf("interleaved write commit: %v", err)
		}
	}
	defer func() { testBetweenSnapshotReads = nil }()

	labelers, declarations, err := reg.ListWithDeclarations(ctx)
	if err != nil {
		t.Fatalf("ListWithDeclarations: %v", err)
	}
	if len(labelers) != 1 || !labelers[0].Enabled {
		t.Fatalf("snapshot read: labelers=%+v, want the pre-write enabled row", labelers)
	}
	if got := declarations[did]; len(got) != 1 || got[0] != "app.bsky.labeler.service" {
		t.Fatalf("snapshot read: declarations=%v, want the pre-write declaration — the interleaved write must not tear the snapshot", got)
	}

	// Each call takes its own snapshot: by now the interleaved write is
	// committed, so GetWithDeclarations must observe the post-write state
	// as a whole (disabled AND undeclared — never a mix).
	row, decs, found, err := reg.GetWithDeclarations(ctx, did)
	if err != nil || !found {
		t.Fatalf("GetWithDeclarations: found=%v err=%v", found, err)
	}
	if row.Enabled {
		t.Fatalf("post-write Get: row=%+v, want the disabled row", row)
	}
	if len(decs) != 0 {
		t.Fatalf("post-write Get: decs=%v, want none", decs)
	}
}
