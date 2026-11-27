package store

import (
	"context"
	"path/filepath"
	"testing"
)

// Independent queries could return an enabled labeler after a concurrent write removes its declaration.
// Both combined read methods must use one snapshot.
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

	// Commit on a separate connection between the two read queries to expose inconsistent snapshots.
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

	// Restore the fixture so GetWithDeclarations must also resist the concurrent write.
	if _, err := s.DB().ExecContext(ctx, `UPDATE labelers SET enabled = 1 WHERE did = ?`, did); err != nil {
		t.Fatalf("reseed enable: %v", err)
	}
	if _, err := s.DB().ExecContext(ctx, `INSERT INTO labeler_declarations(did, collection) VALUES (?, 'app.bsky.labeler.service')`, did); err != nil {
		t.Fatalf("reseed declaration: %v", err)
	}
	row, decs, found, err := reg.GetWithDeclarations(ctx, did)
	if err != nil || !found {
		t.Fatalf("GetWithDeclarations: found=%v err=%v", found, err)
	}
	if !row.Enabled || len(decs) != 1 || decs[0] != "app.bsky.labeler.service" {
		t.Fatalf("snapshot Get: row=%+v decs=%v, want the pre-write enabled row and declaration — the interleaved write must not tear the snapshot", row, decs)
	}

	testBetweenSnapshotReads = nil
	row, decs, _, err = reg.GetWithDeclarations(ctx, did)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if row.Enabled || len(decs) != 0 {
		t.Fatalf("final Get: row=%+v decs=%v, want the post-write disabled row with no declarations", row, decs)
	}
}
