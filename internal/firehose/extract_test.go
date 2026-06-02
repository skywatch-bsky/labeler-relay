package firehose

import (
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// TestExtractLabelerServiceOpsFilters tests that only app.bsky.labeler.service ops are filtered out.
// This test uses a mock CAR (not a valid one) because it only tests path filtering,
// not actual record decoding.
func TestExtractLabelerServiceOpsFilters(t *testing.T) {
	// Use a minimal CAR that won't be loaded (the op is filtered before loading).
	carData := []byte{1, 0} // minimal CAR header: version 1, 0 roots

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   "did:plc:labeler123",
		Blocks: carData,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{
				Action: "create",
				Path:   "app.bsky.feed.post/post123", // Non-labeler op
				Cid:    testCID("post123"),
			},
		},
	}

	ops, err := ExtractLabelerServiceOps(commit)
	if err != nil {
		t.Fatalf("ExtractLabelerServiceOps failed: %v", err)
	}

	if len(ops) != 0 {
		t.Fatalf("expected 0 ops (non-labeler posts filtered), got %d", len(ops))
	}
}

// TestExtractLabelerServiceOpsDecodesRecord drives extraction through a REAL CAR:
// a commit carrying one app.bsky.labeler.service record alongside a non-labeler op.
// It asserts the labeler op is extracted and its record is decoded with fields intact.
func TestExtractLabelerServiceOpsDecodesRecord(t *testing.T) {
	const did = "did:plc:extract-real"
	svc := &bsky.LabelerService{
		LexiconTypeID: "app.bsky.labeler.service",
		CreatedAt:     "2026-06-02T12:00:00Z",
		Policies: &bsky.LabelerDefs_LabelerPolicies{
			LabelValues: []*string{strptr("rude")},
		},
	}
	car, recordCID := buildLabelerServiceCAR(t, did, "self", svc)
	lexCID := util.LexLink(recordCID)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			// A non-labeler op that must be ignored (its CID need not resolve).
			{Action: "create", Path: "app.bsky.feed.post/p1", Cid: testCID("post1")},
			// The real labeler.service op, resolvable from the CAR by its content CID.
			{Action: "create", Path: "app.bsky.labeler.service/self", Cid: &lexCID},
		},
	}

	ops, err := ExtractLabelerServiceOps(commit)
	if err != nil {
		t.Fatalf("ExtractLabelerServiceOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected exactly 1 labeler op, got %d", len(ops))
	}
	got := ops[0]
	if got.RepoDID != did {
		t.Errorf("RepoDID = %q, want %q", got.RepoDID, did)
	}
	if got.Rkey != "self" {
		t.Errorf("Rkey = %q, want self", got.Rkey)
	}
	if got.Action != "create" {
		t.Errorf("Action = %q, want create", got.Action)
	}
	if got.Record == nil {
		t.Fatal("expected decoded record, got nil")
	}
	if got.Record.CreatedAt != "2026-06-02T12:00:00Z" {
		t.Errorf("record CreatedAt = %q, want 2026-06-02T12:00:00Z", got.Record.CreatedAt)
	}
	if got.Record.Policies == nil || len(got.Record.Policies.LabelValues) != 1 ||
		*got.Record.Policies.LabelValues[0] != "rude" {
		t.Errorf("record Policies not decoded faithfully: %+v", got.Record.Policies)
	}
}

// TestExtractLabelerServiceOpsDelete asserts a delete op yields a LabelerServiceOp
// with a nil Record (no CAR record to load).
func TestExtractLabelerServiceOpsDelete(t *testing.T) {
	const did = "did:plc:extract-delete"
	// A delete commit still carries a (here minimal but valid) CAR; build one with a
	// throwaway record so LoadRepoFromCAR succeeds, then reference a delete op.
	car, _ := buildLabelerServiceCAR(t, did, "self", &bsky.LabelerService{
		LexiconTypeID: "app.bsky.labeler.service",
		CreatedAt:     "2026-06-02T00:00:00Z",
	})
	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "delete", Path: "app.bsky.labeler.service/self", Cid: nil},
		},
	}
	ops, err := ExtractLabelerServiceOps(commit)
	if err != nil {
		t.Fatalf("ExtractLabelerServiceOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 delete op, got %d", len(ops))
	}
	if ops[0].Action != "delete" || ops[0].Record != nil {
		t.Errorf("delete op = %+v, want Action=delete Record=nil", ops[0])
	}
}

// TestExtractLabelerServiceOpsEmptyOps tests that commits with no ops return empty slice.
func TestExtractLabelerServiceOpsEmptyOps(t *testing.T) {
	carData := []byte{1, 0}

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   "did:plc:empty",
		Blocks: carData,
		Ops:    []*comatproto.SyncSubscribeRepos_RepoOp{},
	}

	ops, err := ExtractLabelerServiceOps(commit)
	if err != nil {
		t.Fatalf("ExtractLabelerServiceOps failed: %v", err)
	}

	if len(ops) != 0 {
		t.Fatalf("expected 0 ops for empty commit, got %d", len(ops))
	}
}

// TestExtractLabelerServiceOpsPathParsing asserts the rkey is parsed from the op
// Path through a real CAR-backed extraction (not a non-"self" rkey).
func TestExtractLabelerServiceOpsPathParsing(t *testing.T) {
	const did = "did:plc:rkey-parse"
	svc := &bsky.LabelerService{LexiconTypeID: "app.bsky.labeler.service", CreatedAt: "2026-06-02T00:00:00Z"}
	car, recordCID := buildLabelerServiceCAR(t, did, "custom", svc)
	lexCID := util.LexLink(recordCID)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "create", Path: "app.bsky.labeler.service/custom", Cid: &lexCID},
		},
	}

	ops, err := ExtractLabelerServiceOps(commit)
	if err != nil {
		t.Fatalf("ExtractLabelerServiceOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 labeler op, got %d", len(ops))
	}
	if ops[0].Rkey != "custom" {
		t.Errorf("rkey = %q, want custom", ops[0].Rkey)
	}
}



// Helper: create a dummy CID for testing by hashing input.
// LexLink is a type alias for cid.Cid.
func testCID(id string) *util.LexLink {
	// Create a CID from a hash of the input string.
	// This is just for test fixture purposes.
	mh, err := multihash.Sum([]byte(id), multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	cidVal := cid.NewCidV1(cid.DagCBOR, mh)
	lex := util.LexLink(cidVal)
	return &lex
}

// Real CAR fixtures are built by buildLabelerServiceCAR in testhelpers_test.go.
