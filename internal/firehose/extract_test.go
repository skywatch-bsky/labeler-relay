package firehose

import (
	"bytes"
	"context"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// TestExtractLabelerDeclarationOpsFilters tests that only declaration
// collection ops are extracted. The test uses minimal CAR data because
// extraction returns early when no declaration ops are present, so the CAR is
// never loaded—only the path filtering guard is exercised.
func TestExtractLabelerDeclarationOpsFilters(t *testing.T) {
	t.Parallel()

	// Use minimal CAR that won't be loaded (the early-return guard filters ops before CAR load).
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

	ops, _, err := ExtractLabelerDeclarationOps(context.Background(), commit)
	require.NoError(t, err)
	require.Empty(t, ops, "non-declaration ops must be filtered")
}

// TestExtractLabelerDeclarationOpsAllCollections drives extraction through a
// REAL CAR for each of the three declaration collections: the op is extracted
// with its collection and the raw record bytes.
func TestExtractLabelerDeclarationOpsAllCollections(t *testing.T) {
	t.Parallel()

	for _, collection := range []string{
		"app.bsky.labeler.service",
		"town.delve.labeler.service",
		"so.sprk.labeler.service",
	} {
		t.Run(collection, func(t *testing.T) {
			t.Parallel()
			const did = "did:plc:extract-real"
			svc := &bsky.LabelerService{
				LexiconTypeID: collection,
				CreatedAt:     "2026-06-02T12:00:00Z",
				Policies: &bsky.LabelerDefs_LabelerPolicies{
					LabelValues: []*string{strptr("rude")},
				},
			}
			car, recordCID := buildDeclarationCAR(t, did, collection, "self", svc)
			lexCID := util.LexLink(recordCID)

			commit := &comatproto.SyncSubscribeRepos_Commit{
				Repo:   did,
				Blocks: car,
				Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
					// A non-labeler op that must be ignored (its CID need not resolve).
					{Action: "create", Path: "app.bsky.feed.post/p1", Cid: testCID("post1")},
					// The real declaration op, resolvable from the CAR by its content CID.
					{Action: "create", Path: collection + "/self", Cid: &lexCID},
				},
			}

			ops, _, err := ExtractLabelerDeclarationOps(context.Background(), commit)
			require.NoError(t, err)
			require.Len(t, ops, 1)

			got := ops[0]
			require.Equal(t, did, got.RepoDID)
			require.Equal(t, "self", got.Rkey)
			require.Equal(t, "create", got.Action)
			require.Equal(t, collection, got.Collection)
			// The record is the raw block bytes: re-marshaling the source
			// record must reproduce them exactly (byte-faithful passthrough).
			require.NotNil(t, got.Record)
			recCBOR := mustMarshalCBOR(t, svc)
			require.Equal(t, recCBOR, got.Record)
		})
	}
}

// TestExtractLabelerDeclarationOpsDelete asserts a delete op yields a
// LabelerDeclarationOp with a nil Record (no CAR record to load).
func TestExtractLabelerDeclarationOpsDelete(t *testing.T) {
	t.Parallel()

	const did = "did:plc:extract-delete"
	// A delete commit still carries a (here minimal but valid) CAR; build one with a
	// throwaway record so LoadRepoFromCAR succeeds, then reference a delete op.
	car, _ := buildDeclarationCAR(t, did, "app.bsky.labeler.service", "self", &bsky.LabelerService{
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
	ops, _, err := ExtractLabelerDeclarationOps(context.Background(), commit)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.Equal(t, "delete", ops[0].Action)
	require.Equal(t, "app.bsky.labeler.service", ops[0].Collection)
	require.Nil(t, ops[0].Record)
}

func TestExtractLabelerDeclarationOpsEmptyOps(t *testing.T) {
	t.Parallel()

	carData := []byte{1, 0}

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   "did:plc:empty",
		Blocks: carData,
		Ops:    []*comatproto.SyncSubscribeRepos_RepoOp{},
	}

	ops, _, err := ExtractLabelerDeclarationOps(context.Background(), commit)
	require.NoError(t, err)
	require.Empty(t, ops)
}

// TestExtractLabelerDeclarationOpsPathParsing asserts that non-self record keys
// are ignored because the declaration schemas are self-keyed.
func TestExtractLabelerDeclarationOpsPathParsing(t *testing.T) {
	t.Parallel()

	const did = "did:plc:rkey-parse"
	svc := &bsky.LabelerService{LexiconTypeID: "app.bsky.labeler.service", CreatedAt: "2026-06-02T00:00:00Z"}
	car, recordCID := buildDeclarationCAR(t, did, "app.bsky.labeler.service", "custom", svc)
	lexCID := util.LexLink(recordCID)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "create", Path: "app.bsky.labeler.service/custom", Cid: &lexCID},
		},
	}

	ops, skipped, err := ExtractLabelerDeclarationOps(context.Background(), commit)
	require.NoError(t, err)
	require.Empty(t, skipped)
	require.Empty(t, ops)
}

// TestExtractLabelerDeclarationOpsUnknownFieldsRecord asserts a record
// carrying unknown/extra fields (hand-encoded, not schema-shaped) is passed
// through byte-faithfully — extraction validates well-formedness, not
// lexicon conformance.
func TestExtractLabelerDeclarationOpsUnknownFieldsRecord(t *testing.T) {
	t.Parallel()

	const did = "did:plc:extract-unknown"
	// {"futureField": 7, "$type": "town.delve.labeler.service"}
	record := []byte{
		0xa2,
		0x6b, 'f', 'u', 't', 'u', 'r', 'e', 'F', 'i', 'e', 'l', 'd', // "futureField"
		0x07,
		0x65, '$', 't', 'y', 'p', 'e', // "$type"
		0x78, 0x1a, // text(26): "town.delve.labeler.service"
		't', 'o', 'w', 'n', '.', 'd', 'e', 'l', 'v', 'e', '.', 'l', 'a', 'b', 'e', 'l', 'e', 'r', '.', 's', 'e', 'r', 'v', 'i', 'c', 'e',
	}
	car, recordCID := buildDeclarationCARFromBytes(t, did, "town.delve.labeler.service", "self", record)
	lexCID := util.LexLink(recordCID)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "create", Path: "town.delve.labeler.service/self", Cid: &lexCID},
		},
	}

	ops, _, err := ExtractLabelerDeclarationOps(context.Background(), commit)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.Equal(t, record, ops[0].Record)
}

// TestExtractRejectsMalformedRecord verifies AC.13's extraction-level guard:
// a CAR whose record block is malformed CBOR (a map claiming two pairs with
// one present) makes ExtractLabelerDeclarationOps return an error.
func TestExtractRejectsMalformedRecord(t *testing.T) {
	t.Parallel()

	const did = "did:plc:extract-malformed"
	// Well-formed enough for the CAR plumbing, but rejected by the record
	// validation: map(2) with a single pair.
	malformed := []byte{0xa2, 0x61, 'a', 0x01}
	car, recordCID := buildDeclarationCARFromBytes(t, did, "app.bsky.labeler.service", "self", malformed)
	lexCID := util.LexLink(recordCID)

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "create", Path: "app.bsky.labeler.service/self", Cid: &lexCID},
		},
	}

	ops, skipped, err := ExtractLabelerDeclarationOps(context.Background(), commit)
	require.NoError(t, err)
	require.Empty(t, ops)
	require.Len(t, skipped, 1)
	require.Equal(t, "app.bsky.labeler.service/self", skipped[0].Path)
	require.Error(t, skipped[0].Err)
	require.Contains(t, skipped[0].Err.Error(), "invalid declaration record")
}

// mustMarshalCBOR marshals a record to CBOR bytes for byte-equality checks.
func mustMarshalCBOR(t *testing.T, svc *bsky.LabelerService) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := svc.MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return buf.Bytes()
}

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

// Real CAR fixtures are built by buildDeclarationCAR in testhelpers_test.go.
