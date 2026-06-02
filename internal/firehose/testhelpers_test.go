package firehose

import (
	"bytes"
	"context"
	"testing"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	car "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"
)

// buildLabelerServiceCAR builds a REAL atproto repo CAR containing one
// app.bsky.labeler.service record at "app.bsky.labeler.service/<rkey>", plus a
// signed commit and the MST nodes that reference it — exactly the structure
// repo.LoadRepoFromCAR expects. It returns the CAR bytes (suitable for a firehose
// commit's Blocks field) and the real content CID of the record (suitable for the
// op's Cid). This is the genuine fixture the extract/watcher tests decode through
// indigo's real CAR-loading path.
func buildLabelerServiceCAR(t *testing.T, did, rkey string, svc *bsky.LabelerService) ([]byte, cid.Cid) {
	t.Helper()
	ctx := context.Background()
	// dag-cbor + sha2-256 is the codec indigo uses for repo blocks.
	prefix := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256)

	// 1. Encode the record and compute its real content CID.
	var recBuf bytes.Buffer
	if err := svc.MarshalCBOR(&recBuf); err != nil {
		t.Fatalf("marshal labeler service: %v", err)
	}
	recordCID, err := prefix.Sum(recBuf.Bytes())
	if err != nil {
		t.Fatalf("compute record cid: %v", err)
	}
	recBlock, err := blocks.NewBlockWithCid(recBuf.Bytes(), recordCID)
	if err != nil {
		t.Fatalf("record block: %v", err)
	}

	// 2. Build the MST referencing the record by its CID, flush nodes to a store.
	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	if err := bs.Put(ctx, recBlock); err != nil {
		t.Fatalf("put record block: %v", err)
	}
	tree := mst.NewEmptyTree()
	// Insert mutates the tree in place and returns the previous value (unused here).
	if _, err := tree.Insert([]byte("app.bsky.labeler.service/"+rkey), recordCID); err != nil {
		t.Fatalf("mst insert: %v", err)
	}
	mstRootPtr, err := tree.WriteDiffBlocks(ctx, bs)
	if err != nil {
		t.Fatalf("write mst blocks: %v", err)
	}

	// 3. Build and sign a v3 commit pointing at the MST root.
	priv, err := atcrypto.GeneratePrivateKeyP256()
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	clk := syntax.NewTIDClock(0)
	commit := repo.Commit{
		DID:     did,
		Version: repo.ATPROTO_REPO_VERSION,
		Data:    *mstRootPtr,
		Rev:     string(clk.Next()),
	}
	if err := commit.Sign(priv); err != nil {
		t.Fatalf("sign commit: %v", err)
	}
	var commitBuf bytes.Buffer
	if err := commit.MarshalCBOR(&commitBuf); err != nil {
		t.Fatalf("marshal commit: %v", err)
	}
	commitCID, err := prefix.Sum(commitBuf.Bytes())
	if err != nil {
		t.Fatalf("commit cid: %v", err)
	}
	commitBlock, err := blocks.NewBlockWithCid(commitBuf.Bytes(), commitCID)
	if err != nil {
		t.Fatalf("commit block: %v", err)
	}
	if err := bs.Put(ctx, commitBlock); err != nil {
		t.Fatalf("put commit block: %v", err)
	}

	// 4. Serialize a CAR v1: header rooted at the commit, then every block.
	//
	// go-ipfs-blockstore keys by multihash only and reconstructs CIDs with the
	// RAW codec (0x55) in AllKeysChan, which would write frames whose CIDs don't
	// match the dag-cbor CIDs referenced by the commit/MST. Every block in an
	// atproto repo is dag-cbor, so we rebuild each enumerated CID with the
	// dag-cbor codec before writing the frame, and read the data back by multihash.
	var out bytes.Buffer
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{commitCID}, Version: 1}, &out); err != nil {
		t.Fatalf("car header: %v", err)
	}
	keys, err := bs.AllKeysChan(ctx)
	if err != nil {
		t.Fatalf("all keys: %v", err)
	}
	for c := range keys {
		dagCID := cid.NewCidV1(cid.DagCBOR, c.Hash())
		blk, err := bs.Get(ctx, dagCID)
		if err != nil {
			t.Fatalf("get block %s: %v", dagCID, err)
		}
		if err := carutil.LdWrite(&out, dagCID.Bytes(), blk.RawData()); err != nil {
			t.Fatalf("car write block: %v", err)
		}
	}
	return out.Bytes(), recordCID
}
