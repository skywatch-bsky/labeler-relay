// pattern: Functional Core

package firehose

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// LabelerServiceOp is a discovered create/update/delete of an app.bsky.labeler.service record.
type LabelerServiceOp struct {
	RepoDID string                // the labeler's DID
	Action  string                // "create" | "update" | "delete"
	Rkey    string                // record key (usually "self")
	Record  *bsky.LabelerService  // nil for delete
}

// ExtractLabelerServiceOps scans a commit's ops for app.bsky.labeler.service changes,
// loading record bytes from the commit CAR for create/update.
//
// Allocation-light: returns early when no labeler ops are present.
func ExtractLabelerServiceOps(commit *comatproto.SyncSubscribeRepos_Commit) ([]LabelerServiceOp, error) {
	if len(commit.Ops) == 0 {
		return nil, nil
	}

	// Quick scan for labeler ops before loading CAR.
	hasLabelerOps := false
	for _, op := range commit.Ops {
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) == 2 && parts[0] == "app.bsky.labeler.service" {
			hasLabelerOps = true
			break
		}
	}

	// Return early if no labeler ops—don't load CAR.
	if !hasLabelerOps {
		return nil, nil
	}

	// Load the repo from the CAR.
	_, repoObj, err := repo.LoadRepoFromCAR(nil, bytes.NewReader(commit.Blocks))
	if err != nil {
		return nil, fmt.Errorf("failed to load repo from CAR: %w", err)
	}

	var ops []LabelerServiceOp

	for _, op := range commit.Ops {
		// Split path on "/" to extract collection and rkey.
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) != 2 {
			continue
		}

		collection := parts[0]
		rkey := parts[1]

		// Only process labeler.service records.
		if collection != "app.bsky.labeler.service" {
			continue
		}

		var record *bsky.LabelerService

		// For create/update, load and unmarshal the record.
		if op.Action == "create" || op.Action == "update" {
			if op.Cid == nil {
				return nil, fmt.Errorf("create/update op missing CID for %s", op.Path)
			}

			// Load the record from the repo using the CID.
			// LexLink is a cid.Cid value, we need to cast it.
			cidVal := cid.Cid(*op.Cid)
			block, err := repoObj.RecordStore.Get(context.Background(), cidVal)
			if err != nil {
				return nil, fmt.Errorf("failed to get block for %s: %w", op.Path, err)
			}

			// Unmarshal CBOR into LabelerService.
			record = &bsky.LabelerService{}
			cr := cbg.NewCborReader(bytes.NewReader(block.RawData()))
			if err := record.UnmarshalCBOR(cr); err != nil {
				return nil, fmt.Errorf("failed to unmarshal LabelerService: %w", err)
			}
		}
		// For delete, record is nil.

		ops = append(ops, LabelerServiceOp{
			RepoDID: commit.Repo,
			Action:  op.Action,
			Rkey:    rkey,
			Record:  record,
		})
	}

	return ops, nil
}
