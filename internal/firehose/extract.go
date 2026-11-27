// pattern: Functional Core

package firehose

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/ipfs/go-cid"

	"github.com/scarndp/labeler-relay/internal/dagcbor"
)

// declarationCollections recognizes app.bsky, town.delve, and so.sprk labeler
// declarations. All three schemas require the literal "self" record key.
var declarationCollections = map[string]bool{
	"app.bsky.labeler.service":   true,
	"town.delve.labeler.service": true,
	"so.sprk.labeler.service":    true,
}

// LabelerDeclarationOp is a discovered create/update/delete of a labeler
// service declaration record.
type LabelerDeclarationOp struct {
	RepoDID    string // the labeler's DID
	Action     string // "create" | "update" | "delete"
	Rkey       string // always "self"
	Collection string // declaration collection NSID
	Record     []byte // raw DAG-CBOR record block bytes; nil for delete
}

// SkippedDeclaration reports a declaration op skipped because its record is
// malformed or over the decode budget, or because its CID or block is missing.
// CAR load failures remain hard errors.
type SkippedDeclaration struct {
	Path string
	Err  error
}

// ExtractLabelerDeclarationOps extracts self-keyed declaration changes from
// the three declaration collections. Other record keys are ignored.
//
// Create/update records remain raw block bytes: typed decoding would prevent
// byte-faithful passthrough. ValidateMap requires one complete DAG-CBOR map
// with no trailing bytes, but does not enforce lexicon schemas.
// Malformed or oversized records, missing CIDs, and missing blocks enter skipped
// without rejecting valid siblings. CAR load failures return an error.
//
// No matching ops means no CAR load. Delete-only commits still load the CAR
// to validate the repository container.
func ExtractLabelerDeclarationOps(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit) ([]LabelerDeclarationOp, []SkippedDeclaration, error) {
	if len(commit.Ops) == 0 {
		return nil, nil, nil
	}

	hasDeclarationOps := false
	for _, op := range commit.Ops {
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) == 2 && declarationCollections[parts[0]] && parts[1] == "self" {
			hasDeclarationOps = true
			break
		}
	}

	if !hasDeclarationOps {
		return nil, nil, nil
	}

	_, repoObj, err := repo.LoadRepoFromCAR(nil, bytes.NewReader(commit.Blocks))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load repo from CAR: %w", err)
	}

	var ops []LabelerDeclarationOp
	var skipped []SkippedDeclaration

	for _, op := range commit.Ops {
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) != 2 {
			continue
		}

		collection := parts[0]
		rkey := parts[1]

		if !declarationCollections[collection] || rkey != "self" {
			continue
		}

		var record []byte

		if op.Action == "create" || op.Action == "update" {
			if op.Cid == nil {
				skipped = append(skipped, SkippedDeclaration{Path: op.Path, Err: fmt.Errorf("create/update op missing CID")})
				continue
			}

			// LexLink is a cid.Cid value, we need to cast it.
			cidVal := cid.Cid(*op.Cid)
			block, err := repoObj.RecordStore.Get(ctx, cidVal)
			if err != nil {
				skipped = append(skipped, SkippedDeclaration{Path: op.Path, Err: fmt.Errorf("failed to get block: %w", err)})
				continue
			}

			record = block.RawData()
			if len(record) > dagcbor.MaxScanBytes {
				skipped = append(skipped, SkippedDeclaration{Path: op.Path, Err: fmt.Errorf("declaration record exceeds %d byte decode budget", dagcbor.MaxScanBytes)})
				continue
			}
			if err := dagcbor.ValidateMap(record); err != nil {
				skipped = append(skipped, SkippedDeclaration{Path: op.Path, Err: fmt.Errorf("invalid declaration record: %w", err)})
				continue
			}
		}

		ops = append(ops, LabelerDeclarationOp{
			RepoDID:    commit.Repo,
			Action:     op.Action,
			Rkey:       rkey,
			Collection: collection,
			Record:     record,
		})
	}

	return ops, skipped, nil
}
