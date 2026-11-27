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

// declarationCollections is the set of labeler service declaration record
// collections the watcher recognizes: a labeler is discovered by declaring a
// record in any of these. The three schemas are structurally identical
// (self-keyed records with createdAt and policies), differing only in the
// namespace of their policies ref.
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
	Rkey       string // record key (usually "self")
	Collection string // declaration collection NSID
	Record     []byte // raw DAG-CBOR record block bytes; nil for delete
}

// ExtractLabelerDeclarationOps scans a commit's ops for labeler service
// declaration changes in any of the declaration collections, loading record
// bytes from the commit CAR for create/update.
//
// Records are kept as raw block bytes (no typed unmarshal) — the relay passes
// them through byte-faithfully. Each create/update record is validated as one
// complete, well-formed DAG-CBOR map with no trailing bytes; lexicon-schema
// conformance of the record's contents is intentionally NOT validated. An
// invalid record fails extraction with an error, which the watcher logs and
// skips.
//
// Allocation-light: returns early when no declaration ops are present.
func ExtractLabelerDeclarationOps(ctx context.Context, commit *comatproto.SyncSubscribeRepos_Commit) ([]LabelerDeclarationOp, error) {
	if len(commit.Ops) == 0 {
		return nil, nil
	}

	hasDeclarationOps := false
	for _, op := range commit.Ops {
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) == 2 && declarationCollections[parts[0]] {
			hasDeclarationOps = true
			break
		}
	}

	if !hasDeclarationOps {
		return nil, nil
	}

	_, repoObj, err := repo.LoadRepoFromCAR(nil, bytes.NewReader(commit.Blocks))
	if err != nil {
		return nil, fmt.Errorf("failed to load repo from CAR: %w", err)
	}

	var ops []LabelerDeclarationOp

	for _, op := range commit.Ops {
		parts := strings.SplitN(op.Path, "/", 2)
		if len(parts) != 2 {
			continue
		}

		collection := parts[0]
		rkey := parts[1]

		if !declarationCollections[collection] {
			continue
		}

		var record []byte

		if op.Action == "create" || op.Action == "update" {
			if op.Cid == nil {
				return nil, fmt.Errorf("create/update op missing CID for %s", op.Path)
			}

			// LexLink is a cid.Cid value, we need to cast it.
			cidVal := cid.Cid(*op.Cid)
			block, err := repoObj.RecordStore.Get(ctx, cidVal)
			if err != nil {
				return nil, fmt.Errorf("failed to get block for %s: %w", op.Path, err)
			}

			record = block.RawData()
			if err := dagcbor.ValidateMap(record); err != nil {
				return nil, fmt.Errorf("invalid declaration record for %s: %w", op.Path, err)
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

	return ops, nil
}
