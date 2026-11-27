// pattern: Imperative Shell

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Keep user documentation and package contracts consistent with the query API.
func TestDocs_QueryContractDocumented(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	require.NoError(t, err)
	text := string(readme)

	// The documented example response carries the cursor as a JSON number,
	// not a quoted string.
	require.Contains(t, text, `"cursor": 2`,
		"README example must show a numeric id cursor")

	// Prose: first-observed ordering and null last_upstream_seq.
	require.Contains(t, text, "oldest-observed first")
	require.Contains(t, text, "null` when the labeler has never ingested",
		"README must document the null last_upstream_seq semantics")

	// Params: the integer cursor and its validation contract.
	require.Contains(t, text, "`id` of the last labeler on the previous page",
		"README must document the id cursor semantics")
	require.Contains(t, text, "non-negative integer")

	require.Contains(t, text, "`declarations` lists the declaration collections",
		"README must document the declarations array")
	require.Contains(t, text, "`id` is the entry's monotonically increasing identifier",
		"README must document the id field")
	require.Contains(t, text, "`updatedAt` is the date of the most recently observed declaration update",
		"README must document the optional updatedAt")
	require.NotContains(t, text, "`source` is `firehose` for discovered labelers",
		"README must not document the removed source field as a labelerView field")

	require.Contains(t, text, "`town.delve.labeler.service`, or `so.sprk.labeler.service`",
		"README must document all three declaration collections")
	require.Contains(t, text, "passed through byte-faithfully",
		"README must document the byte-faithful record passthrough")
	require.Contains(t, text, "deletes its last declaration record is considered inactive",
		"README must document the inactive-on-declaration-removal behavior")
	require.Contains(t, text, "first tracked declaration and auto-subscribe is enabled",
		"README must qualify zero-to-one re-enable behavior")
	require.Contains(t, text, "operator-disabled labeler that still has a tracked declaration stays disabled",
		"README must document the operator-disable exception")

	serverDoc, err := os.ReadFile("../../internal/server/CLAUDE.md")
	require.NoError(t, err)
	require.Contains(t, string(serverDoc), "id-ordered list")
	require.Contains(t, string(serverDoc), "always present; JSON null when the labeler has never ingested")
	require.Contains(t, string(serverDoc), "a non-integer or negative cursor")
	require.Contains(t, string(serverDoc), "declarations")

	storeDoc, err := os.ReadFile("../../internal/store/CLAUDE.md")
	require.NoError(t, err)
	require.Contains(t, string(storeDoc), "labelers.id` is strictly monotonic and never reused")
	require.Contains(t, string(storeDoc), "minted at first insert")
	require.Contains(t, string(storeDoc), "unique index")
	require.Contains(t, string(storeDoc), "ORDER BY did",
		"store CLAUDE.md must note that ListEnabled intentionally keeps ORDER BY did")
	require.Contains(t, string(storeDoc), "ApplyServiceOp")
}
