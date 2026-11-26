// pattern: Imperative Shell
// Documentation drift guard for the getLabelers query contract: observed
// order, the integer cursor, and the null last_upstream_seq emission must be
// documented in the README and both package CLAUDE.md files.

package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDocs_QueryContractDocumented verifies AC.7: the README example shows a
// numeric cursor and its prose/params describe observed-order + null
// semantics; the server and store CLAUDE.md files state the new contracts.
func TestDocs_QueryContractDocumented(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	require.NoError(t, err)
	text := string(readme)

	// The documented example response carries the cursor as a JSON number,
	// not a quoted string.
	require.Contains(t, text, `"cursor": 2`,
		"README example must show a numeric observed_seq cursor")

	// Prose: first-observed ordering and null last_upstream_seq.
	require.Contains(t, text, "oldest-observed first")
	require.Contains(t, text, "null` when the labeler has never ingested",
		"README must document the null last_upstream_seq semantics")

	// Params: the integer cursor and its validation contract.
	require.Contains(t, text, "observed_seq` of the last labeler on the previous page")
	require.Contains(t, text, "non-negative integer")

	serverDoc, err := os.ReadFile("../../internal/server/CLAUDE.md")
	require.NoError(t, err)
	require.Contains(t, string(serverDoc), "observed_seq-ordered list")
	require.Contains(t, string(serverDoc), "always present; JSON null when the labeler has never ingested")
	require.Contains(t, string(serverDoc), "a non-integer or negative cursor")

	storeDoc, err := os.ReadFile("../../internal/store/CLAUDE.md")
	require.NoError(t, err)
	require.Contains(t, string(storeDoc), "observed_seq")
	require.Contains(t, string(storeDoc), "minted at first insert")
	require.Contains(t, string(storeDoc), "unique index")
	require.Contains(t, string(storeDoc), "ORDER BY did",
		"store CLAUDE.md must note that ListEnabled intentionally keeps ORDER BY did")
}
