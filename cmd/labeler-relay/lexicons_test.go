// pattern: Imperative Shell
// Lexicon shape guard for issues #41/#42: the labelerView field set (id,
// createdAt, optional updatedAt, no source), the id-keyed cursor docs, and
// the #service message's unknown-typed record plus non-required collection.

package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// schemaNode is the subset of lexicon schema JSON these assertions need.
type schemaNode struct {
	Type        string                `json:"type"`
	Description string                `json:"description"`
	Required    []string              `json:"required"`
	Nullable    []string              `json:"nullable"`
	Properties  map[string]schemaNode `json:"properties"`
	Items       *schemaNode           `json:"items"`
	Refs        []string              `json:"refs"`
	KnownValues []string              `json:"knownValues"`
}

// readDefs parses a lexicon document and returns its defs map.
func readDefs(t *testing.T, path string) map[string]json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, path)
	var top struct {
		Lexicon int                        `json:"lexicon"`
		ID      string                     `json:"id"`
		Defs    map[string]json.RawMessage `json:"defs"`
	}
	require.NoError(t, json.Unmarshal(data, &top), path)
	require.Equal(t, 1, top.Lexicon, path)
	require.NotEmpty(t, top.ID, path)
	return top.Defs
}

// TestLabelerViewLexiconShape verifies AC.1: defs.json's labelerView carries
// exactly the issue #41 field set plus declarations (with updatedAt optional),
// getLabelers' cursor descriptions reference id, and subscribeLabelers' #service
// record is unknown-typed with a non-required collection carrying the three
// declaration NSIDs as knownValues.
func TestLabelerViewLexiconShape(t *testing.T) {
	// --- defs.json: labelerView ---
	defs := readDefs(t, "../../lexicons/community/labeler/sync/defs.json")
	var view schemaNode
	require.NoError(t, json.Unmarshal(defs["labelerView"], &view))
	require.Equal(t, "object", view.Type)

	// Required: the issue #41 set plus declarations — and no source.
	require.ElementsMatch(t,
		[]string{"id", "did", "createdAt", "endpoint", "enabled", "last_upstream_seq", "declarations"},
		view.Required, "labelerView required fields")
	require.NotContains(t, view.Required, "source", "source was removed from labelerView")
	require.NotContains(t, view.Required, "updatedAt", "updatedAt must stay optional")

	// Only last_upstream_seq is nullable.
	require.ElementsMatch(t, []string{"last_upstream_seq"}, view.Nullable)

	// Properties: the required set plus the optional updatedAt.
	for _, key := range []string{
		"id", "did", "createdAt", "updatedAt", "endpoint", "enabled", "last_upstream_seq", "declarations",
	} {
		_, ok := view.Properties[key]
		require.True(t, ok, "labelerView must declare %s", key)
	}
	require.NotContains(t, view.Properties, "source")

	// Field types and the enabled description from issue #41.
	require.Equal(t, "integer", view.Properties["id"].Type)
	require.Equal(t, "string", view.Properties["did"].Type)
	require.Equal(t, "string", view.Properties["createdAt"].Type)
	require.Equal(t, "string", view.Properties["updatedAt"].Type)
	require.Equal(t, "string", view.Properties["endpoint"].Type)
	require.Equal(t, "boolean", view.Properties["enabled"].Type)
	require.Equal(t, "integer", view.Properties["last_upstream_seq"].Type)
	require.Equal(t, "array", view.Properties["declarations"].Type)
	require.Contains(t, view.Properties["enabled"].Description, "inactive",
		"the enabled description must carry issue #41's inactive-on-removal wording")

	// declarations items: the three declaration NSIDs as knownValues.
	items := view.Properties["declarations"].Items
	require.NotNil(t, items)
	require.Equal(t, "string", items.Type)
	require.ElementsMatch(t, []string{
		"app.bsky.labeler.service", "town.delve.labeler.service", "so.sprk.labeler.service",
	}, items.KnownValues, "declarations items knownValues must list the three declaration NSIDs")

	// --- getLabelers.json: cursor docs reference id ---
	glDefs := readDefs(t, "../../lexicons/community/labeler/sync/getLabelers.json")
	var glMain struct {
		Parameters *struct {
			Properties map[string]schemaNode `json:"properties"`
		} `json:"parameters"`
		Output *struct {
			Schema schemaNode `json:"schema"`
		} `json:"output"`
	}
	require.NoError(t, json.Unmarshal(glDefs["main"], &glMain))
	require.NotNil(t, glMain.Parameters)
	require.NotNil(t, glMain.Output)

	paramCursor := glMain.Parameters.Properties["cursor"]
	require.Equal(t, "integer", paramCursor.Type)
	require.Contains(t, paramCursor.Description, "The id of the last labeler",
		"the cursor param description must reference id")
	respCursor := glMain.Output.Schema.Properties["cursor"]
	require.Equal(t, "integer", respCursor.Type)
	require.Contains(t, respCursor.Description, "The id of the last labeler",
		"the response cursor description must reference id")

	// --- subscribeLabelers.json: #service record + collection ---
	slDefs := readDefs(t, "../../lexicons/community/labeler/sync/subscribeLabelers.json")
	var service schemaNode
	require.NoError(t, json.Unmarshal(slDefs["service"], &service))
	require.Equal(t, "object", service.Type)

	// collection: non-required, with the three NSID knownValues.
	require.NotContains(t, service.Required, "collection",
		"collection must stay non-required so pre-change stored frames remain valid")
	collection := service.Properties["collection"]
	require.Equal(t, "string", collection.Type)
	require.ElementsMatch(t, []string{
		"app.bsky.labeler.service", "town.delve.labeler.service", "so.sprk.labeler.service",
	}, collection.KnownValues)

	// record: unknown-typed passthrough, not a ref.
	record := service.Properties["record"]
	require.Equal(t, "unknown", record.Type,
		"the record property must be unknown-typed (byte-faithful CBOR passthrough)")
	require.Empty(t, record.Refs, "the record property must not ref app.bsky.labeler.service")

	// The required set stays seq/src/op.
	require.ElementsMatch(t, []string{"seq", "src", "op"}, service.Required)
}
