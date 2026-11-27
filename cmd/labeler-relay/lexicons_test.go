// pattern: Imperative Shell

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

// Keep the published lexicons consistent with query responses and stored service frames.
func TestLabelerViewLexiconShape(t *testing.T) {
	defs := readDefs(t, "../../lexicons/community/labeler/sync/defs.json")
	var view schemaNode
	require.NoError(t, json.Unmarshal(defs["labelerView"], &view))
	require.Equal(t, "object", view.Type)

	require.ElementsMatch(t,
		[]string{"id", "did", "createdAt", "endpoint", "enabled", "last_upstream_seq", "declarations"},
		view.Required, "labelerView required fields")
	require.NotContains(t, view.Required, "source", "source was removed from labelerView")
	require.NotContains(t, view.Required, "updatedAt", "updatedAt must stay optional")

	require.ElementsMatch(t, []string{"last_upstream_seq"}, view.Nullable)

	for _, key := range []string{
		"id", "did", "createdAt", "updatedAt", "endpoint", "enabled", "last_upstream_seq", "declarations",
	} {
		_, ok := view.Properties[key]
		require.True(t, ok, "labelerView must declare %s", key)
	}
	require.NotContains(t, view.Properties, "source")

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

	items := view.Properties["declarations"].Items
	require.NotNil(t, items)
	require.Equal(t, "string", items.Type)
	require.ElementsMatch(t, []string{
		"app.bsky.labeler.service", "town.delve.labeler.service", "so.sprk.labeler.service",
	}, items.KnownValues, "declarations items knownValues must list the three declaration NSIDs")

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

	slDefs := readDefs(t, "../../lexicons/community/labeler/sync/subscribeLabelers.json")
	var service schemaNode
	require.NoError(t, json.Unmarshal(slDefs["service"], &service))
	require.Equal(t, "object", service.Type)

	// Stored legacy frames have no collection field.
	require.NotContains(t, service.Required, "collection",
		"collection must stay non-required so pre-change stored frames remain valid")
	collection := service.Properties["collection"]
	require.Equal(t, "string", collection.Type)
	require.ElementsMatch(t, []string{
		"app.bsky.labeler.service", "town.delve.labeler.service", "so.sprk.labeler.service",
	}, collection.KnownValues)

	record := service.Properties["record"]
	require.Equal(t, "unknown", record.Type,
		"the record property must be unknown-typed (byte-faithful CBOR passthrough)")
	require.Empty(t, record.Refs, "the record property must not ref app.bsky.labeler.service")

	require.ElementsMatch(t, []string{"seq", "src", "op"}, service.Required)
}
