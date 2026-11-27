// pattern: Imperative Shell
// Tests for the public XRPC query endpoints (getLabelers/getLabeler).

package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
)

// queryTestEnv bundles the harness for query endpoint tests: a real registry
// in a temp store mounted behind the handlers via GET-qualified patterns
// (mirroring production mounting). regStore is exposed so tests can induce
// store failures by closing it.
type queryTestEnv struct {
	ts       *httptest.Server
	reg      *store.LabelerRegistry
	regStore *store.Store
	cleanup  func()
}

func newQueryTestEnv(t *testing.T) *queryTestEnv {
	t.Helper()
	p, persistCleanup := testPersist(t)

	regStore, err := store.Open(t.TempDir() + "/reg.db")
	require.NoError(t, err)
	reg := store.NewLabelerRegistry(regStore)

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	srv := server.NewServer(h, p, reg, slog.Default(), 7200, 0)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /xrpc/community.labeler.sync.getLabelers", srv.HandleGetLabelers)
	mux.HandleFunc("GET /xrpc/community.labeler.sync.getLabeler", srv.HandleGetLabeler)

	ts := httptest.NewServer(mux)
	return &queryTestEnv{
		ts:       ts,
		reg:      reg,
		regStore: regStore,
		cleanup: func() {
			ts.Close()
			regStore.Close()
			persistCleanup()
		},
	}
}

// seedLabeler registers one labeler and optionally writes its upstream cursor.
func seedLabeler(t *testing.T, env *queryTestEnv, did, endpoint, source string, enabled bool, cursor *int64) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, env.reg.Upsert(ctx, store.Labeler{
		DID:       did,
		Endpoint:  endpoint,
		Source:    source,
		Enabled:   enabled,
		UpdatedAt: time.Now().Unix(),
	}))
	if cursor != nil {
		require.NoError(t, env.reg.WriteCursor(ctx, did, *cursor))
	}
}

// Seed SQL directly to fix IDs and timestamps independently of the insertion paths.
func seedLabelerRow(t *testing.T, env *queryTestEnv, did, endpoint, source string, enabled bool,
	lastUpstreamSeq *int64, id, createdAt int64, declaredAt *int64, collections []string) {
	t.Helper()
	enabledInt := 0
	if enabled {
		enabledInt = 1
	}
	var lu interface{} = nil
	if lastUpstreamSeq != nil {
		lu = *lastUpstreamSeq
	}
	var decl interface{} = nil
	if declaredAt != nil {
		decl = *declaredAt
	}
	_, err := env.regStore.DB().Exec(
		`INSERT INTO labelers(did, endpoint, source, enabled, last_upstream_seq, id, created_at, declared_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		did, endpoint, source, enabledInt, lu, id, createdAt, decl, createdAt)
	require.NoError(t, err)
	for _, c := range collections {
		_, err := env.regStore.DB().Exec(
			`INSERT INTO labeler_declarations(did, collection) VALUES (?, ?)`, did, c)
		require.NoError(t, err)
	}
}

func ptrInt64(v int64) *int64 { return &v }

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// assertXRPCError decodes the XRPC error shape and checks the error name.
// Returns the message for further assertions.
func assertXRPCError(t *testing.T, resp *http.Response, name string) string {
	t.Helper()
	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, name, body.Error)
	require.NotEmpty(t, body.Message)
	return body.Message
}

// fetchLabelerList GETs the list endpoint and decodes the raw response.
func fetchLabelerList(t *testing.T, rawURL string) (map[string]any, int) {
	t.Helper()
	resp, err := http.Get(rawURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	return body, resp.StatusCode
}

func labelerDIDs(body map[string]any) []string {
	items, _ := body["labelers"].([]any)
	dids := make([]string, 0, len(items))
	for _, item := range items {
		m, _ := item.(map[string]any)
		did, _ := m["did"].(string)
		dids = append(dids, did)
	}
	return dids
}

// updatedAt is the only conditional key.
var viewBaseKeys = []string{"id", "did", "createdAt", "endpoint", "enabled",
	"last_upstream_seq", "declarations"}

// Non-lexical DIDs distinguish ID order from DID order.
func TestGetLabelersViewShape(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	// Cover discovery with declarations, manual registration, and discovery failure.
	seedLabelerRow(t, env, "did:plc:declared", "declared.example.com", "firehose", true, ptrInt64(77),
		1, 1735689540, ptrInt64(1735689600),
		[]string{"app.bsky.labeler.service", "town.delve.labeler.service"})
	seedLabelerRow(t, env, "did:plc:manual", "manual.example.com", "manual", false, nil,
		2, 1704067200, nil, nil)
	seedLabelerRow(t, env, "did:plc:errow", "", "firehose", false, nil,
		3, 1704067260, nil, nil)

	body, status := fetchLabelerList(t, env.ts.URL+"/xrpc/community.labeler.sync.getLabelers")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body["cursor"], "single page must omit cursor")

	items, ok := body["labelers"].([]any)
	require.True(t, ok, "labelers must be an array")
	require.Len(t, items, 3)

	declared, _ := items[0].(map[string]any)
	manual, _ := items[1].(map[string]any)
	errRow, _ := items[2].(map[string]any)

	require.Equal(t, "did:plc:declared", declared["did"])
	require.Equal(t, "did:plc:manual", manual["did"])
	require.Equal(t, "did:plc:errow", errRow["did"])

	require.ElementsMatch(t, append(append([]string(nil), viewBaseKeys...), "updatedAt"), mapKeys(declared))
	require.ElementsMatch(t, viewBaseKeys, mapKeys(manual))
	require.ElementsMatch(t, viewBaseKeys, mapKeys(errRow))
	for _, item := range []map[string]any{declared, manual, errRow} {
		require.NotContains(t, item, "source", "source is no longer a public field")
		require.NotContains(t, item, "last_error")
		require.NotContains(t, item, "require_sig")
		require.NotContains(t, item, "updated_at", "internal bookkeeping timestamp is not the public updatedAt")
	}

	require.Equal(t, float64(1), declared["id"])
	require.Equal(t, float64(2), manual["id"])
	require.Equal(t, float64(3), errRow["id"])

	require.Equal(t, "2024-12-31T23:59:00Z", declared["createdAt"])
	require.Equal(t, "2025-01-01T00:00:00Z", declared["updatedAt"])
	require.Equal(t, "2024-01-01T00:00:00Z", manual["createdAt"])
	require.Equal(t, "2024-01-01T00:01:00Z", errRow["createdAt"])
	require.NotContains(t, manual, "updatedAt", "never-declared row must omit updatedAt")
	require.NotContains(t, errRow, "updatedAt")

	decs, ok := declared["declarations"].([]any)
	require.True(t, ok, "declarations must be an array")
	require.Equal(t, []any{"app.bsky.labeler.service", "town.delve.labeler.service"}, decs)
	for _, item := range []map[string]any{manual, errRow} {
		empty, ok := item["declarations"].([]any)
		require.True(t, ok, "declarations must serialize as [] for undeclared labelers, not null")
		require.Empty(t, empty)
	}

	require.Equal(t, float64(77), declared["last_upstream_seq"])
	require.Nil(t, manual["last_upstream_seq"],
		"never-ingested row must emit last_upstream_seq as null")
	require.Nil(t, errRow["last_upstream_seq"])
	require.Equal(t, "", errRow["endpoint"], "discovery-error row exposes its empty endpoint")
}

func TestGetLabelerViewShape(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	seedLabelerRow(t, env, "did:plc:declared", "declared.example.com", "firehose", true, ptrInt64(42),
		1, 1735689540, ptrInt64(1735689600), []string{"app.bsky.labeler.service"})
	seedLabelerRow(t, env, "did:plc:never", "never.example.com", "manual", true, nil,
		2, 1704067200, nil, nil)

	fetch := func(did string) map[string]any {
		t.Helper()
		resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=" + did)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var body map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		return body
	}

	declared := fetch("did:plc:declared")
	require.ElementsMatch(t, append(append([]string(nil), viewBaseKeys...), "updatedAt"), mapKeys(declared))
	require.Equal(t, "did:plc:declared", declared["did"])
	require.Equal(t, "declared.example.com", declared["endpoint"])
	require.Equal(t, true, declared["enabled"])
	require.Equal(t, float64(42), declared["last_upstream_seq"])
	require.Equal(t, float64(1), declared["id"])
	require.Equal(t, "2024-12-31T23:59:00Z", declared["createdAt"])
	require.Equal(t, "2025-01-01T00:00:00Z", declared["updatedAt"])
	require.Equal(t, []any{"app.bsky.labeler.service"}, declared["declarations"])

	never := fetch("did:plc:never")
	require.ElementsMatch(t, viewBaseKeys, mapKeys(never))
	require.Contains(t, never, "last_upstream_seq",
		"the last_upstream_seq key must always be present")
	require.Nil(t, never["last_upstream_seq"],
		"a never-ingested labeler must emit last_upstream_seq as null")
	require.NotContains(t, never, "updatedAt")
	empty, ok := never["declarations"].([]any)
	require.True(t, ok, "declarations must be an array")
	require.Empty(t, empty)
}

// TestQuery_GetLabelers_EnabledFilter verifies AC.2: absent → all,
// true/false → exact state, other literals → 400 InvalidRequest.
func TestQuery_GetLabelers_EnabledFilter(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	seedLabeler(t, env, "did:plc:on1", "on1.example.com", "manual", true, nil)
	seedLabeler(t, env, "did:plc:on2", "on2.example.com", "manual", true, nil)
	seedLabeler(t, env, "did:plc:off1", "off1.example.com", "manual", false, nil)

	base := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers"

	body, status := fetchLabelerList(t, base)
	require.Equal(t, http.StatusOK, status)
	// id order = insertion order (on1, on2, off1), not DID order.
	require.Equal(t, []string{"did:plc:on1", "did:plc:on2", "did:plc:off1"}, labelerDIDs(body))

	body, status = fetchLabelerList(t, base+"?enabled=true")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, []string{"did:plc:on1", "did:plc:on2"}, labelerDIDs(body))

	body, status = fetchLabelerList(t, base+"?enabled=false")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, []string{"did:plc:off1"}, labelerDIDs(body))

	// Literal booleans only.
	resp, err := http.Get(base + "?enabled=1")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assertXRPCError(t, resp, "InvalidRequest")
}

func TestGetLabelersPaginationWalk(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	// Non-lexical DIDs distinguish insertion order from DID order.
	dids := []string{"did:plc:p5", "did:plc:p3", "did:plc:p1", "did:plc:p4", "did:plc:p2"}
	for i, did := range dids {
		seedLabeler(t, env, did, fmt.Sprintf("p%d.example.com", i+1), "manual", true, nil)
	}

	seen := []string{}
	var cursor int64
	for page := 0; ; page++ {
		require.Less(t, page, 10, "pagination must terminate")
		rawURL := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers?limit=2"
		if page > 0 {
			rawURL += "&cursor=" + strconv.FormatInt(cursor, 10)
		}
		body, status := fetchLabelerList(t, rawURL)
		require.Equal(t, http.StatusOK, status)

		gotDIDs := labelerDIDs(body)
		require.LessOrEqual(t, len(gotDIDs), 2)
		for _, did := range gotDIDs {
			require.NotContains(t, seen, did, "pages must not overlap")
			seen = append(seen, did)
		}

		next, ok := body["cursor"].(float64)
		if !ok {
			// Cursorless response is the last page.
			break
		}
		require.Greater(t, int64(next), cursor, "cursor must advance monotonically")
		cursor = int64(next)
	}

	require.Len(t, seen, 5)
	require.Equal(t, dids, seen,
		"pages must concatenate in id order, which is not DID order here")
}

// TestGetLabelersCursorInvalid verifies AC.4's validation bounds: non-integer
// and negative cursors are rejected with 400 InvalidRequest; a cursor past
// the current maximum is not an error — it yields 200 with an empty labelers
// array and no cursor (a clean end-of-walk).
func TestGetLabelersCursorInvalid(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	seedLabeler(t, env, "did:plc:only", "only.example.com", "manual", true, nil)

	base := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers"
	for _, raw := range []string{"cursor=abc", "cursor=-5", "cursor=1.5"} {
		resp, err := http.Get(base + "?" + raw)
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "raw %q", raw)
		assertXRPCError(t, resp, "InvalidRequest")
		resp.Body.Close()
	}

	// int64 max is far past the current maximum: parseable, so accepted with
	// an empty page and no cursor.
	body, status := fetchLabelerList(t, base+"?cursor=9223372036854775807")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, labelerDIDs(body), "cursor past the maximum must yield an empty page")
	require.NotContains(t, body, "cursor")
}

func TestQuery_GetLabelers_DefaultLimit(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	for i := 1; i <= 60; i++ {
		seedLabeler(t, env, fmt.Sprintf("did:plc:q%02d", i), "q.example.com", "manual", true, nil)
	}

	base := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers"

	body, status := fetchLabelerList(t, base)
	require.Equal(t, http.StatusOK, status)
	require.ElementsMatch(t, []string{"labelers", "cursor"}, mapKeys(body))
	dids := labelerDIDs(body)
	require.Len(t, dids, 50)
	require.Equal(t, "did:plc:q01", dids[0])
	require.Equal(t, "did:plc:q50", dids[49])
	cursor, ok := body["cursor"].(float64)
	require.True(t, ok, "cursor must be a numeric id, got %T", body["cursor"])
	require.Equal(t, float64(50), cursor,
		"cursor must be the id of the page's last row")

	body, status = fetchLabelerList(t, base+"?cursor=50")
	require.Equal(t, http.StatusOK, status)
	require.ElementsMatch(t, []string{"labelers"}, mapKeys(body),
		"final page must omit cursor")
	dids = labelerDIDs(body)
	require.Len(t, dids, 10)
	require.Equal(t, "did:plc:q51", dids[0])
	require.Equal(t, "did:plc:q60", dids[9])
}

// TestQuery_GetLabelers_InvalidLimit verifies AC.3's rejection bounds.
func TestQuery_GetLabelers_InvalidLimit(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	base := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers"
	for _, raw := range []string{"limit=0", "limit=101", "limit=abc", "limit=-5", "limit=1.5"} {
		resp, err := http.Get(base + "?" + raw)
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "raw %q", raw)
		assertXRPCError(t, resp, "InvalidRequest")
		resp.Body.Close()
	}
}

// TestQuery_GetLabeler_NotFound verifies AC.4's unknown-DID path.
func TestQuery_GetLabeler_NotFound(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:unknown")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "labeler not found", assertXRPCError(t, resp, "InvalidRequest"))
}

// TestQuery_GetLabeler_InvalidDID verifies AC.4's malformed-DID path plus the
// missing-parameter path.
func TestQuery_GetLabeler_InvalidDID(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=not-a-did")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "invalid did", assertXRPCError(t, resp, "InvalidRequest"))

	resp2, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler")
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	assertXRPCError(t, resp2, "InvalidRequest")
}

// TestQuery_GetLabelers_StoreFailure verifies AC.4's 500 path: a closed
// registry store yields the XRPC InternalServerError shape without leaking
// internal details.
func TestQuery_GetLabelers_StoreFailure(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	require.NoError(t, env.regStore.Close())

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabelers")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	msg := assertXRPCError(t, resp, "InternalServerError")
	require.NotContains(t, msg, "sql", "internal details must not leak")
}

// TestQuery_GetLabeler_StoreFailure verifies AC.4's 500 path for the single
// lookup.
func TestQuery_GetLabeler_StoreFailure(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	require.NoError(t, env.regStore.Close())

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:any")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assertXRPCError(t, resp, "InternalServerError")
}

// Discovery can fail before an endpoint or declaration is available. Both query APIs must represent that row.
func TestQuery_LexiconValues(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	require.NoError(t, env.reg.RecordError(context.Background(), "did:plc:empty", "discovery failed"))

	body, status := fetchLabelerList(t, env.ts.URL+"/xrpc/community.labeler.sync.getLabelers")
	require.Equal(t, http.StatusOK, status)
	items, ok := body["labelers"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	item, _ := items[0].(map[string]any)
	require.Equal(t, "", item["endpoint"], "empty endpoint must be representable")
	require.Equal(t, false, item["enabled"])
	empty, ok := item["declarations"].([]any)
	require.True(t, ok, "declarations must be an array")
	require.Empty(t, empty, "RecordError row holds no declarations")

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:empty")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var single map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&single))
	require.Equal(t, "", single["endpoint"])
}

// TestQueryLexiconContract verifies AC.5: the lexicon files exist, parse as
// JSON, and declare the integer cursor (param and response) and the
// required+nullable last_upstream_seq consistent with the handlers' actual
// output.
func TestQueryLexiconContract(t *testing.T) {
	type schemaNode struct {
		Type        string                `json:"type"`
		Description string                `json:"description"`
		Required    []string              `json:"required"`
		Nullable    []string              `json:"nullable"`
		Properties  map[string]schemaNode `json:"properties"`
		Items       *schemaNode           `json:"items"`
		Ref         string                `json:"ref"`
		Default     *int                  `json:"default"`
		Minimum     *int                  `json:"minimum"`
		Maximum     *int                  `json:"maximum"`
		KnownValues []string              `json:"knownValues"`
	}
	type paramNode struct {
		Type       string                `json:"type"`
		Required   []string              `json:"required"`
		Properties map[string]schemaNode `json:"properties"`
	}
	type mainDef struct {
		Type       string     `json:"type"`
		Parameters *paramNode `json:"parameters"`
		Output     *struct {
			Encoding string     `json:"encoding"`
			Schema   schemaNode `json:"schema"`
		} `json:"output"`
	}

	readLexicon := func(path string) (string, map[string]json.RawMessage) {
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
		return top.ID, top.Defs
	}

	// getLabelers.
	id, defs := readLexicon("../../lexicons/community/labeler/sync/getLabelers.json")
	require.Equal(t, "community.labeler.sync.getLabelers", id)

	var listMain mainDef
	require.NoError(t, json.Unmarshal(defs["main"], &listMain))
	require.Equal(t, "query", listMain.Type)
	require.NotNil(t, listMain.Parameters)
	require.Equal(t, "params", listMain.Parameters.Type)

	limit, ok := listMain.Parameters.Properties["limit"]
	require.True(t, ok, "limit param must exist")
	require.Equal(t, "integer", limit.Type)
	require.NotNil(t, limit.Default)
	require.Equal(t, 50, *limit.Default)
	require.NotNil(t, limit.Minimum)
	require.Equal(t, 1, *limit.Minimum)
	require.NotNil(t, limit.Maximum)
	require.Equal(t, 100, *limit.Maximum)

	cursor, ok := listMain.Parameters.Properties["cursor"]
	require.True(t, ok, "cursor param must exist")
	require.Equal(t, "integer", cursor.Type,
		"the cursor param is a deliberately non-opaque integer (the row id)")
	require.Contains(t, cursor.Description, "The id of the last labeler",
		"the cursor param must document the id semantics")

	enabled, ok := listMain.Parameters.Properties["enabled"]
	require.True(t, ok, "enabled param must exist")
	require.Equal(t, "boolean", enabled.Type)
	require.Empty(t, listMain.Parameters.Required)

	require.NotNil(t, listMain.Output)
	require.Equal(t, "application/json", listMain.Output.Encoding)
	require.Equal(t, "object", listMain.Output.Schema.Type)
	require.ElementsMatch(t, []string{"labelers"}, listMain.Output.Schema.Required)
	labelersProp, ok := listMain.Output.Schema.Properties["labelers"]
	require.True(t, ok)
	require.NotNil(t, labelersProp.Items)
	require.Equal(t, "community.labeler.sync.defs#labelerView", labelersProp.Items.Ref)

	outCursor, ok := listMain.Output.Schema.Properties["cursor"]
	require.True(t, ok, "response cursor property must exist")
	require.Equal(t, "integer", outCursor.Type,
		"the response cursor is the numeric id of the page's last row")
	require.Contains(t, outCursor.Description, "The id of the last labeler",
		"the response cursor must document the id semantics")

	// The shared labelerView lives in the defs document, not in either
	// query lexicon.
	defsID, sharedDefs := readLexicon("../../lexicons/community/labeler/sync/defs.json")
	require.Equal(t, "community.labeler.sync.defs", defsID)
	require.NotContains(t, sharedDefs, "main", "the defs document carries only shared objects, not a query")
	var view schemaNode
	require.NoError(t, json.Unmarshal(sharedDefs["labelerView"], &view))
	require.Equal(t, "object", view.Type)
	require.ElementsMatch(t,
		[]string{"id", "did", "createdAt", "endpoint", "enabled", "last_upstream_seq", "declarations"},
		view.Required,
		"labelerView required fields must match the handler's always-emitted keys")
	require.NotNil(t, view.Nullable, "last_upstream_seq must be declared nullable")
	require.ElementsMatch(t, []string{"last_upstream_seq"}, view.Nullable,
		"last_upstream_seq is emitted as null for labelers that have never been observed")
	for _, key := range []string{"id", "did", "createdAt", "updatedAt", "endpoint", "enabled", "last_upstream_seq", "declarations"} {
		_, ok := view.Properties[key]
		require.True(t, ok, "labelerView must declare %s", key)
	}
	require.NotContains(t, view.Required, "updatedAt",
		"updatedAt is optional: absent when no declaration was ever observed")
	require.NotContains(t, view.Properties, "source",
		"source was removed from labelerView")
	declarations, ok := view.Properties["declarations"]
	require.True(t, ok)
	require.NotNil(t, declarations.Items)
	require.Equal(t, "string", declarations.Items.Type)
	require.ElementsMatch(t, []string{
		"app.bsky.labeler.service", "town.delve.labeler.service", "so.sprk.labeler.service",
	}, declarations.Items.KnownValues)

	// getLabeler.
	id, defs = readLexicon("../../lexicons/community/labeler/sync/getLabeler.json")
	require.Equal(t, "community.labeler.sync.getLabeler", id)

	var singleMain mainDef
	require.NoError(t, json.Unmarshal(defs["main"], &singleMain))
	require.Equal(t, "query", singleMain.Type)
	require.NotNil(t, singleMain.Parameters)
	require.ElementsMatch(t, []string{"did"}, singleMain.Parameters.Required)
	didProp, ok := singleMain.Parameters.Properties["did"]
	require.True(t, ok)
	require.Equal(t, "string", didProp.Type)

	require.NotNil(t, singleMain.Output)
	require.Equal(t,
		"community.labeler.sync.defs#labelerView",
		singleMain.Output.Schema.Ref)
}
