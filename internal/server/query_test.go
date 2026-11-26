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
	"net/url"
	"os"
	"sort"
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

// TestQuery_GetLabelers_Projection verifies AC.1: every row is projected to
// exactly did/endpoint/source/enabled (+last_upstream_seq when set);
// last_error, require_sig, and updated_at never appear; DID order is stable.
func TestQuery_GetLabelers_Projection(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	// Enabled manual labeler that has ingested.
	seedLabeler(t, env, "did:plc:bbb", "https://bbb.example.com", "manual", true, ptrInt64(77))
	// Disabled manual labeler, never ingested.
	seedLabeler(t, env, "did:plc:aaa", "https://aaa.example.com", "manual", false, nil)
	// Discovery-error row: empty endpoint, firehose source, disabled.
	require.NoError(t, env.reg.RecordError(ctx, "did:plc:ccc", "resolution failed"))

	body, status := fetchLabelerList(t, env.ts.URL+"/xrpc/community.labeler.sync.getLabelers")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body["cursor"], "single page must omit cursor")

	items, ok := body["labelers"].([]any)
	require.True(t, ok, "labelers must be an array")
	require.Len(t, items, 3)

	baseKeys := []string{"did", "endpoint", "source", "enabled"}
	first, _ := items[0].(map[string]any)
	second, _ := items[1].(map[string]any)
	third, _ := items[2].(map[string]any)

	// Ascending DID order from the registry's ORDER BY did.
	require.Equal(t, "did:plc:aaa", first["did"])
	require.Equal(t, "did:plc:bbb", second["did"])
	require.Equal(t, "did:plc:ccc", third["did"])

	require.ElementsMatch(t, baseKeys, mapKeys(first),
		"never-subscribed row must omit last_upstream_seq")
	require.ElementsMatch(t, append(baseKeys, "last_upstream_seq"), mapKeys(second))
	require.Equal(t, float64(77), second["last_upstream_seq"])
	require.ElementsMatch(t, baseKeys, mapKeys(third))

	// Discovery-error row exposes its empty endpoint and no internal fields.
	require.Equal(t, "", third["endpoint"])
	for _, item := range []map[string]any{first, second, third} {
		require.NotContains(t, item, "last_error")
		require.NotContains(t, item, "require_sig")
		require.NotContains(t, item, "updated_at")
	}
}

// TestQuery_GetLabelers_EnabledFilter verifies AC.2: absent → all,
// true/false → exact state, other literals → 400 InvalidRequest.
func TestQuery_GetLabelers_EnabledFilter(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	seedLabeler(t, env, "did:plc:on1", "https://on1.example.com", "manual", true, nil)
	seedLabeler(t, env, "did:plc:on2", "https://on2.example.com", "manual", true, nil)
	seedLabeler(t, env, "did:plc:off1", "https://off1.example.com", "manual", false, nil)

	base := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers"

	body, status := fetchLabelerList(t, base)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, []string{"did:plc:off1", "did:plc:on1", "did:plc:on2"}, labelerDIDs(body))

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

// TestQuery_GetLabelers_Pagination verifies AC.3: cursor walking covers every
// row in DID order with no overlap, and the terminal page omits the cursor.
func TestQuery_GetLabelers_Pagination(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	for i := 1; i <= 5; i++ {
		seedLabeler(t, env, fmt.Sprintf("did:plc:p%d", i), fmt.Sprintf("https://p%d.example.com", i), "manual", true, nil)
	}

	seen := []string{}
	cursor := ""
	for page := 0; ; page++ {
		require.Less(t, page, 10, "pagination must terminate")
		rawURL := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers?limit=2"
		if cursor != "" {
			rawURL += "&cursor=" + url.QueryEscape(cursor)
		}
		body, status := fetchLabelerList(t, rawURL)
		require.Equal(t, http.StatusOK, status)

		dids := labelerDIDs(body)
		require.LessOrEqual(t, len(dids), 2)
		for _, did := range dids {
			require.NotContains(t, seen, did, "pages must not overlap")
			seen = append(seen, did)
		}

		next, _ := body["cursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}

	require.Len(t, seen, 5)
	require.True(t, sort.StringsAreSorted(seen), "pages must concatenate in DID order")
}

// TestQuery_GetLabelers_DefaultLimit verifies AC.3's default-50 page size and
// that the final page omits cursor while a full page carries it.
func TestQuery_GetLabelers_DefaultLimit(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	for i := 1; i <= 60; i++ {
		seedLabeler(t, env, fmt.Sprintf("did:plc:q%02d", i), "https://q.example.com", "manual", true, nil)
	}

	base := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers"

	body, status := fetchLabelerList(t, base)
	require.Equal(t, http.StatusOK, status)
	require.ElementsMatch(t, []string{"labelers", "cursor"}, mapKeys(body))
	dids := labelerDIDs(body)
	require.Len(t, dids, 50)
	require.Equal(t, "did:plc:q01", dids[0])
	require.Equal(t, "did:plc:q50", dids[49])
	cursor, _ := body["cursor"].(string)
	require.Equal(t, "did:plc:q50", cursor)

	body, status = fetchLabelerList(t, base+"?cursor="+url.QueryEscape(cursor))
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

// TestQuery_GetLabeler_Found verifies AC.4's found path.
func TestQuery_GetLabeler_Found(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	seedLabeler(t, env, "did:plc:one", "https://one.example.com", "manual", true, ptrInt64(42))

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:one")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "did:plc:one", body["did"])
	require.Equal(t, "https://one.example.com", body["endpoint"])
	require.Equal(t, "manual", body["source"])
	require.Equal(t, true, body["enabled"])
	require.Equal(t, float64(42), body["last_upstream_seq"])
	require.ElementsMatch(t,
		[]string{"did", "endpoint", "source", "enabled", "last_upstream_seq"}, mapKeys(body))
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

// TestQuery_LexiconValues verifies AC.8's representability requirement: a
// discovery-error row's empty endpoint round-trips through both endpoints.
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
	require.Equal(t, "firehose", item["source"])
	require.Equal(t, false, item["enabled"])

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:empty")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var single map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&single))
	require.Equal(t, "", single["endpoint"])
}

// TestQuery_Lexicons verifies AC.8: the lexicon files exist, parse as JSON,
// and declare params/output consistent with the handlers' actual output.
func TestQuery_Lexicons(t *testing.T) {
	type schemaNode struct {
		Type       string                 `json:"type"`
		Required   []string               `json:"required"`
		Properties map[string]schemaNode  `json:"properties"`
		Items      *schemaNode            `json:"items"`
		Ref        string                 `json:"ref"`
		Default    *int                   `json:"default"`
		Minimum    *int                   `json:"minimum"`
		Maximum    *int                   `json:"maximum"`
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
			Lexicon int                       `json:"lexicon"`
			ID      string                    `json:"id"`
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
	require.Equal(t, "string", cursor.Type)

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
	require.Equal(t, "#labelerView", labelersProp.Items.Ref)

	var view schemaNode
	require.NoError(t, json.Unmarshal(defs["labelerView"], &view))
	require.Equal(t, "object", view.Type)
	require.ElementsMatch(t, []string{"did", "endpoint", "source", "enabled"}, view.Required,
		"labelerView required fields must match the handler's always-emitted keys")
	for _, key := range []string{"did", "endpoint", "source", "enabled", "last_upstream_seq"} {
		_, ok := view.Properties[key]
		require.True(t, ok, "labelerView must declare %s", key)
	}

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
		"community.labeler.sync.getLabelers#labelerView",
		singleMain.Output.Schema.Ref)
}
