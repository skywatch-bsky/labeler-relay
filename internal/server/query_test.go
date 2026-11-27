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

// TestQuery_GetLabelers_Projection verifies AC.3 and AC.6: every row is
// projected to exactly did/endpoint/source/enabled + last_upstream_seq
// (always present, null when never ingested); last_error, require_sig, and
// updated_at never appear; rows arrive in observed order — the fixture's DIDs
// are deliberately out of lexical order (did:plc:zzz observed first) so the
// assertions distinguish observed order from DID order.
func TestQuery_GetLabelers_Projection(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()
	ctx := context.Background()

	// Enabled manual labeler that has ingested; observed first.
	seedLabeler(t, env, "did:plc:zzz", "zzz.example.com", "manual", true, ptrInt64(77))
	// Disabled manual labeler, never ingested; observed second.
	seedLabeler(t, env, "did:plc:mmm", "mmm.example.com", "manual", false, nil)
	// Discovery-error row: empty endpoint, firehose source, disabled; observed third.
	require.NoError(t, env.reg.RecordError(ctx, "did:plc:aaa", "resolution failed"))

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

	// Observed order from the registry's ORDER BY observed_seq.
	require.Equal(t, "did:plc:zzz", first["did"])
	require.Equal(t, "did:plc:mmm", second["did"])
	require.Equal(t, "did:plc:aaa", third["did"])

	// last_upstream_seq is always present: numeric when ingested, null when not.
	require.ElementsMatch(t, append(baseKeys, "last_upstream_seq"), mapKeys(first))
	require.Equal(t, float64(77), first["last_upstream_seq"])
	require.ElementsMatch(t, append(baseKeys, "last_upstream_seq"), mapKeys(second))
	require.Nil(t, second["last_upstream_seq"],
		"never-ingested row must emit last_upstream_seq as null")
	require.ElementsMatch(t, append(baseKeys, "last_upstream_seq"), mapKeys(third))
	require.Nil(t, third["last_upstream_seq"])

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

	seedLabeler(t, env, "did:plc:on1", "on1.example.com", "manual", true, nil)
	seedLabeler(t, env, "did:plc:on2", "on2.example.com", "manual", true, nil)
	seedLabeler(t, env, "did:plc:off1", "off1.example.com", "manual", false, nil)

	base := env.ts.URL + "/xrpc/community.labeler.sync.getLabelers"

	body, status := fetchLabelerList(t, base)
	require.Equal(t, http.StatusOK, status)
	// Observed order = insertion order (on1, on2, off1), not DID order.
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

// TestGetLabelersCursorPaginationNumeric verifies AC.4: page N+1 contains
// exactly the rows with observed_seq > cursor; the response cursor is the
// numeric observed_seq of the page's last row; the final page omits cursor;
// walking pages covers every row exactly once in observed order.
func TestGetLabelersCursorPaginationNumeric(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	// DIDs deliberately out of lexical order; insertion order is the
	// observed order the walk must reproduce.
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
		// The cursor is the observed_seq of the page's last row and must
		// advance monotonically across pages.
		require.Greater(t, int64(next), cursor, "cursor must advance monotonically")
		cursor = int64(next)
	}

	require.Len(t, seen, 5)
	require.Equal(t, dids, seen,
		"pages must concatenate in observed order, which is not DID order here")
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

// TestQuery_GetLabelers_DefaultLimit verifies the default-50 page size and
// that the final page omits cursor while a full page carries the numeric
// observed_seq cursor.
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
	require.True(t, ok, "cursor must be a numeric observed_seq, got %T", body["cursor"])
	require.Equal(t, float64(50), cursor,
		"cursor must be the observed_seq of the page's last row")

	body, status = fetchLabelerList(t, base+"?cursor=50")
	require.Equal(t, http.StatusOK, status)
	require.ElementsMatch(t, []string{"labelers"}, mapKeys(body),
		"final page must omit cursor")
	dids = labelerDIDs(body)
	require.Len(t, dids, 10)
	require.Equal(t, "did:plc:q51", dids[0])
	require.Equal(t, "did:plc:q60", dids[9])
}

// TestGetLabelerViewEmitsNullSeq verifies AC.6 for the single lookup: a
// never-ingested labeler's getLabeler response includes the last_upstream_seq
// key with JSON null, and an ingested labeler's is numeric.
func TestGetLabelerViewEmitsNullSeq(t *testing.T) {
	env := newQueryTestEnv(t)
	defer env.cleanup()

	seedLabeler(t, env, "did:plc:never", "never.example.com", "manual", true, nil)
	seedLabeler(t, env, "did:plc:ingested", "ingested.example.com", "manual", true, ptrInt64(9))

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:never")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Contains(t, body, "last_upstream_seq",
		"the last_upstream_seq key must always be present")
	require.Nil(t, body["last_upstream_seq"],
		"a never-ingested labeler must emit last_upstream_seq as null")
	require.ElementsMatch(t,
		[]string{"did", "endpoint", "source", "enabled", "last_upstream_seq"}, mapKeys(body))

	resp2, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:ingested")
	require.NoError(t, err)
	defer resp2.Body.Close()
	var numeric map[string]any
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&numeric))
	require.Equal(t, float64(9), numeric["last_upstream_seq"])
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

	seedLabeler(t, env, "did:plc:one", "one.example.com", "manual", true, ptrInt64(42))

	resp, err := http.Get(env.ts.URL + "/xrpc/community.labeler.sync.getLabeler?did=did:plc:one")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "did:plc:one", body["did"])
	require.Equal(t, "one.example.com", body["endpoint"])
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

// TestQueryLexiconContract verifies AC.5: the lexicon files exist, parse as
// JSON, and declare the integer cursor (param and response) and the
// required+nullable last_upstream_seq consistent with the handlers' actual
// output.
func TestQueryLexiconContract(t *testing.T) {
	type schemaNode struct {
		Type        string                `json:"type"`
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
		"the cursor param is a deliberately non-opaque integer (observed_seq)")

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
		"the response cursor is the numeric observed_seq of the page's last row")

	// The shared labelerView lives in the defs document, not in either
	// query lexicon.
	defsID, sharedDefs := readLexicon("../../lexicons/community/labeler/sync/defs.json")
	require.Equal(t, "community.labeler.sync.defs", defsID)
	require.NotContains(t, sharedDefs, "main", "the defs document carries only shared objects, not a query")
	var view schemaNode
	require.NoError(t, json.Unmarshal(sharedDefs["labelerView"], &view))
	require.Equal(t, "object", view.Type)
	require.ElementsMatch(t, []string{"did", "endpoint", "source", "enabled", "last_upstream_seq"}, view.Required,
		"labelerView required fields must match the handler's always-emitted keys")
	require.NotNil(t, view.Nullable, "last_upstream_seq must be declared nullable")
	require.ElementsMatch(t, []string{"last_upstream_seq"}, view.Nullable,
		"last_upstream_seq is emitted as null for labelers that have never been observed")
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
		"community.labeler.sync.defs#labelerView",
		singleMain.Output.Schema.Ref)
}
