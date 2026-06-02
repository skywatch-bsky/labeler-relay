package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scarndp/labeler-relay/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDIDResolver implements firehose.DIDResolver for testing.
type fakeDIDResolver struct {
	endpoints map[string]string
	errors    map[string]error
}

func (f *fakeDIDResolver) LabelerEndpoint(ctx context.Context, did string) (string, error) {
	if err, ok := f.errors[did]; ok {
		return "", err
	}
	endpoint, ok := f.endpoints[did]
	if !ok {
		return "", fmt.Errorf("unknown did: %s", did)
	}
	return endpoint, nil
}

func setupTestEnv(t *testing.T) (*store.Store, *API, string) {
	tmpDir := t.TempDir()
	s, err := store.Open(tmpDir + "/test.db")
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	testToken := "test-token-abc123"
	pokeCount := 0
	pokeFn := func() { pokeCount++ }

	resolver := &fakeDIDResolver{
		endpoints: map[string]string{
			"did:plc:labeler1": "https://labeler1.example.com/labels",
			"did:plc:labeler2": "https://labeler2.example.com/labels",
		},
	}

	api := &API{
		registry: store.NewLabelerRegistry(s),
		resolver: resolver,
		poke:     pokeFn,
		token:    testToken,
	}

	return s, api, testToken
}

func TestAdminAPI(t *testing.T) {
	t.Run("AC8.1: POST /admin/labelers creates labeler with source=manual", func(t *testing.T) {
		_, api, token := setupTestEnv(t)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		body := bytes.NewBufferString(`{"did":"did:plc:labeler1"}`)
		req := httptest.NewRequest("POST", "/admin/labelers", body)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusCreated, rec.Code)

		// Assert registry row exists with source=manual and enabled=1
		labeler, found, err := api.registry.Get(context.Background(), "did:plc:labeler1")
		require.NoError(t, err)
		require.True(t, found, "labeler should be in registry")
		assert.Equal(t, "did:plc:labeler1", labeler.DID)
		assert.Equal(t, "https://labeler1.example.com/labels", labeler.Endpoint)
		assert.Equal(t, "manual", labeler.Source)
		assert.True(t, labeler.Enabled)
	})

	t.Run("AC8.2: DELETE /admin/labelers/{did} disables labeler", func(t *testing.T) {
		_, api, token := setupTestEnv(t)

		// Manually add a labeler first
		ctx := context.Background()
		err := api.registry.Upsert(ctx, store.Labeler{
			DID:      "did:plc:labeler1",
			Endpoint: "https://labeler1.example.com/labels",
			Source:   "manual",
			Enabled:  true,
			UpdatedAt: time.Now().Unix(),
		})
		require.NoError(t, err)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		req := httptest.NewRequest("DELETE", "/admin/labelers/did:plc:labeler1", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNoContent, rec.Code)

		// Assert labeler is now disabled
		labeler, found, err := api.registry.Get(ctx, "did:plc:labeler1")
		require.NoError(t, err)
		require.True(t, found)
		assert.False(t, labeler.Enabled)
	})

	t.Run("AC8.3: request without token returns 401", func(t *testing.T) {
		_, api, _ := setupTestEnv(t)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		req := httptest.NewRequest("POST", "/admin/labelers", nil)
		// No Authorization header

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("AC8.4: manual stickiness against firehose upsert", func(t *testing.T) {
		_, api, token := setupTestEnv(t)
		ctx := context.Background()

		// Update resolver to include the test labeler
		resolver := &fakeDIDResolver{
			endpoints: map[string]string{
				"did:plc:labeler1":       "https://labeler1.example.com/labels",
				"did:plc:labeler2":       "https://labeler2.example.com/labels",
				"did:plc:labeler-sticky": "https://sticky.example.com/labels",
			},
		}
		api.resolver = resolver

		// Step 1: Manually add labeler X via API
		handler := api.Routes()
		rec := httptest.NewRecorder()

		body := bytes.NewBufferString(`{"did":"did:plc:labeler-sticky"}`)
		req := httptest.NewRequest("POST", "/admin/labelers", body)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusCreated, rec.Code)

		// Verify X is in registry with source=manual
		x, found, err := api.registry.Get(ctx, "did:plc:labeler-sticky")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "manual", x.Source)
		assert.True(t, x.Enabled)

		// Step 2: Simulate firehose upsert for X (discovery churn).
		// This should NOT change source from manual or toggle enabled.
		err = api.registry.Upsert(ctx, store.Labeler{
			DID:      "did:plc:labeler-sticky",
			Endpoint: "https://updated-endpoint.example.com/labels",
			Source:   "firehose",
			Enabled:  true,
			UpdatedAt: time.Now().Unix(),
		})
		require.NoError(t, err)

		// Assert X's source is STILL manual, enabled is STILL true
		x, found, err = api.registry.Get(ctx, "did:plc:labeler-sticky")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "manual", x.Source, "source should remain manual after firehose upsert")
		assert.True(t, x.Enabled, "enabled should remain true after firehose upsert")

		// Step 3: Simulate a firehose "delete" by calling SetEnabled(false).
		// AC8.4 says manual entries should NOT be auto-disabled.
		// However, the actual watcher logic (Task 3) would only disable if source=firehose.
		// Here we verify the stickiness is enforced by the registry's ON CONFLICT rule,
		// which keeps source and enabled untouched on conflict.
		// The next upsert from firehose with Enabled:false should NOT flip the enabled bit.

		err = api.registry.Upsert(ctx, store.Labeler{
			DID:      "did:plc:labeler-sticky",
			Endpoint: "https://updated-endpoint.example.com/labels",
			Source:   "firehose",
			Enabled:  false, // Simulate discovery deleting the record
			UpdatedAt: time.Now().Unix(),
		})
		require.NoError(t, err)

		// Assert enabled is STILL true (stickiness preserved)
		x, found, err = api.registry.Get(ctx, "did:plc:labeler-sticky")
		require.NoError(t, err)
		require.True(t, found)
		assert.True(t, x.Enabled, "enabled should remain true even if firehose tries to disable (manual stickiness)")
		assert.Equal(t, "manual", x.Source, "source should remain manual")
	})

	t.Run("GET /admin/labelers returns list as JSON", func(t *testing.T) {
		_, api, token := setupTestEnv(t)
		ctx := context.Background()

		// Add two labelers
		err := api.registry.Upsert(ctx, store.Labeler{
			DID:      "did:plc:labeler1",
			Endpoint: "https://labeler1.example.com/labels",
			Source:   "manual",
			Enabled:  true,
			UpdatedAt: time.Now().Unix(),
		})
		require.NoError(t, err)

		err = api.registry.Upsert(ctx, store.Labeler{
			DID:      "did:plc:labeler2",
			Endpoint: "https://labeler2.example.com/labels",
			Source:   "firehose",
			Enabled:  false,
			UpdatedAt: time.Now().Unix(),
		})
		require.NoError(t, err)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		req := httptest.NewRequest("GET", "/admin/labelers", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		var labelers []store.Labeler
		err = json.NewDecoder(rec.Body).Decode(&labelers)
		require.NoError(t, err)
		assert.Equal(t, 2, len(labelers))
		assert.Equal(t, "did:plc:labeler1", labelers[0].DID)
		assert.Equal(t, "did:plc:labeler2", labelers[1].DID)
	})

	t.Run("POST /admin/labelers/{did}/enable toggles enabled", func(t *testing.T) {
		_, api, token := setupTestEnv(t)
		ctx := context.Background()

		// Add a disabled labeler
		err := api.registry.Upsert(ctx, store.Labeler{
			DID:      "did:plc:labeler1",
			Endpoint: "https://labeler1.example.com/labels",
			Source:   "manual",
			Enabled:  false,
			UpdatedAt: time.Now().Unix(),
		})
		require.NoError(t, err)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		req := httptest.NewRequest("POST", "/admin/labelers/did:plc:labeler1/enable", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNoContent, rec.Code)

		// Assert labeler is now enabled
		labeler, found, err := api.registry.Get(ctx, "did:plc:labeler1")
		require.NoError(t, err)
		require.True(t, found)
		assert.True(t, labeler.Enabled)
	})

	t.Run("POST /admin/labelers/{did}/disable toggles disabled", func(t *testing.T) {
		_, api, token := setupTestEnv(t)
		ctx := context.Background()

		// Add an enabled labeler
		err := api.registry.Upsert(ctx, store.Labeler{
			DID:      "did:plc:labeler1",
			Endpoint: "https://labeler1.example.com/labels",
			Source:   "manual",
			Enabled:  true,
			UpdatedAt: time.Now().Unix(),
		})
		require.NoError(t, err)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		req := httptest.NewRequest("POST", "/admin/labelers/did:plc:labeler1/disable", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNoContent, rec.Code)

		// Assert labeler is now disabled
		labeler, found, err := api.registry.Get(ctx, "did:plc:labeler1")
		require.NoError(t, err)
		require.True(t, found)
		assert.False(t, labeler.Enabled)
	})

	t.Run("POST /admin/labelers without token returns 401", func(t *testing.T) {
		_, api, _ := setupTestEnv(t)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		body := bytes.NewBufferString(`{"did":"did:plc:labeler1"}`)
		req := httptest.NewRequest("POST", "/admin/labelers", body)
		// No Authorization header

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("invalid DID in POST returns error", func(t *testing.T) {
		_, api, token := setupTestEnv(t)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		body := bytes.NewBufferString(`{"did":"did:plc:unknown-labeler"}`)
		req := httptest.NewRequest("POST", "/admin/labelers", body)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		handler.ServeHTTP(rec, req)

		// Should get 400 Bad Request or similar error response
		assert.Greater(t, rec.Code, 299) // Any error code
	})

	t.Run("malformed JSON in POST returns 400", func(t *testing.T) {
		_, api, token := setupTestEnv(t)

		handler := api.Routes()
		rec := httptest.NewRecorder()

		body := bytes.NewBufferString(`{invalid json}`)
		req := httptest.NewRequest("POST", "/admin/labelers", body)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
