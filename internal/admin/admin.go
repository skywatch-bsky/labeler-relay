// pattern: Imperative Shell

package admin

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/scarndp/labeler-relay/internal/firehose"
	"github.com/scarndp/labeler-relay/internal/store"
)

// API provides authenticated HTTP handlers for labeler registry management.
type API struct {
	registry *store.LabelerRegistry
	resolver firehose.DIDResolver
	poke     func()
	token    string
}

// NewAPI constructs an API.
func NewAPI(registry *store.LabelerRegistry, resolver firehose.DIDResolver, poke func(), token string) *API {
	return &API{
		registry: registry,
		resolver: resolver,
		poke:     poke,
		token:    token,
	}
}

// Routes returns an http.Handler mounting all admin endpoints.
func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()

	// POST /admin/labelers — add a labeler
	mux.Handle("POST /admin/labelers", RequireBearer(a.token, http.HandlerFunc(a.handlePostLabeler)))

	// DELETE /admin/labelers/{did} — disable a labeler
	mux.Handle("DELETE /admin/labelers/{did}", RequireBearer(a.token, http.HandlerFunc(a.handleDeleteLabeler)))

	// GET /admin/labelers — list all labelers
	mux.Handle("GET /admin/labelers", RequireBearer(a.token, http.HandlerFunc(a.handleGetLabelers)))

	// POST /admin/labelers/{did}/enable — enable a labeler
	mux.Handle("POST /admin/labelers/{did}/enable", RequireBearer(a.token, http.HandlerFunc(a.handleEnableLabeler)))

	// POST /admin/labelers/{did}/disable — disable a labeler
	mux.Handle("POST /admin/labelers/{did}/disable", RequireBearer(a.token, http.HandlerFunc(a.handleDisableLabeler)))

	// PATCH /admin/labelers/{did} — update per-labeler settings (require_sig)
	mux.Handle("PATCH /admin/labelers/{did}", RequireBearer(a.token, http.HandlerFunc(a.handlePatchLabeler)))

	return mux
}

type addLabelerRequest struct {
	DID string `json:"did"`
}

// handlePostLabeler adds a new labeler.
func (a *API) handlePostLabeler(w http.ResponseWriter, r *http.Request) {
	var req addLabelerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
		return
	}

	if req.DID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "did required"})
		return
	}

	// Resolve endpoint from DID.
	endpoint, err := a.resolver.LabelerEndpoint(r.Context(), req.DID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to resolve endpoint: " + err.Error()})
		return
	}

	// Upsert with source=manual.
	labeler := store.Labeler{
		DID:       req.DID,
		Endpoint:  endpoint,
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: time.Now().Unix(),
	}

	if err := a.registry.Upsert(r.Context(), labeler); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to add labeler"})
		return
	}

	// Notify slurper to reconcile.
	a.poke()

	// Echo the persisted row so the response reflects actual registry state.
	persisted, found, err := a.registry.Get(r.Context(), req.DID)
	if err != nil || !found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to read back labeler"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(persisted)
}

// handlePatchLabeler updates per-labeler settings. The only supported field is
// require_sig: true/false sets the override, an explicit null clears it back
// to the global default. The field must be present -- an empty body is a 400,
// not a silent clear.
func (a *API) handlePatchLabeler(w http.ResponseWriter, r *http.Request) {
	did := r.PathValue("did")

	var fields map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
		return
	}

	rawSig, ok := fields["require_sig"]
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "require_sig required (true, false, or null)"})
		return
	}

	var requireSig *bool
	if err := json.Unmarshal(rawSig, &requireSig); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "require_sig must be true, false, or null"})
		return
	}

	if err := a.registry.SetRequireSig(r.Context(), did, requireSig); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "labeler not found"})
		return
	}

	// Notify slurper to reconcile: a sig-policy change restarts the subscription.
	a.poke()

	// Echo the persisted row so the response reflects actual registry state.
	persisted, found, err := a.registry.Get(r.Context(), did)
	if err != nil || !found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to read back labeler"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(persisted)
}

// handleDeleteLabeler disables a labeler.
func (a *API) handleDeleteLabeler(w http.ResponseWriter, r *http.Request) {
	did := r.PathValue("did")

	if err := a.registry.SetEnabled(r.Context(), did, false); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "labeler not found"})
		return
	}

	// Notify slurper to reconcile.
	a.poke()

	w.WriteHeader(http.StatusNoContent)
}

// handleGetLabelers returns all labelers as JSON.
func (a *API) handleGetLabelers(w http.ResponseWriter, r *http.Request) {
	labelers, err := a.registry.List(r.Context())
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to list labelers"})
		return
	}

	if labelers == nil {
		labelers = []store.Labeler{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(labelers)
}

// handleEnableLabeler enables a labeler.
func (a *API) handleEnableLabeler(w http.ResponseWriter, r *http.Request) {
	did := r.PathValue("did")

	if err := a.registry.SetEnabled(r.Context(), did, true); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "labeler not found"})
		return
	}

	// Notify slurper to reconcile.
	a.poke()

	w.WriteHeader(http.StatusNoContent)
}

// handleDisableLabeler disables a labeler.
func (a *API) handleDisableLabeler(w http.ResponseWriter, r *http.Request) {
	did := r.PathValue("did")

	if err := a.registry.SetEnabled(r.Context(), did, false); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "labeler not found"})
		return
	}

	// Notify slurper to reconcile.
	a.poke()

	w.WriteHeader(http.StatusNoContent)
}
