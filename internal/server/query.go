// pattern: Imperative Shell
// query.go implements the public XRPC query endpoints over the labeler
// registry: community.labeler.sync.getLabelers (paginated list with an
// optional enabled filter) and community.labeler.sync.getLabeler (single
// lookup by did). Responses are JSON, projected to the owner-approved public
// field set.

package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/scarndp/labeler-relay/internal/store"
)

const (
	// defaultQueryLimit matches the lexicon's declared default page size.
	defaultQueryLimit = 50
	// maxQueryLimit matches the lexicon's declared maximum page size.
	maxQueryLimit = 100
)

// labelerView is the public projection of a registry labeler row.
// last_error, require_sig, and updated_at are deliberately excluded.
// last_upstream_seq is omitted for labelers that have never ingested.
type labelerView struct {
	DID             string `json:"did"`
	Endpoint        string `json:"endpoint"`
	Source          string `json:"source"`
	Enabled         bool   `json:"enabled"`
	LastUpstreamSeq *int64 `json:"last_upstream_seq,omitempty"`
}

// newLabelerView projects a registry row to its public view.
func newLabelerView(l store.Labeler) labelerView {
	return labelerView{
		DID:             l.DID,
		Endpoint:        l.Endpoint,
		Source:          l.Source,
		Enabled:         l.Enabled,
		LastUpstreamSeq: l.LastUpstreamSeq,
	}
}

// xrpcErrorBody is the JSON shape XRPC requires on error responses:
// {"error": "<name>", "message": "<optional>"}.
type xrpcErrorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// writeXRPCError writes a JSON XRPC error body with the given status. Detailed
// causes are logged by callers; the body carries only the public message.
func writeXRPCError(w http.ResponseWriter, status int, name, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(xrpcErrorBody{Error: name, Message: msg})
}

// getLabelersResponse is the JSON body returned by HandleGetLabelers.
// Cursor is present only when further pages exist.
type getLabelersResponse struct {
	Labelers []labelerView `json:"labelers"`
	Cursor   string        `json:"cursor,omitempty"`
}

// HandleGetLabelers serves GET /xrpc/community.labeler.sync.getLabelers.
//
// Params: limit (integer, default 50, rejected outside 1-100), cursor (the
// did of the last item of the previous page; keyset pagination over the
// DID-ordered list), enabled (literal true/false filter; absent returns all).
func (s *Server) HandleGetLabelers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	query := r.URL.Query()

	limit := defaultQueryLimit
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 1 || parsed > maxQueryLimit {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "limit must be an integer between 1 and 100")
			return
		}
		limit = int(parsed)
	}

	var enabledFilter *bool
	if raw := query.Get("enabled"); raw != "" {
		switch raw {
		case "true":
			v := true
			enabledFilter = &v
		case "false":
			v := false
			enabledFilter = &v
		default:
			// The XRPC spec encodes booleans as the literal strings
			// true/false; anything else is a bad request.
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "enabled must be true or false")
			return
		}
	}

	labelers, err := s.registry.List(ctx)
	if err != nil {
		s.log.Error("query: failed to list labelers", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}

	// List returns rows in ascending DID order; apply the enabled filter in
	// memory to keep a single read path.
	filtered := make([]store.Labeler, 0, len(labelers))
	for _, l := range labelers {
		if enabledFilter == nil || l.Enabled == *enabledFilter {
			filtered = append(filtered, l)
		}
	}

	// Keyset pagination: skip rows at or before the cursor DID, then take one
	// page. Unknown or pruned DIDs degrade to the first page past the cursor.
	start := 0
	if cursor := query.Get("cursor"); cursor != "" {
		for start < len(filtered) && filtered[start].DID <= cursor {
			start++
		}
	}
	end := start + limit
	if end > len(filtered) {
		end = len(filtered)
	}

	views := make([]labelerView, 0, end-start)
	for _, l := range filtered[start:end] {
		views = append(views, newLabelerView(l))
	}

	resp := getLabelersResponse{Labelers: views}
	if end < len(filtered) {
		resp.Cursor = views[len(views)-1].DID
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.log.Error("query: failed to encode response", "err", err)
	}
}

// HandleGetLabeler serves GET /xrpc/community.labeler.sync.getLabeler.
// Param: did (required). Malformed and unknown DIDs both return 400
// InvalidRequest, matching the atproto record-lookup convention.
func (s *Server) HandleGetLabeler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	didParam := r.URL.Query().Get("did")
	if didParam == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "did is required")
		return
	}

	did, err := syntax.ParseDID(didParam)
	if err != nil {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "invalid did")
		return
	}

	labeler, found, err := s.registry.Get(ctx, did.String())
	if err != nil {
		s.log.Error("query: failed to read labeler", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	if !found {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "labeler not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(newLabelerView(labeler)); err != nil {
		s.log.Error("query: failed to encode response", "err", err)
	}
}
