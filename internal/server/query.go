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
	"time"

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
// id is monotonic. createdAt uses RFC3339 UTC.
// updatedAt is present exactly when declared_at is set, not the internal updated_at timestamp.
// declarations is always non-null and uses [] for an empty set.
// last_upstream_seq is always present, with null for labelers that have never ingested.
// source, last_error, require_sig, and internal updated_at are not public fields.
type labelerView struct {
	ID              int64    `json:"id"`
	DID             string   `json:"did"`
	CreatedAt       string   `json:"createdAt"`
	UpdatedAt       *string  `json:"updatedAt,omitempty"`
	Endpoint        string   `json:"endpoint"`
	Enabled         bool     `json:"enabled"`
	LastUpstreamSeq *int64   `json:"last_upstream_seq"`
	Declarations    []string `json:"declarations"`
}

// newLabelerView normalizes nil declarations so JSON emits [], not null.
func newLabelerView(l store.Labeler, declarations []string) labelerView {
	if declarations == nil {
		declarations = []string{}
	}
	v := labelerView{
		ID:              l.ID,
		DID:             l.DID,
		CreatedAt:       formatDatetime(l.CreatedAt),
		Endpoint:        l.Endpoint,
		Enabled:         l.Enabled,
		LastUpstreamSeq: l.LastUpstreamSeq,
		Declarations:    declarations,
	}
	if l.DeclaredAt != nil {
		updatedAt := formatDatetime(*l.DeclaredAt)
		v.UpdatedAt = &updatedAt
	}
	return v
}

// formatDatetime uses whole-second RFC3339 UTC for the public datetime contract.
func formatDatetime(sec int64) string {
	return time.Unix(sec, 0).UTC().Format(time.RFC3339)
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

// getLabelersResponse includes the last row's id as Cursor only when more pages exist.
// IDs start at 1, so omitempty can use zero for an absent cursor.
type getLabelersResponse struct {
	Labelers []labelerView `json:"labelers"`
	Cursor   int64         `json:"cursor,omitempty"`
}

// HandleGetLabelers serves GET /xrpc/community.labeler.sync.getLabelers.
//
// Params: limit (integer, default 50, rejected outside 1-100), cursor (the
// id of the last item of the previous page; keyset pagination over the
// id-ordered list), enabled (literal true/false filter; absent returns all).
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

	var cursor int64
	hasCursor := false
	if raw := query.Get("cursor"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "cursor must be a non-negative integer")
			return
		}
		cursor = parsed
		hasCursor = true
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

	// Read rows and declarations from one SQLite snapshot to prevent mixed-state views.
	labelers, declarations, err := s.registry.ListWithDeclarations(ctx)
	if err != nil {
		s.log.Error("query: failed to read labelers and declarations", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}

	// Filter the id-ordered snapshot in memory to keep one read path.
	filtered := make([]store.Labeler, 0, len(labelers))
	for _, l := range labelers {
		if enabledFilter == nil || l.Enabled == *enabledFilter {
			filtered = append(filtered, l)
		}
	}

	// Keyset pagination uses the row id. A cursor beyond the filtered rows ends the walk without an error.
	start := 0
	if hasCursor {
		for start < len(filtered) && filtered[start].ID <= cursor {
			start++
		}
	}
	end := start + limit
	if end > len(filtered) {
		end = len(filtered)
	}

	views := make([]labelerView, 0, end-start)
	for _, l := range filtered[start:end] {
		views = append(views, newLabelerView(l, declarations[l.DID]))
	}

	resp := getLabelersResponse{Labelers: views}
	if end < len(filtered) {
		resp.Cursor = filtered[end-1].ID
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

	// Read the row and declarations from one SQLite snapshot to prevent a mixed-state view.
	labeler, declarations, found, err := s.registry.GetWithDeclarations(ctx, did.String())
	if err != nil {
		s.log.Error("query: failed to read labeler and declarations", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	if !found {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "labeler not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(newLabelerView(labeler, declarations)); err != nil {
		s.log.Error("query: failed to encode response", "err", err)
	}
}
