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
// last_error, require_sig, and the internal updated_at bookkeeping timestamp
// are deliberately excluded. last_upstream_seq is always emitted: null for
// labelers that have never ingested, numeric otherwise. updatedAt is the
// relay-observation timestamp (declared_at), not internal bookkeeping: it is
// present only when a declaration was observed. declarations is always
// present and non-null; an empty declaration set serializes as [].
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

// newLabelerView projects a registry row and its declaration collections to
// the public view. declarations must be non-nil for the []-not-null JSON
// emission; the helper normalizes nil to an empty slice.
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

// formatDatetime renders unix seconds as an atproto datetime: RFC 3339, UTC,
// whole-second precision, uppercase T and Z.
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

// getLabelersResponse is the JSON body returned by HandleGetLabelers.
// Cursor (the last row's id) is present only when further pages exist; ids
// start at 1, so 0 reliably means "absent" under omitempty.
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

	labelers, err := s.registry.List(ctx)
	if err != nil {
		s.log.Error("query: failed to list labelers", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}

	declarations, err := s.registry.ListDeclarations(ctx)
	if err != nil {
		s.log.Error("query: failed to list declarations", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}

	// List returns rows in ascending id order (first-observed first); apply
	// the enabled filter in memory to keep a single read path.
	filtered := make([]store.Labeler, 0, len(labelers))
	for _, l := range labelers {
		if enabledFilter == nil || l.Enabled == *enabledFilter {
			filtered = append(filtered, l)
		}
	}

	// Keyset pagination: skip rows at or before the cursor id, then take one
	// page. A cursor past the current maximum (or past a range whose rows
	// were later disabled by the filter) yields an empty or partial page with
	// no cursor — a clean end-of-walk, never an error.
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
		// labelerView serializes the row's id, but the response cursor comes
		// from the store rows, not the views.
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

	declarations, err := s.registry.DeclarationsFor(ctx, did.String())
	if err != nil {
		s.log.Error("query: failed to read declarations", "err", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(newLabelerView(labeler, declarations)); err != nil {
		s.log.Error("query: failed to encode response", "err", err)
	}
}
