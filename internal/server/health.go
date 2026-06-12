// pattern: Imperative Shell
// health.go implements the /_health endpoint that reports stream state and
// retention configuration for operational visibility (AC10.3).

package server

import (
	"encoding/json"
	"net/http"
)

// healthResponse is the JSON body returned by HandleHealth.
type healthResponse struct {
	HeadSeq                int64 `json:"head_seq"`
	LabelerCount           int   `json:"labeler_count"`
	RetentionFloor         int64 `json:"retention_floor"`
	RetentionWindowSeconds int64 `json:"retention_window_seconds"`
}

// HandleHealth reports the current stream head, labeler count, retention floor,
// and configured retention window. Returns 200 with a JSON body on success,
// 500 if any of the underlying store reads fail.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	head, err := s.persist.Head(ctx)
	if err != nil {
		s.log.Error("health: failed to read head", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	floor, err := s.persist.RetentionFloor(ctx)
	if err != nil {
		s.log.Error("health: failed to read retention floor", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	labelers, err := s.registry.List(ctx)
	if err != nil {
		s.log.Error("health: failed to list labelers", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp := healthResponse{
		HeadSeq:                head,
		LabelerCount:           len(labelers),
		RetentionFloor:         floor,
		RetentionWindowSeconds: s.retentionWindowSeconds,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.log.Error("health: failed to encode response", "err", err)
	}
}
