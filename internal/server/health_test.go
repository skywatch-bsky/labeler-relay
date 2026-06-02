package server_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
	"github.com/stretchr/testify/require"
)

// testHealthServer builds a real httptest.Server mounting HandleHealth.
func testHealthServer(t *testing.T) (*httptest.Server, *store.LabelPersist, *store.LabelerRegistry, func()) {
	t.Helper()
	p, storeCleanup := testPersist(t)

	regStore, err := store.Open(t.TempDir() + "/reg.db")
	require.NoError(t, err)
	reg := store.NewLabelerRegistry(regStore)

	h := server.NewHub()
	p.SetBroadcaster(h.Broadcast)

	const retentionWindow = 7200
	srv := server.NewServer(h, p, reg, slog.Default(), retentionWindow)

	mux := http.NewServeMux()
	mux.HandleFunc("/_health", srv.HandleHealth)

	ts := httptest.NewServer(mux)
	return ts, p, reg, func() {
		ts.Close()
		storeCleanup()
		regStore.Close()
	}
}

// healthBody is used to decode the JSON health response.
type healthBody struct {
	HeadSeq                int64 `json:"head_seq"`
	LabelerCount           int   `json:"labeler_count"`
	RetentionFloor         int64 `json:"retention_floor"`
	RetentionWindowSeconds int64 `json:"retention_window_seconds"`
}

func getHealth(t *testing.T, ts *httptest.Server) healthBody {
	t.Helper()
	resp, err := http.Get(ts.URL + "/_health")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body healthBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	return body
}

// TestHealth_AC10_3_ReportsHeadSeqLabelerCountAndRetention verifies AC10.3:
// GET /_health returns 200 JSON with head_seq, labeler_count, retention_floor,
// and retention_window_seconds.
func TestHealth_AC10_3_ReportsHeadSeqLabelerCountAndRetention(t *testing.T) {
	ts, p, reg, cleanup := testHealthServer(t)
	defer cleanup()

	ctx := context.Background()

	// Initially: no events, no labelers.
	body := getHealth(t, ts)
	require.Equal(t, int64(0), body.HeadSeq, "head_seq must be 0 with no events")
	require.Equal(t, 0, body.LabelerCount, "labeler_count must be 0 with no labelers")
	require.Equal(t, int64(0), body.RetentionFloor, "retention_floor must be 0 with no events")
	require.Equal(t, int64(7200), body.RetentionWindowSeconds)

	// Persist some events.
	seq1, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", 0x01))
	require.NoError(t, err)
	seq2, err := p.PersistIngest(ctx, labelsEvent("did:plc:b", 0x02))
	require.NoError(t, err)
	seq3, err := p.PersistIngest(ctx, serviceEvent("did:plc:c"))
	require.NoError(t, err)
	_ = seq1
	_ = seq2

	// Register 2 labelers.
	err = reg.Upsert(ctx, store.Labeler{
		DID:       "did:plc:labeler1",
		Endpoint:  "https://labeler1.example.com",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: time.Now().Unix(),
	})
	require.NoError(t, err)
	err = reg.Upsert(ctx, store.Labeler{
		DID:       "did:plc:labeler2",
		Endpoint:  "https://labeler2.example.com",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: time.Now().Unix(),
	})
	require.NoError(t, err)

	body = getHealth(t, ts)
	require.Equal(t, seq3, body.HeadSeq, "head_seq must equal the latest relay_seq")
	require.Equal(t, 2, body.LabelerCount, "labeler_count must equal number of registered labelers")
	require.Equal(t, int64(1), body.RetentionFloor, "retention_floor must equal the oldest event seq")
	require.Equal(t, int64(7200), body.RetentionWindowSeconds)

	// Prune first 2 events — floor should advance.
	cutoff := time.Now().UnixMilli() + 1000
	_, newFloor, err := p.Prune(ctx, cutoff)
	require.NoError(t, err)
	// After pruning all 3 events, floor = 0 (no events). Persist a new one.
	_ = newFloor

	seq4, err := p.PersistIngest(ctx, labelsEvent("did:plc:a", 0x03))
	require.NoError(t, err)

	body = getHealth(t, ts)
	require.Equal(t, seq4, body.HeadSeq, "head_seq must equal new latest event")
	require.Equal(t, seq4, body.RetentionFloor, "retention_floor must equal new oldest event after prune")
}

// TestHealth_EmptyStore verifies the health endpoint handles an empty store gracefully.
func TestHealth_EmptyStore(t *testing.T) {
	ts, _, _, cleanup := testHealthServer(t)
	defer cleanup()

	body := getHealth(t, ts)
	require.Equal(t, int64(0), body.HeadSeq)
	require.Equal(t, 0, body.LabelerCount)
	require.Equal(t, int64(0), body.RetentionFloor)
	require.Equal(t, int64(7200), body.RetentionWindowSeconds)
}
