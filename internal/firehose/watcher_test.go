package firehose

import (
	"bytes"
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"

	"github.com/scarndp/labeler-relay/internal/store"
)

// newWatcherForTest wires a FirehoseWatcher over a real temp-file store with a
// fake resolver and a poke counter.
func newWatcherForTest(t *testing.T, resolver DIDResolver) (*FirehoseWatcher, *store.Store, *int32) {
	t.Helper()
	s, err := store.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var pokeCount int32
	w := NewFirehoseWatcher(
		"http://unused.example.com",
		store.NewLabelerRegistry(s),
		store.NewLabelPersist(s),
		resolver,
		s,
		func() { atomic.AddInt32(&pokeCount, 1) },
		slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)),
	)
	return w, s, &pokeCount
}

// serviceCommit builds a SyncSubscribeRepos_Commit carrying a REAL CAR with one
// app.bsky.labeler.service record and an op referencing it by its real content CID.
func serviceCommit(t *testing.T, did, action string, seq int64, svc *bsky.LabelerService) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()
	car, recordCID := buildLabelerServiceCAR(t, did, "self", svc)
	lexCID := util.LexLink(recordCID)
	return &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Seq:    seq,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: action, Path: "app.bsky.labeler.service/self", Cid: &lexCID},
		},
	}
}

func sampleService(createdAt string) *bsky.LabelerService {
	return &bsky.LabelerService{
		LexiconTypeID: "app.bsky.labeler.service",
		CreatedAt:     createdAt,
		Policies: &bsky.LabelerDefs_LabelerPolicies{
			LabelValues: []*string{strptr("spam"), strptr("nsfw")},
		},
	}
}

func strptr(s string) *string { return &s }

func countServiceEvents(t *testing.T, s *store.Store, did string) int {
	t.Helper()
	var n int
	if err := s.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM events WHERE kind='service' AND labeler_did=?`, did).Scan(&n); err != nil {
		t.Fatalf("count service events: %v", err)
	}
	return n
}

func maxServiceSeq(t *testing.T, s *store.Store, did string) int64 {
	t.Helper()
	var seq int64
	if err := s.DB().QueryRowContext(context.Background(),
		`SELECT COALESCE(MAX(relay_seq),0) FROM events WHERE kind='service' AND labeler_did=?`, did).Scan(&seq); err != nil {
		t.Fatalf("max service seq: %v", err)
	}
	return seq
}

// latestServiceRecord reads the most recent stored #service frame for a labeler
// and decodes the embedded record via the store's own frame decoder, exercising
// the decode→re-encode→decode field-fidelity path.
func latestServiceRecord(t *testing.T, s *store.Store, did string) *bsky.LabelerService {
	t.Helper()
	var frame []byte
	if err := s.DB().QueryRowContext(context.Background(),
		`SELECT frame_cbor FROM events WHERE kind='service' AND labeler_did=? ORDER BY relay_seq DESC LIMIT 1`,
		did).Scan(&frame); err != nil {
		t.Fatalf("read service frame: %v", err)
	}
	decoded, err := store.DecodeServiceFrame(frame)
	if err != nil {
		t.Fatalf("decode service frame: %v", err)
	}
	if decoded.Record == nil {
		t.Fatal("decoded #service frame has nil record")
	}
	return decoded.Record
}

// TestWatcherHandleCommitDiscoversAndEmits drives a real labeler.service create
// commit through the real CAR decode in ExtractLabelerServiceOps and asserts
// AC5.1 (registry upsert source=firehose enabled + poke), AC5.2 (#service persisted
// with field fidelity), then a real update commit for AC5.3 (second #service, higher seq).
func TestWatcherHandleCommitDiscoversAndEmits(t *testing.T) {
	t.Parallel()

	const did = "did:plc:firehose-discover"
	const endpoint = "https://labeler.example.com/xrpc/com.atproto.label.subscribeLabels"

	w, s, pokeCount := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	// --- AC5.1 + AC5.2: create ---
	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(create): %v", err)
	}

	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("AC5.1: labeler not registered: found=%v err=%v", found, err)
	}
	if lab.Source != "firehose" {
		t.Errorf("AC5.1: source = %q, want firehose", lab.Source)
	}
	if !lab.Enabled {
		t.Error("AC5.1: labeler should be enabled")
	}
	if lab.Endpoint != endpoint {
		t.Errorf("AC5.1: endpoint = %q, want %q", lab.Endpoint, endpoint)
	}
	if atomic.LoadInt32(pokeCount) == 0 {
		t.Error("AC5.1: poke should have fired on discovery")
	}

	if n := countServiceEvents(t, s, did); n != 1 {
		t.Fatalf("AC5.2: expected 1 #service event, got %d", n)
	}
	rec := latestServiceRecord(t, s, did)
	if rec.CreatedAt != "2026-06-02T00:00:00Z" {
		t.Errorf("AC5.2: CreatedAt = %q, want 2026-06-02T00:00:00Z", rec.CreatedAt)
	}
	if rec.Policies == nil || len(rec.Policies.LabelValues) != 2 {
		t.Fatalf("AC5.2: Policies not round-tripped: %+v", rec.Policies)
	}
	if *rec.Policies.LabelValues[0] != "spam" || *rec.Policies.LabelValues[1] != "nsfw" {
		t.Errorf("AC5.2: LabelValues = [%s %s], want [spam nsfw]",
			*rec.Policies.LabelValues[0], *rec.Policies.LabelValues[1])
	}

	// --- AC5.3: update produces a second #service with a higher relay_seq ---
	firstSeq := maxServiceSeq(t, s, did)
	if err := w.handleCommit(ctx, serviceCommit(t, did, "update", 2, sampleService("2026-06-02T01:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(update): %v", err)
	}
	if n := countServiceEvents(t, s, did); n != 2 {
		t.Fatalf("AC5.3: expected 2 #service events after update, got %d", n)
	}
	if secondSeq := maxServiceSeq(t, s, did); secondSeq <= firstSeq {
		t.Errorf("AC5.3: second relay_seq (%d) should exceed first (%d)", secondSeq, firstSeq)
	}
	// AC5.3: the second #service carries the updated record.
	if rec := latestServiceRecord(t, s, did); rec.CreatedAt != "2026-06-02T01:00:00Z" {
		t.Errorf("AC5.3: updated record CreatedAt = %q, want 2026-06-02T01:00:00Z", rec.CreatedAt)
	}
}

// TestWatcherHandleCommitNoEndpoint asserts the ErrNoLabelerEndpoint path: the
// watcher records last_error, does NOT enable the labeler, emits no #service,
// does not poke, and does not crash.
func TestWatcherHandleCommitNoEndpoint(t *testing.T) {
	t.Parallel()

	const did = "did:plc:firehose-noendpoint"
	w, s, pokeCount := newWatcherForTest(t, &fakeDIDResolver{errs: map[string]error{did: ErrNoLabelerEndpoint}})
	ctx := context.Background()

	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit: %v", err)
	}

	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("expected a registry row recording the error: found=%v err=%v", found, err)
	}
	if lab.Enabled {
		t.Error("labeler with no endpoint should not be enabled")
	}
	if lab.LastError == "" {
		t.Error("expected last_error to be recorded for a labeler with no endpoint")
	}
	if n := countServiceEvents(t, s, did); n != 0 {
		t.Errorf("expected no #service events when endpoint resolution fails, got %d", n)
	}
	if atomic.LoadInt32(pokeCount) != 0 {
		t.Error("poke should not fire when the labeler is not enabled")
	}
}
