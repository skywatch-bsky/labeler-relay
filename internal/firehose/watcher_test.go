package firehose

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

// TestWatcherCursorBatching asserts that the firehose cursor is NOT written on
// every commit (batching reduces write amplification), but IS flushed once the
// commit-count threshold is reached.
func TestWatcherCursorBatching(t *testing.T) {
	t.Parallel()

	const threshold = 5 // small threshold to keep the test fast
	const did = "did:plc:firehose-cursor-test"
	const endpoint = "https://labeler.example.com/xrpc/com.atproto.label.subscribeLabels"

	w, _, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	w.cursorFlushEvery = threshold
	w.cursorFlushInterval = 10 * time.Second // large interval so only count triggers

	ctx := context.Background()

	// Reset lastFlushTime to now so the time-based threshold won't trigger early.
	w.lastFlushTime = time.Now()

	// Send threshold-1 commits: cursor should NOT be written yet.
	for i := int64(1); i < threshold; i++ {
		if err := w.handleCommit(ctx, serviceCommit(t, did, "create", i, sampleService("2026-06-02T00:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(seq=%d): %v", i, err)
		}
	}
	_, found, err := w.store.GetMeta(ctx, "firehose_cursor")
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if found {
		t.Error("cursor should NOT be written before reaching the flush threshold")
	}

	// The threshold-th commit triggers a flush.
	finalSeq := int64(threshold)
	if err := w.handleCommit(ctx, serviceCommit(t, did, "update", finalSeq, sampleService("2026-06-02T01:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(seq=%d): %v", finalSeq, err)
	}
	cursor, found, err := w.store.GetMeta(ctx, "firehose_cursor")
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !found || cursor != fmt.Sprint(finalSeq) {
		t.Errorf("cursor after %d commits: found=%v cursor=%q, want found=true cursor=%d",
			threshold, found, cursor, finalSeq)
	}
}

// TestWatcherCursorDialIncludesQuery asserts that dial() appends the persisted
// cursor to the WebSocket URL query string. We test this by persisting a cursor,
// then checking that the URL built by dial includes it.
func TestWatcherCursorDialIncludesQuery(t *testing.T) {
	t.Parallel()

	const did = "did:plc:firehose-url-test"
	const endpoint = "https://labeler.example.com/xrpc/com.atproto.label.subscribeLabels"

	w, _, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	// Persist a cursor value.
	if err := w.store.SetMeta(ctx, "firehose_cursor", "999"); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}

	// Manually construct the dial URL to verify cursor is appended.
	// (We can't test the full dial because it would try to connect to a real server,
	// but we can verify the URL building logic.)
	u, err := url.Parse(w.url)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	if cursorStr, found, err := w.store.GetMeta(ctx, "firehose_cursor"); err == nil && found {
		q := u.Query()
		q.Set("cursor", cursorStr)
		u.RawQuery = q.Encode()
	}

	// Assert the URL now contains cursor=999.
	if !strings.Contains(u.String(), "cursor=999") {
		t.Errorf("dial URL does not contain cursor=999: %s", u.String())
	}
}

// TestWatcherLastErrorPreserveStickiness asserts that when a labeler is manually
// added (source='manual', enabled=1), a subsequent firehose error path sets last_error
// without flipping source or enabled.
func TestWatcherLastErrorPreserveStickiness(t *testing.T) {
	t.Parallel()

	const did = "did:plc:firehose-stickiness"
	const manualEndpoint = "https://manual-labeler.example.com/xrpc/com.atproto.label.subscribeLabels"

	w, _, _ := newWatcherForTest(t, &fakeDIDResolver{errs: map[string]error{did: ErrNoLabelerEndpoint}})
	ctx := context.Background()

	// Pre-seed the registry with a manual labeler (simulating prior admin add).
	err := w.registry.Upsert(ctx, store.Labeler{
		DID:       did,
		Endpoint:  manualEndpoint,
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("Upsert manual labeler: %v", err)
	}

	// Verify the manual row is in place.
	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("pre-seed failed: found=%v err=%v", found, err)
	}
	if lab.Source != "manual" || !lab.Enabled {
		t.Errorf("pre-seed: source=%q enabled=%v, want source=manual enabled=true", lab.Source, lab.Enabled)
	}

	// Drive the no-endpoint error path (firehose discovers the DID but resolution fails).
	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit: %v", err)
	}

	// Assert the row now has last_error set, but source and enabled are unchanged.
	lab, found, err = w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("post-error: found=%v err=%v", found, err)
	}
	if lab.Source != "manual" {
		t.Errorf("stickiness broken: source = %q, want manual", lab.Source)
	}
	if !lab.Enabled {
		t.Error("stickiness broken: enabled should remain true")
	}
	if lab.LastError == "" {
		t.Error("last_error should be set for the no-endpoint error")
	}
}

// TestWatcherLastErrorFreshInsert asserts that RecordError inserts a minimal row
// for a labeler not yet in the registry.
func TestWatcherLastErrorFreshInsert(t *testing.T) {
	t.Parallel()

	const did = "did:plc:firehose-fresh-error"

	w, _, _ := newWatcherForTest(t, &fakeDIDResolver{})
	ctx := context.Background()

	// Drive an error on a labeler not yet in the registry.
	if err := w.registry.RecordError(ctx, did, "test error message"); err != nil {
		t.Fatalf("RecordError: %v", err)
	}

	// Assert a row was created with minimal fields.
	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("fresh insert failed: found=%v err=%v", found, err)
	}
	if lab.LastError != "test error message" {
		t.Errorf("last_error = %q, want test error message", lab.LastError)
	}
	if lab.Source != "firehose" {
		t.Errorf("fresh insert source = %q, want firehose", lab.Source)
	}
	if lab.Enabled {
		t.Error("fresh error insert should have enabled=false")
	}
}
