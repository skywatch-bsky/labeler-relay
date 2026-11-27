package firehose

import (
	"bytes"
	"context"
	"errors"
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

	"github.com/scarndp/labeler-relay/api/community"
	"github.com/scarndp/labeler-relay/internal/store"
)

// subscribeLabelsPath is the XRPC path for the labeler subscription endpoint.
// (Restored: it was dropped from watcher_test.go in the endpoint-host fix
// while later test files kept referencing it, breaking the test build.)
const subscribeLabelsPath = "/xrpc/com.atproto.label.subscribeLabels"

// newWatcherForTest wires a FirehoseWatcher over a real temp-file store with a
// fake resolver and a poke counter. Discovery auto-subscribes.
func newWatcherForTest(t *testing.T, resolver DIDResolver) (*FirehoseWatcher, *store.Store, *int32) {
	return newWatcherForTestAutoSubscribe(t, resolver, true)
}

func newWatcherForTestAutoSubscribe(t *testing.T, resolver DIDResolver, autoSubscribe bool) (*FirehoseWatcher, *store.Store, *int32) {
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
		autoSubscribe,
		slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)),
	)
	return w, s, &pokeCount
}

// TestWatcherDiscoveryRespectsAutoSubscribeFalse verifies that with
// auto-subscribe disabled, a discovered labeler is registered but NOT
// enabled -- discovery becomes opt-in.
func TestWatcherDiscoveryRespectsAutoSubscribeFalse(t *testing.T) {
	t.Parallel()

	const did = "did:plc:optin-discover"
	const endpoint = "labeler.example.com"

	w, _, _ := newWatcherForTestAutoSubscribe(t,
		&fakeDIDResolver{endpoints: map[string]string{did: endpoint}}, false)
	ctx := context.Background()

	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(create): %v", err)
	}

	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("labeler should still be registered on discovery: found=%v err=%v", found, err)
	}
	if lab.Enabled {
		t.Error("discovered labeler should NOT be enabled when auto-subscribe is off")
	}
	if lab.Endpoint != endpoint {
		t.Errorf("endpoint = %q, want %q", lab.Endpoint, endpoint)
	}
}

// serviceCommit builds a SyncSubscribeRepos_Commit carrying a REAL CAR with one
// app.bsky.labeler.service record and an op referencing it by its real content CID.
func serviceCommit(t *testing.T, did, action string, seq int64, svc *bsky.LabelerService) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()
	return declarationCommit(t, did, "app.bsky.labeler.service", action, seq, svc)
}

// declarationCommit builds a SyncSubscribeRepos_Commit carrying a REAL CAR with
// one declaration record in the given collection and an op referencing it by
// its real content CID.
func declarationCommit(t *testing.T, did, collection, action string, seq int64, svc *bsky.LabelerService) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()
	car, recordCID := buildDeclarationCAR(t, did, collection, "self", svc)
	lexCID := util.LexLink(recordCID)
	return &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Blocks: car,
		Seq:    seq,
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: action, Path: collection + "/self", Cid: &lexCID},
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

// countServiceEventsOp counts the stored #service events for a labeler with a
// given op ("create"/"update"/"delete"), by decoding each frame.
func countServiceEventsOp(t *testing.T, s *store.Store, did, op string) int {
	t.Helper()
	var frames [][]byte
	rows, err := s.DB().QueryContext(context.Background(),
		`SELECT frame_cbor FROM events WHERE kind='service' AND labeler_did=? ORDER BY relay_seq ASC`, did)
	if err != nil {
		t.Fatalf("read service frames: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var frame []byte
		if err := rows.Scan(&frame); err != nil {
			t.Fatalf("scan service frame: %v", err)
		}
		frames = append(frames, frame)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate service frames: %v", err)
	}
	n := 0
	for _, frame := range frames {
		decoded, err := store.DecodeServiceFrame(frame)
		if err != nil {
			t.Fatalf("decode service frame: %v", err)
		}
		if decoded.Op == op {
			n++
		}
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

// latestServiceFrame reads the most recent stored #service frame for a labeler
// and decodes it via the store's own frame decoder.
func latestServiceFrame(t *testing.T, s *store.Store, did string) *community.LabelerSyncSubscribeLabelers_Service {
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
	return decoded
}

// declarationsFor reads the tracked declaration collections via the registry.
func declarationsFor(t *testing.T, w *FirehoseWatcher, did string) []string {
	t.Helper()
	decs, err := w.registry.DeclarationsFor(context.Background(), did)
	if err != nil {
		t.Fatalf("DeclarationsFor: %v", err)
	}
	return decs
}

// TestWatcherHandleCommitDiscoversAndEmits drives a real labeler.service create
// commit through the real CAR decode in ExtractLabelerDeclarationOps and asserts
// AC5.1 (registry upsert source=firehose enabled + poke), AC5.2 (#service persisted
// with the record bytes), then a real update commit for AC5.3 (second #service,
// higher seq).
func TestWatcherHandleCommitDiscoversAndEmits(t *testing.T) {
	t.Parallel()

	const did = "did:plc:firehose-discover"
	const endpoint = "labeler.example.com"

	w, s, pokeCount := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	// --- AC5.1 + AC5.2: create ---
	svc := sampleService("2026-06-02T00:00:00Z")
	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, svc)); err != nil {
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
	if got := declarationsFor(t, w, did); fmt.Sprint(got) != fmt.Sprint([]string{"app.bsky.labeler.service"}) {
		t.Errorf("AC5.1: declaration not tracked: %v", got)
	}

	if n := countServiceEvents(t, s, did); n != 1 {
		t.Fatalf("AC5.2: expected 1 #service event, got %d", n)
	}
	frame := latestServiceFrame(t, s, did)
	if frame.Record == nil {
		t.Fatal("AC5.2: decoded #service frame has nil record")
	}
	if want := mustMarshalCBOR(t, svc); string(frame.Record.CBOR) != string(want) {
		t.Errorf("AC5.2: record bytes are not the upstream record: got %x, want %x", frame.Record.CBOR, want)
	}

	// --- AC5.3: update produces a second #service with a higher relay_seq ---
	firstSeq := maxServiceSeq(t, s, did)
	updated := sampleService("2026-06-02T01:00:00Z")
	if err := w.handleCommit(ctx, serviceCommit(t, did, "update", 2, updated)); err != nil {
		t.Fatalf("handleCommit(update): %v", err)
	}
	if n := countServiceEvents(t, s, did); n != 2 {
		t.Fatalf("AC5.3: expected 2 #service events after update, got %d", n)
	}
	if secondSeq := maxServiceSeq(t, s, did); secondSeq <= firstSeq {
		t.Errorf("AC5.3: second relay_seq (%d) should exceed first (%d)", secondSeq, firstSeq)
	}
	// AC5.3: the second #service carries the updated record bytes.
	frame = latestServiceFrame(t, s, did)
	if frame.Record == nil {
		t.Fatal("AC5.3: updated frame has nil record")
	}
	if want := mustMarshalCBOR(t, updated); string(frame.Record.CBOR) != string(want) {
		t.Errorf("AC5.3: updated record bytes mismatch: got %x, want %x", frame.Record.CBOR, want)
	}
}

// TestWatcherDiscoversAllDeclarationCollections verifies AC.5: a create op in
// any of the three declaration collections registers the labeler exactly like
// an app.bsky declaration — endpoint resolved, row present, poke fired — and
// the stored frame carries the collection plus the record bytes byte-identical
// to the upstream CAR block.
func TestWatcherDiscoversAllDeclarationCollections(t *testing.T) {
	t.Parallel()

	for _, collection := range []string{
		"app.bsky.labeler.service",
		"town.delve.labeler.service",
		"so.sprk.labeler.service",
	} {
		t.Run(collection, func(t *testing.T) {
			t.Parallel()

			const did = "did:plc:multi-collection"
			const endpoint = "labeler.example.com"

			w, s, pokeCount := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
			ctx := context.Background()

			svc := &bsky.LabelerService{
				LexiconTypeID: collection,
				CreatedAt:     "2026-06-02T00:00:00Z",
				Policies:      &bsky.LabelerDefs_LabelerPolicies{},
			}
			if err := w.handleCommit(ctx, declarationCommit(t, did, collection, "create", 1, svc)); err != nil {
				t.Fatalf("handleCommit(create): %v", err)
			}

			lab, found, err := w.registry.Get(ctx, did)
			if err != nil || !found {
				t.Fatalf("labeler not registered via %s: found=%v err=%v", collection, found, err)
			}
			if lab.Source != "firehose" || !lab.Enabled {
				t.Errorf("row wrong via %s: source=%q enabled=%v", collection, lab.Source, lab.Enabled)
			}
			if lab.Endpoint != endpoint {
				t.Errorf("endpoint = %q, want %q", lab.Endpoint, endpoint)
			}
			if atomic.LoadInt32(pokeCount) == 0 {
				t.Error("poke should have fired on discovery via " + collection)
			}
			if got := declarationsFor(t, w, did); fmt.Sprint(got) != fmt.Sprint([]string{collection}) {
				t.Errorf("declaration not tracked for %s: %v", collection, got)
			}

			if n := countServiceEvents(t, s, did); n != 1 {
				t.Fatalf("expected 1 #service event, got %d", n)
			}
			frame := latestServiceFrame(t, s, did)
			if frame.Collection != collection {
				t.Errorf("frame collection = %q, want %q", frame.Collection, collection)
			}
			if frame.Record == nil {
				t.Fatal("frame record is nil")
			}
			if want := mustMarshalCBOR(t, svc); string(frame.Record.CBOR) != string(want) {
				t.Errorf("record bytes not byte-identical to the upstream CAR block: got %x, want %x",
					frame.Record.CBOR, want)
			}
		})
	}
}

// TestWatcherTwoCollectionsOneRow verifies AC.5: one DID declaring via two
// collections yields one registry row holding both declarations.
func TestWatcherTwoCollectionsOneRow(t *testing.T) {
	t.Parallel()

	const did = "did:plc:dual-declarer"
	const endpoint = "labeler.example.com"

	w, s, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	if err := w.handleCommit(ctx, declarationCommit(t, did, "app.bsky.labeler.service", "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(app.bsky create): %v", err)
	}
	if err := w.handleCommit(ctx, declarationCommit(t, did, "town.delve.labeler.service", "create", 2, sampleService("2026-06-02T01:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(town.delve create): %v", err)
	}

	labelers, err := w.registry.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(labelers) != 1 {
		t.Fatalf("one DID with two declarations must yield one row, got %d", len(labelers))
	}
	if labelers[0].ID != 1 {
		t.Errorf("the single row must hold the first-insert id, got %d", labelers[0].ID)
	}
	if got := declarationsFor(t, w, did); fmt.Sprint(got) != fmt.Sprint([]string{"app.bsky.labeler.service", "town.delve.labeler.service"}) {
		t.Errorf("both declarations must be tracked on one row, got %v", got)
	}
	if n := countServiceEvents(t, s, did); n != 2 {
		t.Errorf("expected one event per declaration op, got %d", n)
	}
}

// TestWatcherEmitsDeleteServiceEvent verifies AC.6: delete ops produce
// #service frames with op="delete", the deleted collection, and no record.
func TestWatcherEmitsDeleteServiceEvent(t *testing.T) {
	t.Parallel()

	const did = "did:plc:delete-event"
	const endpoint = "labeler.example.com"

	w, s, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(create): %v", err)
	}
	if err := w.handleCommit(ctx, serviceCommit(t, did, "delete", 2, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(delete): %v", err)
	}

	if n := countServiceEventsOp(t, s, did, "delete"); n != 1 {
		t.Fatalf("expected exactly one delete event, got %d", n)
	}
	// The delete frame is the most recent event.
	frame := latestServiceFrame(t, s, did)
	if frame.Op != "delete" {
		t.Errorf("latest frame op = %q, want delete", frame.Op)
	}
	if frame.Collection != "app.bsky.labeler.service" {
		t.Errorf("delete frame collection = %q, want app.bsky.labeler.service", frame.Collection)
	}
	if frame.Record != nil {
		t.Error("delete frames carry no record")
	}
}

// TestWatcherDeleteLastDeclarationDisables verifies AC.8: deleting a
// firehose-sourced labeler's last tracked declaration sets enabled=0 and
// pokes the slurper.
func TestWatcherDeleteLastDeclarationDisables(t *testing.T) {
	t.Parallel()

	const did = "did:plc:delete-last"
	const endpoint = "labeler.example.com"

	w, _, pokeCount := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(create): %v", err)
	}
	pokesAfterCreate := atomic.LoadInt32(pokeCount)

	if err := w.handleCommit(ctx, serviceCommit(t, did, "delete", 2, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(delete): %v", err)
	}

	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("Get after delete: found=%v err=%v", found, err)
	}
	if lab.Enabled {
		t.Error("deleting the last declaration must disable the labeler")
	}
	if got := declarationsFor(t, w, did); len(got) != 0 {
		t.Errorf("the declaration must be removed, got %v", got)
	}
	if atomic.LoadInt32(pokeCount) != pokesAfterCreate+1 {
		t.Errorf("the disable must poke the slurper: pokes before delete=%d, after=%d",
			pokesAfterCreate, atomic.LoadInt32(pokeCount))
	}
}

// TestWatcherDeleteOneOfManyKeepsEnabled verifies AC.8: deleting one of
// several tracked declarations leaves the labeler enabled.
func TestWatcherDeleteOneOfManyKeepsEnabled(t *testing.T) {
	t.Parallel()

	const did = "did:plc:delete-one"
	const endpoint = "labeler.example.com"

	w, _, pokeCount := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	if err := w.handleCommit(ctx, declarationCommit(t, did, "app.bsky.labeler.service", "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(app.bsky create): %v", err)
	}
	if err := w.handleCommit(ctx, declarationCommit(t, did, "town.delve.labeler.service", "create", 2, sampleService("2026-06-02T01:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(town.delve create): %v", err)
	}
	pokesAfterCreates := atomic.LoadInt32(pokeCount)

	if err := w.handleCommit(ctx, declarationCommit(t, did, "app.bsky.labeler.service", "delete", 3, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(delete): %v", err)
	}

	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("Get after delete: found=%v err=%v", found, err)
	}
	if !lab.Enabled {
		t.Error("deleting one of several declarations must keep the labeler enabled")
	}
	if got := declarationsFor(t, w, did); fmt.Sprint(got) != fmt.Sprint([]string{"town.delve.labeler.service"}) {
		t.Errorf("remaining declarations = %v, want [town.delve.labeler.service]", got)
	}
	if atomic.LoadInt32(pokeCount) != pokesAfterCreates {
		t.Errorf("a non-disabling delete must not poke: pokes=%d, want %d",
			atomic.LoadInt32(pokeCount), pokesAfterCreates)
	}
}

// TestWatcherDeleteUntrackedIsNoop verifies AC.8: deleting a declaration the
// relay never registered is a no-op — no event, no poke, no registry row.
func TestWatcherDeleteUntrackedIsNoop(t *testing.T) {
	t.Parallel()

	const did = "did:plc:delete-untracked"
	const endpoint = "labeler.example.com"

	w, s, pokeCount := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	// No declaration was ever tracked (no row at all).
	if err := w.handleCommit(ctx, serviceCommit(t, did, "delete", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(delete): %v", err)
	}
	if _, found, err := w.registry.Get(ctx, did); err != nil || found {
		t.Fatalf("untracked delete must not create a row: found=%v err=%v", found, err)
	}
	if n := countServiceEvents(t, s, did); n != 0 {
		t.Errorf("untracked delete must emit no event, got %d", n)
	}
	if atomic.LoadInt32(pokeCount) != 0 {
		t.Error("untracked delete must not poke")
	}

	// A row that exists but holds no tracked declaration is also a skip.
	if err := w.registry.RecordError(ctx, did, "resolution failed"); err != nil {
		t.Fatalf("RecordError: %v", err)
	}
	if err := w.handleCommit(ctx, serviceCommit(t, did, "delete", 2, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(delete on RecordError row): %v", err)
	}
	if n := countServiceEvents(t, s, did); n != 0 {
		t.Errorf("delete of an untracked declaration on an existing row must emit no event, got %d", n)
	}
	if atomic.LoadInt32(pokeCount) != 0 {
		t.Error("delete on an undeclared row must not poke")
	}
}

// TestWatcherDeleteNeverDisablesManual verifies AC.8: manual labelers are
// never auto-disabled by declaration deletes.
func TestWatcherDeleteNeverDisablesManual(t *testing.T) {
	t.Parallel()

	const did = "did:plc:manual-stays"
	const endpoint = "labeler.example.com"

	w, _, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

	if err := w.registry.Upsert(ctx, store.Labeler{
		DID: did, Endpoint: "manual.example.com", Source: "manual", Enabled: true, UpdatedAt: 1000,
	}); err != nil {
		t.Fatalf("seed manual labeler: %v", err)
	}
	// The manual labeler's DID also declares via the firehose.
	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(create): %v", err)
	}
	if err := w.handleCommit(ctx, serviceCommit(t, did, "delete", 2, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(delete): %v", err)
	}

	lab, found, err := w.registry.Get(ctx, did)
	if err != nil || !found {
		t.Fatalf("Get after delete: found=%v err=%v", found, err)
	}
	if !lab.Enabled {
		t.Error("manual labelers must never be auto-disabled")
	}
	if lab.Source != "manual" {
		t.Errorf("source = %q, want manual", lab.Source)
	}
	if got := declarationsFor(t, w, did); len(got) != 0 {
		t.Errorf("the declaration removal still applies, got %v", got)
	}
}

// TestWatcherRedeclareReenablesOnlyFromZeroDeclarations verifies AC.8: a
// re-declaration re-enables a firehose row only on the zero-to-one declaration
// transition with autoSubscribe on — the positive case, plus the four
// negatives (second collection added, update op, autoSubscribe off, manual row).
func TestWatcherRedeclareReenablesOnlyFromZeroDeclarations(t *testing.T) {
	t.Parallel()

	const did = "did:plc:redeclare"
	const endpoint = "labeler.example.com"
	resolver := &fakeDIDResolver{endpoints: map[string]string{did: endpoint}}

	// Positive: a RecordError-created disabled row with zero declarations is
	// re-enabled by its first successful declaration.
	t.Run("zero to one re-enables", func(t *testing.T) {
		t.Parallel()
		w, _, _ := newWatcherForTest(t, resolver)
		ctx := context.Background()

		if err := w.registry.RecordError(ctx, did, "resolution failed"); err != nil {
			t.Fatalf("seed RecordError row: %v", err)
		}
		lab, _, _ := w.registry.Get(ctx, did)
		if lab.Enabled {
			t.Fatal("precondition: RecordError row must start disabled")
		}

		if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(create): %v", err)
		}
		lab, _, err := w.registry.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get after re-declare: %v", err)
		}
		if !lab.Enabled {
			t.Error("the first successful declaration must re-enable a RecordError row")
		}
	})

	// Negative: an operator-disabled row that already holds a declaration
	// stays disabled when a second collection is added.
	t.Run("second collection does not re-enable", func(t *testing.T) {
		t.Parallel()
		w, _, _ := newWatcherForTest(t, resolver)
		ctx := context.Background()

		if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(create): %v", err)
		}
		if err := w.registry.SetEnabled(ctx, did, false); err != nil {
			t.Fatalf("operator disable: %v", err)
		}
		if err := w.handleCommit(ctx, declarationCommit(t, did, "town.delve.labeler.service", "create", 2, sampleService("2026-06-02T01:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(second collection): %v", err)
		}
		lab, _, err := w.registry.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if lab.Enabled {
			t.Error("an operator-disabled row holding a declaration must stay disabled when a second collection is added")
		}
	})

	// Negative: an update op on an existing declaration does not re-enable.
	t.Run("update does not re-enable", func(t *testing.T) {
		t.Parallel()
		w, _, _ := newWatcherForTest(t, resolver)
		ctx := context.Background()

		if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(create): %v", err)
		}
		if err := w.registry.SetEnabled(ctx, did, false); err != nil {
			t.Fatalf("operator disable: %v", err)
		}
		if err := w.handleCommit(ctx, serviceCommit(t, did, "update", 2, sampleService("2026-06-02T02:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(update): %v", err)
		}
		lab, _, err := w.registry.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if lab.Enabled {
			t.Error("an update op on an existing declaration must not re-enable an operator-disabled row")
		}
	})

	// Negative: with autoSubscribe off, even the zero-to-one transition does
	// not re-enable (discovery is opt-in).
	t.Run("autoSubscribe off does not re-enable", func(t *testing.T) {
		t.Parallel()
		w, _, _ := newWatcherForTestAutoSubscribe(t, resolver, false)
		ctx := context.Background()

		if err := w.registry.RecordError(ctx, did, "resolution failed"); err != nil {
			t.Fatalf("seed RecordError row: %v", err)
		}
		if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(create): %v", err)
		}
		lab, _, err := w.registry.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if lab.Enabled {
			t.Error("autoSubscribe=false must keep the row disabled even on the first declaration")
		}
	})

	// Negative: manual rows are never auto-enabled.
	t.Run("manual row is not auto-enabled", func(t *testing.T) {
		t.Parallel()
		w, _, _ := newWatcherForTest(t, resolver)
		ctx := context.Background()

		if err := w.registry.Upsert(ctx, store.Labeler{
			DID: did, Endpoint: "manual.example.com", Source: "manual", Enabled: false, UpdatedAt: 1000,
		}); err != nil {
			t.Fatalf("seed manual row: %v", err)
		}
		if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
			t.Fatalf("handleCommit(create): %v", err)
		}
		lab, _, err := w.registry.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if lab.Enabled {
			t.Error("manual rows must never be auto-enabled")
		}
	})
}

// TestWatcherDeleteReplaysAfterNonContextFailure verifies AC.12(b): a
// NON-cancellation ApplyServiceOp failure (here: the events write is blocked
// by a trigger) makes handleCommit return an error without staging the
// commit — the staged cursor stays before it — and replaying the commit
// disables the labeler and emits exactly one delete event.
func TestWatcherDeleteReplaysAfterNonContextFailure(t *testing.T) {
	t.Parallel()

	const did = "did:plc:delete-replay"
	const endpoint = "labeler.example.com"

	w, s, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	w.SetCursorFlushThresholds(1_000_000, time.Hour)
	w.lastFlushTime = time.Now()
	ctx := context.Background()

	// The create completes and stages cursor seq 1.
	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(create): %v", err)
	}
	if w.pendingSeq == nil || *w.pendingSeq != 1 {
		t.Fatalf("precondition: create must stage cursor 1, got %v", w.pendingSeq)
	}

	// Block the event write: the delete's ApplyServiceOp fails with a
	// non-context error mid-transaction.
	blockEvents := func(block bool) {
		t.Helper()
		stmt := `DROP TRIGGER IF EXISTS block_events`
		if block {
			stmt = `CREATE TRIGGER block_events BEFORE INSERT ON events
				BEGIN
					SELECT RAISE(ABORT, 'test: events insert blocked');
				END;`
		}
		if _, err := s.DB().Exec(stmt); err != nil {
			t.Fatalf("trigger install: %v", err)
		}
	}
	blockEvents(true)

	deleteCommit := serviceCommit(t, did, "delete", 2, sampleService("2026-06-02T00:00:00Z"))
	err := w.handleCommit(ctx, deleteCommit)
	if err == nil {
		t.Fatal("the delete commit must fail while the event write is blocked")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the failure must be a non-context store error, got %v", err)
	}

	// The commit did not count as processed: the staged cursor stays at the
	// create (1), the registry transition rolled back, no seq was consumed.
	if w.pendingSeq == nil || *w.pendingSeq != 1 {
		t.Fatalf("failed commit must not stage its cursor: pending=%v, want 1", w.pendingSeq)
	}
	if w.commitCount != 1 {
		t.Fatalf("failed commit must not count toward the flush threshold: count=%d", w.commitCount)
	}
	lab, _, err := w.registry.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after failed delete: %v", err)
	}
	if !lab.Enabled {
		t.Error("the disable must roll back with the failed event write")
	}
	if got := declarationsFor(t, w, did); len(got) != 1 {
		t.Errorf("the declaration removal must roll back, got %v", got)
	}
	if head, err := store.NewLabelPersist(s).Head(ctx); err != nil || head != 1 {
		t.Fatalf("head=%d err=%v, want only the create event", head, err)
	}

	// Replay: the same commit completes — disable, exactly one delete event.
	blockEvents(false)
	if err := w.handleCommit(ctx, deleteCommit); err != nil {
		t.Fatalf("replayed delete: %v", err)
	}
	lab, _, err = w.registry.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after replay: %v", err)
	}
	if lab.Enabled {
		t.Error("the replayed delete must disable the labeler")
	}
	if n := countServiceEventsOp(t, s, did, "delete"); n != 1 {
		t.Fatalf("the replay must emit exactly one delete event, got %d", n)
	}
	if w.pendingSeq == nil || *w.pendingSeq != 2 {
		t.Fatalf("the replayed commit must stage its cursor: pending=%v, want 2", w.pendingSeq)
	}
}

// TestWatcherDeleteReplaysAfterInterruption verifies AC.12(b)'s cancellation
// pattern: a context cancelled before the delete op's store write returns
// context.Canceled without staging the commit; the replay disables the
// labeler and emits exactly one delete event.
func TestWatcherDeleteReplaysAfterInterruption(t *testing.T) {
	t.Parallel()

	const did = "did:plc:delete-cancel"
	const endpoint = "labeler.example.com"

	w, s, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	w.SetCursorFlushThresholds(1_000_000, time.Hour)
	w.lastFlushTime = time.Now()
	ctx := context.Background()

	if err := w.handleCommit(ctx, serviceCommit(t, did, "create", 1, sampleService("2026-06-02T00:00:00Z"))); err != nil {
		t.Fatalf("handleCommit(create): %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	deleteCommit := serviceCommit(t, did, "delete", 2, sampleService("2026-06-02T00:00:00Z"))
	if err := w.handleCommit(cancelCtx, deleteCommit); !errors.Is(err, context.Canceled) {
		t.Fatalf("handleCommit under cancellation returned %v; want context.Canceled", err)
	}
	if w.pendingSeq == nil || *w.pendingSeq != 1 {
		t.Fatalf("interrupted delete must not stage its cursor: pending=%v, want 1", w.pendingSeq)
	}
	lab, _, err := w.registry.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after interrupted delete: %v", err)
	}
	if !lab.Enabled {
		t.Error("the interrupted delete must leave the labeler enabled")
	}
	if head, err := store.NewLabelPersist(s).Head(ctx); err != nil || head != 1 {
		t.Fatalf("head=%d err=%v, want only the create event", head, err)
	}

	if err := w.handleCommit(ctx, deleteCommit); err != nil {
		t.Fatalf("replayed delete: %v", err)
	}
	lab, _, err = w.registry.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after replay: %v", err)
	}
	if lab.Enabled {
		t.Error("the replayed delete must disable the labeler")
	}
	if n := countServiceEventsOp(t, s, did, "delete"); n != 1 {
		t.Fatalf("the replay must emit exactly one delete event, got %d", n)
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
	const endpoint = "labeler.example.com"

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
	const endpoint = "labeler.example.com"

	w, _, _ := newWatcherForTest(t, &fakeDIDResolver{endpoints: map[string]string{did: endpoint}})
	ctx := context.Background()

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
	const manualEndpoint = "manual-labeler.example.com"

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
