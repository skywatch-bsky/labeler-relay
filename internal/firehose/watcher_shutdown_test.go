package firehose

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/gorilla/websocket"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	car "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/scarndp/labeler-relay/internal/store"
)

// buildLabelerCommitCAR builds a real atproto repo CAR holding one
// app.bsky.labeler.service record per rkey and a signed commit, and returns
// the commit event that carries it: every record's op in one commit.
func buildLabelerCommitCAR(t *testing.T, did string, rkeys []string) *comatproto.SyncSubscribeRepos_Commit {
	t.Helper()
	ctx := context.Background()
	prefix := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256)

	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
	tree := mst.NewEmptyTree()
	recordCIDs := make([]cid.Cid, len(rkeys))
	for i, rkey := range rkeys {
		rec := &bsky.LabelerService{LexiconTypeID: "app.bsky.labeler.service", CreatedAt: "2026-01-01T00:00:00Z"}
		var recBuf bytes.Buffer
		if err := rec.MarshalCBOR(&recBuf); err != nil {
			t.Fatalf("marshal record %d: %v", i, err)
		}
		c, err := prefix.Sum(recBuf.Bytes())
		if err != nil {
			t.Fatalf("record cid %d: %v", i, err)
		}
		blk, err := blocks.NewBlockWithCid(recBuf.Bytes(), c)
		if err != nil {
			t.Fatalf("record block %d: %v", i, err)
		}
		if err := bs.Put(ctx, blk); err != nil {
			t.Fatalf("put record block %d: %v", i, err)
		}
		if _, err := tree.Insert([]byte("app.bsky.labeler.service/"+rkey), c); err != nil {
			t.Fatalf("mst insert %d: %v", i, err)
		}
		recordCIDs[i] = c
	}
	root, err := tree.WriteDiffBlocks(ctx, bs)
	if err != nil {
		t.Fatalf("write mst blocks: %v", err)
	}
	keys, err := bs.AllKeysChan(ctx)
	if err != nil {
		t.Fatalf("mst keys: %v", err)
	}
	type carBlock struct {
		c    cid.Cid
		data []byte
	}
	var all []carBlock
	for k := range keys {
		c := cid.NewCidV1(cid.DagCBOR, k.Hash())
		blk, err := bs.Get(ctx, c)
		if err != nil {
			t.Fatalf("get mst block %s: %v", c, err)
		}
		all = append(all, carBlock{c: c, data: blk.RawData()})
	}

	priv, err := atcrypto.GeneratePrivateKeyP256()
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	commit := repo.Commit{DID: did, Version: repo.ATPROTO_REPO_VERSION, Data: *root, Rev: string(syntax.NewTIDClock(0).Next())}
	if err := commit.Sign(priv); err != nil {
		t.Fatalf("sign commit: %v", err)
	}
	var commitBuf bytes.Buffer
	if err := commit.MarshalCBOR(&commitBuf); err != nil {
		t.Fatalf("marshal commit: %v", err)
	}
	commitCID, err := prefix.Sum(commitBuf.Bytes())
	if err != nil {
		t.Fatalf("commit cid: %v", err)
	}
	all = append(all, carBlock{c: commitCID, data: commitBuf.Bytes()})

	var out bytes.Buffer
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{commitCID}, Version: 1}, &out); err != nil {
		t.Fatalf("car header: %v", err)
	}
	for _, b := range all {
		if err := carutil.LdWrite(&out, b.c.Bytes(), b.data); err != nil {
			t.Fatalf("car write block: %v", err)
		}
	}

	ops := make([]*comatproto.SyncSubscribeRepos_RepoOp, len(rkeys))
	for i, rkey := range rkeys {
		ops[i] = &comatproto.SyncSubscribeRepos_RepoOp{Action: "create", Path: "app.bsky.labeler.service/" + rkey, Cid: (*lexutil.LexLink)(&recordCIDs[i])}
	}
	return &comatproto.SyncSubscribeRepos_Commit{
		Repo:   did,
		Seq:    10,
		Rev:    commit.Rev,
		Commit: lexutil.LexLink(commitCID),
		Ops:    ops,
		Blocks: out.Bytes(),
	}
}

// gatedResolver answers every call at once except the gateAt-th, which it
// holds until its context ends: a DID lookup still in flight when the process
// is told to stop.
type gatedResolver struct {
	mu      sync.Mutex
	gateAt  int
	calls   int
	entered chan struct{}
}

func (r *gatedResolver) LabelerEndpoint(ctx context.Context, did string) (string, error) {
	r.mu.Lock()
	r.calls++
	n := r.calls
	r.mu.Unlock()
	if n != r.gateAt {
		return "http://127.0.0.1:1" + subscribeLabelsPath, nil
	}
	select {
	case <-r.entered:
	default:
		close(r.entered)
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func (r *gatedResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// replayingFirehose is a subscribeRepos endpoint with one commit. It records
// the cursor of every dial and sends the commit on each connection whose
// cursor is below it, then holds the connection open.
type replayingFirehose struct {
	srv    *httptest.Server
	mu     sync.Mutex
	cursor []string
	commit *comatproto.SyncSubscribeRepos_Commit
}

func newReplayingFirehose(t *testing.T, commit *comatproto.SyncSubscribeRepos_Commit) *replayingFirehose {
	t.Helper()
	f := &replayingFirehose{commit: commit}
	upgrader := websocket.Upgrader{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		f.mu.Lock()
		f.cursor = append(f.cursor, cursor)
		f.mu.Unlock()

		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		if seq, err := strconv.ParseInt(cursor, 10, 64); err == nil && seq >= f.commit.Seq {
			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					return
				}
			}
		}
		var buf bytes.Buffer
		cw := cbg.NewCborWriter(&buf)
		if err := (&stream.EventHeader{Op: stream.EvtKindMessage, MsgType: "#commit"}).MarshalCBOR(cw); err != nil {
			return
		}
		if err := f.commit.MarshalCBOR(cw); err != nil {
			return
		}
		if err := ws.WriteMessage(websocket.BinaryMessage, buf.Bytes()); err != nil {
			return
		}
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *replayingFirehose) URL() string {
	return "ws://" + f.srv.Listener.Addr().String() + "/xrpc/com.atproto.sync.subscribeRepos"
}

// dials lists the cursor of every dial so far, in order.
func (f *replayingFirehose) dials() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cursor...)
}

// awaitHead waits until the store holds want events.
func awaitHead(t *testing.T, persist *store.LabelPersist, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		head, err := persist.Head(context.Background())
		if err != nil {
			t.Fatalf("read head: %v", err)
		}
		if head == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("head = %d; want %d", head, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestShutdownMidCommitReplaysCommit checks that a commit interrupted by a
// graceful shutdown is not counted as processed. The watcher must leave its
// cursor at the last fully processed commit so that the restart replays the
// interrupted one, instead of resuming after it and silently dropping the
// labeler ops that never ran.
//
// The first case is the shape a real repository produces: the labeler service
// record is a singleton at rkey "self", so a commit carries one labeler op.
// The second shows that the loss does not depend on the record shape.
func TestShutdownMidCommitReplaysCommit(t *testing.T) {
	cases := []struct {
		name   string
		rkeys  []string
		gateAt int
	}{
		{"single-op-self-rkey", []string{"self"}, 1},
		{"two-ops-one-commit", []string{"a", "b"}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			did := "did:plc:shutdownmidcommittest"
			fh := newReplayingFirehose(t, buildLabelerCommitCAR(t, did, tc.rkeys))

			st, err := store.Open(t.TempDir() + "/relay.db")
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer st.Close()
			registry := store.NewLabelerRegistry(st)
			persist := store.NewLabelPersist(st)
			resolver := &gatedResolver{gateAt: tc.gateAt, entered: make(chan struct{})}

			// The previous commit was fully processed and its cursor is durable.
			if err := st.SetMeta(ctx, "firehose_cursor", "9"); err != nil {
				t.Fatalf("seed cursor: %v", err)
			}

			w := NewFirehoseWatcher(fh.URL(), registry, persist, resolver, st, func() {}, true,
				slog.New(slog.NewTextHandler(io.Discard, nil)))
			// Thresholds that cannot fire: only the flush on the way out can move the cursor.
			w.SetCursorFlushThresholds(1_000_000, time.Hour)

			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- w.Run(runCtx) }()

			select {
			case <-resolver.entered:
			case <-time.After(10 * time.Second):
				cancel()
				t.Fatalf("the resolver was never asked for labeler op %d", tc.gateAt)
			}
			// The ops before it completed; this one is in flight. Stop the process.
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run returned %v; want context.Canceled", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("Run did not return after cancellation")
			}

			got, found, err := st.GetMeta(ctx, "firehose_cursor")
			if err != nil || !found {
				t.Fatalf("read cursor: found=%v err=%v", found, err)
			}
			if got != "9" {
				row, rowFound, _ := registry.Get(ctx, did)
				head, _ := persist.Head(ctx)
				t.Fatalf("after a shutdown with labeler op %d of %d still in flight, firehose_cursor = %q; want \"9\", the last fully processed commit. "+
					"With 10 the restart resumes after this commit and the op is never replayed. (labeler row found=%v enabled=%v last_error=%q, head=%d, resolver calls=%d)",
					tc.gateAt, len(tc.rkeys), got, rowFound, row.Enabled, row.LastError, head, resolver.callCount())
			}

			// Restart over the same store with a resolver that answers at once.
			// The firehose is asked for everything after 9, the commit replays,
			// and every labeler op in it completes: the ops that completed
			// before the stop are stored again, and the interrupted one joins
			// them.
			restarted := NewFirehoseWatcher(fh.URL(), registry, persist, &gatedResolver{}, st, func() {}, true,
				slog.New(slog.NewTextHandler(io.Discard, nil)))
			restarted.SetCursorFlushThresholds(1_000_000, time.Hour)
			restartCtx, stopRestart := context.WithCancel(ctx)
			restartDone := make(chan error, 1)
			go func() { restartDone <- restarted.Run(restartCtx) }()
			defer func() {
				stopRestart()
				select {
				case err := <-restartDone:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("restarted Run returned %v; want context.Canceled", err)
					}
				case <-time.After(10 * time.Second):
					t.Errorf("restarted Run did not return after cancellation")
				}
			}()
			awaitHead(t, persist, int64(tc.gateAt-1+len(tc.rkeys)))

			row, found, err := registry.Get(ctx, did)
			if err != nil || !found || !row.Enabled || row.LastError != "" || row.Endpoint != "http://127.0.0.1:1"+subscribeLabelsPath {
				t.Fatalf("after the restart the labeler row is missing or wrong: found=%v err=%v row=%+v", found, err, row)
			}
			if dials := fh.dials(); len(dials) != 2 || dials[0] != "9" || dials[1] != "9" {
				t.Fatalf("firehose dials carried cursors %q; want [\"9\" \"9\"]", dials)
			}
		})
	}
}
