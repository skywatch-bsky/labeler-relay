package firehose

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/scarndp/labeler-relay/internal/store"
)

type endpointResolverFunc func(context.Context, string) (string, error)

func (f endpointResolverFunc) LabelerEndpoint(ctx context.Context, did string) (string, error) {
	return f(ctx, did)
}

func TestHandleCommitCancellationDuringUpsert(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/relay.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	registry := store.NewLabelerRegistry(st)
	persist := store.NewLabelPersist(st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver := endpointResolverFunc(func(context.Context, string) (string, error) {
		cancel()
		return "http://127.0.0.1:1" + subscribeLabelsPath, nil
	})
	w := NewFirehoseWatcher("", registry, persist, resolver, st, func() {
		t.Error("interrupted commit must not poke")
	}, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.SetCursorFlushThresholds(1_000_000, time.Hour)
	w.lastFlushTime = time.Now()
	w.maybeFlushCursor(context.Background(), 9)

	did := "did:plc:cancelupserttest"
	err = w.handleCommit(ctx, buildLabelerCommitCAR(t, did, []string{"self"}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("handleCommit returned %v; want context.Canceled", err)
	}
	if _, found, err := registry.Get(context.Background(), did); err != nil || found {
		t.Fatalf("cancelled upsert: found=%v err=%v; want no registry row", found, err)
	}
	if head, err := persist.Head(context.Background()); err != nil || head != 0 {
		t.Fatalf("head=%d err=%v; want 0", head, err)
	}
	if w.pendingSeq == nil || *w.pendingSeq != 9 || w.commitCount != 1 {
		t.Fatalf("interrupted commit changed cursor batching: pending=%v count=%d", w.pendingSeq, w.commitCount)
	}
}

func TestHandleCommitCancellationDuringPersistIngest(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/relay.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	registry := store.NewLabelerRegistry(st)
	persist := store.NewLabelPersist(st)
	commit := buildLabelerCommitCAR(t, "did:plc:cancelpersisttest", []string{"self"})

	// A completed write's broadcaster holds PersistIngest's mutex while the
	// watcher finishes its upsert. Cancellation then precedes its transaction.
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	persist.SetBroadcaster(func(store.LiveEvent) {
		close(entered)
		<-release
	})
	seedDone := make(chan error, 1)
	go func() {
		_, err := persist.PersistIngest(context.Background(), store.IngestEvent{
			Kind: "service", LabelerDID: commit.Repo, Op: "create", Record: sampleService("2026-01-01T00:00:00Z"),
		})
		seedDone <- err
	}()
	defer func() {
		unblock()
		if err := <-seedDone; err != nil {
			t.Errorf("seed PersistIngest: %v", err)
		}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("seed did not reach broadcaster")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewFirehoseWatcher("", registry, persist, &gatedResolver{}, st, func() {
		t.Error("interrupted commit must not poke")
	}, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.SetCursorFlushThresholds(1_000_000, time.Hour)
	w.lastFlushTime = time.Now()
	w.maybeFlushCursor(context.Background(), 9)
	done := make(chan error, 1)
	go func() { done <- w.handleCommit(ctx, commit) }()
	joined := false
	defer func() {
		cancel()
		unblock()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("handleCommit did not return during cleanup")
			}
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, found, err := registry.Get(context.Background(), commit.Repo)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watcher did not finish registry upsert")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	unblock()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handleCommit returned %v; want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handleCommit did not return after cancellation")
	}
	if head, err := persist.Head(context.Background()); err != nil || head != 1 {
		t.Fatalf("head=%d err=%v; want only seed event", head, err)
	}
	if w.pendingSeq == nil || *w.pendingSeq != 9 || w.commitCount != 1 {
		t.Fatalf("interrupted commit changed cursor batching: pending=%v count=%d", w.pendingSeq, w.commitCount)
	}
}

func TestShutdownAfterCompletedCommitFlushesCursor(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/relay.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SetMeta(context.Background(), "firehose_cursor", "9"); err != nil {
		t.Fatal(err)
	}
	commit := buildLabelerCommitCAR(t, "did:plc:completedcommittest", []string{"self"})
	fh := newReplayingFirehose(t, commit)
	persist := store.NewLabelPersist(st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// poke runs after both registry and service-event writes have succeeded.
	w := NewFirehoseWatcher(fh.URL(), store.NewLabelerRegistry(st), persist, &gatedResolver{}, st,
		cancel, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.SetCursorFlushThresholds(1_000_000, time.Hour)
	if err := w.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v; want context.Canceled", err)
	}
	if head, err := persist.Head(context.Background()); err != nil || head != 1 {
		t.Fatalf("head=%d err=%v; want 1", head, err)
	}
	if cursor, found, err := st.GetMeta(context.Background(), "firehose_cursor"); err != nil || !found || cursor != "10" {
		t.Fatalf("cursor=%q found=%v err=%v; want 10", cursor, found, err)
	}
}
