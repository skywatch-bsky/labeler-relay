package store

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeNow is a simple injectable clock: call advance() to move time forward.
type fakeNow struct {
	ms int64
}

func (f *fakeNow) nowMs() int64 {
	return f.ms
}

func (f *fakeNow) advance(d time.Duration) {
	f.ms += d.Milliseconds()
}

// TestPruneJobTickDeletesOldEventsAndAdvancesFloor verifies AC6.1:
// events older than the window are deleted and the retention floor advances.
//
// Time is injected via fakeNow — no real time.Sleep for the window.
func TestPruneJobTickDeletesOldEventsAndAdvancesFloor(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(s)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	clock := &fakeNow{ms: 10_000}

	window := 5 * time.Second // 5 000 ms

	job := NewPruneJob(persist, window, time.Hour, clock.nowMs, log)

	// Insert events at known timestamps by bypassing PersistIngest so we own ingest_ts.
	// t=3000 → "old" (falls before cutoff = 10000 - 5000 = 5000)
	insertEventAt(t, s, ctx, "labels", 3_000)
	// t=4000 → "old"
	insertEventAt(t, s, ctx, "labels", 4_000)
	// t=6000 → "young" (survives)
	insertEventAt(t, s, ctx, "labels", 6_000)
	// t=9000 → "young"
	insertEventAt(t, s, ctx, "labels", 9_000)

	// Run one tick: cutoff = 10000 - 5000 = 5000, so events at 3000 and 4000 are deleted.
	job.tick(ctx)

	// Two old events pruned.
	floor, err := persist.RetentionFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), floor, "floor should advance to first surviving event (relay_seq=3)")

	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), head, "head should remain at relay_seq=4")

	var surviving []int64
	err = persist.PlaybackFrames(ctx, 0, func(le LiveEvent) error {
		surviving = append(surviving, le.RelaySeq)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, surviving, "only events at ts 6000 and 9000 should survive")
}

// TestPruneJobTickDeletesBothKinds verifies AC6.2: both "labels" and "service"
// events older than the window are pruned.
func TestPruneJobTickDeletesBothKinds(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(s)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))

	clock := &fakeNow{ms: 10_000}
	window := 5 * time.Second

	job := NewPruneJob(persist, window, time.Hour, clock.nowMs, log)

	// Old events: one of each kind.
	insertEventKindAt(t, s, ctx, "labels", 2_000)
	insertEventKindAt(t, s, ctx, "service", 3_000)
	// Young event: survives.
	insertEventKindAt(t, s, ctx, "labels", 7_000)

	job.tick(ctx)

	floor, err := persist.RetentionFloor(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), floor, "floor should be the one surviving event")

	head, err := persist.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), head, "head matches sole surviving event")
}

// TestPruneJobRunCancelsCleanly verifies that Run returns context.Canceled on
// ctx cancellation and does not hang.
func TestPruneJobRunCancelsCleanly(t *testing.T) {
	s, cleanup := testStore(t)
	defer cleanup()

	persist := NewLabelPersist(s)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	clock := &fakeNow{ms: 0}

	// Use a long interval so the ticker never fires; we only test cancellation.
	job := NewPruneJob(persist, time.Hour, time.Hour, clock.nowMs, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- job.Run(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("PruneJob.Run did not return after context cancellation")
	}
}

// insertEventAt inserts a raw event row with a specific ingest_ts (default kind="labels").
func insertEventAt(t *testing.T, s *Store, ctx context.Context, kind string, ingestTs int64) {
	t.Helper()
	_, err := s.DB().ExecContext(ctx,
		`INSERT INTO events(kind, labeler_did, upstream_seq, frame_cbor, ingest_ts)
		 VALUES (?, 'did:plc:test', NULL, x'00', ?)`,
		kind, ingestTs,
	)
	if err != nil {
		t.Fatalf("insertEventAt(%d): %v", ingestTs, err)
	}
}

// insertEventKindAt is an alias with an explicit kind for AC6.2 readability.
func insertEventKindAt(t *testing.T, s *Store, ctx context.Context, kind string, ingestTs int64) {
	t.Helper()
	insertEventAt(t, s, ctx, kind, ingestTs)
}
