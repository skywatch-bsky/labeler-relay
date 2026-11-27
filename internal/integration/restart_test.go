package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/scarndp/labeler-relay/internal/store"
)

// Reopening reruns migrations, which must preserve IDs and their allocation order.
// Unlike TestResumeSlurper, this test closes the database. Its users have no background goroutines.
func TestIDAndCreatedAtSurviveRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dbPath := filepath.Join(t.TempDir(), "restart.db")

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	registry := store.NewLabelerRegistry(s)
	_ = store.NewLabelPersist(s) // mirrors production wiring; no goroutines

	// DIDs deliberately out of lexical order so observed order is
	// distinguishable from DID order.
	dids := []string{"did:plc:zzz", "did:plc:mmm", "did:plc:aaa"}
	for i, did := range dids {
		if err := registry.Upsert(ctx, store.Labeler{
			DID:       did,
			Endpoint:  "https://example.com",
			Source:    "firehose",
			Enabled:   true,
			UpdatedAt: int64(1000 + i),
		}); err != nil {
			t.Fatalf("Upsert %s: %v", did, err)
		}
	}

	list1, err := registry.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list1) != 3 {
		t.Fatalf("expected 3 labelers, got %d", len(list1))
	}
	before := make(map[string]int64, len(list1))
	for i, l := range list1 {
		if l.DID != dids[i] {
			t.Fatalf("expected list order %v, got %v", dids, didOrder(list1))
		}
		if l.ID != int64(i+1) {
			t.Fatalf("expected %s id=%d, got %d", l.DID, i+1, l.ID)
		}
		before[l.DID] = l.ID
	}

	// Crash-restart boundary: shut down every database user, close the store.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen the same file: store.Open re-runs the migration.
	s2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	registry2 := store.NewLabelerRegistry(s2)
	_ = store.NewLabelPersist(s2)

	list2, err := registry2.List(ctx)
	if err != nil {
		t.Fatalf("List after reopen: %v", err)
	}
	if len(list2) != 3 {
		t.Fatalf("expected 3 labelers after reopen, got %d", len(list2))
	}
	for _, l := range list2 {
		if before[l.DID] != l.ID {
			t.Errorf("%s id changed across restart: before=%d after=%d",
				l.DID, before[l.DID], l.ID)
		}
	}
	for i, l := range list2 {
		if l.DID != dids[i] {
			t.Errorf("observed order changed across restart: want %v, got %v", dids, didOrder(list2))
			break
		}
	}

	// A labeler registered after reopening mints above the previous maximum.
	if err := registry2.Upsert(ctx, store.Labeler{
		DID:       "did:plc:post-restart",
		Endpoint:  "https://example.com",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: 9000,
	}); err != nil {
		t.Fatalf("post-restart Upsert: %v", err)
	}
	got, exists, err := registry2.Get(ctx, "did:plc:post-restart")
	if err != nil || !exists {
		t.Fatalf("Get post-restart: exists=%v err=%v", exists, err)
	}
	var maxBefore int64
	for _, v := range before {
		if v > maxBefore {
			maxBefore = v
		}
	}
	if got.ID <= maxBefore {
		t.Errorf("post-restart id=%d must exceed the previous maximum %d",
			got.ID, maxBefore)
	}
}

// didOrder extracts the DID slice from a labeler list for error messages.
func didOrder(labelers []store.Labeler) []string {
	dids := make([]string, 0, len(labelers))
	for _, l := range labelers {
		dids = append(dids, l.DID)
	}
	return dids
}
