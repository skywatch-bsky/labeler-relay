package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/scarndp/labeler-relay/internal/store"
)

func TestRegistry(t *testing.T) {
	ctx := context.Background()

	t.Run("Upsert then Get round-trips all fields", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		// Insert a labeler
		requiredSig := true
		labeler := store.Labeler{
			DID:        "did:plc:test123",
			Endpoint:   "example.com",
			Source:     "manual",
			Enabled:    true,
			RequireSig: &requiredSig,
			LastError:  "test error",
			UpdatedAt:  1234567890000,
		}

		if err := reg.Upsert(ctx, labeler); err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}

		// Get it back
		retrieved, exists, err := reg.Get(ctx, labeler.DID)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !exists {
			t.Fatal("labeler not found")
		}

		// Verify all fields match
		if retrieved.DID != labeler.DID {
			t.Errorf("DID mismatch: expected %q, got %q", labeler.DID, retrieved.DID)
		}
		if retrieved.Endpoint != labeler.Endpoint {
			t.Errorf("Endpoint mismatch: expected %q, got %q", labeler.Endpoint, retrieved.Endpoint)
		}
		if retrieved.Source != labeler.Source {
			t.Errorf("Source mismatch: expected %q, got %q", labeler.Source, retrieved.Source)
		}
		if retrieved.Enabled != labeler.Enabled {
			t.Errorf("Enabled mismatch: expected %v, got %v", labeler.Enabled, retrieved.Enabled)
		}
		if (retrieved.RequireSig == nil) != (labeler.RequireSig == nil) {
			t.Errorf("RequireSig nil mismatch")
		}
		if retrieved.RequireSig != nil && *retrieved.RequireSig != *labeler.RequireSig {
			t.Errorf("RequireSig value mismatch")
		}
		if retrieved.LastError != labeler.LastError {
			t.Errorf("LastError mismatch: expected %q, got %q", labeler.LastError, retrieved.LastError)
		}
		if retrieved.UpdatedAt != labeler.UpdatedAt {
			t.Errorf("UpdatedAt mismatch: expected %d, got %d", labeler.UpdatedAt, retrieved.UpdatedAt)
		}
	})

	t.Run("List / ListEnabled return correct subsets after SetEnabled", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		// Insert three labelers
		labeler1 := store.Labeler{
			DID: "did:plc:test1", Source: "manual", Enabled: true, UpdatedAt: 1000,
		}
		labeler2 := store.Labeler{
			DID: "did:plc:test2", Source: "manual", Enabled: true, UpdatedAt: 2000,
		}
		labeler3 := store.Labeler{
			DID: "did:plc:test3", Source: "manual", Enabled: false, UpdatedAt: 3000,
		}

		if err := reg.Upsert(ctx, labeler1); err != nil {
			t.Fatalf("Upsert 1 failed: %v", err)
		}
		if err := reg.Upsert(ctx, labeler2); err != nil {
			t.Fatalf("Upsert 2 failed: %v", err)
		}
		if err := reg.Upsert(ctx, labeler3); err != nil {
			t.Fatalf("Upsert 3 failed: %v", err)
		}

		// List all
		all, err := reg.List(ctx)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(all) != 3 {
			t.Errorf("expected 3 labelers, got %d", len(all))
		}

		// List enabled
		enabled, err := reg.ListEnabled(ctx)
		if err != nil {
			t.Fatalf("ListEnabled failed: %v", err)
		}
		if len(enabled) != 2 {
			t.Errorf("expected 2 enabled labelers, got %d", len(enabled))
		}

		// Disable labeler1
		if err := reg.SetEnabled(ctx, labeler1.DID, false); err != nil {
			t.Fatalf("SetEnabled failed: %v", err)
		}

		// List enabled again
		enabled, err = reg.ListEnabled(ctx)
		if err != nil {
			t.Fatalf("ListEnabled failed: %v", err)
		}
		if len(enabled) != 1 {
			t.Errorf("expected 1 enabled labeler after disabling, got %d", len(enabled))
		}
		if enabled[0].DID != labeler2.DID {
			t.Errorf("expected labeler2 to be the only enabled, got %q", enabled[0].DID)
		}
	})

	t.Run("WriteCursor then ReadCursor returns the written seq", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		// Insert a labeler
		labeler := store.Labeler{
			DID: "did:plc:test", Source: "manual", Enabled: true, UpdatedAt: 1000,
		}
		if err := reg.Upsert(ctx, labeler); err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}

		// Initially should be nil
		seq, err := reg.ReadCursor(ctx, labeler.DID)
		if err != nil {
			t.Fatalf("ReadCursor failed: %v", err)
		}
		if seq != nil {
			t.Errorf("expected nil cursor initially, got %v", seq)
		}

		// Write a cursor
		testSeq := int64(12345)
		if err := reg.WriteCursor(ctx, labeler.DID, testSeq); err != nil {
			t.Fatalf("WriteCursor failed: %v", err)
		}

		// Read it back
		seq, err = reg.ReadCursor(ctx, labeler.DID)
		if err != nil {
			t.Fatalf("ReadCursor failed: %v", err)
		}
		if seq == nil {
			t.Fatal("expected non-nil cursor")
		}
		if *seq != testSeq {
			t.Errorf("expected cursor %d, got %d", testSeq, *seq)
		}
	})

	t.Run("Stickiness: firehose upsert does not downgrade manual source or flip enabled", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		// Insert a labeler from manual with enabled=true
		did := "did:plc:stickiness-test"
		manualLabeler := store.Labeler{
			DID:       did,
			Endpoint:  "example.com",
			Source:    "manual",
			Enabled:   true,
			UpdatedAt: 1000,
		}
		if err := reg.Upsert(ctx, manualLabeler); err != nil {
			t.Fatalf("initial Upsert failed: %v", err)
		}

		// Verify it's stored correctly
		retrieved, exists, err := reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !exists {
			t.Fatal("labeler not found")
		}
		if retrieved.Source != "manual" {
			t.Errorf("expected source=manual, got %q", retrieved.Source)
		}
		if !retrieved.Enabled {
			t.Errorf("expected enabled=true, got false")
		}

		// Now upsert from firehose with different endpoint and enabled field
		firehoseLabeler := store.Labeler{
			DID:       did,
			Endpoint:  "different.com",
			Source:    "firehose",
			Enabled:   true,
			UpdatedAt: 2000,
		}
		if err := reg.Upsert(ctx, firehoseLabeler); err != nil {
			t.Fatalf("firehose Upsert failed: %v", err)
		}

		// Verify that source is STILL manual and enabled is STILL true
		retrieved, exists, err = reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get after firehose upsert failed: %v", err)
		}
		if !exists {
			t.Fatal("labeler not found after firehose upsert")
		}
		if retrieved.Source != "manual" {
			t.Errorf("expected source to stay 'manual', got %q", retrieved.Source)
		}
		if !retrieved.Enabled {
			t.Errorf("expected enabled to stay true, got false")
		}
		// The endpoint should have been updated
		if retrieved.Endpoint != "different.com" {
			t.Errorf("expected endpoint to update to firehose value, got %q", retrieved.Endpoint)
		}

		// Test the case where manual has enabled=false
		// First disable the manual labeler
		if err := reg.SetEnabled(ctx, did, false); err != nil {
			t.Fatalf("SetEnabled failed: %v", err)
		}

		retrieved, _, _ = reg.Get(ctx, did)
		if retrieved.Enabled {
			t.Fatal("labeler should be disabled")
		}

		// Upsert from firehose with enabled=true
		firehoseLabeler2 := store.Labeler{
			DID:       did,
			Endpoint:  "another.com",
			Source:    "firehose",
			Enabled:   true,
			UpdatedAt: 3000,
		}
		if err := reg.Upsert(ctx, firehoseLabeler2); err != nil {
			t.Fatalf("second firehose Upsert failed: %v", err)
		}

		// Verify that enabled is STILL false and source is STILL manual
		retrieved, _, _ = reg.Get(ctx, did)
		if retrieved.Enabled {
			t.Errorf("expected enabled to stay false, got true")
		}
		if retrieved.Source != "manual" {
			t.Errorf("expected source to stay 'manual', got %q", retrieved.Source)
		}
	})

	t.Run("Manual upsert claims a wedged firehose row: enables it and preserves the cursor", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		// Simulate the discovery-error wedge: RecordError inserts a minimal
		// disabled firehose row.
		did := "did:plc:wedged"
		if err := reg.RecordError(ctx, did, "no atproto_labeler service endpoint"); err != nil {
			t.Fatalf("RecordError failed: %v", err)
		}

		wedged, exists, err := reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !exists {
			t.Fatal("labeler not found after RecordError")
		}
		if wedged.Enabled {
			t.Fatal("expected RecordError row to be disabled")
		}
		if wedged.Source != "firehose" {
			t.Fatalf("expected RecordError row source=firehose, got %q", wedged.Source)
		}

		// Give the row a cursor so we can verify the manual upsert preserves it.
		if err := reg.WriteCursor(ctx, did, 42); err != nil {
			t.Fatalf("WriteCursor failed: %v", err)
		}

		// Manual upsert (the admin-add path) must claim the row.
		manual := store.Labeler{
			DID:       did,
			Endpoint:  "recovered.example.com",
			Source:    "manual",
			Enabled:   true,
			UpdatedAt: 5000,
		}
		if err := reg.Upsert(ctx, manual); err != nil {
			t.Fatalf("manual Upsert failed: %v", err)
		}

		retrieved, exists, err := reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get after manual upsert failed: %v", err)
		}
		if !exists {
			t.Fatal("labeler not found after manual upsert")
		}
		if !retrieved.Enabled {
			t.Error("expected manual upsert to enable the wedged labeler")
		}
		if retrieved.Source != "manual" {
			t.Errorf("expected source=manual after manual upsert, got %q", retrieved.Source)
		}
		if retrieved.Endpoint != manual.Endpoint {
			t.Errorf("expected endpoint %q, got %q", manual.Endpoint, retrieved.Endpoint)
		}

		seq, err := reg.ReadCursor(ctx, did)
		if err != nil {
			t.Fatalf("ReadCursor failed: %v", err)
		}
		if seq == nil || *seq != 42 {
			t.Errorf("expected cursor 42 preserved across manual upsert, got %v", seq)
		}
	})

	t.Run("Get returns exists=false for missing labeler", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		_, exists, err := reg.Get(ctx, "did:plc:nonexistent")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if exists {
			t.Error("expected exists=false for missing labeler")
		}
	})

	t.Run("SetRequireSig updates and clears the per-labeler override", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		const did = "did:plc:sig-policy"
		if err := reg.Upsert(ctx, store.Labeler{
			DID:       did,
			Endpoint:  "labeler.example.com",
			Source:    "manual",
			Enabled:   true,
			UpdatedAt: 1234567890,
		}); err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}

		// Set an explicit override.
		val := false
		if err := reg.SetRequireSig(ctx, did, &val); err != nil {
			t.Fatalf("SetRequireSig failed: %v", err)
		}
		got, _, err := reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got.RequireSig == nil || *got.RequireSig != false {
			t.Errorf("expected require_sig override false, got %v", got.RequireSig)
		}

		// Clear the override back to the global default.
		if err := reg.SetRequireSig(ctx, did, nil); err != nil {
			t.Fatalf("SetRequireSig(nil) failed: %v", err)
		}
		got, _, err = reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if got.RequireSig != nil {
			t.Errorf("expected require_sig override cleared, got %v", *got.RequireSig)
		}

		// Unknown DID errors.
		if err := reg.SetRequireSig(ctx, "did:plc:missing", &val); err == nil {
			t.Error("expected error for unknown labeler")
		}
	})

	t.Run("Successful upsert clears a previously recorded error", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer s.Close()

		reg := store.NewLabelerRegistry(s)

		const did = "did:plc:errored-then-resolved"

		// Discovery fails first: a minimal disabled row with an error.
		if err := reg.RecordError(ctx, did, "no atproto_labeler service endpoint"); err != nil {
			t.Fatalf("RecordError failed: %v", err)
		}

		// Discovery later succeeds: the upsert must clear the stale error.
		if err := reg.Upsert(ctx, store.Labeler{
			DID:       did,
			Endpoint:  "labeler.example.com",
			Source:    "firehose",
			Enabled:   true,
			UpdatedAt: 1234567890,
		}); err != nil {
			t.Fatalf("Upsert failed: %v", err)
		}

		got, exists, err := reg.Get(ctx, did)
		if err != nil || !exists {
			t.Fatalf("Get failed: exists=%v err=%v", exists, err)
		}
		if got.LastError != "" {
			t.Errorf("expected last_error cleared after successful upsert, got %q", got.LastError)
		}
	})
}

// TestUpsertMintsMonotonicID verifies AC.1: first inserts mint
// id 1, 2, 3 in insertion order (the DIDs here are deliberately out
// of lexical order, so this cannot pass on DID order); re-upserting an
// existing labeler through both the firehose and the manual path leaves its
// original id untouched.
func TestUpsertMintsMonotonicID(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	reg := store.NewLabelerRegistry(s)

	dids := []string{"did:plc:zzz", "did:plc:mmm", "did:plc:aaa"}
	for i, did := range dids {
		if err := reg.Upsert(ctx, store.Labeler{
			DID:       did,
			Endpoint:  "example.com",
			Source:    "firehose",
			Enabled:   true,
			UpdatedAt: int64(1000 + i),
		}); err != nil {
			t.Fatalf("Upsert %s failed: %v", did, err)
		}
	}

	// Minted 1, 2, 3 in insertion order.
	for i, did := range dids {
		got, exists, err := reg.Get(ctx, did)
		if err != nil || !exists {
			t.Fatalf("Get %s failed: exists=%v err=%v", did, exists, err)
		}
		if got.ID != int64(i+1) {
			t.Errorf("expected %s id=%d, got %d", did, i+1, got.ID)
		}
	}

	// Re-upsert through the firehose path: no re-mint.
	if err := reg.Upsert(ctx, store.Labeler{
		DID:       dids[0],
		Endpoint:  "v2.example.com",
		Source:    "firehose",
		Enabled:   true,
		UpdatedAt: 2000,
	}); err != nil {
		t.Fatalf("firehose re-upsert failed: %v", err)
	}

	// Re-upsert through the manual path: still no re-mint.
	if err := reg.Upsert(ctx, store.Labeler{
		DID:       dids[0],
		Endpoint:  "v3.example.com",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: 3000,
	}); err != nil {
		t.Fatalf("manual re-upsert failed: %v", err)
	}

	got, _, err := reg.Get(ctx, dids[0])
	if err != nil {
		t.Fatalf("Get after re-upserts failed: %v", err)
	}
	if got.ID != 1 {
		t.Errorf("expected id to stay 1 after re-upserts, got %d", got.ID)
	}

	// A later first insert mints above the existing maximum, not from a
	// re-upsert's stale view of the table.
	if err := reg.Upsert(ctx, store.Labeler{
		DID:       "did:plc:late",
		Endpoint:  "example.com",
		Source:    "firehose",
		Enabled:   true,
		UpdatedAt: 4000,
	}); err != nil {
		t.Fatalf("late insert failed: %v", err)
	}
	late, exists, err := reg.Get(ctx, "did:plc:late")
	if err != nil || !exists {
		t.Fatalf("Get late failed: exists=%v err=%v", exists, err)
	}
	if late.ID != 4 {
		t.Errorf("expected late insert id=4, got %d", late.ID)
	}
}

// TestRecordErrorMintsID verifies AC.1 for the discovery-error path:
// RecordError on a fresh DID mints the next id like any first
// insert; RecordError again on the same DID leaves it unchanged.
func TestRecordErrorMintsID(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	reg := store.NewLabelerRegistry(s)

	// Two regular inserts first: id 1 and 2.
	for i, did := range []string{"did:plc:one", "did:plc:two"} {
		if err := reg.Upsert(ctx, store.Labeler{
			DID:       did,
			Endpoint:  "example.com",
			Source:    "manual",
			Enabled:   true,
			UpdatedAt: int64(1000 + i),
		}); err != nil {
			t.Fatalf("Upsert %s failed: %v", did, err)
		}
	}

	// RecordError on a fresh DID mints 3.
	const errored = "did:plc:errored"
	if err := reg.RecordError(ctx, errored, "resolution failed"); err != nil {
		t.Fatalf("RecordError failed: %v", err)
	}
	got, exists, err := reg.Get(ctx, errored)
	if err != nil || !exists {
		t.Fatalf("Get errored failed: exists=%v err=%v", exists, err)
	}
	if got.ID != 3 {
		t.Errorf("expected RecordError mint id=3, got %d", got.ID)
	}

	// RecordError again on the existing DID: unchanged.
	if err := reg.RecordError(ctx, errored, "still failing"); err != nil {
		t.Fatalf("second RecordError failed: %v", err)
	}
	got, _, err = reg.Get(ctx, errored)
	if err != nil {
		t.Fatalf("Get errored after second RecordError failed: %v", err)
	}
	if got.ID != 3 {
		t.Errorf("expected id to stay 3 after repeat RecordError, got %d", got.ID)
	}

	// The next first insert continues above it.
	if err := reg.Upsert(ctx, store.Labeler{
		DID:       "did:plc:four",
		Endpoint:  "example.com",
		Source:    "manual",
		Enabled:   true,
		UpdatedAt: 5000,
	}); err != nil {
		t.Fatalf("Upsert four failed: %v", err)
	}
	fourth, exists, err := reg.Get(ctx, "did:plc:four")
	if err != nil || !exists {
		t.Fatalf("Get four failed: exists=%v err=%v", exists, err)
	}
	if fourth.ID != 4 {
		t.Errorf("expected next insert id=4, got %d", fourth.ID)
	}
}

// requireDeclaredAt asserts the labeler's declared_at equals want.
func requireDeclaredAt(t *testing.T, reg *store.LabelerRegistry, ctx context.Context, did string, want int64) {
	t.Helper()
	got, _, err := reg.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get %s: %v", did, err)
	}
	if got.DeclaredAt == nil {
		t.Fatalf("declared_at is NULL, want %d", want)
	}
	if *got.DeclaredAt != want {
		t.Fatalf("declared_at = %d, want %d", *got.DeclaredAt, want)
	}
}

// TestCreatedAtSetOnceAndPreserved verifies AC.3: created_at is set once at
// first insert and never updated — re-upserts through both stickiness paths,
// RecordError, cursor writes, SetEnabled, SetRequireSig, and even
// ApplyServiceOp (the only declared_at writer) all preserve it.
func TestCreatedAtSetOnceAndPreserved(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "created.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()
	reg := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	const did = "did:plc:created"
	if err := reg.Upsert(ctx, store.Labeler{
		DID: did, Endpoint: "one.example.com", Source: "firehose", Enabled: true, UpdatedAt: 1000,
	}); err != nil {
		t.Fatalf("first Upsert: %v", err)
	}
	first, _, err := reg.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after first insert: %v", err)
	}
	if first.CreatedAt <= 0 {
		t.Fatalf("first insert must set created_at, got %d", first.CreatedAt)
	}

	unchanged := func(step string) {
		t.Helper()
		got, _, err := reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get at step %q: %v", step, err)
		}
		if got.CreatedAt != first.CreatedAt {
			t.Fatalf("created_at changed at step %q: %d != %d", step, got.CreatedAt, first.CreatedAt)
		}
	}

	if err := reg.Upsert(ctx, store.Labeler{DID: did, Endpoint: "two.example.com", Source: "firehose", Enabled: true, UpdatedAt: 2000}); err != nil {
		t.Fatalf("firehose re-upsert: %v", err)
	}
	unchanged("firehose re-upsert")
	if err := reg.Upsert(ctx, store.Labeler{DID: did, Endpoint: "three.example.com", Source: "manual", Enabled: false, UpdatedAt: 3000}); err != nil {
		t.Fatalf("manual re-upsert: %v", err)
	}
	unchanged("manual re-upsert")
	if err := reg.RecordError(ctx, did, "boom"); err != nil {
		t.Fatalf("RecordError: %v", err)
	}
	unchanged("RecordError")
	if err := reg.WriteCursor(ctx, did, 55); err != nil {
		t.Fatalf("WriteCursor: %v", err)
	}
	unchanged("WriteCursor")
	if err := reg.SetEnabled(ctx, did, true); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	unchanged("SetEnabled")
	sig := true
	if err := reg.SetRequireSig(ctx, did, &sig); err != nil {
		t.Fatalf("SetRequireSig: %v", err)
	}
	unchanged("SetRequireSig")

	if _, err := persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "create", DID: did, Collection: "app.bsky.labeler.service",
		Record: []byte{0xa1, 0x61, 'a', 0x01}, Endpoint: "four.example.com", AutoSubscribe: true,
	}); err != nil {
		t.Fatalf("ApplyServiceOp: %v", err)
	}
	unchanged("ApplyServiceOp")
}

// TestDeclaredAtOnlyMovesOnDeclarationOps verifies AC.3: declared_at is
// written only by ApplyServiceOp and advances on every applied declaration
// op (create, update, tracked delete), while admin upserts, RecordError,
// cursor writes, SetEnabled, and SetRequireSig never touch it.
//
// The sentinel dance (forcing declared_at to 1000 between phases) makes
// "advanced" strict even when two ops land in the same wall-clock second.
func TestDeclaredAtOnlyMovesOnDeclarationOps(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(filepath.Join(t.TempDir(), "declared.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()
	reg := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	const did = "did:plc:declared"
	if err := reg.Upsert(ctx, store.Labeler{
		DID: did, Endpoint: "one.example.com", Source: "firehose", Enabled: true, UpdatedAt: 1000,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// No declaration observed yet: every writer class leaves declared_at
	// NULL.
	assertNull := func(step string) {
		t.Helper()
		got, _, err := reg.Get(ctx, did)
		if err != nil {
			t.Fatalf("Get at step %q: %v", step, err)
		}
		if got.DeclaredAt != nil {
			t.Fatalf("declared_at must stay NULL after %q, got %d", step, *got.DeclaredAt)
		}
	}
	if err := reg.Upsert(ctx, store.Labeler{DID: did, Endpoint: "two.example.com", Source: "firehose", Enabled: true, UpdatedAt: 2000}); err != nil {
		t.Fatalf("firehose re-upsert: %v", err)
	}
	assertNull("firehose re-upsert")
	if err := reg.Upsert(ctx, store.Labeler{DID: did, Endpoint: "three.example.com", Source: "manual", Enabled: true, UpdatedAt: 3000}); err != nil {
		t.Fatalf("manual re-upsert: %v", err)
	}
	assertNull("manual re-upsert")
	if err := reg.RecordError(ctx, did, "still failing"); err != nil {
		t.Fatalf("RecordError: %v", err)
	}
	assertNull("RecordError")
	if err := reg.WriteCursor(ctx, did, 7); err != nil {
		t.Fatalf("WriteCursor: %v", err)
	}
	assertNull("WriteCursor")
	if err := reg.SetEnabled(ctx, did, false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	assertNull("SetEnabled")
	if err := reg.SetRequireSig(ctx, did, nil); err != nil {
		t.Fatalf("SetRequireSig: %v", err)
	}
	assertNull("SetRequireSig")

	// The first applied declaration op sets it.
	if _, err := persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "create", DID: did, Collection: "app.bsky.labeler.service",
		Record: []byte{0xa1, 0x61, 'a', 0x01}, Endpoint: "four.example.com", AutoSubscribe: true,
	}); err != nil {
		t.Fatalf("ApplyServiceOp create: %v", err)
	}
	got, _, err := reg.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after create: %v", err)
	}
	if got.DeclaredAt == nil || *got.DeclaredAt <= 0 {
		t.Fatalf("create must set declared_at, got %v", got.DeclaredAt)
	}

	// Sentinel: prove the non-declaration writers hold the value still.
	setSentinel := func() {
		if _, err := s.DB().Exec(`UPDATE labelers SET declared_at = 1000 WHERE did = ?`, did); err != nil {
			t.Fatalf("sentinel update: %v", err)
		}
	}
	setSentinel()
	if err := reg.Upsert(ctx, store.Labeler{DID: did, Endpoint: "five.example.com", Source: "firehose", Enabled: true, UpdatedAt: 4000}); err != nil {
		t.Fatalf("admin upsert: %v", err)
	}
	requireDeclaredAt(t, reg, ctx, did, 1000)
	if err := reg.RecordError(ctx, did, "again"); err != nil {
		t.Fatalf("RecordError: %v", err)
	}
	requireDeclaredAt(t, reg, ctx, did, 1000)
	if err := reg.WriteCursor(ctx, did, 9); err != nil {
		t.Fatalf("WriteCursor: %v", err)
	}
	requireDeclaredAt(t, reg, ctx, did, 1000)
	if err := reg.SetEnabled(ctx, did, true); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	requireDeclaredAt(t, reg, ctx, did, 1000)
	sig := false
	if err := reg.SetRequireSig(ctx, did, &sig); err != nil {
		t.Fatalf("SetRequireSig: %v", err)
	}
	requireDeclaredAt(t, reg, ctx, did, 1000)

	// Every applied declaration op advances it from the sentinel.
	if _, err := persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "update", DID: did, Collection: "app.bsky.labeler.service",
		Record: []byte{0xa1, 0x61, 'b', 0x02}, Endpoint: "five.example.com", AutoSubscribe: true,
	}); err != nil {
		t.Fatalf("ApplyServiceOp update: %v", err)
	}
	got, _, err = reg.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if got.DeclaredAt == nil || *got.DeclaredAt <= 1000 {
		t.Fatalf("update must advance declared_at past the sentinel, got %v", got.DeclaredAt)
	}

	setSentinel()
	if _, err := persist.ApplyServiceOp(ctx, store.ServiceOp{
		Op: "delete", DID: did, Collection: "app.bsky.labeler.service",
	}); err != nil {
		t.Fatalf("ApplyServiceOp delete: %v", err)
	}
	got, _, err = reg.Get(ctx, did)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got.DeclaredAt == nil || *got.DeclaredAt <= 1000 {
		t.Fatalf("tracked delete must advance declared_at past the sentinel, got %v", got.DeclaredAt)
	}
}
