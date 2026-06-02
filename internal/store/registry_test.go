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
			Endpoint:   "https://example.com/labelers",
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
			Endpoint:  "https://example.com/v1",
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
			Endpoint:  "https://different.com/v2",
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
		if retrieved.Endpoint != "https://different.com/v2" {
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
			Endpoint:  "https://another.com/v3",
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
}
