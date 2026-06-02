package firehose

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/scarndp/labeler-relay/internal/store"
)

// TestFirehoseWatcherCreation tests that FirehoseWatcher can be created with proper dependencies.
func TestFirehoseWatcherCreation(t *testing.T) {
	// Create test store.
	tempDir := t.TempDir()
	dbPath := tempDir + "/test.db"
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	registry := store.NewLabelerRegistry(s)
	persist := store.NewLabelPersist(s)

	// Create a fake DID resolver.
	fakeDIDResolver := &fakeDIDResolver{
		endpoints: map[string]string{
			"did:plc:labeler1": "https://labeler1.example.com/xrpc/com.atproto.label.subscribeLabels",
		},
	}

	// Create the watcher.
	log := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	watcher := NewFirehoseWatcher(
		"http://unused.example.com",
		registry,
		persist,
		fakeDIDResolver,
		s,
		func() {},
		log,
	)

	// Verify it was created successfully.
	if watcher == nil {
		t.Fatal("FirehoseWatcher should not be nil")
	}
}
