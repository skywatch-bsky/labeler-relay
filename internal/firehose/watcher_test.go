package firehose

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/scarndp/labeler-relay/internal/store"
)

// TestFirehoseWatcherStructure tests that FirehoseWatcher can be instantiated.
func TestFirehoseWatcherStructure(t *testing.T) {
	// Create test store in a file path.
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
			"did:plc:labeler1": "https://example.com/labeler",
		},
	}

	// Create a poke function.
	pokeFunc := func() {
		// no-op for test
	}

	// Create the watcher.
	log := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	watcher := NewFirehoseWatcher(
		"http://example.com/firehose",
		registry,
		persist,
		fakeDIDResolver,
		s,
		pokeFunc,
		log,
	)

	// Just verify it's creatable (full integration test would require real server).
	if watcher == nil {
		t.Fatal("watcher should not be nil")
	}
}

