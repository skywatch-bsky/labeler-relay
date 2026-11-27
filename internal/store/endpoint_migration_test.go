package store

import (
	"path/filepath"
	"testing"
)

func TestEndpointMigrationRollsBack(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "rollback.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, endpoint := range []string{"https://a.example/path", "http://b.example/path"} {
		if _, err := s.DB().Exec("INSERT INTO labelers (did, endpoint, source, enabled, updated_at, observed_seq) VALUES (?, ?, 'manual', 1, 123, ?)", endpoint, endpoint, i+1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.DB().Exec("CREATE TRIGGER block_endpoint BEFORE UPDATE OF endpoint ON labelers WHEN NEW.endpoint = 'b.example' BEGIN SELECT RAISE(ABORT, 'test: update blocked'); END"); err != nil {
		t.Fatal(err)
	}
	if err := migrateEndpoints(s.DB()); err == nil {
		t.Fatal("expected migration failure")
	}
	var count int
	if err := s.DB().QueryRow("SELECT COUNT(*) FROM labelers WHERE endpoint = did AND updated_at = 123").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("rollback preserved %d rows, want 2", count)
	}
}

func TestEndpointMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ did, endpoint, want string }{
		{"a", "https://a.example/xrpc/com.atproto.label.subscribeLabels", "a.example"},
		{"b", "http://b.example:8443/xrpc/com.atproto.label.subscribeLabels", "b.example:8443"},
		{"empty", "", ""}, {"bare", "bare.example", "bare.example"},
		{"invalid", "https:///", "https:///"},
	}
	for _, tt := range cases {
		if _, err := s.DB().Exec("INSERT INTO labelers (did, endpoint, source, enabled, updated_at, observed_seq) VALUES (?, ?, 'manual', 1, 123, (SELECT COALESCE(MAX(observed_seq), 0)+1 FROM labelers))", tt.did, tt.endpoint); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		s, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, tt := range cases {
			var endpoint string
			var updatedAt int64
			if err := s.DB().QueryRow("SELECT endpoint, updated_at FROM labelers WHERE did = ?", tt.did).Scan(&endpoint, &updatedAt); err != nil {
				t.Fatal(err)
			}
			if endpoint != tt.want || updatedAt != 123 {
				t.Errorf("%s: endpoint=%q updated_at=%d, want %q and 123", tt.did, endpoint, updatedAt, tt.want)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
