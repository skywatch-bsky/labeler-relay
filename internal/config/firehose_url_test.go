package config

import (
	"strings"
	"testing"
)

// TestLoad_RejectsUndialableFirehoseURL checks that a LABELER_RELAY_FIREHOSE_URL
// the firehose watcher could not dial is refused at startup: the watcher dials
// the value exactly as given, so an http(s) URL, a bare host, or a URL without
// a host would otherwise fail on every dial while the relay keeps reporting
// healthy.
func TestLoad_RejectsUndialableFirehoseURL(t *testing.T) {
	const path = "/xrpc/com.atproto.sync.subscribeRepos"
	for _, raw := range []string{
		"https://relay.example" + path,
		"http://relay.example" + path,
		"relay.example" + path,
		"wss://",
		"ws://" + path,
		"not a url",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "token")
			t.Setenv("LABELER_RELAY_FIREHOSE_URL", raw)
			_, err := Load()
			if err == nil {
				t.Fatalf("Load accepted LABELER_RELAY_FIREHOSE_URL=%q", raw)
			}
			if !strings.Contains(err.Error(), "firehose_url") {
				t.Fatalf("Load rejected %q with %q; want an error naming firehose_url", raw, err)
			}
		})
	}

	for _, raw := range []string{"wss://relay.example" + path, "ws://127.0.0.1:8080" + path} {
		t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "token")
		t.Setenv("LABELER_RELAY_FIREHOSE_URL", raw)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load rejected a valid firehose URL %q: %v", raw, err)
		}
		if cfg.FirehoseURL != raw {
			t.Fatalf("FirehoseURL = %q; want %q", cfg.FirehoseURL, raw)
		}
	}
}
