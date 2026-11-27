package config_test

import (
	"testing"

	"github.com/scarndp/labeler-relay/internal/config"
	"github.com/stretchr/testify/require"
)

func TestLoad_RejectsUndialableFirehoseURL(t *testing.T) {
	const path = "/xrpc/com.atproto.sync.subscribeRepos"
	for _, raw := range []string{
		"https://relay.example" + path,
		"http://relay.example" + path,
		"relay.example" + path,
		"wss://",
		"ws://" + path,
		"not a url",
		"wss://:443" + path,
		"ws://:8080" + path,
		"wss://[]:443" + path,
		"wss://username@relay.example" + path,
		"ws://username:password@relay.example" + path,
		"wss://@relay.example" + path,
		"wss://relay.example:" + path,
		"ws://relay.example:abc" + path,
		"wss://relay.example:-1" + path,
		"ws://relay.example:0" + path,
		"wss://relay.example:65536" + path,
		"ws://[::1]:65536" + path,
		"ws://relay.example:80:90" + path,
		"ws://::1" + path,
		"ws://::1:8080" + path,
		"wss://[::1]:80:90" + path,
		"ws://relay.example]:80" + path,
		"ws://[relay.example]:80" + path,
		"ws://[127.0.0.1]:80" + path,
		"ws://[:::1]:80" + path,
	} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "token")
			t.Setenv("LABELER_RELAY_FIREHOSE_URL", raw)

			_, err := config.Load()

			require.ErrorContains(t, err, "firehose_url")
		})
	}
}

func TestLoad_AcceptsWebSocketFirehoseURL(t *testing.T) {
	const path = "/xrpc/com.atproto.sync.subscribeRepos"
	for _, raw := range []string{
		"wss://relay.example" + path,
		"ws://relay.example" + path,
		"wss://relay.example:443" + path,
		"ws://127.0.0.1:8080" + path,
		"ws://relay.example:1" + path,
		"wss://relay.example:65535" + path,
		"ws://[::1]" + path,
		"wss://[::1]:443" + path,
		"ws://[2001:db8::1]" + path,
		"wss://[::ffff:192.0.2.1]:443" + path,
		"ws://[fe80::1%25eth0]" + path,
		"wss://[fe80::1%25eth0]:443" + path,
	} {
		t.Run(raw, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("LABELER_RELAY_ADMIN_TOKEN", "token")
			t.Setenv("LABELER_RELAY_FIREHOSE_URL", raw)

			cfg, err := config.Load()

			require.NoError(t, err)
			require.Equal(t, raw, cfg.FirehoseURL)
		})
	}
}
