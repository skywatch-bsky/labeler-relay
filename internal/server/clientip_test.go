// pattern: Imperative Shell
// Tests for the trusted-proxy-aware client-IP resolver backing the query
// rate limiter's keying decision.

package server

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	addr, err := netip.ParseAddr(s)
	require.NoError(t, err, "test address %q must parse", s)
	return addr
}

// TestResolveClientIP checks trust boundaries, chain selection, fallbacks,
// and IPv4-mapped address normalization.
func TestResolveClientIP(t *testing.T) {
	loopback := loopbackTrusted
	loopbackPlusPrivate := []netip.Prefix{
		netip.MustParsePrefix("127.0.0.1/32"),
		netip.MustParsePrefix("::1/128"),
		netip.MustParsePrefix("10.0.0.0/8"),
	}

	cases := []struct {
		name    string
		peer    netip.Addr
		xff     []string
		trusted []netip.Prefix
		want    string
	}{
		// Untrusted peers ignore XFF.
		{
			name:    "untrusted peer ignores xff",
			peer:    mustAddr(t, "203.0.113.7"),
			xff:     []string{"9.9.9.9"},
			trusted: loopback,
			want:    "203.0.113.7",
		},
		{
			name:    "empty trusted list ignores xff",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"9.9.9.9"},
			trusted: nil,
			want:    "127.0.0.1",
		},
		{
			name:    "untrusted peer with spoofed chain ignores xff",
			peer:    mustAddr(t, "203.0.113.7"),
			xff:     []string{"6.6.6.6, 9.9.9.9"},
			trusted: loopback,
			want:    "203.0.113.7",
		},

		// Trusted chains resolve from the rightmost hop.
		{
			name:    "single trusted hop",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"9.9.9.9"},
			trusted: loopback,
			want:    "9.9.9.9",
		},
		{
			name:    "spoofed leading entries resolve to the rightmost non-trusted hop",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"6.6.6.6, 9.9.9.9"},
			trusted: loopback,
			want:    "9.9.9.9",
		},
		{
			name:    "multi-hop chain through a trusted intermediate proxy",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"10.0.0.1, 9.9.9.9"},
			trusted: loopbackPlusPrivate,
			want:    "9.9.9.9",
		},
		{
			name:    "whitespace around entries is tolerated",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"  9.9.9.9  "},
			trusted: loopback,
			want:    "9.9.9.9",
		},
		{
			name:    "multiple header lines are walked right to left",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"10.0.0.1", "9.9.9.9"},
			trusted: loopbackPlusPrivate,
			want:    "9.9.9.9",
		},

		{
			name:    "walk continues past a trusted intermediate hop",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"9.9.9.9, 10.0.0.1"},
			trusted: loopbackPlusPrivate,
			want:    "9.9.9.9",
		},
		{
			name:    "walk crosses a trusted hop on a separate header line",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"6.6.6.6, 9.9.9.9", "::ffff:10.0.0.1"},
			trusted: loopbackPlusPrivate,
			want:    "9.9.9.9",
		},
		{
			name:    "malformed entry reached after a trusted hop falls back to peer",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"not-an-ip, 10.0.0.1"},
			trusted: loopbackPlusPrivate,
			want:    "127.0.0.1",
		},
		{
			name:    "zoned entry reached after a trusted hop falls back to peer",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"fe80::1%eth0, 10.0.0.1"},
			trusted: loopbackPlusPrivate,
			want:    "127.0.0.1",
		},

		// Fallbacks all degrade to the peer.
		{
			name:    "absent header falls back to peer",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     nil,
			trusted: loopback,
			want:    "127.0.0.1",
		},
		{
			name:    "all-trusted chain falls back to peer",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"127.0.0.1"},
			trusted: loopback,
			want:    "127.0.0.1",
		},
		{
			name:    "malformed entry falls back to peer",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"not-an-ip"},
			trusted: loopback,
			want:    "127.0.0.1",
		},
		{
			name:    "zoned entry falls back to peer",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"fe80::1%eth0"},
			trusted: loopback,
			want:    "127.0.0.1",
		},

		// IPv4-mapped spellings normalize to their IPv4 form.
		{
			name:    "mapped peer is trusted by the ipv4 loopback prefix",
			peer:    mustAddr(t, "::ffff:127.0.0.1"),
			xff:     []string{"9.9.9.9"},
			trusted: loopback,
			want:    "9.9.9.9",
		},
		{
			name:    "mapped trusted intermediate hop resolves to the real client",
			peer:    mustAddr(t, "::ffff:127.0.0.1"),
			xff:     []string{"::ffff:10.0.0.1, 9.9.9.9"},
			trusted: loopbackPlusPrivate,
			want:    "9.9.9.9",
		},
		{
			name:    "mapped non-trusted hop keys as its ipv4 form",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"::ffff:9.9.9.9"},
			trusted: loopback,
			want:    "9.9.9.9",
		},
		{
			name:    "mapped spoofed chain resolves to the mapped real client's ipv4 form",
			peer:    mustAddr(t, "127.0.0.1"),
			xff:     []string{"6.6.6.6, ::ffff:8.8.8.8"},
			trusted: loopback,
			want:    "8.8.8.8",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, resolveClientIP(tc.peer, tc.xff, tc.trusted))
		})
	}
}
