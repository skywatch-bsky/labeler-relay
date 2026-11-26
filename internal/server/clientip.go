// pattern: Functional Core

package server

import (
	"net/netip"
	"strings"
)

// resolveClientIP returns the rate-limit key for peer. For a trusted peer,
// it walks X-Forwarded-For right to left and skips trusted hops. This selects
// the client only when every proxy on the path is trusted. Missing, malformed,
// zoned, or all-trusted chains fall back to peer, so bad input shares a bucket
// instead of opening a bypass.
func resolveClientIP(peer netip.Addr, forwardedFor []string, trusted []netip.Prefix) string {
	if !trustedContains(peer, trusted) {
		return normalizeAddr(peer).String()
	}
	// Header lines appear in order and entries within a line are
	// comma-separated; walk both levels right to left.
	for i := len(forwardedFor) - 1; i >= 0; i-- {
		hops := strings.Split(forwardedFor[i], ",")
		for j := len(hops) - 1; j >= 0; j-- {
			addr, err := netip.ParseAddr(strings.TrimSpace(hops[j]))
			if err != nil || addr.Zone() != "" {
				return normalizeAddr(peer).String()
			}
			addr = normalizeAddr(addr)
			if trustedContains(addr, trusted) {
				continue
			}
			return addr.String()
		}
	}
	return normalizeAddr(peer).String()
}

// normalizeAddr maps IPv4-mapped addresses to IPv4 before bucket and trust checks.
func normalizeAddr(addr netip.Addr) netip.Addr {
	return addr.Unmap().WithZone("")
}

// trustedContains reports whether addr falls inside any trusted prefix.
func trustedContains(addr netip.Addr, trusted []netip.Prefix) bool {
	addr = normalizeAddr(addr)
	for _, prefix := range trusted {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}
