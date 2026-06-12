// pattern: Functional Core

package slurper

import (
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/scarndp/labeler-relay/internal/store"
)

// SigRequired returns whether a labeler must have signed labels, resolving the
// per-labeler override against the global default. nil override => use default.
func SigRequired(override *bool, globalDefault bool) bool {
	if override != nil {
		return *override
	}
	return globalDefault
}

// KeepLabel reports whether a label should be relayed given the sig policy.
// A label is kept if it has a non-empty Sig, OR sig is not required.
func KeepLabel(label *comatproto.LabelDefs_Label, sigRequired bool) bool {
	if len(label.Sig) > 0 {
		return true
	}
	return !sigRequired
}

// SubscriptionConfigChanged reports whether registry fields consumed by a
// running subscription differ between the snapshot taken at subscription
// start and the current registry row. Endpoint feeds the dial loop and
// RequireSig feeds the sig policy; a change to either requires a restart.
func SubscriptionConfigChanged(snapshot, current store.Labeler) bool {
	if snapshot.Endpoint != current.Endpoint {
		return true
	}
	if (snapshot.RequireSig == nil) != (current.RequireSig == nil) {
		return true
	}
	return snapshot.RequireSig != nil && *snapshot.RequireSig != *current.RequireSig
}
