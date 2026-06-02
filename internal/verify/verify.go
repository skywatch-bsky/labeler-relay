// pattern: Functional Core
// verify.go provides helpers for resolving a labeler's #atproto_label public
// key from its DID document and for verifying that a relayed label's signature
// is intact against that key.

package verify

import (
	"context"
	"fmt"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// Resolver is the subset of identity.Resolver used by this package. It
// matches identity.Resolver and is kept narrow so callers can pass any
// resolver (real or mock) without depending on the full interface.
type Resolver interface {
	ResolveDID(ctx context.Context, did syntax.DID) (*identity.DIDDocument, error)
}

// ResolveLabelKey resolves the #atproto_label public key for the given labeler DID.
// It resolves the DID document, finds the verification method whose ID ends in
// "#atproto_label", and parses the PublicKeyMultibase field into an atcrypto.PublicKey.
func ResolveLabelKey(ctx context.Context, r Resolver, did string) (atcrypto.PublicKey, error) {
	doc, err := r.ResolveDID(ctx, syntax.DID(did))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve DID %s: %w", did, err)
	}

	for _, vm := range doc.VerificationMethod {
		if strings.HasSuffix(vm.ID, "#atproto_label") {
			key, err := atcrypto.ParsePublicMultibase(vm.PublicKeyMultibase)
			if err != nil {
				return nil, fmt.Errorf("failed to parse #atproto_label key for %s: %w", did, err)
			}
			return key, nil
		}
	}

	return nil, fmt.Errorf("no #atproto_label verification method in DID document for %s", did)
}

// VerifyRelayedLabel maps the relayed comatproto.LabelDefs_Label fields into a
// labeling.Label and verifies its signature against the origin key.
// All signed fields are copied faithfully: Cid, Cts, Exp, Neg, Src, Uri, Val, Ver, Sig.
func VerifyRelayedLabel(label *comatproto.LabelDefs_Label, originKey atcrypto.PublicKey) error {
	l := labeling.FromLexicon(label)
	return l.VerifySignature(originKey)
}
