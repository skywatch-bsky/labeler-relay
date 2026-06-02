// pattern: Functional Core

package verify_test

import (
	"context"
	"strings"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/require"

	"github.com/scarndp/labeler-relay/internal/verify"
)

// mockResolver is a minimal identity.Resolver backed by a static DID document.
type mockResolver struct {
	docs map[syntax.DID]*identity.DIDDocument
}

func (m *mockResolver) ResolveDID(_ context.Context, did syntax.DID) (*identity.DIDDocument, error) {
	doc, ok := m.docs[did]
	if !ok {
		return nil, identity.ErrDIDNotFound
	}
	return doc, nil
}

func (m *mockResolver) ResolveDIDRaw(_ context.Context, _ syntax.DID) ([]byte, error) {
	return nil, nil
}

func (m *mockResolver) ResolveHandle(_ context.Context, _ syntax.Handle) (syntax.DID, error) {
	return "", nil
}

// buildMockResolver creates a resolver with one verification method whose ID
// ends in "#atproto_label" carrying the given public key's multibase encoding.
func buildMockResolver(did string, pub atcrypto.PublicKey) *mockResolver {
	return &mockResolver{
		docs: map[syntax.DID]*identity.DIDDocument{
			syntax.DID(did): {
				DID: syntax.DID(did),
				VerificationMethod: []identity.DocVerificationMethod{
					{
						ID:                 did + "#atproto_label",
						Type:               "Multikey",
						Controller:         did,
						PublicKeyMultibase: pub.Multibase(),
					},
				},
			},
		},
	}
}

// signedLexLabel creates a comatproto.LabelDefs_Label signed by privKey.
func signedLexLabel(t *testing.T, privKey atcrypto.PrivateKey, src, uri, val string) *comatproto.LabelDefs_Label {
	t.Helper()
	ver := labeling.ATPROTO_LABEL_VERSION
	l := &labeling.Label{
		SourceDID: src,
		URI:       uri,
		Val:       val,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Version:   ver,
	}
	require.NoError(t, l.Sign(privKey))
	lex := l.ToLexicon()
	return &lex
}

// TestVerifyRelayedLabel_PassesWithCorrectKey checks that a label signed by a
// P-256 key verifies when that exact key is supplied.
func TestVerifyRelayedLabel_PassesWithCorrectKey(t *testing.T) {
	privKey, err := atcrypto.GeneratePrivateKeyP256()
	require.NoError(t, err)

	pubKey, err := privKey.PublicKey()
	require.NoError(t, err)

	label := signedLexLabel(t, privKey, "did:plc:example", "at://did:plc:example/post/1", "spam")

	require.NoError(t, verify.VerifyRelayedLabel(label, pubKey))
}

// TestVerifyRelayedLabel_FailsWithWrongKey checks that the same label fails
// verification against a different key.
func TestVerifyRelayedLabel_FailsWithWrongKey(t *testing.T) {
	privKey, err := atcrypto.GeneratePrivateKeyP256()
	require.NoError(t, err)

	label := signedLexLabel(t, privKey, "did:plc:example", "at://did:plc:example/post/1", "spam")

	// Generate a completely independent key.
	otherPriv, err := atcrypto.GeneratePrivateKeyP256()
	require.NoError(t, err)
	otherPub, err := otherPriv.PublicKey()
	require.NoError(t, err)

	err = verify.VerifyRelayedLabel(label, otherPub)
	require.Error(t, err)
}

// TestVerifyRelayedLabel_FailsOnMutatedField checks that mutating a signed
// field after signing causes verification to fail (canonicalization is
// field-sensitive).
func TestVerifyRelayedLabel_FailsOnMutatedField(t *testing.T) {
	privKey, err := atcrypto.GeneratePrivateKeyP256()
	require.NoError(t, err)

	pubKey, err := privKey.PublicKey()
	require.NoError(t, err)

	label := signedLexLabel(t, privKey, "did:plc:example", "at://did:plc:example/post/1", "spam")

	// Mutate Val after signing — the signature now covers the original value.
	label.Val = "not-spam"

	err = verify.VerifyRelayedLabel(label, pubKey)
	require.Error(t, err)
}

// TestVerifyRelayedLabel_UnsignedReturnsError checks that a label with no sig
// returns an error rather than panicking or silently succeeding.
func TestVerifyRelayedLabel_UnsignedReturnsError(t *testing.T) {
	privKey, err := atcrypto.GeneratePrivateKeyP256()
	require.NoError(t, err)

	pubKey, err := privKey.PublicKey()
	require.NoError(t, err)

	label := &comatproto.LabelDefs_Label{
		Src: "did:plc:example",
		Uri: "at://did:plc:example/post/1",
		Val: "spam",
		Cts: time.Now().UTC().Format(time.RFC3339),
		Sig: nil,
	}

	err = verify.VerifyRelayedLabel(label, pubKey)
	require.Error(t, err)
}

// TestResolveLabelKey_FoundAndParsed checks that ResolveLabelKey resolves the
// #atproto_label verification method from the mock resolver and returns a key
// that matches the original.
func TestResolveLabelKey_FoundAndParsed(t *testing.T) {
	const did = "did:plc:testlabeler"

	privKey, err := atcrypto.GeneratePrivateKeyP256()
	require.NoError(t, err)

	pubKey, err := privKey.PublicKey()
	require.NoError(t, err)

	resolver := buildMockResolver(did, pubKey)

	resolvedKey, err := verify.ResolveLabelKey(context.Background(), resolver, did)
	require.NoError(t, err)

	// The resolved key must verify a label signed with the original private key.
	label := signedLexLabel(t, privKey, did, "at://did:plc:user/post/1", "test")
	require.NoError(t, verify.VerifyRelayedLabel(label, resolvedKey))
}

// TestResolveLabelKey_MissingMethod checks that a DID with no #atproto_label
// verification method returns a clear error.
func TestResolveLabelKey_MissingMethod(t *testing.T) {
	const did = "did:plc:nolabelkey"

	resolver := &mockResolver{
		docs: map[syntax.DID]*identity.DIDDocument{
			syntax.DID(did): {
				DID: syntax.DID(did),
				VerificationMethod: []identity.DocVerificationMethod{
					{
						ID:                 did + "#atproto",
						Type:               "Multikey",
						Controller:         did,
						PublicKeyMultibase: "zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU9169",
					},
				},
			},
		},
	}

	_, err := verify.ResolveLabelKey(context.Background(), resolver, did)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "atproto_label"), "error should mention atproto_label, got: %v", err)
}

// TestResolveLabelKey_UnresolvableDID checks that a DID not in the resolver
// returns an error.
func TestResolveLabelKey_UnresolvableDID(t *testing.T) {
	resolver := &mockResolver{docs: map[syntax.DID]*identity.DIDDocument{}}

	_, err := verify.ResolveLabelKey(context.Background(), resolver, "did:plc:doesnotexist")
	require.Error(t, err)
}
