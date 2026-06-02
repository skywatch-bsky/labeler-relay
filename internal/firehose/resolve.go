// pattern: Imperative Shell

package firehose

import (
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// ErrNoLabelerEndpoint is returned when a DID document lacks an atproto_labeler service endpoint.
var ErrNoLabelerEndpoint = fmt.Errorf("no atproto_labeler service endpoint in DID document")

// DIDResolver is a thin interface we own for resolving labeler endpoints from DIDs.
// This allows tests to mock DID resolution without mocking indigo's resolver directly.
type DIDResolver interface {
	LabelerEndpoint(ctx context.Context, did string) (endpoint string, err error)
}

// indigoResolver adapts indigo's identity.Resolver to our DIDResolver interface.
type indigoResolver struct {
	inner identity.Resolver
}

// NewIndigoResolver wraps an indigo identity.Resolver.
func NewIndigoResolver(r identity.Resolver) DIDResolver {
	return &indigoResolver{inner: r}
}

// LabelerEndpoint resolves the atproto_labeler service endpoint for a DID.
func (r *indigoResolver) LabelerEndpoint(ctx context.Context, did string) (string, error) {
	// Resolve the DID document.
	didDoc, err := r.inner.ResolveDID(ctx, syntax.DID(did))
	if err != nil {
		return "", fmt.Errorf("failed to resolve DID: %w", err)
	}

	// Find the atproto_labeler service.
	for _, svc := range didDoc.Service {
		if svc.Type == "atproto_labeler" {
			return svc.ServiceEndpoint, nil
		}
	}

	// No atproto_labeler service found.
	return "", ErrNoLabelerEndpoint
}
