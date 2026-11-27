// pattern: Imperative Shell

package firehose

import (
	"context"
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/scarndp/labeler-relay/internal/labelerendpoint"
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

// LabelerEndpoint returns the advertised labeler host and optional port.
// The relay builds its own secure subscription URL, regardless of the advertised scheme.
func (r *indigoResolver) LabelerEndpoint(ctx context.Context, did string) (string, error) {
	didDoc, err := r.inner.ResolveDID(ctx, syntax.DID(did))
	if err != nil {
		return "", fmt.Errorf("failed to resolve DID: %w", err)
	}

	// Match on Type="AtprotoLabeler" as primary, or ID containing "#atproto_labeler" as fallback.
	var base string
	for _, svc := range didDoc.Service {
		if svc.Type == "AtprotoLabeler" || svc.Type == "atproto_labeler" {
			base = svc.ServiceEndpoint
			break
		}
	}
	if base == "" {
		for _, svc := range didDoc.Service {
			if strings.Contains(svc.ID, "#atproto_labeler") {
				base = svc.ServiceEndpoint
				break
			}
		}
	}
	if base == "" {
		return "", ErrNoLabelerEndpoint
	}

	return labelerendpoint.Host(base)
}
