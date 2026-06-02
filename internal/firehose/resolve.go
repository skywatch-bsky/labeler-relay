// pattern: Imperative Shell

package firehose

import (
	"context"
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// subscribeLabelsPath is the XRPC path for the labeler subscription endpoint.
// AT Protocol labelers advertise only a base serviceEndpoint in their DID docs;
// the subscription path is appended by convention.
const subscribeLabelsPath = "/xrpc/com.atproto.label.subscribeLabels"

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

// LabelerEndpoint resolves the atproto_labeler service endpoint for a DID and
// returns the full subscribeLabels WebSocket subscription URL. AT Protocol
// labelers advertise only a base serviceEndpoint (e.g. "https://mod.bsky.app")
// in their DID documents; this method appends the XRPC subscription path.
func (r *indigoResolver) LabelerEndpoint(ctx context.Context, did string) (string, error) {
	// Resolve the DID document.
	didDoc, err := r.inner.ResolveDID(ctx, syntax.DID(did))
	if err != nil {
		return "", fmt.Errorf("failed to resolve DID: %w", err)
	}

	// Find the atproto_labeler service.
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

	// Append the subscribeLabels XRPC path if not already present.
	// The DID doc serviceEndpoint is a base URL; callers (slurper) expect the
	// full path so they can directly dial after scheme conversion.
	if !strings.HasSuffix(base, subscribeLabelsPath) {
		base = strings.TrimRight(base, "/") + subscribeLabelsPath
	}

	return base, nil
}
