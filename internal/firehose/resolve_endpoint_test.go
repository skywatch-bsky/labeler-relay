package firehose

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// docDirectory is an identity.Resolver whose DID document advertises one
// atproto_labeler service at a fixed endpoint.
type docDirectory struct{ endpoint string }

func (d docDirectory) ResolveDID(context.Context, syntax.DID) (*identity.DIDDocument, error) {
	return &identity.DIDDocument{
		Service: []identity.DocService{{ID: "#atproto_labeler", Type: "AtprotoLabeler", ServiceEndpoint: d.endpoint}},
	}, nil
}

func (d docDirectory) ResolveDIDRaw(context.Context, syntax.DID) (json.RawMessage, error) {
	return nil, fmt.Errorf("not used")
}

func (d docDirectory) ResolveHandle(context.Context, syntax.Handle) (syntax.DID, error) {
	return "", fmt.Errorf("not used")
}

// TestLabelerEndpointShapes checks the endpoint rule against the shapes a DID
// document's serviceEndpoint can take: the subscribeLabels path is appended
// to a base URL, and an endpoint that already contains the path is left
// alone, whichever way its trailing slash is written.
func TestLabelerEndpointShapes(t *testing.T) {
	t.Parallel()

	const full = "https://labeler.example" + subscribeLabelsPath
	cases := []struct {
		name     string
		endpoint string
		want     string
	}{
		{"bare base", "https://labeler.example", full},
		{"base with a trailing slash", "https://labeler.example/", full},
		{"base with a port", "http://labeler.example:8443/", "http://labeler.example:8443" + subscribeLabelsPath},
		{"already the full path", full, full},
		{"full path with a trailing slash", full + "/", full},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := NewIndigoResolver(docDirectory{endpoint: tc.endpoint})
			got, err := res.LabelerEndpoint(context.Background(), "did:plc:endpointshapes0000000")
			if err != nil {
				t.Fatalf("LabelerEndpoint: %v", err)
			}
			if got != tc.want {
				t.Fatalf("serviceEndpoint %q resolved to %q; want %q", tc.endpoint, got, tc.want)
			}
		})
	}
}
