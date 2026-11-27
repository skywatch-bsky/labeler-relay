package firehose

import (
	"context"
	"testing"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

type hostDIDResolver struct {
	identity.Resolver
	endpoint string
}

func (r hostDIDResolver) ResolveDID(context.Context, syntax.DID) (*identity.DIDDocument, error) {
	return &identity.DIDDocument{Service: []identity.DocService{{ID: "#atproto_labeler", Type: "AtprotoLabeler", ServiceEndpoint: r.endpoint}}}, nil
}

func TestLabelerEndpointReturnsHost(t *testing.T) {
	for _, tt := range []struct {
		endpoint, want string
		wantErr        bool
	}{
		{"https://ozone.good-example.com", "ozone.good-example.com", false},
		{"http://ozone.good-example.com", "ozone.good-example.com", false},
		{"https://labeler.example:8443/", "labeler.example:8443", false},
		{"https://labeler.example/xrpc/com.atproto.label.subscribeLabels", "labeler.example", false},
		{"https://labeler.example/xrpc/com.atproto.label.subscribeLabels/", "labeler.example", false},
		{"https://labeler.example/?token=x", "labeler.example", false},
		{"https:///", "", true},
	} {
		t.Run(tt.endpoint, func(t *testing.T) {
			got, err := NewIndigoResolver(hostDIDResolver{endpoint: tt.endpoint}).LabelerEndpoint(context.Background(), "did:plc:labeler123")
			if (err != nil) != tt.wantErr {
				t.Fatalf("LabelerEndpoint(%q) error = %v, want error %v", tt.endpoint, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("LabelerEndpoint(%q) = %q, want %q", tt.endpoint, got, tt.want)
			}
		})
	}
}
