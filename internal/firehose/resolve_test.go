package firehose

import (
	"context"
	"fmt"
	"testing"
)

// fakeDIDResolver implements DIDResolver for testing.
type fakeDIDResolver struct {
	endpoints map[string]string // did -> endpoint
	errs      map[string]error   // did -> error
}

func (f *fakeDIDResolver) LabelerEndpoint(ctx context.Context, did string) (string, error) {
	if err, ok := f.errs[did]; ok {
		return "", err
	}
	if ep, ok := f.endpoints[did]; ok {
		return ep, nil
	}
	return "", ErrNoLabelerEndpoint
}

// TestLabelerEndpointSuccess tests successful endpoint resolution.
func TestLabelerEndpointSuccess(t *testing.T) {
	fake := &fakeDIDResolver{
		endpoints: map[string]string{
			"did:plc:labeler123": "https://example.com/labeler",
		},
	}

	ep, err := fake.LabelerEndpoint(context.Background(), "did:plc:labeler123")
	if err != nil {
		t.Fatalf("LabelerEndpoint failed: %v", err)
	}

	if ep != "https://example.com/labeler" {
		t.Errorf("expected endpoint 'https://example.com/labeler', got %q", ep)
	}
}

// TestLabelerEndpointNotFound tests ErrNoLabelerEndpoint.
func TestLabelerEndpointNotFound(t *testing.T) {
	fake := &fakeDIDResolver{
		endpoints: make(map[string]string),
	}

	ep, err := fake.LabelerEndpoint(context.Background(), "did:plc:nonlabeler")
	if err != ErrNoLabelerEndpoint {
		t.Fatalf("expected ErrNoLabelerEndpoint, got %v", err)
	}

	if ep != "" {
		t.Errorf("expected empty endpoint on error, got %q", ep)
	}
}

// TestLabelerEndpointError tests error passthrough.
func TestLabelerEndpointError(t *testing.T) {
	customErr := fmt.Errorf("network error")
	fake := &fakeDIDResolver{
		errs: map[string]error{
			"did:plc:broken": customErr,
		},
	}

	ep, err := fake.LabelerEndpoint(context.Background(), "did:plc:broken")
	if err != customErr {
		t.Fatalf("expected network error, got %v", err)
	}

	if ep != "" {
		t.Errorf("expected empty endpoint on error, got %q", ep)
	}
}

// TestIndigoResolverWrapperfails with error if identity.Resolver is not available at test time.
// This test documents the wrapper but doesn't require integration testing.
func TestIndigoResolverWrapperType(t *testing.T) {
	// Verify that indigoResolver implements DIDResolver.
	var _ DIDResolver = (*indigoResolver)(nil)
}

// TestErrNoLabelerEndpointType verifies the error type exists.
func TestErrNoLabelerEndpointType(t *testing.T) {
	if ErrNoLabelerEndpoint == nil {
		t.Fatal("ErrNoLabelerEndpoint should not be nil")
	}
}
