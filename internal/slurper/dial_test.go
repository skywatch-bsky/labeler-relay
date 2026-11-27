package slurper

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scarndp/labeler-relay/internal/store"
)

func TestSubscriptionDialsSecureSubscribePath(t *testing.T) {
	requests := make(chan string, 1)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("subscription request did not use TLS")
		}
		requests <- r.URL.RequestURI()
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	s, err := store.Open(t.TempDir() + "/relay.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	reg := store.NewLabelerRegistry(s)
	l := store.Labeler{DID: "did:plc:dial", Endpoint: srv.Listener.Addr().String(), Source: "manual", Enabled: true}
	if err := reg.Upsert(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	if err := reg.WriteCursor(context.Background(), l.DID, 42); err != nil {
		t.Fatal(err)
	}
	sub := &subscription{labeler: l, registry: reg, dialTLS: srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sub.dial(ctx); err == nil {
		t.Fatal("expected rejected WebSocket handshake")
	}
	select {
	case got := <-requests:
		if got != "/xrpc/com.atproto.label.subscribeLabels?cursor=42" {
			t.Fatalf("request URI = %q", got)
		}
	case <-ctx.Done():
		t.Fatal("no secure subscription request received")
	}
}
