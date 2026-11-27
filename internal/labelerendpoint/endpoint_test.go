package labelerendpoint

import "testing"

func TestHost(t *testing.T) {
	for _, tt := range []struct {
		input, want string
		wantErr     bool
	}{
		{"https://a.example", "a.example", false},
		{"http://a.example:8443/path?token=x", "a.example:8443", false},
		{"custom://a.example/path/", "a.example", false},
		{"a.example", "a.example", false},
		{"host:8443", "host:8443", false},
		{"127.0.0.1:8443", "127.0.0.1:8443", false},
		{"[::1]:8443", "[::1]:8443", false},
		{"a.example/path?query=x", "a.example", false},
		{"https://[::1]:8443/", "[::1]:8443", false},
		{"", "", true}, {"https:///", "", true}, {"/path", "", true},
		{"not a host", "", true}, {"https://host:bad", "", true},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Host(tt.input)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("Host(%q) = %q, %v; want %q, error %v", tt.input, got, err, tt.want, tt.wantErr)
			}
			if err == nil {
				again, err := Host(got)
				if err != nil || again != got {
					t.Fatalf("Host is not idempotent: %q, %v", again, err)
				}
			}
		})
	}
}

func TestSubscribeURL(t *testing.T) {
	zero, positive, negative := int64(0), int64(42), int64(-1)
	for _, tt := range []struct {
		endpoint string
		cursor   *int64
		want     string
		wantErr  bool
	}{
		{"a.example", nil, "wss://a.example/xrpc/com.atproto.label.subscribeLabels", false},
		{"http://a.example:8443/old?token=x", &positive, "wss://a.example:8443/xrpc/com.atproto.label.subscribeLabels?cursor=42", false},
		{"https://a.example", &zero, "wss://a.example/xrpc/com.atproto.label.subscribeLabels?cursor=0", false},
		{"custom://a.example", &negative, "wss://a.example/xrpc/com.atproto.label.subscribeLabels?cursor=-1", false},
		{"", nil, "", true},
	} {
		got, err := SubscribeURL(tt.endpoint, tt.cursor)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("SubscribeURL(%q) = %q, %v; want %q, error %v", tt.endpoint, got, err, tt.want, tt.wantErr)
		}
	}
}
