package server_test

import (
	"bytes"
	"testing"

	"github.com/scarndp/labeler-relay/internal/server"
	"github.com/scarndp/labeler-relay/internal/store"
	rapid "pgregory.net/rapid"
)

// decodeFrame reads a frame payload (header CBOR + body CBOR) written by
// WriteMessage or WriteError and returns the decoded header and body bytes.
func decodeFrame(t *testing.T, payload []byte) (hdr server.FrameHeader, body []byte) {
	t.Helper()
	r := bytes.NewReader(payload)
	if err := hdr.UnmarshalCBOR(r); err != nil {
		t.Fatalf("UnmarshalCBOR header: %v", err)
	}
	body = make([]byte, r.Len())
	copy(body, payload[len(payload)-r.Len():])
	return hdr, body
}

// decodeErrorBody reads an ErrorFrameBody from raw CBOR bytes.
func decodeErrorBody(t *testing.T, raw []byte) server.ErrorFrameBody {
	t.Helper()
	var b server.ErrorFrameBody
	if err := b.UnmarshalCBOR(bytes.NewReader(raw)); err != nil {
		t.Fatalf("UnmarshalCBOR ErrorFrameBody: %v", err)
	}
	return b
}

// TestFrames_WriteMessage_PBTRoundtrip verifies that WriteMessage is byte-faithful:
// decode(WriteMessage(t, body)) recovers the same t and exact body bytes.
// Property: roundtrip — the body is not re-encoded, only the header is added.
func TestFrames_WriteMessage_PBTRoundtrip(t *testing.T) {
	msgTypes := []string{"#labels", "#service", "#info"}

	rapid.Check(t, func(rt *rapid.T) {
		msgType := rapid.SampledFrom(msgTypes).Draw(rt, "msgType")
		bodyBytes := rapid.SliceOf(rapid.Byte()).Draw(rt, "body")

		var buf bytes.Buffer
		if err := server.WriteMessage(&buf, msgType, bodyBytes); err != nil {
			rt.Fatalf("WriteMessage: %v", err)
		}

		payload := buf.Bytes()
		r := bytes.NewReader(payload)

		var hdr server.FrameHeader
		if err := hdr.UnmarshalCBOR(r); err != nil {
			rt.Fatalf("UnmarshalCBOR: %v", err)
		}

		// Header assertions.
		if hdr.Op != 1 {
			rt.Fatalf("expected op=1, got %d", hdr.Op)
		}
		if hdr.T != msgType {
			rt.Fatalf("expected t=%q, got %q", msgType, hdr.T)
		}

		// Body bytes must be the remaining bytes exactly — byte-faithful.
		remaining := make([]byte, r.Len())
		if _, err := r.Read(remaining); err != nil && r.Len() > 0 {
			rt.Fatalf("reading body: %v", err)
		}
		if !bytes.Equal(remaining, bodyBytes) {
			rt.Fatalf("body not byte-faithful: got %x, want %x", remaining, bodyBytes)
		}
	})
}

// TestFrames_WriteMessage_PBTRoundtrip_ExplicitEdgeCases covers edge-case bodies
// with explicit examples to complement the PBT random inputs.
func TestFrames_WriteMessage_PBTRoundtrip_ExplicitEdgeCases(t *testing.T) {
	cases := []struct {
		name    string
		msgType string
		body    []byte
	}{
		{"empty body", "#labels", []byte{}},
		{"single byte body", "#service", []byte{0x00}},
		{"nil-equiv empty body", "#info", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := server.WriteMessage(&buf, tc.msgType, tc.body); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			hdr, body := decodeFrame(t, buf.Bytes())
			if hdr.Op != 1 {
				t.Errorf("expected op=1, got %d", hdr.Op)
			}
			if hdr.T != tc.msgType {
				t.Errorf("expected t=%q, got %q", tc.msgType, hdr.T)
			}
			want := tc.body
			if want == nil {
				want = []byte{}
			}
			if !bytes.Equal(body, want) {
				t.Errorf("body mismatch: got %x, want %x", body, want)
			}
		})
	}
}

// TestFrames_WriteError_FutureCursor verifies AC2.3: WriteError("FutureCursor", "")
// produces op:-1, error:"FutureCursor".
func TestFrames_WriteError_FutureCursor(t *testing.T) {
	var buf bytes.Buffer
	if err := server.WriteError(&buf, "FutureCursor", ""); err != nil {
		t.Fatalf("WriteError: %v", err)
	}

	hdr, bodyBytes := decodeFrame(t, buf.Bytes())

	if hdr.Op != -1 {
		t.Errorf("expected op=-1, got %d", hdr.Op)
	}
	if hdr.T != "" {
		t.Errorf("expected empty t for error frame, got %q", hdr.T)
	}

	errBody := decodeErrorBody(t, bodyBytes)
	if errBody.Error != "FutureCursor" {
		t.Errorf("expected error=FutureCursor, got %q", errBody.Error)
	}
}

// TestFrames_WriteMessage_OutdatedCursorInfo verifies AC2.4: the OutdatedCursor
// response is a MESSAGE frame (op:1, t:"#info"), not an error frame.
func TestFrames_WriteMessage_OutdatedCursorInfo(t *testing.T) {
	msg := "OutdatedCursor: your cursor is below the retention floor"
	infoBody, err := store.EncodeInfoFrame("OutdatedCursor", &msg)
	if err != nil {
		t.Fatalf("EncodeInfoFrame: %v", err)
	}

	var buf bytes.Buffer
	if err := server.WriteMessage(&buf, "#info", infoBody); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	hdr, body := decodeFrame(t, buf.Bytes())

	if hdr.Op != 1 {
		t.Errorf("expected op=1 (message, not error), got %d", hdr.Op)
	}
	if hdr.T != "#info" {
		t.Errorf("expected t=#info, got %q", hdr.T)
	}
	if !bytes.Equal(body, infoBody) {
		t.Errorf("body not byte-faithful: got %x, want %x", body, infoBody)
	}

	// Decode the info body to verify name field.
	decoded, err := store.DecodeInfoFrame(body)
	if err != nil {
		t.Fatalf("DecodeInfoFrame: %v", err)
	}
	if decoded.Name != "OutdatedCursor" {
		t.Errorf("expected name=OutdatedCursor, got %q", decoded.Name)
	}
}

// TestFrames_WriteError_MessageField verifies that WriteError propagates the
// message field correctly.
func TestFrames_WriteError_MessageField(t *testing.T) {
	var buf bytes.Buffer
	if err := server.WriteError(&buf, "ConsumerTooSlow", "consumer is too slow"); err != nil {
		t.Fatalf("WriteError: %v", err)
	}

	hdr, bodyBytes := decodeFrame(t, buf.Bytes())
	if hdr.Op != -1 {
		t.Errorf("expected op=-1, got %d", hdr.Op)
	}

	errBody := decodeErrorBody(t, bodyBytes)
	if errBody.Error != "ConsumerTooSlow" {
		t.Errorf("expected error=ConsumerTooSlow, got %q", errBody.Error)
	}
	if errBody.Message != "consumer is too slow" {
		t.Errorf("expected message=%q, got %q", "consumer is too slow", errBody.Message)
	}
}
