// pattern: Functional Core
// frames.go encodes XRPC subscription wire frames. Each WebSocket binary
// message is a single frame: header CBOR then body CBOR concatenated.
// The header discriminates messages (op:1) from errors (op:-1).

package server

import (
	"bytes"
	"io"
)

// FrameHeader is the CBOR-encoded frame header for every XRPC subscription
// message. Op=1 for messages, Op=-1 for errors. T is the message type
// ("#labels", "#service", "#info"); it is omitted when empty (error frames).
// CBOR field order follows cbor-gen canonical sort (ascending byte-length,
// then lex): "t" (len 1) first, "op" (len 2) second.
type FrameHeader struct {
	T  string `cborgen:"t,omitempty"`
	Op int64  `cborgen:"op"`
}

// ErrorFrameBody is the CBOR body written after a {op:-1} error header.
type ErrorFrameBody struct {
	Error   string `cborgen:"error"`
	Message string `cborgen:"message"`
}

const (
	opMessage = int64(1)
	opError   = int64(-1)
)

// WriteMessage writes a {op:1, t:msgType} header followed by body verbatim
// as one concatenated binary payload to w. The body bytes are the stored
// frame_cbor and are not re-encoded.
func WriteMessage(w io.Writer, msgType string, body []byte) error {
	hdr := FrameHeader{Op: opMessage, T: msgType}
	var buf bytes.Buffer
	if err := hdr.MarshalCBOR(&buf); err != nil {
		return err
	}
	if _, err := buf.Write(body); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// WriteError writes a {op:-1} header followed by an {error, message} body
// as one concatenated binary payload to w.
func WriteError(w io.Writer, errName, message string) error {
	hdr := FrameHeader{Op: opError}
	body := ErrorFrameBody{Error: errName, Message: message}

	var buf bytes.Buffer
	if err := hdr.MarshalCBOR(&buf); err != nil {
		return err
	}
	if err := body.MarshalCBOR(&buf); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}
