package community

import (
	"bytes"
	"fmt"
	"io"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"github.com/scarndp/labeler-relay/internal/dagcbor"
)

// LabelerSyncSubscribeLabelers_Labels is the #labels variant of the
// community.labeler.sync.subscribeLabelers output stream.
//
// The frame nests the upstream subscribeLabels record so downstream consumers
// receive self-describing, validatable AT Protocol objects:
//   - Seq: relay-minted global sequence number
//   - Src: origin labeler DID
//   - UpstreamSeq: the seq from the upstream labeler's subscribeLabels stream
//   - Labels: byte-faithful label records from upstream
type LabelerSyncSubscribeLabelers_Labels struct {
	Seq         int64                         `json:"seq" cborgen:"seq"`
	Src         string                        `json:"src" cborgen:"src"`
	UpstreamSeq int64                         `json:"upstreamSeq" cborgen:"upstreamSeq"`
	Labels      []*comatproto.LabelDefs_Label `json:"labels" cborgen:"labels"`
}

// RawRecord is a pre-encoded DAG-CBOR record embedded verbatim in a frame.
// The bytes are never re-encoded: MarshalCBOR writes them as-is, and
// UnmarshalCBOR consumes exactly one CBOR item (a checked scan, so malformed
// or overflow-prone containers in stored frames are rejected rather than
// over-read).
//
// It is a struct wrapper — not a []byte-derived type — so cbor-gen cannot
// special-case it as a byte string; the generator emits method calls, the
// same pattern it uses for indigo's external types. The methods are
// handwritten and must never be regenerated.
//
// The one-item decode is load-bearing: in the frame map, "collection" (10
// bytes) sorts after "record" (6 bytes), so an over-reading decode would
// swallow the following field.
type RawRecord struct {
	CBOR []byte
}

// MarshalCBOR writes the embedded CBOR bytes verbatim.
func (r *RawRecord) MarshalCBOR(w io.Writer) error {
	if r == nil || len(r.CBOR) == 0 {
		return fmt.Errorf("community.RawRecord: cannot marshal an empty CBOR payload")
	}
	_, err := w.Write(r.CBOR)
	return err
}

// UnmarshalCBOR consumes exactly one CBOR item from cr into r.CBOR. Reads are
// exact-size, so bytes after the item remain for the containing frame's
// decode loop.
func (r *RawRecord) UnmarshalCBOR(cr io.Reader) error {
	var buf bytes.Buffer
	if err := dagcbor.ScanOne(cr, &buf); err != nil {
		return err
	}
	r.CBOR = buf.Bytes()
	return nil
}

// LabelerSyncSubscribeLabelers_Service is the #service variant, carrying a
// labeler service declaration observed via firehose discovery.
//
//   - Seq: relay-minted global sequence number
//   - Src: labeler DID
//   - Op: firehose operation ("create", "update", "delete")
//   - Collection: the declaration collection NSID ("" on frames recorded
//     before multi-collection support; those records are
//     app.bsky.labeler.service)
//   - Record: the full declaration record, byte-faithful (nil for delete)
//
// FIDELITY NOTE: the service record is the upstream CAR block's raw DAG-CBOR
// bytes, passed through verbatim — not decoded and re-encoded.
type LabelerSyncSubscribeLabelers_Service struct {
	Seq        int64      `json:"seq" cborgen:"seq"`
	Src        string     `json:"src" cborgen:"src"`
	Op         string     `json:"op" cborgen:"op"`
	Collection string     `json:"collection,omitempty" cborgen:"collection,omitempty"`
	Record     *RawRecord `json:"record,omitempty" cborgen:"record,omitempty"`
}

// LabelerSyncSubscribeLabelers_Info is the #info variant (e.g. OutdatedCursor).
type LabelerSyncSubscribeLabelers_Info struct {
	Name    string  `json:"name" cborgen:"name"`
	Message *string `json:"message,omitempty" cborgen:"message,omitempty"`
}
