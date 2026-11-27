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

// RawRecord preserves an upstream CAR block's DAG-CBOR bytes without re-encoding.
// The struct wrapper prevents cbor-gen from treating a []byte-derived type as a byte string.
// Both handwritten CBOR methods use pointer receivers to match cbor-gen's interfaces.
// Do not regenerate these methods.
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

// UnmarshalCBOR validates and consumes exactly one DAG-CBOR item.
// In the frame map, "record" sorts before "collection". Reading past one item corrupts the following field.
func (r *RawRecord) UnmarshalCBOR(cr io.Reader) error {
	var buf bytes.Buffer
	if err := dagcbor.ScanOne(cr, &buf); err != nil {
		return err
	}
	r.CBOR = buf.Bytes()
	return nil
}

// LabelerSyncSubscribeLabelers_Service carries a firehose declaration in a #service frame.
// Seq is relay-minted. Src is the labeler DID. Op is create, update, or delete.
// An empty Collection identifies a legacy app.bsky.labeler.service frame.
// Record preserves the upstream CAR block bytes and is nil for delete.
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
