package community

import (
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
)

// LabelerSyncSubscribeLabelers_Labels is the #labels variant of the
// community.labeler.sync.subscribeLabelers output stream. Seq is the
// relay-minted global sequence; Labels are byte-faithful to upstream.
type LabelerSyncSubscribeLabelers_Labels struct {
	Seq    int64                         `json:"seq" cborgen:"seq"`
	Src    string                        `json:"src" cborgen:"src"`
	Labels []*comatproto.LabelDefs_Label `json:"labels" cborgen:"labels"`
}

// LabelerSyncSubscribeLabelers_Service is the #service variant, carrying a
// full app.bsky.labeler.service record under the relay-minted Seq.
//
// FIDELITY NOTE: unlike #labels (whose label Sig bytes are passed through
// verbatim), the #service record is decoded from the firehose CAR into a typed
// *bsky.LabelerService and RE-ENCODED here. It is therefore NOT byte-faithful
// to the upstream bytes — it is field-faithful (all fields preserved) but the
// CBOR encoding may differ. No acceptance criterion requires service-record
// signature fidelity; service records are advisory metadata, not signed labels.
// Only labels are byte-faithful in this relay.
type LabelerSyncSubscribeLabelers_Service struct {
	Seq    int64                `json:"seq" cborgen:"seq"`
	Src    string               `json:"src" cborgen:"src"`
	Record *bsky.LabelerService `json:"record" cborgen:"record"`
}

// LabelerSyncSubscribeLabelers_Info is the #info variant (e.g. OutdatedCursor).
type LabelerSyncSubscribeLabelers_Info struct {
	Name    string  `json:"name" cborgen:"name"`
	Message *string `json:"message,omitempty" cborgen:"message,omitempty"`
}
