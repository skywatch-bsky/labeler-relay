package community

import (
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
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

// LabelerSyncSubscribeLabelers_Service is the #service variant, carrying a
// full app.bsky.labeler.service record observed via firehose discovery.
//
//   - Seq: relay-minted global sequence number
//   - Src: labeler DID
//   - Op: firehose operation ("create", "update", "delete")
//   - Record: full app.bsky.labeler.service record (nil for delete)
//
// FIDELITY NOTE: the service record is decoded from the firehose CAR into a
// typed *bsky.LabelerService and re-encoded here. It is field-faithful but
// not byte-faithful. No acceptance criterion requires service-record signature
// fidelity; only labels are byte-faithful.
type LabelerSyncSubscribeLabelers_Service struct {
	Seq    int64                `json:"seq" cborgen:"seq"`
	Src    string               `json:"src" cborgen:"src"`
	Op     string               `json:"op" cborgen:"op"`
	Record *bsky.LabelerService `json:"record,omitempty" cborgen:"record,omitempty"`
}

// LabelerSyncSubscribeLabelers_Info is the #info variant (e.g. OutdatedCursor).
type LabelerSyncSubscribeLabelers_Info struct {
	Name    string  `json:"name" cborgen:"name"`
	Message *string `json:"message,omitempty" cborgen:"message,omitempty"`
}
