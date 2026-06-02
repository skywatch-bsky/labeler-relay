// pattern: Functional Core

package store

import (
	"bytes"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/scarndp/labeler-relay/api/community"
)

// EncodeLabelsFrame builds a #labels output body carrying the relay seq,
// upstream seq, and byte-faithful upstream labels, and returns its CBOR bytes.
func EncodeLabelsFrame(seq int64, src string, upstreamSeq int64, labels []*comatproto.LabelDefs_Label) ([]byte, error) {
	frame := &community.LabelerSyncSubscribeLabelers_Labels{
		Seq:         seq,
		Src:         src,
		UpstreamSeq: upstreamSeq,
		Labels:      labels,
	}

	var buf bytes.Buffer
	if err := frame.MarshalCBOR(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeLabelsFrame decodes a CBOR-encoded #labels frame and returns it.
func DecodeLabelsFrame(data []byte) (*community.LabelerSyncSubscribeLabelers_Labels, error) {
	frame := &community.LabelerSyncSubscribeLabelers_Labels{}
	if err := frame.UnmarshalCBOR(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return frame, nil
}

// EncodeServiceFrame builds a #service output body and returns its CBOR bytes.
func EncodeServiceFrame(seq int64, src string, op string, rec *bsky.LabelerService) ([]byte, error) {
	frame := &community.LabelerSyncSubscribeLabelers_Service{
		Seq:    seq,
		Src:    src,
		Op:     op,
		Record: rec,
	}

	var buf bytes.Buffer
	if err := frame.MarshalCBOR(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeServiceFrame decodes a CBOR-encoded #service frame and returns it.
func DecodeServiceFrame(data []byte) (*community.LabelerSyncSubscribeLabelers_Service, error) {
	frame := &community.LabelerSyncSubscribeLabelers_Service{}
	if err := frame.UnmarshalCBOR(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return frame, nil
}

// EncodeInfoFrame builds an #info output body (e.g. OutdatedCursor).
func EncodeInfoFrame(name string, message *string) ([]byte, error) {
	frame := &community.LabelerSyncSubscribeLabelers_Info{
		Name:    name,
		Message: message,
	}

	var buf bytes.Buffer
	if err := frame.MarshalCBOR(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeInfoFrame decodes a CBOR-encoded #info frame and returns it.
func DecodeInfoFrame(data []byte) (*community.LabelerSyncSubscribeLabelers_Info, error) {
	frame := &community.LabelerSyncSubscribeLabelers_Info{}
	if err := frame.UnmarshalCBOR(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return frame, nil
}
