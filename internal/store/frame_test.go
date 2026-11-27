package store

import (
	"bytes"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestFrameEncodeDecodeRoundtrip is a property-based test verifying that
// encoded frames can be decoded and recover the original seq, src, and
// labels byte-for-byte (byte-faithfulness per AC1.3).
func TestFrameEncodeDecodeRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		seq := rapid.Int64Min(1).Draw(t, "seq")
		src := rapid.StringMatching(`did:plc:[a-z0-9]+`).Draw(t, "src")

		// Generate labels with random Sig bytes (byte-faithfulness test)
		numLabels := rapid.IntRange(0, 10).Draw(t, "numLabels")
		labels := make([]*comatproto.LabelDefs_Label, numLabels)
		sigBytes := make([][]byte, numLabels)

		for i := 0; i < numLabels; i++ {
			sig := rapid.SliceOf(rapid.Byte()).Draw(t, "sig")
			sigBytes[i] = sig
			cidStr := "bafy123"

			labels[i] = &comatproto.LabelDefs_Label{
				Src: src,
				Uri: "at://did:plc:example/app.bsky.feed.post/abc123",
				Cid: &cidStr,
				Val: "label",
				Cts: "2026-06-01T00:00:00Z",
				Sig: sig,
			}
		}

		upstreamSeq := rapid.Int64Min(1).Draw(t, "upstreamSeq")

		// Encode
		encoded, err := EncodeLabelsFrame(seq, src, upstreamSeq, labels)
		require.NoError(t, err)
		require.NotEmpty(t, encoded)

		// Decode
		decoded, err := DecodeLabelsFrame(encoded)
		require.NoError(t, err)

		// Verify seq, src, and upstreamSeq preserved
		require.Equal(t, seq, decoded.Seq)
		require.Equal(t, src, decoded.Src)
		require.Equal(t, upstreamSeq, decoded.UpstreamSeq)
		require.Equal(t, len(labels), len(decoded.Labels))

		// Verify Sig bytes are byte-faithful (roundtrip property for non-empty sigs).
		// IMPORTANT: CBOR/go-cbor behavior: empty []byte{} encodes and then decodes
		// as nil (LexBytes(nil)), which is semantically identical but not identical
		// in identity (nil != []byte{}). This is a known CBOR limitation for empty
		// byte sequences. For non-empty signatures, roundtrip is byte-faithful.
		for i, label := range labels {
			decodedSig := decoded.Labels[i].Sig
			if len(label.Sig) > 0 {
				// Non-empty sigs must roundtrip byte-for-byte
				require.Equal(t, []byte(label.Sig), []byte(decodedSig),
					"label %d: Sig bytes must match exactly for non-empty signatures", i)
			} else {
				// Empty sigs decode to nil (CBOR limitation); assert semantic equivalence
				require.True(t, len(decodedSig) == 0 || decodedSig == nil,
					"label %d: empty Sig should decode to nil or empty (CBOR limitation)", i)
			}
		}
	})
}

// TestFrameEncodeDecodeLabels verifies known label encode/decode.
func TestFrameEncodeDecodeLabels(t *testing.T) {
	seq := int64(1)
	src := "did:plc:test"
	cidStr := "bafy123"
	labels := []*comatproto.LabelDefs_Label{
		{
			Src: src,
			Uri: "at://did:plc:user/app.bsky.feed.post/123",
			Cid: &cidStr,
			Val: "spam",
			Cts: "2026-06-01T00:00:00Z",
			Sig: []byte{0x01, 0x02, 0x03},
		},
	}

	upstreamSeq := int64(99)
	encoded, err := EncodeLabelsFrame(seq, src, upstreamSeq, labels)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := DecodeLabelsFrame(encoded)
	require.NoError(t, err)

	require.Equal(t, seq, decoded.Seq)
	require.Equal(t, src, decoded.Src)
	require.Equal(t, upstreamSeq, decoded.UpstreamSeq)
	require.Len(t, decoded.Labels, 1)

	label := decoded.Labels[0]
	require.Equal(t, src, label.Src)
	require.Equal(t, "at://did:plc:user/app.bsky.feed.post/123", label.Uri)
	require.NotNil(t, label.Cid)
	require.Equal(t, "bafy123", *label.Cid)
	require.Equal(t, "spam", label.Val)
	require.Equal(t, "2026-06-01T00:00:00Z", label.Cts)
	// Sig is LexBytes, which is []uint8 compatible
	require.Equal(t, []byte{0x01, 0x02, 0x03}, []byte(label.Sig))
}

func TestFrameEncodeDecodeService(t *testing.T) {
	seq := int64(42)
	src := "did:plc:example"
	op := "create"
	collection := "town.delve.labeler.service"

	rec := &bsky.LabelerService{
		CreatedAt: "2026-06-01T00:00:00Z",
		Policies:  &bsky.LabelerDefs_LabelerPolicies{},
	}
	var recBuf bytes.Buffer
	require.NoError(t, rec.MarshalCBOR(&recBuf))
	record := recBuf.Bytes()

	encoded, err := EncodeServiceFrame(seq, src, op, collection, record)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := DecodeServiceFrame(encoded)
	require.NoError(t, err)

	require.Equal(t, seq, decoded.Seq)
	require.Equal(t, src, decoded.Src)
	require.Equal(t, op, decoded.Op)
	require.Equal(t, collection, decoded.Collection)
	require.NotNil(t, decoded.Record)
	require.Equal(t, record, decoded.Record.CBOR,
		"the record must round-trip byte-for-byte")

	encodedDelete, err := EncodeServiceFrame(seq, src, "delete", collection, nil)
	require.NoError(t, err)
	decodedDelete, err := DecodeServiceFrame(encodedDelete)
	require.NoError(t, err)
	require.Equal(t, "delete", decodedDelete.Op)
	require.Equal(t, collection, decodedDelete.Collection)
	require.Nil(t, decodedDelete.Record, "delete frames carry no record")
}

// A typed record decoder would discard unknown fields. Preserve the original bytes instead.
func TestServiceFrameByteFaithfulRecord(t *testing.T) {
	// {"unknownField": "some value", "$type": "app.bsky.labeler.service",
	//  "createdAt": "2026-06-02T00:00:00Z"}
	record := []byte{
		0xa3,                                                             // map(3)
		0x6c, 'u', 'n', 'k', 'n', 'o', 'w', 'n', 'F', 'i', 'e', 'l', 'd', // "unknownField"
		0x6a, 's', 'o', 'm', 'e', ' ', 'v', 'a', 'l', 'u', 'e', // "some value"
		0x65, '$', 't', 'y', 'p', 'e', // "$type"
		0x78, 0x18, // text(24): "app.bsky.labeler.service"
		'a', 'p', 'p', '.', 'b', 's', 'k', 'y', '.', 'l', 'a', 'b', 'e', 'l', 'e', 'r', '.', 's', 'e', 'r', 'v', 'i', 'c', 'e',
		0x69, 'c', 'r', 'e', 'a', 't', 'e', 'd', 'A', 't', // "createdAt"
		0x74, '2', '0', '2', '6', '-', '0', '6', '-', '0', '2', 'T', '0', '0', ':', '0', '0', ':', '0', '0', 'Z',
	}

	encoded, err := EncodeServiceFrame(7, "did:plc:raw", "update", "so.sprk.labeler.service", record)
	require.NoError(t, err)

	decoded, err := DecodeServiceFrame(encoded)
	require.NoError(t, err)
	require.Equal(t, "so.sprk.labeler.service", decoded.Collection)
	require.Equal(t, "update", decoded.Op)
	require.NotNil(t, decoded.Record)
	require.Equal(t, record, decoded.Record.CBOR,
		"hand-encoded record with unknown fields must survive byte-identically")
}

func TestDecodeLegacyServiceFrameWithoutCollection(t *testing.T) {
	// Hand-built legacy frame: map(4){"op":"create","seq":42,"src":"did:plc:x",
	// "record":{"a":1}} — the exact field order the old generator emitted
	// (op, seq, src, record) and no collection key.
	legacy := []byte{
		0xa4,           // map(4)
		0x62, 'o', 'p', // "op"
		0x66, 'c', 'r', 'e', 'a', 't', 'e', // "create"
		0x63, 's', 'e', 'q', // "seq"
		0x18, 0x2a, // 42
		0x63, 's', 'r', 'c', // "src"
		0x69, 'd', 'i', 'd', ':', 'p', 'l', 'c', ':', 'x', // "did:plc:x"
		0x66, 'r', 'e', 'c', 'o', 'r', 'd', // "record"
		0xa1, 0x61, 'a', 0x01, // {"a": 1}
	}

	decoded, err := DecodeServiceFrame(legacy)
	require.NoError(t, err)
	require.Equal(t, int64(42), decoded.Seq)
	require.Equal(t, "create", decoded.Op)
	require.Equal(t, "did:plc:x", decoded.Src)
	require.Equal(t, "", decoded.Collection,
		"legacy frames carry no collection; consumers default to app.bsky")
	require.NotNil(t, decoded.Record)
	require.Equal(t, []byte{0xa1, 0x61, 'a', 0x01}, decoded.Record.CBOR)
}

// The collection key sorts after record in the CBOR map.
// A record decoder that consumes more than one item would swallow collection.
func TestRawRecordDecodesOneItemOnly(t *testing.T) {
	record := []byte{0xa1, 0x61, 'a', 0x01}
	encoded, err := EncodeServiceFrame(3, "did:plc:one", "create", "app.bsky.labeler.service", record)
	require.NoError(t, err)

	decoded, err := DecodeServiceFrame(encoded)
	require.NoError(t, err)
	require.Equal(t, record, decoded.Record.CBOR)
	require.Equal(t, "app.bsky.labeler.service", decoded.Collection,
		"the record decode must not consume the following collection field")

	// Malformed record value: a map claiming 2 pairs with only 1 present.
	malformed := []byte{0xa2, 0x61, 'a', 0x01}
	badFrame, err := EncodeServiceFrame(3, "did:plc:one", "create", "app.bsky.labeler.service", malformed)
	require.NoError(t, err)
	_, err = DecodeServiceFrame(badFrame)
	require.Error(t, err, "malformed record containers must be rejected at decode")
}

// TestFrameEncodeDecodeInfo verifies info frame encode/decode.
func TestFrameEncodeDecodeInfo(t *testing.T) {
	name := "OutdatedCursor"
	message := "cursor is before retention floor"

	encoded, err := EncodeInfoFrame(name, &message)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := DecodeInfoFrame(encoded)
	require.NoError(t, err)

	require.Equal(t, name, decoded.Name)
	require.NotNil(t, decoded.Message)
	require.Equal(t, message, *decoded.Message)
}

// TestFrameEmptyLabels verifies encoding/decoding empty label list.
func TestFrameEmptyLabels(t *testing.T) {
	seq := int64(1)
	src := "did:plc:test"
	labels := []*comatproto.LabelDefs_Label{}

	encoded, err := EncodeLabelsFrame(seq, src, 0, labels)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := DecodeLabelsFrame(encoded)
	require.NoError(t, err)

	require.Equal(t, seq, decoded.Seq)
	require.Equal(t, src, decoded.Src)
	require.Empty(t, decoded.Labels)
}

// TestFrameInfoWithoutMessage verifies info frame with nil message.
func TestFrameInfoWithoutMessage(t *testing.T) {
	name := "FutureCursor"

	encoded, err := EncodeInfoFrame(name, nil)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := DecodeInfoFrame(encoded)
	require.NoError(t, err)

	require.Equal(t, name, decoded.Name)
	require.Nil(t, decoded.Message)
}
