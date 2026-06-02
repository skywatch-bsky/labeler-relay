package store

import (
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

// TestFrameEncodeDecodeService verifies service frame encode/decode.
func TestFrameEncodeDecodeService(t *testing.T) {
	seq := int64(42)
	src := "did:plc:example"
	rec := &bsky.LabelerService{
		CreatedAt: "2026-06-01T00:00:00Z",
		Policies:  &bsky.LabelerDefs_LabelerPolicies{},
	}

	op := "create"
	encoded, err := EncodeServiceFrame(seq, src, op, rec)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	decoded, err := DecodeServiceFrame(encoded)
	require.NoError(t, err)

	require.Equal(t, seq, decoded.Seq)
	require.Equal(t, src, decoded.Src)
	require.Equal(t, op, decoded.Op)
	require.NotNil(t, decoded.Record)
	require.Equal(t, "2026-06-01T00:00:00Z", decoded.Record.CreatedAt)
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
