// pattern: Functional Core

package dagcbor

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// testCID returns a syntactically valid CIDv1 (dag-cbor, sha2-256).
func testCID(t *testing.T) []byte {
	t.Helper()
	c, err := cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256).Sum([]byte("dagcbor-test"))
	require.NoError(t, err)
	return c.Bytes()
}

// cidLink encodes a valid DAG-CBOR tag-42 CID link.
func cidLink(t *testing.T) []byte {
	t.Helper()
	payload := append([]byte{0x00}, testCID(t)...)
	out := []byte{0xd8, 0x2a, 0x58, byte(len(payload))}
	return append(out, payload...)
}

// float64Item encodes a float64 (0xfb + 8 big-endian bits).
func float64Item(f float64) []byte {
	out := []byte{0xfb}
	var bits [8]byte
	binary.BigEndian.PutUint64(bits[:], math.Float64bits(f))
	return append(out, bits[:]...)
}

// nestedMaps builds n nested single-pair maps {"a": ...} around inner.
func nestedMaps(n int, inner []byte) []byte {
	out := inner
	for i := 0; i < n; i++ {
		out = append([]byte{0xa1, 0x61, 0x61}, out...)
	}
	return out
}

// bareReader wraps a reader exposing only Read, so tests cannot accidentally
// rely on byte-slice or other conveniences.
type bareReader struct {
	r io.Reader
}

func (b bareReader) Read(p []byte) (int, error) { return b.r.Read(p) }

func TestValidateRecordCBOR(t *testing.T) {
	t.Parallel()

	validNested := []byte{
		0xa4,                // map(4)
		0x63, 'n', 'u', 'm', // "num"
		0x18, 0x2a, // uint(42)
		0x62, 'n', 'g', // "ng"
		0x20,           // negative int(-1)
		0x62, 'a', 'r', // "ar"
		0x82, 0xf4, 0xf5, // array(false, true)
		0x64, 'm', 'a', 'p', 's', // "maps"
		0xa1, 0x61, 'k', 0xf6, // {"k": null}
	}

	cases := []struct {
		name string
		data []byte
		err  string // empty means the case must validate
	}{
		{name: "valid simple map", data: []byte{0xa1, 0x61, 0x61, 0x01}},
		{name: "valid nested structure", data: validNested},
		{name: "valid deep-but-bounded nesting", data: nestedMaps(60, []byte{0xf4})},
		{
			name: "valid CID link",
			data: append([]byte{0xa1, 0x61, 'c'}, cidLink(t)...),
		},
		{
			name: "valid finite float64",
			data: append([]byte{0xa1, 0x61, 'f'}, float64Item(1.5)...),
		},
		{
			name: "valid negative zero float64",
			data: append([]byte{0xa1, 0x61, 'f'}, float64Item(math.Copysign(0, -1))...),
		},
		{
			// Relaxed-canonical: non-minimal integer and map-length encodings
			// are accepted because bytes pass through verbatim.
			name: "non-minimal int and map length accepted",
			data: []byte{0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // map(1) as 8-byte length
				0x61, 'a', // "a"
				0x1b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05}, // 5 as 8-byte int
		},
		{
			name: "empty input",
			data: nil,
			err:  "empty",
		},
		{
			name: "non-map top level",
			data: []byte{0x01},
			err:  "top-level item must be a map",
		},
		{
			name: "truncated map pair",
			data: []byte{0xa2, 0x61, 0x61, 0x01}, // claims 2 pairs, holds 1
			err:  "claims",
		},
		{
			name: "truncated string payload",
			data: []byte{0xa1, 0x61, 0x61, 0x63, 'a', 'b'}, // 3-byte text, 2 present
			err:  "truncated",
		},
		{
			name: "trailing bytes",
			data: []byte{0xa1, 0x61, 0x61, 0x01, 0xf4},
			err:  "trailing",
		},
		{
			name: "indefinite-length map",
			data: bytes.Join([][]byte{{0xbf}, {0x61, 0x61, 0x01}, {0xff}}, nil),
			err:  "indefinite",
		},
		{
			name: "indefinite-length array value",
			data: []byte{0xa1, 0x61, 0x61, 0x9f, 0x01, 0xff},
			err:  "indefinite",
		},
		{
			name: "indefinite-length string value",
			data: []byte{0xa1, 0x61, 0x61, 0x7f, 0x61, 0x61, 0xff},
			err:  "indefinite",
		},
		{
			// ~2^63 pairs: the exact overflow shape that wraps cbor-gen's
			// ScanForLinks counter.
			name: "huge claimed map length",
			data: append([]byte{0xbb},
				[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...),
			err: "claims",
		},
		{
			name: "huge claimed array length",
			data: append([]byte{0xa1, 0x61, 0x61, 0x9b},
				[]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...),
			err: "claims",
		},
		{
			name: "huge claimed string length",
			data: append([]byte{0xa1, 0x61, 0x61, 0x5b},
				[]byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...), // 2^32
			err: "truncated",
		},
		{
			name: "reserved additional info 28",
			data: []byte{0xa1, 0x61, 0x61, 0x1c},
			err:  "reserved",
		},
		{
			name: "break code as value",
			data: []byte{0xa1, 0x61, 0x61, 0xff},
			err:  "unsupported simple value",
		},
		{
			name: "non-text map key",
			data: []byte{0xa1, 0x01, 0x01}, // integer key
			err:  "key must be a text string",
		},
		{
			name: "duplicate map keys",
			data: []byte{0xa2, 0x61, 0x61, 0x01, 0x61, 0x61, 0x02},
			err:  "duplicate",
		},
		{
			name: "invalid UTF-8 text value",
			data: []byte{0xa1, 0x61, 0x61, 0x62, 0xfe, 0xff},
			err:  "UTF-8",
		},
		{
			name: "invalid UTF-8 map key",
			data: []byte{0xa1, 0x62, 0xfe, 0xff, 0x01},
			err:  "UTF-8",
		},
		{
			name: "epoch date tag",
			data: []byte{0xa1, 0x61, 0x61, 0xc1, 0x01},
			err:  "tag 1 is not allowed",
		},
		{
			name: "tag 42 with empty payload",
			data: append([]byte{0xa1, 0x61, 'c'}, []byte{0xd8, 0x2a, 0x40}...),
			err:  "empty",
		},
		{
			name: "tag 42 payload without identity prefix",
			data: append([]byte{0xa1, 0x61, 'c'}, []byte{0xd8, 0x2a, 0x43, 0x01, 0x02, 0x03}...),
			err:  "identity multibase prefix",
		},
		{
			name: "tag 42 payload with invalid CID bytes",
			data: append([]byte{0xa1, 0x61, 'c'}, []byte{0xd8, 0x2a, 0x42, 0x00, 0x01}...),
			err:  "invalid CID",
		},
		{
			name: "tag 42 payload that is not a byte string",
			data: append([]byte{0xa1, 0x61, 'c'}, []byte{0xd8, 0x2a, 0x01}...),
			err:  "CID payload must be a byte string",
		},
		{
			name: "NaN float64",
			data: append([]byte{0xa1, 0x61, 'f'}, float64Item(math.NaN())...),
			err:  "non-finite",
		},
		{
			name: "+Inf float64",
			data: append([]byte{0xa1, 0x61, 'f'}, float64Item(math.Inf(1))...),
			err:  "non-finite",
		},
		{
			name: "-Inf float64",
			data: append([]byte{0xa1, 0x61, 'f'}, float64Item(math.Inf(-1))...),
			err:  "non-finite",
		},
		{
			name: "undefined simple value",
			data: []byte{0xa1, 0x61, 0x61, 0xf7},
			err:  "unsupported simple value",
		},
		{
			name: "unallocated simple value 16",
			data: []byte{0xa1, 0x61, 0x61, 0xf0},
			err:  "unsupported simple value",
		},
		{
			name: "two-byte simple value",
			data: []byte{0xa1, 0x61, 0x61, 0xf8, 0x00},
			err:  "unsupported simple value",
		},
		{
			name: "float16 value",
			data: []byte{0xa1, 0x61, 0x61, 0xf9, 0x00, 0x00},
			err:  "unsupported simple value",
		},
		{
			name: "float32 value",
			data: []byte{0xa1, 0x61, 0x61, 0xfa, 0x00, 0x00, 0x00, 0x00},
			err:  "unsupported simple value",
		},
		{
			name: "over-deep nesting",
			data: nestedMaps(70, []byte{0xf4}),
			err:  "nesting deeper",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateMap(tc.data)
			if tc.err == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.err)
		})
	}
}

// TestDagCborScanOne verifies the streaming scanner: exact one-item
// consumption from a bare io.Reader (bytes after the item remain for the
// caller), verbatim copy semantics, and the same malformed-input rejections
// as ValidateMap — including huge claimed containers on truncated input,
// which must fail without allocating.
func TestDagCborScanOne(t *testing.T) {
	t.Parallel()

	record := []byte{0xa3, 0x61, 'a', 0x01, 0x61, 'b', 0xf4, 0x61, 'c', 0xf5}

	// One item consumed exactly; the following bytes survive in the reader.
	trailing := []byte{0x61, 0x63, 'o', 'l'} // a text string, as a following frame field would be
	src := bytes.NewReader(append(append([]byte{}, record...), trailing...))
	var out bytes.Buffer
	require.NoError(t, ScanOne(bareReader{r: src}, &out))
	require.Equal(t, record, out.Bytes(), "consumed bytes must copy verbatim")
	remaining := make([]byte, len(trailing))
	n, err := src.Read(remaining)
	require.NoError(t, err)
	require.Equal(t, len(trailing), n)
	require.Equal(t, trailing, remaining, "bytes after the item must remain for the caller")

	// No writer: still consumes exactly one item.
	src2 := bytes.NewReader(append(append([]byte{}, record...), trailing...))
	require.NoError(t, ScanOne(bareReader{r: src2}, nil))
	remaining2 := make([]byte, len(trailing))
	n, err = src2.Read(remaining2)
	require.NoError(t, err)
	require.Equal(t, len(trailing), n)
	require.Equal(t, trailing, remaining2)

	// A bare reader with only Read drives the whole scan (io.ReadFull path).
	require.NoError(t, ValidateMap(record))

	// Huge claimed map on truncated input: fails fast, no allocation.
	huge := append([]byte{0xbb},
		[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...)
	huge = append(huge, 0x61, 'a') // begins one pair, then EOF
	require.ErrorContains(t, ScanOne(bytes.NewReader(huge), nil), "truncated")

	// Claimed string beyond the scan budget: rejected before any allocation.
	overBudget := append([]byte{0x5b},
		[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x01}...) // 2 MiB + 1
	require.ErrorContains(t, ScanOne(bytes.NewReader(overBudget), nil), "scan budget")

	// Malformed inputs rejected with the same classes as ValidateMap.
	rejected := []struct {
		name string
		data []byte
		msg  string
	}{
		{"duplicate keys", []byte{0xa2, 0x61, 'a', 0x01, 0x61, 'a', 0x02}, "duplicate"},
		{"non-text key", []byte{0xa1, 0x01, 0x01}, "text string"},
		{"indefinite map", []byte{0xbf, 0xff}, "indefinite"},
		{"NaN", append([]byte{0xfb}, func() []byte {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], math.Float64bits(math.NaN()))
			return b[:]
		}()...), "non-finite"},
		{"empty stream", nil, "truncated"},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, ScanOne(bytes.NewReader(tc.data), nil), tc.msg)
		})
	}
}

// TestScanOneRejectsOverBudgetItem verifies the consumed-bytes budget with a
// real (not merely claimed) oversized item: a valid byte string just past
// MaxScanBytes fails rather than buffering unboundedly.
func TestScanOneRejectsOverBudgetItem(t *testing.T) {
	t.Parallel()

	item := make([]byte, MaxScanBytes+1)
	item[0] = 0x5b // byte string, 8-byte length
	binary.BigEndian.PutUint64(item[1:9], uint64(MaxScanBytes+1))
	require.ErrorContains(t, ScanOne(bytes.NewReader(item), nil), "scan budget")
}
