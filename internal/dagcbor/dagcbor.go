// pattern: Functional Core
//
// Package dagcbor provides checked, bounded, one-item DAG-CBOR scanning for
// record bytes the relay passes through verbatim.
//
// Two APIs, because a byte slice knows its remaining length and a stream does
// not:
//
//   - ValidateMap validates a complete byte slice as exactly one well-formed
//     DAG-CBOR map. Container counts are checked against the remaining slice
//     bytes before descending (every array element is at least 1 byte, every
//     map pair at least 2), so a header claiming a huge container is rejected
//     immediately, with no allocation and no counter that can overflow.
//
//   - ScanOne (scan.go) reads exactly one CBOR item from an io.Reader, copying
//     every consumed byte to an optional io.Writer. Overflow safety comes from
//     per-item iteration rather than a remaining-length pre-check: a
//     definite-length container is consumed item by item, each header read
//     consuming real bytes, so a huge claimed length simply hits EOF
//     mid-container. No allocation ever derives from a claimed length. A
//     recursion depth cap and an explicit consumed-bytes budget bound
//     pathological input.
//
// Both enforce the same well-formedness policy (the DAG-CBOR data-model
// contract, per https://ipld.io/specs/codecs/dag-cbor/spec/):
//
//   - definite lengths only; indefinite-length items (additional info 31) are
//     rejected, as are the reserved additional infos 28-30
//   - major types 0, 1, 2, 3, 4, and 5 are accepted; text strings (major type
//     3) must be valid UTF-8
//   - major type 7 accepts exactly false (0xf4), true (0xf5), null (0xf6),
//     and finite float64 (0xfb); every other simple value (including
//     undefined 0xf7 and unallocated codes) and float16/float32 are rejected,
//     as are non-finite floats
//   - map keys must be text strings, and duplicate keys within one map are
//     rejected
//   - tags are rejected except tag 42, whose payload must be a non-empty byte
//     string carrying the 0x00 identity multibase prefix followed by a
//     syntactically valid CID (parsed with github.com/ipfs/go-cid)
//
// Deliberate relaxation, documented for future readers: this is a
// well-formedness/data-model validator in the historical-decoder posture, not
// a canonical-encoding validator. Map key ordering and minimal
// integer/length encodings are NOT enforced, because the bytes are passed
// through verbatim and never re-encoded.
//
// cbor-gen's ScanForLinks is deliberately NOT used for this: its
// remaining-value counter does `remaining += extra * 2` in uint64 without an
// overflow check, so a map header claiming ~2^63 pairs wraps the counter to
// zero and the scan "succeeds" on truncated input.
package dagcbor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/ipfs/go-cid"
)

// maxDepth bounds CBOR recursion depth (nesting of arrays and maps).
const maxDepth = 64

// MaxScanBytes is the consumed-bytes budget ScanOne enforces as
// belt-and-braces against pathological input. Real declaration records are
// ~1 KB.
const MaxScanBytes = 1 << 20 // 1 MiB

// errTruncated reports input that ends before the item it describes.
var errTruncated = errors.New("dagcbor: truncated input")

// Major types, per RFC 8949.
const (
	majUnsignedInt = 0
	majNegativeInt = 1
	majByteString  = 2
	majTextString  = 3
	majArray       = 4
	majMap         = 5
	majTag         = 6
	majSimple      = 7
)

// simpleValue validates a major-type-7 initial byte. The 8-byte float64
// payload (after 0xfb) is validated separately by each scanner as it is
// consumed.
func simpleValue(fb byte) error {
	switch fb {
	case 0xf4, 0xf5, 0xf6, 0xfb:
		return nil
	default:
		return fmt.Errorf("dagcbor: unsupported simple value or float width (0x%02x)", fb)
	}
}

// checkFloat64 rejects NaN and infinities; all other float64 bits are
// well-formed DAG-CBOR.
func checkFloat64(bits uint64) error {
	f := math.Float64frombits(bits)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return errors.New("dagcbor: non-finite float64")
	}
	return nil
}

// argLen decodes the additional-info field of a major-type 0-6 initial byte:
// either an immediate argument value (immediate=true, the value equals ai) or
// the width in bytes of the argument that follows.
func argLen(ai byte) (n int, immediate bool, err error) {
	switch {
	case ai < 24:
		return 0, true, nil
	case ai == 24:
		return 1, false, nil
	case ai == 25:
		return 2, false, nil
	case ai == 26:
		return 4, false, nil
	case ai == 27:
		return 8, false, nil
	case ai == 28, ai == 29, ai == 30:
		return 0, false, fmt.Errorf("dagcbor: reserved additional info %d", ai)
	default: // 31
		return 0, false, errors.New("dagcbor: indefinite-length items are not allowed")
	}
}

// decodeArg decodes big-endian argument bytes into the argument value.
func decodeArg(b []byte) uint64 {
	var extra uint64
	for _, x := range b {
		extra = extra<<8 | uint64(x)
	}
	return extra
}

// checkTag rejects every tag except the DAG-CBOR CID tag (42).
func checkTag(tag uint64) error {
	if tag != 42 {
		return fmt.Errorf("dagcbor: tag %d is not allowed (only the CID tag 42)", tag)
	}
	return nil
}

// checkCID validates a tag-42 byte-string payload: the 0x00 identity
// multibase prefix followed by a syntactically valid CID.
func checkCID(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("dagcbor: CID payload is empty")
	}
	if payload[0] != 0x00 {
		return errors.New("dagcbor: CID payload must start with the 0x00 identity multibase prefix")
	}
	if _, err := cid.Cast(payload[1:]); err != nil {
		return fmt.Errorf("dagcbor: invalid CID: %w", err)
	}
	return nil
}

// ValidateMap checks that data is exactly one complete, well-formed DAG-CBOR
// map: no trailing bytes, no indefinite lengths, and every container count
// consistent with the remaining bytes. It performs no allocation proportional
// to any claimed length.
func ValidateMap(data []byte) error {
	if len(data) == 0 {
		return errors.New("dagcbor: empty input")
	}
	if data[0]>>5 != majMap {
		return fmt.Errorf("dagcbor: top-level item must be a map, got major type %d", data[0]>>5)
	}
	s := sliceScanner{data: data}
	if err := s.value(0); err != nil {
		return err
	}
	if s.pos != len(s.data) {
		return fmt.Errorf("dagcbor: %d trailing byte(s) after top-level item", len(s.data)-s.pos)
	}
	return nil
}

// sliceScanner walks a complete in-memory item.
type sliceScanner struct {
	data []byte
	pos  int
}

// remaining returns the number of unread bytes.
func (s *sliceScanner) remaining() int { return len(s.data) - s.pos }

// initial reads and decodes one item header (initial byte plus, for major
// types 0-6, its argument bytes), advancing past it.
func (s *sliceScanner) initial() (maj byte, extra uint64, err error) {
	if s.pos >= len(s.data) {
		return 0, 0, errTruncated
	}
	fb := s.data[s.pos]
	s.pos++
	maj = fb >> 5

	if maj == majSimple {
		if err := simpleValue(fb); err != nil {
			return 0, 0, err
		}
		if fb != 0xfb {
			return maj, 0, nil
		}
		if s.remaining() < 8 {
			return 0, 0, errTruncated
		}
		bits := binary.BigEndian.Uint64(s.data[s.pos : s.pos+8])
		s.pos += 8
		if err := checkFloat64(bits); err != nil {
			return 0, 0, err
		}
		return maj, 0, nil
	}

	n, immediate, err := argLen(fb & 0x1f)
	if err != nil {
		return 0, 0, err
	}
	if immediate {
		return maj, uint64(fb & 0x1f), nil
	}
	if s.remaining() < n {
		return 0, 0, errTruncated
	}
	extra = decodeArg(s.data[s.pos : s.pos+n])
	s.pos += n
	return maj, extra, nil
}

// value validates one complete item at the current position, advancing past
// it.
func (s *sliceScanner) value(depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("dagcbor: nesting deeper than %d levels", maxDepth)
	}
	maj, extra, err := s.initial()
	if err != nil {
		return err
	}
	switch maj {
	case majUnsignedInt, majNegativeInt, majSimple:
		return nil
	case majByteString:
		if extra > uint64(s.remaining()) {
			return errTruncated
		}
		s.pos += int(extra)
		return nil
	case majTextString:
		if extra > uint64(s.remaining()) {
			return errTruncated
		}
		if !utf8.Valid(s.data[s.pos : s.pos+int(extra)]) {
			return errors.New("dagcbor: invalid UTF-8 in text string")
		}
		s.pos += int(extra)
		return nil
	case majArray:
		// Every element is at least 1 byte, so a claimed count beyond the
		// remaining bytes is rejected before iterating. The comparison avoids
		// multiplication so no claimed count can overflow it.
		if extra > uint64(s.remaining()) {
			return fmt.Errorf("dagcbor: array claims %d elements but only %d bytes remain", extra, s.remaining())
		}
		for i := uint64(0); i < extra; i++ {
			if err := s.value(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case majMap:
		// Every pair is at least 2 bytes (key header + value header).
		// Dividing the remaining byte count instead of multiplying the pair
		// count keeps the check overflow-free even for a claimed ~2^63 pairs.
		if extra > uint64(s.remaining())/2 {
			return fmt.Errorf("dagcbor: map claims %d pairs but only %d bytes remain", extra, s.remaining())
		}
		seen := make(map[string]struct{})
		for i := uint64(0); i < extra; i++ {
			if err := s.mapPair(depth, seen); err != nil {
				return err
			}
		}
		return nil
	case majTag:
		if err := checkTag(extra); err != nil {
			return err
		}
		return s.cidPayload()
	default:
		return fmt.Errorf("dagcbor: unsupported major type %d", maj)
	}
}

// mapPair validates one key/value pair, registering the key in seen.
func (s *sliceScanner) mapPair(depth int, seen map[string]struct{}) error {
	maj, extra, err := s.initial()
	if err != nil {
		return err
	}
	if maj != majTextString {
		return errors.New("dagcbor: map key must be a text string")
	}
	if extra > uint64(s.remaining()) {
		return errTruncated
	}
	key := s.data[s.pos : s.pos+int(extra)]
	if !utf8.Valid(key) {
		return errors.New("dagcbor: invalid UTF-8 in map key")
	}
	if _, dup := seen[string(key)]; dup {
		return fmt.Errorf("dagcbor: duplicate map key %q", key)
	}
	seen[string(key)] = struct{}{}
	s.pos += int(extra)
	return s.value(depth + 1)
}

// cidPayload validates the byte string following a tag-42 header.
func (s *sliceScanner) cidPayload() error {
	maj, extra, err := s.initial()
	if err != nil {
		return err
	}
	if maj != majByteString {
		return errors.New("dagcbor: CID payload must be a byte string")
	}
	if extra > uint64(s.remaining()) {
		return errTruncated
	}
	payload := s.data[s.pos : s.pos+int(extra)]
	s.pos += int(extra)
	return checkCID(payload)
}
