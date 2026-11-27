// pattern: Functional Core

package dagcbor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// ScanOne reads exactly one DAG-CBOR item from r and copies its bytes to w,
// which may be nil. MaxScanBytes bounds consumed bytes and string allocations.
// Nesting is limited to maxDepth (64).
//
// Exact-size reads leave subsequent frame fields for the caller, including
// when r is a cbor-gen CborReader positioned mid-frame.
func ScanOne(r io.Reader, w io.Writer) error {
	sc := &streamScanner{r: r, w: w}
	return sc.value(0)
}

// streamScanner consumes items from a stream, copying consumed bytes to w.
type streamScanner struct {
	r        io.Reader
	w        io.Writer
	consumed int64
}

// read consumes exactly len(p) bytes, enforcing the scan budget and copying
// the bytes to w. Reads are exact-size (io.ReadFull), never read-ahead.
func (s *streamScanner) read(p []byte) error {
	if int64(len(p)) > MaxScanBytes-s.consumed {
		return fmt.Errorf("dagcbor: item exceeds the %d byte scan budget", MaxScanBytes)
	}
	if _, err := io.ReadFull(s.r, p); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return errTruncated
		}
		return err
	}
	s.consumed += int64(len(p))
	if s.w != nil {
		if _, err := s.w.Write(p); err != nil {
			return err
		}
	}
	return nil
}

// initial reads and decodes one item header, copying its bytes to w.
func (s *streamScanner) initial() (maj byte, extra uint64, err error) {
	var fbBuf [1]byte
	if err = s.read(fbBuf[:]); err != nil {
		return 0, 0, err
	}
	fb := fbBuf[0]
	maj = fb >> 5

	if maj == majSimple {
		if err := simpleValue(fb); err != nil {
			return 0, 0, err
		}
		if fb != 0xfb {
			return maj, 0, nil
		}
		var payload [8]byte
		if err = s.read(payload[:]); err != nil {
			return 0, 0, err
		}
		if err := checkFloat64(binary.BigEndian.Uint64(payload[:])); err != nil {
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
	var arg [8]byte
	if err = s.read(arg[:n]); err != nil {
		return 0, 0, err
	}
	return maj, decodeArg(arg[:n]), nil
}

func (s *streamScanner) value(depth int) error {
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
	case majByteString, majTextString:
		return s.stringItem(maj, extra)
	case majArray:
		// Each element consumes real bytes. Huge counts reach EOF or the byte
		// budget without overflowing a remaining-value counter.
		for i := uint64(0); i < extra; i++ {
			if err := s.value(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case majMap:
		return s.mapPairs(extra, depth)
	case majTag:
		if err := checkTag(extra); err != nil {
			return err
		}
		return s.cidPayload()
	default:
		return fmt.Errorf("dagcbor: unsupported major type %d", maj)
	}
}

// stringItem consumes a byte or text string of the given length, validating
// UTF-8 for text strings. The bytes are held only for the current item,
// bounded by the scan budget.
func (s *streamScanner) stringItem(maj byte, extra uint64) error {
	buf, err := s.readBounded(extra)
	if err != nil {
		return err
	}
	if maj == majTextString && !utf8.Valid(buf) {
		return errors.New("dagcbor: invalid UTF-8 in text string")
	}
	return nil
}

// mapPairs consumes the pairs of a definite-length map, requiring text keys
// and rejecting duplicates within the map.
func (s *streamScanner) mapPairs(pairs uint64, depth int) error {
	seen := make(map[string]struct{})
	for i := uint64(0); i < pairs; i++ {
		maj, extra, err := s.initial()
		if err != nil {
			return err
		}
		if maj != majTextString {
			return errors.New("dagcbor: map key must be a text string")
		}
		key, err := s.readBounded(extra)
		if err != nil {
			return err
		}
		if !utf8.Valid(key) {
			return errors.New("dagcbor: invalid UTF-8 in map key")
		}
		if _, dup := seen[string(key)]; dup {
			return fmt.Errorf("dagcbor: duplicate map key %q", key)
		}
		seen[string(key)] = struct{}{}
		if err := s.value(depth + 1); err != nil {
			return err
		}
	}
	return nil
}

// cidPayload consumes and validates the byte string following a tag-42
// header.
func (s *streamScanner) cidPayload() error {
	maj, extra, err := s.initial()
	if err != nil {
		return err
	}
	if maj != majByteString {
		return errors.New("dagcbor: CID payload must be a byte string")
	}
	if extra == 0 {
		return errors.New("dagcbor: CID payload is empty")
	}
	payload, err := s.readBounded(extra)
	if err != nil {
		return err
	}
	return checkCID(payload)
}

func (s *streamScanner) readBounded(extra uint64) ([]byte, error) {
	if extra > MaxScanBytes {
		return nil, fmt.Errorf("dagcbor: item of %d bytes exceeds the %d byte scan budget", extra, MaxScanBytes)
	}
	buf := make([]byte, extra)
	if err := s.read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}
