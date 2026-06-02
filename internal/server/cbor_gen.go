// Code generated manually for FrameHeader and ErrorFrameBody.
// These match what github.com/whyrusleeping/cbor-gen would produce for
// the corresponding struct definitions with cborgen tags.

package server

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf
var _ = cid.Undef

// MarshalCBOR encodes FrameHeader as a CBOR map.
// Fields with omitempty are excluded when zero-valued.
// Field order (cbor-gen canonical: ascending by key byte-length, then lex):
//   "t"  (len 1, omitempty — excluded when empty)
//   "op" (len 2, always present)
func (t *FrameHeader) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	fieldCount := 2
	if t.T == "" {
		fieldCount--
	}

	if _, err := cw.Write(cbg.CborEncodeMajorType(cbg.MajMap, uint64(fieldCount))); err != nil {
		return err
	}

	if t.T != "" {
		if len("t") > cbg.MaxLength {
			return xerrors.Errorf("Value in field \"t\" was too long")
		}
		if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("t"))); err != nil {
			return err
		}
		if _, err := cw.WriteString("t"); err != nil {
			return err
		}
		if len(t.T) > cbg.MaxLength {
			return xerrors.Errorf("Value in field t.T was too long")
		}
		if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.T))); err != nil {
			return err
		}
		if _, err := cw.WriteString(t.T); err != nil {
			return err
		}
	}

	if len("op") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"op\" was too long")
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("op"))); err != nil {
		return err
	}
	if _, err := cw.WriteString("op"); err != nil {
		return err
	}
	if t.Op >= 0 {
		if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.Op)); err != nil {
			return err
		}
	} else {
		if err := cw.WriteMajorTypeHeader(cbg.MajNegativeInt, uint64(-t.Op-1)); err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalCBOR decodes a CBOR map into FrameHeader.
func (t *FrameHeader) UnmarshalCBOR(r io.Reader) (err error) {
	*t = FrameHeader{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("FrameHeader: map struct too large (%d)", extra)
	}

	n := extra
	nameBuf := make([]byte, 2)
	for i := uint64(0); i < n; i++ {
		nameLen, ok, err := cbg.ReadFullStringIntoBuf(cr, nameBuf, cbg.MaxLength)
		if err != nil {
			return err
		}
		if !ok {
			if err := cbg.ScanForLinks(cr, func(cid.Cid) {}); err != nil {
				return err
			}
			continue
		}

		switch string(nameBuf[:nameLen]) {
		case "t":
			sval, err := cbg.ReadStringWithMax(cr, cbg.MaxLength)
			if err != nil {
				return err
			}
			t.T = string(sval)
		case "op":
			maj, extra, err := cr.ReadHeader()
			if err != nil {
				return err
			}
			var extraI int64
			switch maj {
			case cbg.MajUnsignedInt:
				extraI = int64(extra)
				if extraI < 0 {
					return fmt.Errorf("int64 positive overflow")
				}
			case cbg.MajNegativeInt:
				extraI = int64(extra)
				if extraI < 0 {
					return fmt.Errorf("int64 negative overflow")
				}
				extraI = -1 - extraI
			default:
				return fmt.Errorf("wrong type for int64 field: %d", maj)
			}
			t.Op = int64(extraI)
		default:
			if err := cbg.ScanForLinks(r, func(cid.Cid) {}); err != nil {
				return err
			}
		}
	}

	return nil
}

// MarshalCBOR encodes ErrorFrameBody as a CBOR 2-entry map.
// Field order: "error" (len 5), "message" (len 7).
func (t *ErrorFrameBody) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{0xa2}); err != nil {
		return err
	}

	if len("error") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"error\" was too long")
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("error"))); err != nil {
		return err
	}
	if _, err := cw.WriteString("error"); err != nil {
		return err
	}
	if len(t.Error) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Error was too long")
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Error))); err != nil {
		return err
	}
	if _, err := cw.WriteString(t.Error); err != nil {
		return err
	}

	if len("message") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"message\" was too long")
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("message"))); err != nil {
		return err
	}
	if _, err := cw.WriteString("message"); err != nil {
		return err
	}
	if len(t.Message) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Message was too long")
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Message))); err != nil {
		return err
	}
	if _, err := cw.WriteString(t.Message); err != nil {
		return err
	}

	return nil
}

// UnmarshalCBOR decodes a CBOR map into ErrorFrameBody.
func (t *ErrorFrameBody) UnmarshalCBOR(r io.Reader) (err error) {
	*t = ErrorFrameBody{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("ErrorFrameBody: map struct too large (%d)", extra)
	}

	n := extra
	nameBuf := make([]byte, 7)
	for i := uint64(0); i < n; i++ {
		nameLen, ok, err := cbg.ReadFullStringIntoBuf(cr, nameBuf, cbg.MaxLength)
		if err != nil {
			return err
		}
		if !ok {
			if err := cbg.ScanForLinks(cr, func(cid.Cid) {}); err != nil {
				return err
			}
			continue
		}

		switch string(nameBuf[:nameLen]) {
		case "error":
			sval, err := cbg.ReadStringWithMax(cr, cbg.MaxLength)
			if err != nil {
				return err
			}
			t.Error = string(sval)
		case "message":
			sval, err := cbg.ReadStringWithMax(cr, cbg.MaxLength)
			if err != nil {
				return err
			}
			t.Message = string(sval)
		default:
			if err := cbg.ScanForLinks(r, func(cid.Cid) {}); err != nil {
				return err
			}
		}
	}

	return nil
}
