# dagcbor

Last verified: 2026-11-27

## Purpose
Checked, bounded, one-item DAG-CBOR scanning for the record bytes the relay passes through verbatim. Backs record validation at firehose ingest and `RawRecord`'s one-item decode in stored #service frames.

## Contracts
- **Exposes**: `ValidateMap(data []byte) error` (one complete well-formed DAG-CBOR map, no trailing bytes), `ScanOne(r io.Reader, w io.Writer) error` (reads exactly one CBOR item from a stream, copying consumed bytes to w), `MaxScanBytes`
- **Guarantees**:
  - Two distinct APIs because a byte slice knows its remaining length and a stream does not: ValidateMap checks container counts against the remaining slice bytes before descending (every array element is at least 1 byte, every map pair at least 2, written as division/magnitude comparisons so no claimed count can overflow the check); ScanOne iterates definite-length containers item by item, each header read consuming real bytes, so a huge claimed length simply hits EOF mid-container. Neither allocates from a claimed length.
  - ScanOne bounds the whole item by MaxScanBytes (1 MiB) and nesting by a depth cap (64). Reads are exact-size (io.ReadFull), so bytes after the item remain for the caller — including bytes after the record inside a containing frame.
  - Shared well-formedness policy (the DAG-CBOR data-model contract): definite lengths only (additional info 31 and reserved 28-30 rejected); major types 0-5 accepted with UTF-8 validation on text strings; major type 7 accepts exactly false/true/null and finite float64; map keys must be text strings with duplicates rejected; tags rejected except tag 42, whose payload must be a non-empty byte string with the 0x00 identity multibase prefix and a syntactically valid CID (parsed with go-cid).
- **Expects**: Pure functions; no store or network access.

## Dependencies
- **Uses**: github.com/ipfs/go-cid (tag-42 payload validation)
- **Used by**: firehose (record validation at extraction), api/community (RawRecord.UnmarshalCBOR)
- **Boundary**: Must not import store, firehose, or any other internal package

## Key Decisions
- cbor-gen's `ScanForLinks` is deliberately NOT used: its remaining-value counter does `remaining += extra * 2` in uint64 without an overflow check, so a map header claiming ~2^63 pairs wraps the counter to zero and the scan "succeeds" on truncated input. This package is the safe replacement.
- Well-formedness/data-model validator, not a canonical-encoding validator: map key ordering and minimal integer/length encodings are deliberately NOT enforced, because the bytes are passed through verbatim and never re-encoded (non-minimal encodings are accepted).
- The string items ScanOne reads are held for the current item only, bounded by the scan budget (needed for UTF-8 validation and duplicate-key detection).

## Key Files
- `dagcbor.go` - package doc, shared policy helpers, ValidateMap + slice scanner
- `scan.go` - ScanOne + streaming scanner

## Gotchas
- RawRecord's one-item decode is load-bearing: in the #service frame map, "collection" (10 bytes) sorts after "record" (6 bytes), so an over-reading decode would swallow the following field.
- cbor-gen's CborReader/peeker is non-buffering (single-byte peek/unread), so ScanOne's exact reads through it are position-exact mid-frame.
