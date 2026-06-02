# Labeler Relay Implementation Plan — Phase 7

**Goal:** Prove faithful aggregation against two real labelers end-to-end: cryptographic verification, uncontaminated provenance, and global re-sequencing.

**Architecture:** An E2E test/harness boots the full relay process (Phase 6 wiring) pointed at the two real labelers — `mod.bsky.app` (`did:plc:ar7c4by46qjdydhdevvrndac`) and `ozone.skywatch.blue` (`did:plc:e4elbtctnfqocyfcml6h2lf7`) — registered via the admin API. It consumes the relay's OWN `community.labeler.sync.subscribeLabelers` output and asserts: each relayed label still cryptographically verifies against its origin labeler's `#atproto_label` key; provenance is uncontaminated across the two upstream seq spaces; interleaved events carry a single strictly-increasing `relay_seq`; and backfill replays both sources in order.

**Tech Stack:** Go, indigo (`atproto/identity`, `atproto/labeling`, `atproto/atcrypto`), all internal packages.

**Scope:** 7 phases. This is Phase 7 of 7 (final).

**Codebase verified:** 2026-06-01. indigo @ `5368f55344e0b5e203ab4d9501e1ddebd9983ad7`. Phases 1–6 produced the full working relay. Live label-signing was verified during design (`mod.bsky.app` 60/60, `ozone.skywatch.blue` 60/60 signed on the wire).

---

## Acceptance Criteria Coverage

This phase completes:

### labeler-relay.AC1: Faithful label passthrough
- **labeler-relay.AC1.1 Success:** A label relayed from `mod.bsky.app` cryptographically verifies against `did:plc:ar7c4by46qjdydhdevvrndac#atproto_label`.
- **labeler-relay.AC1.2 Success:** A label relayed from `ozone.skywatch.blue` verifies against `did:plc:e4elbtctnfqocyfcml6h2lf7#atproto_label`.
- **labeler-relay.AC1.3 Success:** A label's `sig` bytes and all signed fields are byte-for-byte identical in the relay's output to what arrived from upstream. *(Final end-to-end confirmation; mechanics proven in Phase 3.)*
- **labeler-relay.AC1.4 Edge:** A label's `src` in the output equals its origin labeler's DID (no rewrite to the relay's identity). *(Final end-to-end confirmation.)*

### labeler-relay.AC11: N=2 aggregation integrity
- **labeler-relay.AC11.1 Success:** With both `mod.bsky.app` and `ozone.skywatch.blue` connected, the output contains labels from both, each verifying against its own origin key.
- **labeler-relay.AC11.2 Edge:** Provenance is uncontaminated — no label from one labeler is attributed to the other across the two upstream seq spaces.

---

## Key research findings (ground truth from indigo @ 5368f553)

- **Crypto verify helper exists:** `atproto/labeling.Label.VerifySignature(pubkey atcrypto.PublicKey) error`:
  ```go
  func (l *Label) VerifySignature(pubkey atcrypto.PublicKey) error {
      if l.Sig == nil { return fmt.Errorf("can not verify unsigned commit") }
      b, err := l.UnsignedBytes() // canonical CBOR of signed fields, Sig nil
      if err != nil { return err }
      return pubkey.HashAndVerify(b, l.Sig)
  }
  ```
  `UnsignedBytes()` marshals the label with `Sig` intentionally nil → canonical signed bytes. `pubkey.HashAndVerify` does SHA-256 + verify.
- **Mapping our output label → `labeling.Label`:** the relayed label is a `comatproto.LabelDefs_Label`. The harness maps its fields (`Cid, Cts, Exp, Neg, Src, Uri, Val, Ver, Sig`) into `labeling.Label` to call `VerifySignature`. Field-for-field; `Sig` carries straight across as `[]byte`. **The verification only passes if passthrough was byte-faithful — this IS the AC1.3 proof.**
- **Key resolution:** `identity.Resolver.ResolveDID(ctx, did)` → `DIDDocument.VerificationMethod`; find the method whose `ID` ends in `#atproto_label`; parse its `PublicKeyMultibase` via `atproto/atcrypto` into an `atcrypto.PublicKey`.

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Origin-key resolution and label verification helper

**Verifies:** labeler-relay.AC1.1, labeler-relay.AC1.2 (the verify mechanism)

**Files:**
- Create: `internal/verify/verify.go`
- Test: `internal/verify/verify_test.go` (unit with a self-signed fixture; live test build-tagged)

**Implementation:**

```go
// ResolveLabelKey resolves a labeler DID to its #atproto_label public key.
func ResolveLabelKey(ctx context.Context, r identity.Resolver, did string) (atcrypto.PublicKey, error)

// VerifyRelayedLabel maps a relayed comatproto.LabelDefs_Label into a
// labeling.Label and verifies its signature against the origin key.
func VerifyRelayedLabel(label *comatproto.LabelDefs_Label, originKey atcrypto.PublicKey) error
```

`ResolveLabelKey`: `ResolveDID`, iterate `VerificationMethod` for `ID` ending `#atproto_label`, parse `PublicKeyMultibase`. `VerifyRelayedLabel`: copy fields into `labeling.Label`, call `VerifySignature(originKey)`.

**Testing:**
- Unit (offline, deterministic, never skips): generate a keypair via `atcrypto`, sign a label's `UnsignedBytes`, build a `LabelDefs_Label` with that sig, and assert `VerifyRelayedLabel` passes with the right key and FAILS with a different key. This proves the mapping is correct without network.
- Negative: mutate one signed field (e.g. `Val`) after signing → verification fails (proves the canonicalization is field-sensitive).
- A live resolution test for the two real DIDs is added in Task 3 behind `//go:build integration`.

**Verification:**
Run: `go test ./internal/verify/ -run TestVerify -v`
Expected: pass.

**Commit:** `feat: add origin-key resolution and relayed-label verification`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: E2E consumer harness

**Verifies:** labeler-relay.AC1.3, labeler-relay.AC1.4 (provenance plumbing for the live test)

**Files:**
- Create: `internal/e2e/harness.go`
- Test: (driven by Task 3)

**Implementation:**

A harness that:
- Boots the relay (`run()`/wiring from Phase 6) with a temp DB on a free port, no real firehose (discovery via admin API instead, to keep the test bounded).
- Registers both labelers via the admin API (`POST /admin/labelers`) with a test token.
- Connects a WebSocket client to the relay's own `community.labeler.sync.subscribeLabelers`.
- Decodes each frame by reading the `{op,t}` header FIRST (the union discriminator — pinned contract: header `t`, not body `$type`), then decoding the matching body:
  - `t == "#labels"` → `community.LabelerSyncSubscribeLabelers_Labels` → `(relaySeq, src, []*LabelDefs_Label)`.
  - `t == "#service"` → `community.LabelerSyncSubscribeLabelers_Service` → `(relaySeq, src, record)`.
  - `t == "#info"` → `community.LabelerSyncSubscribeLabelers_Info` → `(name, message)`.
  - `op == -1` (error frame) → decode `{error, message}`.
  This exercises the FULL union decode path end-to-end (not just `#labels`), proving the wire contract is internally consistent.
- Exposes `Collect(ctx, n int) ([]CollectedLabel, error)` collecting `#labels` frames (the AC1/AC11 subject), and `CollectAny(ctx, n int) ([]Frame, error)` returning all decoded frames regardless of type (used to assert the union decoder handles `#service`/`#info` without error). `CollectedLabel` carries the relay seq, the declared output `Src`, and the label.

```go
type CollectedLabel struct {
    RelaySeq  int64
    OutputSrc string                    // the #labels frame's src field
    Label     *comatproto.LabelDefs_Label
}
```

Keep the harness resilient: real labelers may be quiet; `Collect` waits (condition-based, bounded timeout) until `n` labels arrive or the deadline hits, returning a clear error on timeout (never a silent skip).

**Testing:** exercised by Task 3.

**Commit:** `feat: add e2e harness that consumes the relay output stream`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_TASK_3 -->
### Task 3: N=2 live integration test

**Verifies:** labeler-relay.AC1.1, labeler-relay.AC1.2, labeler-relay.AC1.3, labeler-relay.AC1.4, labeler-relay.AC11.1, labeler-relay.AC11.2

**Files:**
- Create: `internal/e2e/n2_test.go` (build tag `//go:build integration` — hits the real network)
- Modify: `Makefile` (add `test-integration` target running `go test -tags integration ./...`)

**Implementation:**

The test (under `integration` tag so the default suite stays offline, and it FAILS loudly rather than skipping if the network/labelers are unreachable — no silent skip):

1. Resolve both origin keys via `verify.ResolveLabelKey` for the two real DIDs.
2. Boot the relay; register both labelers via admin API.
3. `Collect` a batch of labels until at least one from EACH origin has arrived (bounded timeout; fail with a clear message if a labeler produced nothing in the window).
4. Assertions:
   - **AC1.1/AC1.2/AC11.1:** every collected label `VerifyRelayedLabel`s against the key of the origin identified by its `Src`. A `mod.bsky.app` label verifies against `did:plc:ar7c4by46qjdydhdevvrndac#atproto_label`; an `ozone.skywatch.blue` label against `did:plc:e4elbtctnfqocyfcml6h2lf7#atproto_label`.
   - **AC1.4 / AC11.2 (provenance):** for each collected label, the `#labels` frame's `OutputSrc` AND the label's own `Label.Src` equal the SAME origin DID, and that DID is one of the two expected. No label carries the relay's identity. Critically: a label whose `Label.Src` is `mod.bsky.app` must NEVER appear under an `OutputSrc` of `ozone.skywatch.blue` or vice versa (cross-contamination check across the two upstream seq spaces).
   - **AC1.3:** each label that verifies cryptographically is itself proof of byte-faithful passthrough (a single re-encoded byte would break the signature). Additionally assert the relay-side `Sig` equals what verification consumed (already guaranteed by using the same bytes).
   - **AC11.1 re-seq monotonicity:** the collected `RelaySeq` values are strictly increasing in delivery order, with no duplicates, even though they interleave labels from both origins.
5. **Backfill check:** record the max relay seq seen; reconnect with `cursor = maxSeq - K`; assert the backfill replays labels from BOTH sources in ascending relay_seq order with no gap/dup (ties AC2.2 to the real two-source stream).
6. **Union decode check:** using `CollectAny`, assert the harness decodes every frame type it receives without error — in particular that any `#service` frame (emitted when a registered labeler's service record is observed) and any `#info` frame decode cleanly via the header-`t` discriminator. This proves the union wire contract is consumable, not just the `#labels` subset.

**Testing:** this IS the test. It must fail (red) — not skip — if either labeler is unreachable, the key can't resolve, or no labels arrive in the window.

**Verification:**
Run: `make test-integration` (or `go test -tags integration ./internal/e2e/ -run TestN2 -v`)
Expected: pass against the live labelers.

Also run the full offline suite to confirm nothing regressed:
Run: `go test ./... -race`
Expected: all green.

**Commit:** `test: add N=2 live e2e proving faithful aggregation and provenance`
<!-- END_TASK_3 -->

---

## Phase 7 Done When

- A relayed label from each origin **cryptographically verifies against that origin's `#atproto_label` key** (`mod.bsky.app` and `ozone.skywatch.blue`). — AC1.1, AC1.2, AC11.1
- `src`/provenance is uncontaminated across the two seq spaces — no cross-attribution, no relay-identity rewrite. — AC1.4, AC11.2
- Sig bytes / signed fields are byte-for-byte faithful (proven transitively by verification). — AC1.3
- Interleaved events carry a single strictly-increasing `relay_seq`; backfill replays both sources in order. — AC11.1, AC2.2
- The integration test fails loudly (never skips) when the network or labelers are unavailable.

Run: `make test-integration` → pass. `go test ./... -race` → all green.

**This completes the v1 Definition of Done.**

**Executor note:** Verification passing is the load-bearing assertion — it simultaneously proves AC1.1/1.2 (crypto) and AC1.3 (byte-faithfulness), because any lossy re-encode anywhere in Phases 1–6 would invalidate the signature. If verification fails, suspect a CBOR re-marshal in the persist/frame path (Phase 2/3), not the verify code.
