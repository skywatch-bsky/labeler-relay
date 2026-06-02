# Labeler Relay — Test Requirements

Maps every acceptance criterion from the design (`labeler-relay.AC1.1` … `labeler-relay.AC11.2`)
to the test that verifies it. The implementation phase files are the source of truth for *how*
each AC is tested and *where* the test lives; this document aggregates that into a single matrix.

## Conventions and key decisions reflected here

- **Relay seq is ours.** `events.relay_seq` (SQLite `INTEGER PRIMARY KEY AUTOINCREMENT`) is the
  monotonic global sequence, embedded inside the stored frame bytes. The backfill→live seam is OUR
  code (`server.StreamFrom` over `LabelPersist.Playback` + a local `Hub`), NOT indigo's
  `EventManager.Subscribe` (which can't sequence label events — `XRPCStreamEvent.Sequence()`
  returns `-1` for them).
- **Broadcast under the persist mutex.** `PersistIngest` broadcasts while holding `mu`, so
  broadcast order == commit order == seq order. Two dedicated tests guard this ordering:
  `internal/store/persist_test.go` (Phase 2 Task 4) and `internal/server/hub_test.go`
  (Phase 5 Task 1). A single-goroutine "in order" test cannot catch a reorder, which is why both
  are concurrent.
- **Fidelity split.** Only **labels** are byte-faithful (`Sig` bytes and signed fields pass through
  verbatim). **`#service`** records are **field-faithful** (decoded → re-encoded), by design — no AC
  requires service-record byte/signature fidelity.
- **AC1.3 byte-faithfulness is proven in layers:** frame roundtrip PBT at Phase 2
  (`internal/store/frame_test.go`), through-the-slurper at Phase 3
  (`internal/slurper/subscription_test.go`), and transitively via crypto verification at Phase 7
  (a single lossy byte breaks the signature).
- **Multi-phase ACs.** Some ACs are partially covered in one phase and completed in another. The
  matrix lists every phase that touches an AC and marks which phase **COMPLETES** it.
- **Test type legend:** `unit` (pure / no I/O), `integration` (real SQLite / local fake WS
  servers / httptest, all offline), `e2e (network)` (live, hits `mod.bsky.app` and
  `ozone.skywatch.blue`, gated behind `//go:build integration`).
- **Network-dependent tests never skip.** Phase 7's live tests fail loudly (red) if the network or
  either labeler is unreachable. They are automated, just network-dependent.

---

## Coverage matrix

| AC | Type | Test file | Completing phase | Status |
|----|------|-----------|------------------|--------|
| AC1.1 | e2e (network) | `internal/e2e/n2_test.go`; helper `internal/verify/verify_test.go` | Phase 7 | Automated (network) |
| AC1.2 | e2e (network) | `internal/e2e/n2_test.go`; helper `internal/verify/verify_test.go` | Phase 7 | Automated (network) |
| AC1.3 | unit + integration + e2e (network) | `internal/store/frame_test.go` (P2), `internal/slurper/subscription_test.go` (P3), `internal/e2e/n2_test.go` (P7) | Phase 7 | Automated (network completes) |
| AC1.4 | integration + e2e (network) | `internal/slurper/subscription_test.go` (P3), `internal/e2e/n2_test.go` (P7) | Phase 7 | Automated (network completes) |
| AC2.1 | integration | `internal/server/hub_test.go`, `internal/server/subscribe_test.go` | Phase 5 | Automated |
| AC2.2 | integration | `internal/store/playback_test.go` (P2), `internal/server/seam_test.go` + `internal/server/subscribe_test.go` (P5); cross-checked live in `internal/e2e/n2_test.go` (P7) | Phase 5 | Automated |
| AC2.3 | integration | `internal/store/playback_test.go` (P2), `internal/server/subscribe_test.go` (P5) | Phase 5 | Automated |
| AC2.4 | integration | `internal/store/playback_test.go` (P2), `internal/server/subscribe_test.go` (P5) | Phase 5 | Automated |
| AC3.1 | integration | `internal/store/persist_test.go` | Phase 2 | Automated |
| AC3.2 | integration (`-race`) | `internal/store/persist_test.go` | Phase 2 | Automated |
| AC3.3 | integration | `internal/store/retention_test.go` | Phase 2 | Automated |
| AC4.1 | integration | `internal/slurper/subscription_test.go`, `internal/slurper/slurper_test.go` | Phase 3 | Automated |
| AC4.2 | unit + integration | `internal/slurper/policy_test.go`, `internal/slurper/subscription_test.go` | Phase 3 | Automated |
| AC4.3 | unit + integration | `internal/slurper/policy_test.go`, `internal/slurper/subscription_test.go` | Phase 3 | Automated |
| AC5.1 | unit + integration | `internal/firehose/extract_test.go`, `internal/firehose/resolve_test.go`, `internal/firehose/watcher_test.go` | Phase 4 | Automated |
| AC5.2 | unit + integration | `internal/firehose/extract_test.go`, `internal/firehose/watcher_test.go` | Phase 4 | Automated |
| AC5.3 | integration | `internal/firehose/watcher_test.go` | Phase 4 | Automated |
| AC6.1 | integration | `internal/store/retention_test.go` (P2), `internal/store/prunejob_test.go` (P6) | Phase 6 | Automated |
| AC6.2 | integration | `internal/store/retention_test.go` (P2), `internal/store/prunejob_test.go` (P6) | Phase 6 | Automated |
| AC7.1 | unit + integration | `internal/slurper/ratelimit_test.go` (P3), `internal/server/metrics_endpoint_test.go` (P6) | Phase 6 | Automated |
| AC7.2 | unit + integration | `internal/slurper/ratelimit_test.go`, `internal/slurper/slurper_test.go` | Phase 3 | Automated |
| AC8.1 | integration | `internal/admin/admin_test.go` | Phase 4 | Automated |
| AC8.2 | integration | `internal/admin/admin_test.go` | Phase 4 | Automated |
| AC8.3 | unit + integration | `internal/admin/auth_test.go`, `internal/admin/admin_test.go` | Phase 4 | Automated |
| AC8.4 | integration | `internal/store/registry_test.go` (P2 storage level), `internal/admin/admin_test.go` (P4 full path) | Phase 4 | Automated |
| AC9.1 | integration | `internal/server/hub_test.go`, `internal/server/subscribe_test.go` | Phase 5 | Automated |
| AC9.2 | integration | `internal/server/seam_test.go`, `internal/server/subscribe_test.go` | Phase 5 | Automated |
| AC10.1 | integration | `internal/integration/resume_test.go` | Phase 6 | Automated |
| AC10.2 | integration | `internal/integration/resume_test.go` | Phase 6 | Automated |
| AC10.3 | integration | `internal/server/health_test.go` (P5), `internal/server/metrics_endpoint_test.go` (P6) | Phase 6 | Automated |
| AC11.1 | e2e (network) | `internal/e2e/n2_test.go` | Phase 7 | Automated (network) |
| AC11.2 | e2e (network) | `internal/e2e/n2_test.go` | Phase 7 | Automated (network) |

**Totals:** 30 acceptance criteria. **30 automated, 0 human-verified.** Of the 30, **5 are
network-dependent** (`AC1.1`, `AC1.2`, `AC11.1`, `AC11.2`, plus `AC1.3`/`AC1.4` whose *final*
confirmation is network-dependent — those two are also independently proven offline, so they are
not at-risk if the network is down). All network-dependent tests fail loudly rather than skip.

---

## AC1 — Faithful label passthrough

- **AC1.1 Success:** *A label relayed from `mod.bsky.app` cryptographically verifies against
  `did:plc:ar7c4by46qjdydhdevvrndac#atproto_label`.*
  - **e2e (network)** — `internal/e2e/n2_test.go` (Phase 7 Task 3), backed by the verify helper in
    `internal/verify/verify_test.go` (Phase 7 Task 1).
  - Asserts: every collected `mod.bsky.app` label passes `VerifyRelayedLabel` against the key
    resolved from that DID's `#atproto_label` verification method. The offline helper test proves
    the verify mechanism itself (self-signed fixture verifies with the right key, fails with a
    wrong key, fails when a signed field is mutated).

- **AC1.2 Success:** *A label relayed from `ozone.skywatch.blue` verifies against
  `did:plc:e4elbtctnfqocyfcml6h2lf7#atproto_label`.*
  - **e2e (network)** — `internal/e2e/n2_test.go` (Phase 7 Task 3); same verify helper as AC1.1.
  - Asserts: every collected `ozone.skywatch.blue` label verifies against that DID's
    `#atproto_label` key.

- **AC1.3 Success:** *A label's `sig` bytes and all signed fields are byte-for-byte identical in
  the relay's output to what arrived from upstream.*
  - **unit (PBT)** — `internal/store/frame_test.go` (Phase 2 Task 3): for arbitrary labels with
    random `Sig []byte`, `DecodeLabelsFrame(EncodeLabelsFrame(...))` yields `Sig` byte-for-byte
    identical (the frame-encode foundation).
  - **integration** — `internal/slurper/subscription_test.go` (Phase 3 Task 3): a local fake WS
    server emits a label with a specific random `Sig`; reading it back through the slurper +
    store + Playback yields a byte-identical `Sig` (passthrough through the real decode path).
  - **e2e (network), completing** — `internal/e2e/n2_test.go` (Phase 7 Task 3): a successful crypto
    verification *is* the byte-faithfulness proof (any lossy re-encode anywhere in Phases 1–6
    would break the signature).

- **AC1.4 Edge:** *A label's `src` in the output equals its origin labeler's DID (no rewrite to
  the relay's identity).*
  - **integration** — `internal/slurper/subscription_test.go` (Phase 3 Task 3): asserts the
    persisted/emitted `src` equals the fake labeler's DID and the stored `LabelDefs_Label.Src` is
    unchanged.
  - **e2e (network), completing** — `internal/e2e/n2_test.go` (Phase 7 Task 3): each frame's
    `OutputSrc` and the label's own `Label.Src` equal the same expected origin DID; never the
    relay's identity.

---

## AC2 — Unified output stream & cursors

- **AC2.1 Success:** *A consumer connecting with no cursor receives live `#labels` and `#service`
  messages on `community.labeler.sync.subscribeLabelers`.*
  - **integration** — `internal/server/hub_test.go` (Phase 5 Task 1) and
    `internal/server/subscribe_test.go` (Phase 5 Task 4).
  - Asserts: with `Hub` wired as the `LabelPersist` broadcaster, a no-cursor subscriber receives a
    persisted `labels` then `service` event in order; the concurrent-ordering variant fires
    N×M `PersistIngest` calls and asserts the consumer observes strictly-ascending `RelaySeq`
    (live-path counterpart to the Phase 2 broadcast-ordering guard).

- **AC2.2 Success:** *A consumer connecting with `cursor=N` receives backfill from `relay_seq > N`
  in order, then continues live with no gap or duplicate at the seam.*
  - **integration (storage portion)** — `internal/store/playback_test.go` (Phase 2 Task 5):
    `Playback(since=4)` yields exactly seqs 5..10 ascending.
  - **integration (seam, completing)** — `internal/server/seam_test.go` (Phase 5 Task 2) and
    `internal/server/subscribe_test.go` (Phase 5 Task 4): events persisted *during* backfill are
    delivered exactly once across the seam (the `> lastBackfill` dedup boundary), ascending, no
    gap/dup.
  - **cross-check (network)** — `internal/e2e/n2_test.go` (Phase 7 Task 3) backfill step replays
    both real sources in ascending relay_seq with no gap/dup.

- **AC2.3 Failure:** *A `cursor` above the current head returns a `FutureCursor` error frame.*
  - **integration** — `internal/store/playback_test.go` (Phase 2 Task 5):
    `CursorStatus(head+5) == CursorFuture`. `internal/server/subscribe_test.go` (Phase 5 Task 4):
    connecting with `cursor=head+5` returns a `FutureCursor` error frame and closes the conn.

- **AC2.4 Edge:** *A `cursor` below the retention floor returns `#info OutdatedCursor` and resumes
  from the floor.*
  - **integration** — `internal/store/playback_test.go` (Phase 2 Task 5):
    `CursorStatus(below-floor) == CursorOutdated`, floor reported correctly.
    `internal/server/subscribe_test.go` (Phase 5 Task 4): below-floor cursor yields a
    `#info OutdatedCursor` message frame, then backfill resumes from the floor.

---

## AC3 — Global sequencing

- **AC3.1 Success:** *Interleaved events from multiple labelers receive a single
  strictly-increasing `relay_seq`.*
  - **integration** — `internal/store/persist_test.go` (Phase 2 Task 4): persist a mix of
    `labels`/`service` from two DIDs; returned seqs are `1,2,3,…` strictly increasing, no gaps.

- **AC3.2 Success:** *`relay_seq` is monotonic under concurrent ingest from all upstreams.*
  - **integration (`-race`)** — `internal/store/persist_test.go` (Phase 2 Task 4): N goroutines ×
    M `PersistIngest` concurrently; collected seqs form exactly `{1..N*M}` (unique, monotonic).
    A second sub-test registers a broadcaster and asserts the captured broadcast order is strictly
    ascending — the regression guard for "broadcast order == seq order" (fails if broadcast moves
    outside `mu`).

- **AC3.3 Edge:** *`relay_seq` never rewinds or reuses a value across crash, restart, and prune.*
  - **integration** — `internal/store/retention_test.go` (Phase 2 Task 6): persist 1..5, prune
    all, persist again → new seq is 6 not 1; close and re-`Open` the DB → seq continues from the
    high-water mark, never rewinds (guaranteed by `AUTOINCREMENT` + `sqlite_sequence`).

---

## AC4 — Ingest & unsigned policy

- **AC4.1 Success:** *A signed label from an enabled labeler is persisted and emitted.*
  - **integration** — `internal/slurper/subscription_test.go` (Phase 3 Task 3): fake emits one
    signed label → it lands in `events` and `PersistIngest` returns a seq.
    `internal/slurper/slurper_test.go` (Phase 3 Task 4): multi-labeler reconcile → each enabled
    labeler ingests.

- **AC4.2 Failure:** *With `require_sig=true`, an unsigned label is dropped and
  `dropped_unsigned{labeler_did}` increments.*
  - **unit** — `internal/slurper/policy_test.go` (Phase 3 Task 1): `KeepLabel` drops an unsigned
    label under `required=true`.
  - **integration** — `internal/slurper/subscription_test.go` (Phase 3 Task 3): an unsigned label
    is dropped and the `DroppedUnsigned{labeler_did}` counter increments.

- **AC4.3 Success:** *With a per-labeler `require_sig=false` override, that labeler's unsigned
  labels are relayed.*
  - **unit** — `internal/slurper/policy_test.go` (Phase 3 Task 1): `SigRequired(override=false)`
    resolves to false; `KeepLabel` keeps an unsigned label.
  - **integration** — `internal/slurper/subscription_test.go` (Phase 3 Task 3): with the override,
    unsigned labels are relayed.

---

## AC5 — Firehose discovery & #service

- **AC5.1 Success:** *A `subscribeRepos` commit creating an `app.bsky.labeler.service` record
  registers the labeler with `source='firehose'`, `enabled=1`.*
  - **unit** — `internal/firehose/extract_test.go` (Phase 4 Task 1): a commit with a
    `labeler.service` create yields exactly one `LabelerServiceOp`; a non-labeler commit yields
    none. `internal/firehose/resolve_test.go` (Phase 4 Task 2): endpoint resolution from the DID
    doc (offline via a fake `DIDResolver`).
  - **integration** — `internal/firehose/watcher_test.go` (Phase 4 Task 3): after the create
    commit, a registry row exists with `source='firehose'`, `enabled=1`, and the resolved endpoint.

- **AC5.2 Success:** *That same commit emits a `#service` message carrying the full
  `app.bsky.labeler.service` record.*
  - **unit** — `internal/firehose/extract_test.go` (Phase 4 Task 1): extracted `Record` carries
    the fixture's `CreatedAt`/`Policies`.
  - **integration** — `internal/firehose/watcher_test.go` (Phase 4 Task 3): a `#service` event is
    persisted (`kind='service'`); decoding the frame body confirms all record fields survive the
    decode→re-encode→decode round trip (field-fidelity, not byte/signature fidelity by design).

- **AC5.3 Success:** *A labeler updating its service record produces a new `#service` message
  downstream.*
  - **integration** — `internal/firehose/watcher_test.go` (Phase 4 Task 3): an update commit
    produces a second `#service` event with a higher `relay_seq`.

---

## AC6 — Retention window

- **AC6.1 Success:** *Events older than the configured window are pruned and the retention floor
  advances.*
  - **integration (logic)** — `internal/store/retention_test.go` (Phase 2 Task 6): prune deletes
    old rows and advances `RetentionFloor` to the oldest survivor.
  - **integration (scheduled job, completing)** — `internal/store/prunejob_test.go` (Phase 6
    Task 1): the `PruneJob` tick (with an injected clock) deletes old events and advances the floor.

- **AC6.2 Edge:** *Both `#labels` and `#service` events are subject to the same window.*
  - **integration** — `internal/store/retention_test.go` (Phase 2 Task 6) and
    `internal/store/prunejob_test.go` (Phase 6 Task 1): an old `labels` and an old `service` event
    are both pruned (no kind filter on the prune query).

---

## AC7 — Rate limiting

- **AC7.1 Success:** *A labeler exceeding its per-second/per-hour limit is throttled at its own
  goroutine.*
  - **unit/integration** — `internal/slurper/ratelimit_test.go` (Phase 3 Task 2): a `perSec=5`
    limiter passes the first 5 quickly and paces the rest (bounded timing assertion).
  - **integration (observability, completing)** — `internal/server/metrics_endpoint_test.go`
    (Phase 6 Task 3): a throttled upstream is reflected in the throttle/dropped counters scraped
    from `/metrics`.

- **AC7.2 Edge:** *Throttling one upstream does not starve or block ingest from other labelers.*
  - **unit** — `internal/slurper/ratelimit_test.go` (Phase 3 Task 2): with two independent
    `Limiter`s, calls on the idle one return immediately while the saturated one blocks
    (condition-based).
  - **integration** — `internal/slurper/slurper_test.go` (Phase 3 Task 4): saturate labeler A's
    limiter while B streams freely; B's ingested count keeps rising while A is throttled.

---

## AC8 — Admin API

- **AC8.1 Success:** *`POST /admin/labelers {did}` with a valid token registers a labeler
  (`source='manual'`).*
  - **integration** — `internal/admin/admin_test.go` (Phase 4 Task 5): `POST` with valid token +
    did → 201; registry row `source='manual'`, `enabled=1`.

- **AC8.2 Success:** *`DELETE /admin/labelers/{did}` removes/disables a labeler.*
  - **integration** — `internal/admin/admin_test.go` (Phase 4 Task 5): `DELETE` → 204;
    `enabled=0`.

- **AC8.3 Failure:** *A request without a valid bearer token is rejected (401).*
  - **unit** — `internal/admin/auth_test.go` (Phase 4 Task 4): no/wrong token → 401; correct
    `Bearer <token>` → reaches `next` (constant-time compare).
  - **integration** — `internal/admin/admin_test.go` (Phase 4 Task 5): one end-to-end no-token
    call → 401.

- **AC8.4 Edge:** *A manually-added labeler is not auto-disabled by discovery churn.*
  - **integration (storage level)** — `internal/store/registry_test.go` (Phase 2 Task 2):
    upserting a `firehose` row over an existing `manual` row leaves `source` and `enabled`
    untouched.
  - **integration (full path, completing)** — `internal/admin/admin_test.go` (Phase 4 Task 5):
    add manual labeler X, drive a firehose upsert and a firehose delete for X, assert X stays
    `source='manual'` and is not auto-disabled.

---

## AC9 — Consumer lifecycle

- **AC9.1 Success:** *A consumer too slow to drain is disconnected rather than blocking the
  broadcast.*
  - **integration** — `internal/server/hub_test.go` (Phase 5 Task 1): a tiny-buffer subscriber
    that doesn't drain is dropped (channel closed) while a second draining subscriber keeps
    receiving every event. `internal/server/subscribe_test.go` (Phase 5 Task 4): a stalled WS
    client is disconnected (`ConsumerTooSlow`) while a well-behaved client keeps receiving.

- **AC9.2 Success:** *A reconnecting consumer resumes from its last cursor and backfills the gap.*
  - **integration** — `internal/server/seam_test.go` (Phase 5 Task 2): consume to seq K, cancel,
    re-`StreamFrom(since=K)` → first delivered is K+1, no gap.
    `internal/server/subscribe_test.go` (Phase 5 Task 4): reconnection resumes from the last
    cursor.

---

## AC10 — Crash resume & observability

- **AC10.1 Success:** *After a crash and restart, the relay resumes each upstream from its last
  durable bookmark (at-least-once).*
  - **integration** — `internal/integration/resume_test.go` (Phase 6 Task 2): ingest a slurper to
    upstream_seq=K, shut down, start a NEW slurper over the same store; the fake server's next
    connection carries `cursor=K`. Duplicate-at-seam allowed; gap not.

- **AC10.2 Success:** *The firehose consumer resumes from its persisted cursor after restart.*
  - **integration** — `internal/integration/resume_test.go` (Phase 6 Task 2): a second
    `FirehoseWatcher` process dials `subscribeRepos` with the persisted `meta` cursor. (Cursor
    write is also exercised at Phase 4 Task 3 `watcher_test.go`.)

- **AC10.3 Success:** *A health/metrics endpoint reports head seq, labeler count, and retention
  window.*
  - **integration (health, partial)** — `internal/server/health_test.go` (Phase 5 Task 5):
    `GET /_health` → 200 JSON with correct `head_seq`, `labeler_count`, `retention_floor`.
  - **integration (metrics, completing)** — `internal/server/metrics_endpoint_test.go` (Phase 6
    Task 3): `/metrics` exposes `labeler_relay_head_seq`, `labeler_relay_connected_upstreams`,
    `labeler_relay_consumer_count`, `labeler_relay_dropped_unsigned_total`.

---

## AC11 — N=2 aggregation integrity

- **AC11.1 Success:** *With both `mod.bsky.app` and `ozone.skywatch.blue` connected, the output
  contains labels from both, each verifying against its own origin key.*
  - **e2e (network)** — `internal/e2e/n2_test.go` (Phase 7 Task 3): collect labels until at least
    one from each origin arrives; each verifies against its origin's `#atproto_label` key; the
    collected `RelaySeq` values are strictly increasing in delivery order with no duplicates even
    though labels from both origins interleave.

- **AC11.2 Edge:** *Provenance is uncontaminated — no label from one labeler is attributed to the
  other across the two upstream seq spaces.*
  - **e2e (network)** — `internal/e2e/n2_test.go` (Phase 7 Task 3): for each collected label the
    frame's `OutputSrc` and the label's own `Label.Src` are the same expected origin DID; a
    `mod.bsky.app` label never appears under an `ozone.skywatch.blue` `OutputSrc` or vice versa;
    no label carries the relay's identity.

---

## Network-dependent tests (run mode)

`internal/e2e/n2_test.go` and the live resolution test in `internal/verify/verify_test.go` are
gated behind `//go:build integration`. They require reachability of `mod.bsky.app`
(`did:plc:ar7c4by46qjdydhdevvrndac`) and `ozone.skywatch.blue`
(`did:plc:e4elbtctnfqocyfcml6h2lf7`). They **fail loudly rather than skip** when the network or a
labeler is unreachable, a key cannot resolve, or no labels arrive within the bounded window.

- Run offline suite: `go test ./... -race`
- Run network suite: `make test-integration` (or `go test -tags integration ./internal/e2e/ -run TestN2 -v`)
