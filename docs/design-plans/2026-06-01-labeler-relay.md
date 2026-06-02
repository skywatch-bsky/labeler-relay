# Labeler Relay Design

## Summary

The labeler relay is a Go service that aggregates multiple AT Protocol label streams into a single, unified output. Each labeler in the Bluesky ecosystem exposes a `com.atproto.label.subscribeLabels` WebSocket subscription — the relay connects to all of them simultaneously, ingests their label events, and rebroadcasts them as one re-sequenced stream under a new XRPC lexicon (`community.labeler.sync.subscribeLabelers`). The output carries a single monotonic sequence number minted by the relay, so downstream consumers need only track one cursor regardless of how many upstream labelers are aggregated.

The implementation is built on top of Bluesky's `indigo` library, which already solves the fan-out problem for the main AT Protocol firehose relay. The relay reuses `indigo`'s `EventManager` and `EventPersistence` interface for the generic core (fan-out, backfill-to-live handoff, seq minting) and writes a custom `LabelSlurper` ingest engine in its place, since the existing `Slurper` is tightly coupled to repo-stream semantics. Labelers are discovered automatically by watching the AT Protocol firehose for `app.bsky.labeler.service` record operations, or registered manually via an authenticated admin API. Labels are passed through byte-faithfully — the relay does not re-encode or re-sign them — so downstream consumers can still cryptographically verify each label against its origin labeler's signing key.

## Definition of Done

v1 is complete when:

- The relay aggregates **two or more real labelers** (`mod.bsky.app` and `ozone.skywatch.blue`) into a single output stream `community.labeler.sync.subscribeLabelers`.
- The output stream is a union of `#labels`, `#service`, and `#info` messages sharing one monotonic, relay-assigned global `seq`.
- Consumers can backfill from a `cursor` within the retention window and seamlessly hand off to live tailing.
- Labelers are discovered two ways: automatically by watching `com.atproto.sync.subscribeRepos` for `app.bsky.labeler.service` records, and manually via an authenticated admin HTTP API.
- A consumer can verify any relayed label against its **origin labeler's** `#atproto_label` signing key — proving byte-faithful passthrough (end-to-end, across both upstreams).
- **Operational hardening:** per-labeler upstream rate limiting works under load; the retention-window prune job is validated; crash-restart resume is proven (at-least-once delivery); and a metrics/health endpoint is live.

## Acceptance Criteria

### labeler-relay.AC1: Faithful label passthrough
- **labeler-relay.AC1.1 Success:** A label relayed from `mod.bsky.app` cryptographically verifies against `did:plc:ar7c4by46qjdydhdevvrndac#atproto_label`.
- **labeler-relay.AC1.2 Success:** A label relayed from `ozone.skywatch.blue` verifies against `did:plc:e4elbtctnfqocyfcml6h2lf7#atproto_label`.
- **labeler-relay.AC1.3 Success:** A label's `sig` bytes and all signed fields are byte-for-byte identical in the relay's output to what arrived from upstream.
- **labeler-relay.AC1.4 Edge:** A label's `src` in the output equals its origin labeler's DID (no rewrite to the relay's identity).

### labeler-relay.AC2: Unified output stream & cursors
- **labeler-relay.AC2.1 Success:** A consumer connecting with no cursor receives live `#labels` and `#service` messages on `community.labeler.sync.subscribeLabelers`.
- **labeler-relay.AC2.2 Success:** A consumer connecting with `cursor=N` receives backfill from `relay_seq > N` in order, then continues live with no gap or duplicate at the seam.
- **labeler-relay.AC2.3 Failure:** A `cursor` above the current head returns a `FutureCursor` error frame.
- **labeler-relay.AC2.4 Edge:** A `cursor` below the retention floor returns `#info OutdatedCursor` and resumes from the floor.

### labeler-relay.AC3: Global sequencing
- **labeler-relay.AC3.1 Success:** Interleaved events from multiple labelers receive a single strictly-increasing `relay_seq`.
- **labeler-relay.AC3.2 Success:** `relay_seq` is monotonic under concurrent ingest from all upstreams.
- **labeler-relay.AC3.3 Edge:** `relay_seq` never rewinds or reuses a value across crash, restart, and prune.

### labeler-relay.AC4: Ingest & unsigned policy
- **labeler-relay.AC4.1 Success:** A signed label from an enabled labeler is persisted and emitted.
- **labeler-relay.AC4.2 Failure:** With `require_sig=true`, an unsigned label is dropped and `dropped_unsigned{labeler_did}` increments.
- **labeler-relay.AC4.3 Success:** With a per-labeler `require_sig=false` override, that labeler's unsigned labels are relayed.

### labeler-relay.AC5: Firehose discovery & #service
- **labeler-relay.AC5.1 Success:** A `subscribeRepos` commit creating an `app.bsky.labeler.service` record registers the labeler with `source='firehose'`, `enabled=1`.
- **labeler-relay.AC5.2 Success:** That same commit emits a `#service` message carrying the full `app.bsky.labeler.service` record.
- **labeler-relay.AC5.3 Success:** A labeler updating its service record produces a new `#service` message downstream.

### labeler-relay.AC6: Retention window
- **labeler-relay.AC6.1 Success:** Events older than the configured window are pruned and the retention floor advances.
- **labeler-relay.AC6.2 Edge:** Both `#labels` and `#service` events are subject to the same window.

### labeler-relay.AC7: Rate limiting
- **labeler-relay.AC7.1 Success:** A labeler exceeding its per-second/per-hour limit is throttled at its own goroutine.
- **labeler-relay.AC7.2 Edge:** Throttling one upstream does not starve or block ingest from other labelers.

### labeler-relay.AC8: Admin API
- **labeler-relay.AC8.1 Success:** `POST /admin/labelers {did}` with a valid token registers a labeler (`source='manual'`).
- **labeler-relay.AC8.2 Success:** `DELETE /admin/labelers/{did}` removes/disables a labeler.
- **labeler-relay.AC8.3 Failure:** A request without a valid bearer token is rejected (401).
- **labeler-relay.AC8.4 Edge:** A manually-added labeler is not auto-disabled by discovery churn.

### labeler-relay.AC9: Consumer lifecycle
- **labeler-relay.AC9.1 Success:** A consumer too slow to drain is disconnected rather than blocking the broadcast.
- **labeler-relay.AC9.2 Success:** A reconnecting consumer resumes from its last cursor and backfills the gap.

### labeler-relay.AC10: Crash resume & observability
- **labeler-relay.AC10.1 Success:** After a crash and restart, the relay resumes each upstream from its last durable bookmark (at-least-once).
- **labeler-relay.AC10.2 Success:** The firehose consumer resumes from its persisted cursor after restart.
- **labeler-relay.AC10.3 Success:** A health/metrics endpoint reports head seq, labeler count, and retention window.

### labeler-relay.AC11: N=2 aggregation integrity
- **labeler-relay.AC11.1 Success:** With both `mod.bsky.app` and `ozone.skywatch.blue` connected, the output contains labels from both, each verifying against its own origin key.
- **labeler-relay.AC11.2 Edge:** Provenance is uncontaminated — no label from one labeler is attributed to the other across the two upstream seq spaces.

## Glossary

- **AT Protocol (atproto)**: The open, federated social protocol underlying Bluesky. Defines the data model, lexicons, identity (DIDs), and repository structure.
- **Labeler**: An AT Protocol service that emits content labels (e.g., spam, adult content) about posts or accounts. Identified by a DID and exposes a `subscribeLabels` WebSocket.
- **Label (`com.atproto.label.defs#label`)**: A structured assertion about a subject (post, account, etc.) — contains fields like `src`, `uri`, `val`, `cts`, and a cryptographic signature (`sig`).
- **`subscribeLabels` (`com.atproto.label.subscribeLabels`)**: The WebSocket subscription that a labeler exposes, emitting `#labels` and `#info` frames.
- **`subscribeRepos` (`com.atproto.sync.subscribeRepos`)**: The AT Protocol firehose — a WebSocket stream of all repository commits across the network. Used here for labeler auto-discovery.
- **`app.bsky.labeler.service`**: An AT Protocol record type that a labeler publishes in its own repo, advertising that it is a labeler service and where it can be reached.
- **DID (Decentralised Identifier)**: A W3C standard self-sovereign identifier (e.g., `did:plc:ar7c4by46qjdydhdevvrndac`) used to identify accounts and services in atproto.
- **DID document**: A JSON document resolved from a DID that lists the subject's public keys and service endpoints. The relay uses these to locate labeler `subscribeLabels` endpoints.
- **`#atproto_label` signing key**: The public key declared in a labeler's DID document specifically for signing labels. Consumers verify label `sig` fields against this key.
- **`did:plc`**: A DID method maintained by Bluesky's PLC directory — the most common identity type on Bluesky.
- **XRPC**: The HTTP/WebSocket RPC convention used by AT Protocol. Lexicon-defined; NSID-addressed.
- **NSID (Namespaced Identifier)**: Reverse-DNS identifiers that name XRPC methods and record types (e.g., `community.labeler.sync.subscribeLabelers`).
- **Lexicon**: The AT Protocol schema definition language — JSON files that define XRPC methods, record types, and data shapes. Compiled to Go structs via `cbor-gen`.
- **CBOR**: The binary encoding format used for AT Protocol event frames on WebSocket streams. `cbor-gen` generates the marshalling code from lexicon schemas.
- **`indigo`**: Bluesky's open-source Go monorepo (`github.com/bluesky-social/indigo`) containing the reference relay, PDS, and shared libraries used here.
- **`EventManager` (indigo)**: The indigo component that handles downstream fan-out, backfill-to-live handoff, and slow-consumer disconnection. Reused directly in this relay.
- **`EventPersistence` (indigo interface)**: The interface between `EventManager` and durable storage — defines `Persist()` (mint seq, write frame) and `Playback(cursor, callback)`. The relay implements this with `LabelPersist` over SQLite.
- **Slurper (indigo pattern)**: Indigo's pattern for managing per-host upstream connections: one goroutine per host, redial with exponential backoff, per-host cursor persistence, per-host rate limiting. `LabelSlurper` mirrors this pattern.
- **WAL (Write-Ahead Logging)**: A SQLite journal mode that allows concurrent reads during writes, used here to let the backfill query path run alongside the serialised write path.
- **relay_seq**: The relay-minted global sequence number assigned to each persisted event. Monotonic across all upstream labelers; the single cursor value consumers track.
- **Upstream cursor / bookmark**: The per-labeler sequence number recorded from the upstream `subscribeLabels` stream, used to resume ingest after a crash without re-fetching the full history.
- **Retention window**: A configurable time horizon (default ~14 days) beyond which old events are pruned. Defines the floor below which consumers receive an `OutdatedCursor` info message.
- **At-least-once delivery**: The crash-resume guarantee: events may be re-emitted after a restart (the relay re-fetches from the last durable bookmark), but will never be silently lost. Safe because labels are idempotent.
- **Token-bucket rate limiter**: A per-upstream rate limiting algorithm that allows short bursts up to a bucket capacity and then throttles to a steady-state rate. Used to prevent a misbehaving labeler from flooding the ingest path.
- **`#service` frame**: An event type in the unified output stream carrying a full `app.bsky.labeler.service` record, emitted whenever a labeler creates or updates its service record.
- **`#info OutdatedCursor`**: A protocol frame returned to consumers whose cursor falls below the retention floor, instructing them to resume from the oldest available event rather than receiving an error.
- **MST (Merkle Search Tree)**: The data structure AT Protocol uses for content-addressable repo storage. Relevant only in that `LabelSlurper` explicitly drops MST validation that the indigo `Slurper` performs — labels don't use it.

## Architecture

A single Go process aggregates many labelers' `com.atproto.label.subscribeLabels` streams and rebroadcasts them as one re-sequenced stream, reusing bluesky's `indigo` library for the generic event-fanout machinery.

The design mirrors indigo's own firehose Relay, which already solves "N upstream subscriptions → one monotonic seq → one output stream." That problem splits cleanly into a **custom ingest side** (indigo's `Slurper` is welded to repo-stream semantics, so we write our own) and a **generic core** (indigo's `EventManager` + `EventPersistence` interface + CBOR codegen, reused wholesale).

### Component map

```
        DISCOVERY                    INGEST                  CORE                 OUTPUT
  ┌──────────────────┐
  │ FirehoseWatcher  │── new DID ──┐
  │ (subscribeRepos, │             │
  │  app.bsky.       │             ▼
  │  labeler.service)│      ┌──────────────┐
  └──────────────────┘      │ LabelerReg   │   the set of labelers we track
  ┌──────────────────┐      │  (SQLite)    │
  │  Admin HTTP API  │──────│              │
  └──────────────────┘      └──────┬───────┘
                                   │ drives
                                   ▼
                            ┌──────────────┐    labels    ┌──────────────┐
              upstream WS   │ LabelSlurper │──(drop-      │ EventManager │
        labeler A ─────────▶│  goroutine/  │   unsigned,  │  (indigo,    │
        labeler B ─────────▶│  labeler,    │   ingest     │  fan-out)    │
            ...             │  redial,     │   chan)      └──────┬───────┘
                            │  ratelimit   │                     │
                            └──────────────┘              ┌──────▼────────┐
                                   │ funnels to           │ LabelPersist  │
                                   └─────────────────────▶│ (SQLite, WAL) │
                                                          │ seq + mapping │
                                                          │ + window prune│
                                                          └──────┬────────┘
                                                                 │ Playback(cursor)
                            ┌──────────────────────────┐         │
   downstream consumers ◀───│ subscribeLabelers server │◀────────┘
   (wss://relay/...)        │ + #service emission      │  backfill then live-tail
                            └──────────────────────────┘
```

### Components

1. **FirehoseWatcher** — consumes `com.atproto.sync.subscribeRepos`, filters commits touching `app.bsky.labeler.service`. On create/update: resolves the labeler's DID doc, upserts a registry row, and emits a `#service` event into the ingest funnel. One code path feeds both discovery and the `#service` output stream.
2. **Admin HTTP API** — authenticated CRUD over the labeler registry (the manual discovery path).
3. **LabelerRegistry** — SQLite-backed source of truth: tracked labelers, resolved endpoints, discovery source, enabled flag, per-labeler upstream resume cursor.
4. **LabelSlurper** — custom ingest engine: one redial-looping goroutine per enabled labeler, rate-limited, dropping unsigned labels, funneling verified-present-sig labels into a single ingest channel.
5. **LabelPersist** — implements indigo's `EventPersistence`: mints the global `relay_seq`, persists the label/service frame plus `(labeler_did, upstream_seq → relay_seq)` mapping, enforces the bounded retention window.
6. **EventManager** — indigo's component, reused for downstream fan-out and backfill→live handoff.
7. **Output server** — serves `community.labeler.sync.subscribeLabelers` (backfill-from-cursor then live tail) plus admin and health endpoints.

### Key design decisions (verified during brainstorming)

- **True-relay global seq.** The relay mints its own monotonic `relay_seq` across all labelers (like the firehose Relay over PDSes), keeping a single-cursor contract. Per-labeler upstream cursors are tracked separately (resume position), distinct from the output seq.
- **Bounded retention window.** Conservative default (~14d), configurable. Below the floor, consumers get `#info OutdatedCursor`. Deep/full backfill is explicitly out of scope for v1. Sized for the aggregate of dozens-to-hundreds of labelers, not just bluesky.
- **Drop unsigned, strict passthrough.** `require_sig=true` by default (with per-labeler override). The relay does a **structural** sig-presence check, not cryptographic verification — it cannot pass on its own trust, so the paranoid consumer verifies anyway. Relayed labels are byte-faithful: sig bytes and signed fields are never re-encoded lossily. *Verified live: `mod.bsky.app` 60/60 and `ozone.skywatch.blue` 60/60 labels signed on the wire, including pre-spec 2023/2024 backfill.*
- **Unified union output.** Following `subscribeRepos`'s multi-payload union precedent, labels and service records share one stream and one cursor space. `#service` carries the full `app.bsky.labeler.service` record (self-contained, no consumer refetch).
- **Auto-subscribe discovery.** Firehose-discovered labelers go live immediately (`enabled=1`); abuse is mitigated by per-upstream rate limiting, not by manual approval gates. Admin can disable by exception.

## Existing Patterns

This is a greenfield repository — no prior code exists. The design follows patterns from bluesky's `indigo` library (`github.com/bluesky-social/indigo`), specifically the firehose Relay (`cmd/relay`):

- **Slurper pattern** (`cmd/relay/relay/slurper.go`): per-host goroutine map, redial-with-backoff, per-host cursor persistence, per-host rate limiting. `LabelSlurper` mirrors this structure but speaks `subscribeLabels` frames instead of `subscribeRepos`, and drops the repo-specific commit/MST validation.
- **EventPersistence interface** (`cmd/relay/stream/persist/persist.go`): `Persist(ctx, *XRPCStreamEvent)` + `Playback(since, callback)`. The relay's seq is minted inside the persistence layer under a single writer (e.g. `DiskPersistence.doPersist`: `seq := dp.curSeq; dp.curSeq++`). `LabelPersist` is a new implementation of this interface backed by SQLite.
- **EventManager** (`cmd/relay/stream/eventmgr/event_manager.go`): publish-subscribe fanout, backfill→live cutover, slow-consumer disconnect. Reused directly; it already carries `LabelLabels`/`LabelInfo` event variants.
- **Lexicon codegen**: `XRPCStreamEvent`, `LabelDefs_Label`, `LabelSubscribeLabels_*`, and `Labeler_Service` structs are generated under `api/atproto` and `api/bsky`. DID resolution (plc + web, cached) comes from `atproto/identity`.

Divergence from indigo: the `Slurper` ingest side is repo-specific (validates commits, MST structure, `RepoCommit`/`RepoSync` event kinds), so `LabelSlurper` is written fresh rather than adapted. The generic core is reused without modification.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: Project scaffolding & lexicon
**Goal:** A buildable Go module with the output lexicon defined and Go types generated.

**Components:**
- `go.mod` depending on `github.com/bluesky-social/indigo`
- `lexicons/community/labeler/sync/subscribeLabelers.json` — the output subscription lexicon (union of `#labels`, `#service`, `#info`; `cursor` param; `FutureCursor` error)
- Generated Go structs from the lexicon via indigo's `cbor-gen` tooling (`gen/` + committed `*_cbor.go`)
- `cmd/labeler-relay/main.go` entry point (wiring stub)

**Dependencies:** None (first phase)

**Done when:** `go build ./...` succeeds; lexicon JSON validates; generated types compile.
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Registry & persistence layer
**Goal:** SQLite-backed registry and the `EventPersistence` implementation that mints the global seq.

**Components:**
- `internal/store/` — SQLite (WAL) schema: `events` (relay_seq PK AUTOINCREMENT, kind, labeler_did, upstream_seq, frame_cbor, ingest_ts), `labelers` (registry: did PK, endpoint, source, enabled, last_upstream_seq, last_error), `meta` (firehose cursor, schema version)
- `LabelPersist` in `internal/store/` — implements indigo's `EventPersistence`: serialized `Persist()` mints `relay_seq`, writes frame + mapping; `Playback(cursor)` streams `relay_seq`-ordered frames; emits `OutdatedCursor` below the floor
- `LabelerRegistry` accessors (upsert, enable/disable, list, cursor read/write)

**Dependencies:** Phase 1

**Done when:** Tests pass for: seq monotonicity under concurrent ingest; seq never rewinds across simulated crash+restart+prune; Playback boundary behaviour (OutdatedCursor / FutureCursor); registry CRUD. Covers `labeler-relay.AC2.*`, `labeler-relay.AC3.*`, `labeler-relay.AC6.*`.
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: LabelSlurper (ingest)
**Goal:** Connect to upstream labelers, decode frames, drop unsigned, funnel into persistence.

**Components:**
- `internal/slurper/` — per-labeler goroutine with redial (exponential backoff + jitter), `subscribeLabels` frame decode, `#labels`/`#info` handling
- Unsigned-drop policy (`require_sig`, per-labeler override) with `dropped_unsigned{labeler_did}` metric
- Single ingest channel → `Persist()`; persist-before-bookmark ordering; batched `last_upstream_seq` flush
- Registry-driven reconcile loop (enable spins up a goroutine; disable tears it down)
- Per-upstream token-bucket rate limiter (per-second / per-hour)

**Dependencies:** Phase 2

**Done when:** Tests pass for: frame decode round-trip with **sig bytes byte-for-byte preserved**; unsigned labels dropped + counted; reconcile loop start/stop; rate limiter throttles a flooding upstream without starving the funnel. Covers `labeler-relay.AC1.*`, `labeler-relay.AC4.*`, `labeler-relay.AC7.*`.
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Discovery (firehose + admin API)
**Goal:** Both discovery paths populate the registry; firehose updates also emit `#service`.

**Components:**
- `internal/firehose/` — `subscribeRepos` consumer filtering `app.bsky.labeler.service` ops; DID-doc resolution for `#atproto_labeler` endpoint; upsert with `source='firehose'`, `enabled=1`; emits `#service` event (full record) into the ingest funnel; persists firehose cursor in `meta`
- `internal/admin/` — authenticated HTTP API: `POST /admin/labelers`, `DELETE /admin/labelers/{did}`, `GET /admin/labelers`, enable/disable; bearer-token auth

**Dependencies:** Phase 2, Phase 3

**Done when:** Tests pass for: firehose commit with a `labeler.service` op → registry upsert + `#service` emission; admin add/remove/list with auth enforcement; manual entries are sticky. Covers `labeler-relay.AC5.*`, `labeler-relay.AC8.*`.
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: Output server
**Goal:** Serve the unified `subscribeLabelers` stream with backfill→live handoff.

**Components:**
- `internal/server/` — XRPC/WebSocket handler for `community.labeler.sync.subscribeLabelers`: parse `cursor`, `Playback` backfill, hand off to `EventManager` live tail; encode `{op,t}` header + stored body frame for `#labels`/`#service`/`#info`
- Slow-consumer disconnect (reuse EventManager behaviour)
- `GET /_health` (or `describeRelay`): head seq, labeler count, retention window

**Dependencies:** Phase 2, Phase 3, Phase 4

**Done when:** Tests pass for: backfill from a mid-stream cursor replays in `relay_seq` order then continues live with no gap/dup at the seam; `OutdatedCursor` below floor; `FutureCursor` above head; health endpoint reports correct state. Covers `labeler-relay.AC2.*`, `labeler-relay.AC9.*`.
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Operational hardening
**Goal:** Prune job, crash-resume, metrics — meeting the hardened Definition of Done.

**Components:**
- Periodic prune job: `DELETE FROM events WHERE ingest_ts < now - window`, advancing the retention floor (both `#labels` and `#service` share the window)
- Crash-restart resume: replay firehose + each labeler from last durable bookmark (at-least-once)
- Metrics surface (per-labeler ingest rate, dropped-unsigned, connected upstreams, consumer count, head seq)
- Config plumbing (`retention_window`, `auto_subscribe_discovered`, `require_sig`, `upstream_rate_limit`, `admin_token`, `db_path`, `listen_addr`, `firehose_url`)

**Dependencies:** Phase 5

**Done when:** Tests pass for: prune advances floor and below-floor cursors get `OutdatedCursor`; crash+restart resumes both upstreams without loss (at-least-once, idempotent); rate limiting verified under simulated load; metrics/health endpoint live. Covers `labeler-relay.AC6.*`, `labeler-relay.AC7.*`, `labeler-relay.AC10.*`.
<!-- END_PHASE_6 -->

<!-- START_PHASE_7 -->
### Phase 7: N=2 end-to-end smoke test
**Goal:** Prove faithful aggregation against two real labelers.

**Components:**
- E2E test/harness pointing at `mod.bsky.app` (`did:plc:ar7c4by46qjdydhdevvrndac`) and `ozone.skywatch.blue` (`did:plc:e4elbtctnfqocyfcml6h2lf7`)
- Consume the relay's own `subscribeLabelers` output and assert passthrough integrity, provenance, and global re-seq monotonicity

**Dependencies:** Phase 6

**Done when:** A relayed label from each origin still **cryptographically verifies against that origin's `#atproto_label` key**; `src`/provenance is uncontaminated across the two seq spaces; interleaved events carry a single strictly-increasing `relay_seq` and backfill replays both sources in order. Covers `labeler-relay.AC1.*`, `labeler-relay.AC11.*`.
<!-- END_PHASE_7 -->

## Additional Considerations

**Crash consistency / delivery semantics.** Ordering is push-to-ingest → durable persist (mints seq) → advance upstream bookmark. A crash mid-flight re-fetches from the last durable bookmark, so delivery is **at-least-once**. Labels are declarative and idempotent by `(src, uri, val, cts)`, so re-emission downstream is harmless.

**Scale ceiling.** SQLite (WAL, batched-transaction writes) comfortably handles the aggregate write rate of hundreds of labelers (single serialized writer, well under SQLite's throughput). If the relay ever needs thousands of labelers with unbounded retention, the swap point is the `EventPersistence` interface (SQLite → Postgres, separating read replicas for backfill from the write path). Not a v1 concern.

**`require_sig` reversibility.** Default `true`, but a per-labeler override exists. If a tail labeler is observed (via the `dropped_unsigned` metric) emitting unsigned labels and is deemed worth carrying, the flag flips without a code change.
