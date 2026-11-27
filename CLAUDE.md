# labeler-relay

Last verified: 2026-11-27

## Tech Stack
- Language: Go 1.27.3 (enforced minimum in go.mod; security floor)
- Database: SQLite (WAL mode, modernc.org/sqlite)
- Wire format: CBOR (whyrusleeping/cbor-gen for codegen)
- WebSocket: gorilla/websocket
- AT Protocol: bluesky-social/indigo (stream handling, identity resolution, crypto)
- Metrics: prometheus/client_golang
- Rate limiting: RussellLuo/slidingwindow

## Commands
- `make build` - Build all packages
- `make test` - Run unit tests
- `make test-integration` - Run E2E tests (requires network, `-tags integration`)
- `make cborgen` - Regenerate CBOR marshalling for output lexicon types
- `make lint` - Run go vet

## Project Structure
- `cmd/labeler-relay/` - Entry point; wires all components via errgroup
- `api/community/` - Output lexicon Go types + CBOR codegen (do not hand-edit `cbor_gen.go`)
- `lexicons/` - Lexicon JSON schemas for `community.labeler.sync.subscribeLabelers` (labels + labeler service declarations from the `app.bsky.labeler.service`, `town.delve.labeler.service`, and `so.sprk.labeler.service` collections) and the public query endpoints (`community.labeler.sync.getLabelers`/`getLabeler`, sharing the `labelerView` object with `id`/`createdAt`/optional `updatedAt`/`declarations`)
- `gen/` - CBOR codegen driver (run via `make cborgen`)
- `internal/store/` - SQLite store, LabelPersist (seq minting + broadcast; `ApplyServiceOp` for atomic labeler declaration ops), LabelerRegistry (incl. `labeler_declarations`), PruneJob
- `internal/labelerendpoint/` - Pure host extraction and secure subscription URL construction. Stored and API endpoints contain only the host and optional port.
- `internal/dagcbor/` - Checked, bounded, one-item DAG-CBOR scanning (slice validation + streaming) backing declaration-record validation and `RawRecord`'s frame decode
- `internal/slurper/` - Per-labeler upstream subscriptions with redial, rate limiting, sig policy
- `internal/firehose/` - Firehose watcher for labeler auto-discovery (all three declaration collections), DID resolution
- `internal/server/` - Output WebSocket server: Hub fan-out, backfill-to-live seam, XRPC framing; public query endpoints (`getLabelers`/`getLabeler`) with per-client-IP rate limiting
- `internal/admin/` - Bearer-auth HTTP CRUD for labeler registry management
- `internal/config/` - Env-based config loading with validation
- `internal/metrics/` - Prometheus metric definitions
- `internal/verify/` - Label signature verification helpers (E2E provenance checks)
- `internal/e2e/` - Integration test harness and E2E scenarios
- `internal/integration/` - Crash-restart resume integration tests

## Conventions
- Functional Core / Imperative Shell: each file declares its pattern in a `// pattern:` comment
- FCIS callback injection for metrics: components expose `Set*Callback` methods; main.go wires them. No domain package imports the metrics package.
- All env vars prefixed `LABELER_RELAY_`
- Config validation uses lowercase-fragment error messages for composability
- Env reading confined to `internal/config/` -- never in library packages

## Boundaries
- Never hand-edit `cbor_gen.go` files -- run `make cborgen`
- `internal/` packages are not importable outside this module
- The lexicon JSON in `lexicons/` is the source of truth for the wire protocol
