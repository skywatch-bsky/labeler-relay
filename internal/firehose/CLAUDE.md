# Firehose

Last verified: 2026-06-02

## Purpose
Auto-discovers labelers by watching the AT Protocol firehose for `app.bsky.labeler.service` record creates/updates. Resolves labeler endpoints from DID documents and registers them in the store.

## Contracts
- **Exposes**: `FirehoseWatcher` (Run), `DIDResolver` interface, `NewIndigoResolver`, `ExtractLabelerServiceOps` (pure CAR extraction), `ErrNoLabelerEndpoint`
- **Guarantees**:
  - Discovery upserts with `source="firehose"` preserve manual labeler stickiness (store's ON CONFLICT rule)
  - Firehose cursor is persisted to `meta` table after each commit for crash recovery
  - Calls `poke()` after discovery to trigger immediate slurper reconcile
  - #service events are persisted to the output stream for downstream consumers
- **Expects**: Valid firehose URL. Resolver can reach DID documents over the network.

## Dependencies
- **Uses**: store (LabelerRegistry, LabelPersist, Store for meta cursor), indigo (stream, repo, identity, syntax)
- **Used by**: main.go (Run)
- **Boundary**: Must not import slurper, server, admin, or metrics

## Key Decisions
- `DIDResolver` is an interface we own: decouples from indigo's concrete resolver for testability.
- Endpoint resolution appends `/xrpc/com.atproto.label.subscribeLabels` to the DID doc's base serviceEndpoint. AT Protocol convention; labelers don't advertise the full path.
- Delete ops are intentionally ignored: the relay only enables on discovery, never auto-disables.

## Invariants
- `ExtractLabelerServiceOps` returns early without loading CAR if no labeler ops exist (allocation-light fast path)
- The resolved endpoint always ends with the subscribeLabels XRPC path
- Firehose cursor key in meta table is `"firehose_cursor"`

## Key Files
- `watcher.go` - FirehoseWatcher: redial loop, commit handling, discovery + poke
- `extract.go` - Pure CAR extraction of LabelerServiceOp from commits
- `resolve.go` - DIDResolver interface, IndigoResolver adapter, endpoint resolution
