# Firehose

Last verified: 2026-11-27

## Purpose
Auto-discovers labelers by watching the AT Protocol firehose for labeler service declarations in the `app.bsky.labeler.service`, `town.delve.labeler.service`, and `so.sprk.labeler.service` collections (creates, updates, and deletes). Resolves labeler endpoints from DID documents and registers them in the store.

## Contracts
- **Exposes**: `FirehoseWatcher` (Run), `DIDResolver` interface, `NewIndigoResolver`, `ExtractLabelerDeclarationOps` (pure CAR extraction), `ErrNoLabelerEndpoint`
- **Guarantees**:
  - Every declaration op — create, update, and delete — routes through the store's atomic `LabelPersist.ApplyServiceOp`: the registry transition (upsert, declaration tracking, enable/disable) and the #service event commit in one transaction. Nothing mutates the registry outside it, so no step can be half-applied.
  - Discovery upserts with `source="firehose"` preserve manual labeler stickiness (store's ON CONFLICT rule)
  - Newly discovered labelers are enabled per `LABELER_RELAY_AUTO_SUBSCRIBE_DISCOVERED` (constructor's autoSubscribe); with it off, discovery registers them disabled for operator opt-in. Existing rows keep their enabled state either way.
  - **Re-enable rule**: a re-declaration re-enables a firehose-sourced row only on the zero-to-one declaration transition with autoSubscribe on. An operator-disabled row that already holds a declaration stays disabled when a second collection is added or an existing declaration is updated. Manual rows are never auto-enabled or auto-disabled.
  - **Delete handling**: deleting a firehose-sourced labeler's last tracked declaration disables it and pokes the slurper; deleting one of several keeps it enabled; deleting an untracked declaration is a no-op (skipped, no event); manual labelers are never auto-disabled. Delete ops emit `#service` frames with `op:"delete"`, the deleted collection, and no record.
  - **Incomplete-commit error contract**: any `ApplyServiceOp` failure — context cancellation, SQLite failure, frame-encode failure, anything — makes `handleCommit` return the error immediately WITHOUT staging the commit, so the firehose cursor stays before it and the redial replays it. Already-committed upstream ops are deduplicated by commit sequence, DID, collection, rkey, and action; replay skips them without a second event or reconcile poke. Resolver/RecordError failures keep their separate best-effort, non-fatal policy: they record the error in the registry and continue, tracking no declaration and emitting no event.
  - Firehose cursor is batched to `meta` table every 100 commits or 5 seconds (configurable via SetCursorFlushThresholds), with a deferred flush on disconnect for crash recovery. A commit whose processing did not complete is never staged, so a restart replays it instead of resuming after it
  - Calls `poke()` after a create/update and after an enabling-changing delete (a disable needs a slurper reconcile; a delete event alone does not)
  - #service events are persisted to the output stream for downstream consumers
- **Expects**: Valid firehose URL. Resolver can reach DID documents over the network.

## Dependencies
- **Uses**: store (LabelerRegistry, LabelPersist incl. ApplyServiceOp, Store for meta cursor), indigo (stream, repo, identity, syntax), labelerendpoint (Host), dagcbor (record validation)
- **Used by**: main.go (Run)
- **Boundary**: Must not import slurper, server, admin, or metrics

## Key Decisions
- `DIDResolver` is an interface we own: decouples from indigo's concrete resolver for testability.
- Endpoint resolution extracts only the host and optional port from the DID document serviceEndpoint. The slurper builds its own secure subscription URL. The DID-document `#atproto_labeler`/`AtprotoLabeler` service entry is the endpoint for all three declaration types — one labeler host backs all declarations, so the registry stays keyed by DID with one endpoint.
- Records are extracted as raw CAR block bytes (no typed unmarshal) and validated with `dagcbor.ValidateMap`: one complete, well-formed DAG-CBOR map with no trailing bytes. Lexicon-schema conformance is not validated. All three declaration schemas are self-keyed: non-`self` paths are ignored. Malformed records, records over the decode budget, missing CIDs, and missing blocks are skipped individually and logged at Warn level, while valid siblings proceed. CAR load failures remain hard extraction errors; the watcher logs them, counts the commit as processed, and deliberately drops its declaration ops because replaying the same corrupt bytes would loop forever. The watcher passes CommitSeq and Rkey into ServiceOp for replay deduplication.
- No network record verification (`com.atproto.repo.getRecord`) on the delete path — deliberately, to keep the watcher offline-simple.

## Invariants
- `ExtractLabelerDeclarationOps` returns early without loading CAR if no self-keyed declaration ops exist (allocation-light fast path)
- The resolved endpoint contains no scheme, path, or query
- Firehose cursor key in meta table is `"firehose_cursor"`
- Declaration tracking is incremental, not authoritative: the set reflects only declaration ops this relay observed

## Key Files
- `watcher.go` - FirehoseWatcher: redial loop, commit handling, atomic op application + poke
- `extract.go` - Pure CAR extraction of LabelerDeclarationOp from commits (all three collections, record validation)
- `resolve.go` - DIDResolver interface, IndigoResolver adapter, endpoint resolution

## Gotchas
- A labeler that declared a new-lexicon collection before this relay started watching and later deletes its `app.bsky` record is disabled even though it still declares the other collection (the relay never observed that declaration). Re-declaring re-enables it.
- RecordError-path rows carry no declaration and NULL `declared_at`; their `declarations` view is empty and `updatedAt` absent until a successful declaration op arrives. This is also what makes the zero-to-one re-enable rule correct.
- Old `#service` frames stored before multi-collection support replay verbatim: they decode with `Collection == ""` (consumers default to app.bsky) and carry typed re-encoded records, not byte-faithful ones.
