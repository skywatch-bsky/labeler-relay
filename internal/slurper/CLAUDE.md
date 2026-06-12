# Slurper

Last verified: 2026-06-02

## Purpose
Manages per-labeler upstream WebSocket subscriptions. Reconciles the set of active subscriptions against enabled labelers in the registry, handles redial with exponential backoff, rate limiting, and unsigned-label drop policy.

## Contracts
- **Exposes**: `LabelSlurper` (New, Run, Reconcile, Shutdown), `Limiter` (per-labeler rate limiter), `SigRequired`/`KeepLabel` (pure policy functions)
- **Guarantees**:
  - One goroutine per enabled labeler; Reconcile is idempotent
  - Reconcile restarts a subscription when its registry config (endpoint, require_sig) changed -- `SubscriptionConfigChanged` in policy.go is the diff
  - Subscriptions resume from persisted `last_upstream_seq` cursor
  - Cursor is written after successful persist (crash-safe ordering)
  - Labels are passed through byte-faithful (unmodified) -- the relay never re-signs
  - Rate limiting is per-labeler, never cross-labeler
- **Expects**: Registry returns enabled labelers with valid endpoints. Persist never blocks indefinitely.

## Dependencies
- **Uses**: store (LabelPersist, LabelerRegistry, IngestEvent), indigo stream/schedulers. Metrics decoupled via callback injection (onDropUnsigned, onIngested, onThrottled, onUpstreams — wired from main.go).
- **Used by**: main.go (Run + Reconcile), firehose (poke triggers Reconcile), admin (poke triggers Reconcile)
- **Boundary**: Must not import server or admin

## Key Decisions
- Indigo's `sequential.NewScheduler` + `HandleRepoStream` for frame parsing: reuses battle-tested upstream WS consumer rather than hand-rolling.
- Exponential backoff with 10% jitter, capped at 30s: prevents thundering herd on upstream recovery.
- Sliding window rate limiter (per-sec + per-hour): two-tier protection against burst and sustained flood.

## Invariants
- `subscription.run` blocks until context cancellation -- it never returns nil on its own
- Backoff resets are implicit: a successful dial resets the loop but the backoff variable persists per dial cycle (capped at max)
- `KeepLabel` is a pure function: label is kept if it has a sig OR sig is not required

## Key Files
- `slurper.go` - LabelSlurper: reconcile loop, subscription lifecycle management
- `subscription.go` - Per-labeler WS connection: dial, frame handling, cursor flushing
- `policy.go` - Pure functions: SigRequired, KeepLabel
- `ratelimit.go` - Limiter: sliding window Wait with throttle callback

## Gotchas
- All metrics in subscription.go flow through injected callbacks (onDropUnsigned, onIngested), matching the FCIS callback pattern used across the codebase.
- The limiter's `Wait` polls at 10ms intervals -- not event-driven. Acceptable for label throughput but would need rework for high-volume use.
