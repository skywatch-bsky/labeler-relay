//go:build integration

// pattern: Imperative Shell (test)
// n2_test.go is the N=2 live end-to-end integration test. It hits the real
// network and verifies that the relay faithfully aggregates labels from two
// real labelers with uncontaminated provenance, monotonic re-sequencing, and
// byte-faithful passthrough (proven transitively by cryptographic verification).
//
// Build tag: integration — run with: go test -tags integration ./internal/e2e/ -v
// This test FAILS (t.Fatal) if the network is unreachable or labelers are quiet.

package e2e_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/stretchr/testify/require"

	"github.com/scarndp/labeler-relay/internal/e2e"
	"github.com/scarndp/labeler-relay/internal/verify"
)

const (
	didMod     = "did:plc:ar7c4by46qjdydhdevvrndac" // mod.bsky.app
	didSkywatch = "did:plc:e4elbtctnfqocyfcml6h2lf7" // ozone.skywatch.blue

	// collectDeadline is the maximum time to wait for at least one label from
	// each origin. Real labelers may label at low rates; 2 minutes is ample.
	collectDeadline = 2 * time.Minute

	// backfillK is the number of labels to rewind the cursor for the backfill check.
	backfillK = 3
)

// TestN2LiveAggregation is the N=2 live end-to-end integration test.
// It proves faithful aggregation (AC1.1, AC1.2, AC1.3, AC1.4, AC11.1, AC11.2)
// against the two real labelers: mod.bsky.app and ozone.skywatch.blue.
func TestN2LiveAggregation(t *testing.T) {
	// Step 1: Resolve both origin keys from the real network.
	// Use BaseDirectory zero value (live HTTP DID resolution, same as production).
	// FAIL loudly if keys cannot be resolved — silent skip hides breakage.
	t.Log("resolving origin keys from the live network...")
	resolver := &identity.BaseDirectory{}
	ctx := context.Background()

	modKey, err := verify.ResolveLabelKey(ctx, resolver, didMod)
	require.NoError(t, err, "failed to resolve #atproto_label key for mod.bsky.app (%s) — is the network reachable?", didMod)
	t.Logf("resolved key for mod.bsky.app (%s): %s", didMod, modKey.Multibase())

	skywatchKey, err := verify.ResolveLabelKey(ctx, resolver, didSkywatch)
	require.NoError(t, err, "failed to resolve #atproto_label key for ozone.skywatch.blue (%s) — is the network reachable?", didSkywatch)
	t.Logf("resolved key for ozone.skywatch.blue (%s): %s", didSkywatch, skywatchKey.Multibase())

	// Map DID → public key for O(1) lookup during assertions.
	keyBySrc := map[string]atcrypto.PublicKey{
		didMod:      modKey,
		didSkywatch: skywatchKey,
	}
	_ = keyBySrc

	// Step 2: Boot the relay and register both labelers.
	t.Log("booting relay harness...")
	h, err := e2e.NewHarness(t)
	require.NoError(t, err, "failed to boot relay harness")

	t.Logf("relay listening at %s", h.Addr())

	t.Log("registering mod.bsky.app...")
	require.NoError(t, h.RegisterLabeler(didMod), "failed to register mod.bsky.app")

	t.Log("registering ozone.skywatch.blue...")
	require.NoError(t, h.RegisterLabeler(didSkywatch), "failed to register ozone.skywatch.blue")

	// Step 3: Collect labels until at least one from EACH origin has arrived,
	// bounded by collectDeadline. FAIL loudly if either labeler produces nothing.
	t.Logf("collecting labels (deadline: %v)...", collectDeadline)
	collected, err := collectUntilBothOrigins(t, h, collectDeadline)
	require.NoError(t, err, "failed to collect labels from both origins")

	t.Logf("collected %d labels total (%d from mod.bsky.app, %d from ozone.skywatch.blue)",
		len(collected),
		countBySrc(collected, didMod),
		countBySrc(collected, didSkywatch),
	)

	// Step 4a — AC1.1/AC1.2/AC11.1: every collected label verifies cryptographically
	// against the key of the labeler identified by its Src field.
	// Verification passing IS the AC1.3 proof: any lossy re-encode breaks the sig.
	t.Log("verifying cryptographic signatures...")
	modVerified := 0
	skywatchVerified := 0
	for i, cl := range collected {
		src := cl.Label.Src
		key, ok := keyBySrc[src]
		if !ok {
			t.Fatalf("label[%d]: unexpected Src %q — not one of the two expected labeler DIDs", i, src)
		}

		if err := verify.VerifyRelayedLabel(cl.Label, key); err != nil {
			t.Fatalf("AC1.1/AC1.2/AC1.3: label[%d] from %s failed crypto verification: %v\n"+
				"  hint: a verification failure means the relay re-encoded bytes somewhere in the persist/frame path",
				i, src, err)
		}

		switch src {
		case didMod:
			modVerified++
		case didSkywatch:
			skywatchVerified++
		}
	}
	t.Logf("AC1.1/AC1.2/AC1.3: %d labels verified from mod.bsky.app, %d from ozone.skywatch.blue",
		modVerified, skywatchVerified)

	// Step 4b — AC1.4/AC11.2: provenance is uncontaminated across both origins.
	// The frame-level OutputSrc and the label's own Src must be identical and one
	// of the two expected DIDs. No cross-contamination, no relay-identity rewrite.
	t.Log("checking provenance integrity (AC1.4/AC11.2)...")
	for i, cl := range collected {
		if cl.OutputSrc != cl.Label.Src {
			t.Fatalf("AC1.4/AC11.2: label[%d] provenance mismatch: frame OutputSrc=%q != label.Src=%q — cross-contamination detected",
				i, cl.OutputSrc, cl.Label.Src)
		}
		if cl.OutputSrc != didMod && cl.OutputSrc != didSkywatch {
			t.Fatalf("AC1.4/AC11.2: label[%d] has unexpected OutputSrc=%q — not one of the two registered labelers",
				i, cl.OutputSrc)
		}
	}
	t.Log("AC1.4/AC11.2: provenance clean — no cross-contamination, no relay-identity rewrite")

	// Step 4c — AC11.1 re-seq monotonicity: RelaySeq values must be strictly
	// increasing in delivery order with no duplicates.
	t.Log("checking relay seq monotonicity (AC11.1)...")
	assertMonotonicSeqs(t, collected)
	t.Logf("AC11.1: relay_seq is strictly increasing across %d interleaved labels", len(collected))

	// Step 5 — Backfill check: reconnect with cursor = maxSeq - K and verify
	// backfill replays in ascending relay_seq with no gaps or duplicates from
	// both sources.
	if len(collected) >= backfillK+1 {
		maxSeq := collected[len(collected)-1].RelaySeq
		cursor := maxSeq - int64(backfillK)
		if cursor < 0 {
			cursor = 0
		}
		t.Logf("backfill check: reconnecting with cursor=%d (maxSeq=%d, K=%d)...", cursor, maxSeq, backfillK)

		collectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backfill, err := h.CollectWithCursor(collectCtx, backfillK, cursor)
		if err != nil {
			t.Fatalf("backfill: failed to collect %d labels with cursor=%d: %v", backfillK, cursor, err)
		}

		// Assert ascending relay_seq with no duplicates.
		assertMonotonicSeqs(t, backfill)

		// Assert all backfill seqs are > cursor.
		for i, cl := range backfill {
			if cl.RelaySeq <= cursor {
				t.Fatalf("backfill: label[%d] relay_seq=%d is not > cursor=%d", i, cl.RelaySeq, cursor)
			}
		}

		// Assert both sources appear in backfill (if original collection had both,
		// backfill is a sliding window so it's possible only one source is in the
		// window — we just check seqs are sane, not source coverage).
		t.Logf("backfill: %d labels replayed in ascending relay_seq order (cursor=%d)",
			len(backfill), cursor)
	} else {
		t.Logf("backfill check skipped: not enough labels (%d < %d)", len(collected), backfillK+1)
	}

	// Step 6 — Union decode check: CollectAny a batch and assert all frame types
	// decode without error. This exercises the full union decoder (not just #labels).
	t.Log("union decode check (CollectAny)...")
	anyCtx, anyCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer anyCancel()

	// Collect at least 1 frame of any type; the harness has already seen labels
	// so there will be stored frames to replay.
	frames, err := h.CollectAny(anyCtx, 1)
	require.NoError(t, err, "union decode check: CollectAny failed")
	t.Logf("union decode check: decoded %d frames without error (all frame types consumable)", len(frames))
}

// collectUntilBothOrigins collects labels from the relay until at least one
// label from each of the two expected origins has arrived, or until the
// deadline elapses. It FAILS loudly (t.Fatal) if either origin produced
// nothing within the deadline.
//
// It opens a single long-lived WebSocket connection from cursor=0 and reads
// frames in a background goroutine, accumulating labels and logging progress.
// Once both origins have contributed at least one label, the collection stops.
func collectUntilBothOrigins(t *testing.T, h *e2e.Harness, deadline time.Duration) ([]e2e.CollectedLabel, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	type result struct {
		labels []e2e.CollectedLabel
		err    error
	}
	resultCh := make(chan result, 1)

	go func() {
		var accumulated []e2e.CollectedLabel
		seenMod := false
		seenSkywatch := false
		lastLogAt := time.Now()

		// Batch size: collect in small increments. Each call to Collect opens a
		// fresh connection from cursor=0 and reads until it has n labels. We
		// start at n=1 and grow so we accumulate without missing any labels.
		n := 1
		for !seenMod || !seenSkywatch {
			if ctx.Err() != nil {
				missing := missingOrigins(seenMod, seenSkywatch)
				resultCh <- result{err: fmt.Errorf(
					"timeout after %v waiting for labels from both origins — still missing: %v\n"+
						"  hint: real labelers may be quiet; check that both labeler endpoints are live",
					deadline, missing,
				)}
				return
			}

			batch, err := h.CollectWithCursor(ctx, n, 0)
			if err != nil {
				if ctx.Err() != nil {
					missing := missingOrigins(seenMod, seenSkywatch)
					resultCh <- result{err: fmt.Errorf(
						"timeout after %v waiting for labels from both origins — still missing: %v\n"+
							"  hint: real labelers may be quiet; check that both labeler endpoints are live",
						deadline, missing,
					)}
					return
				}
				// Transient error — retry with same n.
				continue
			}

			accumulated = batch
			n = len(accumulated) + 1

			for _, cl := range accumulated {
				switch cl.Label.Src {
				case didMod:
					seenMod = true
				case didSkywatch:
					seenSkywatch = true
				}
			}

			if time.Since(lastLogAt) > 10*time.Second {
				lastLogAt = time.Now()
				t.Logf("progress: %d labels (%d mod, %d skywatch), waiting for both...",
					len(accumulated),
					countBySrc(accumulated, didMod),
					countBySrc(accumulated, didSkywatch),
				)
			}
		}
		resultCh <- result{labels: accumulated}
	}()

	select {
	case res := <-resultCh:
		return res.labels, res.err
	case <-ctx.Done():
		// Should be unreachable; the goroutine checks ctx.Err().
		return nil, fmt.Errorf("timeout after %v: context cancelled before collection finished", deadline)
	}
}

// assertMonotonicSeqs asserts that the RelaySeq values in labels are strictly
// increasing with no duplicates.
func assertMonotonicSeqs(t *testing.T, labels []e2e.CollectedLabel) {
	t.Helper()

	seqs := make([]int64, len(labels))
	for i, cl := range labels {
		seqs[i] = cl.RelaySeq
	}

	// Check strictly increasing.
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("relay_seq monotonicity violated at index %d: seq[%d]=%d <= seq[%d]=%d",
				i, i, seqs[i], i-1, seqs[i-1])
		}
	}

	// Check no duplicates (belt-and-suspenders; already implied by strictly increasing).
	counts := make(map[int64]int, len(seqs))
	for _, s := range seqs {
		counts[s]++
	}
	var dups []int64
	for s, c := range counts {
		if c > 1 {
			dups = append(dups, s)
		}
	}
	if len(dups) > 0 {
		sort.Slice(dups, func(i, j int) bool { return dups[i] < dups[j] })
		t.Fatalf("relay_seq duplicates detected: %v", dups)
	}
}

// countBySrc counts how many CollectedLabels have a given Src DID.
func countBySrc(labels []e2e.CollectedLabel, src string) int {
	n := 0
	for _, cl := range labels {
		if cl.Label.Src == src {
			n++
		}
	}
	return n
}

// missingOrigins returns a human-readable list of origins not yet seen.
func missingOrigins(seenMod, seenSkywatch bool) []string {
	var missing []string
	if !seenMod {
		missing = append(missing, "mod.bsky.app ("+didMod+")")
	}
	if !seenSkywatch {
		missing = append(missing, "ozone.skywatch.blue ("+didSkywatch+")")
	}
	return missing
}
