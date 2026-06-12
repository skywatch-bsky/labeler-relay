package backoff

import (
	"testing"
	"time"
)

func TestNextMsFirstAttemptUsesInitial(t *testing.T) {
	t.Parallel()

	if got := NextMs(0, 0); got != InitialMs {
		t.Errorf("expected first attempt to use InitialMs=%d, got %d", InitialMs, got)
	}
}

func TestNextMsDoublesOnQuickFailure(t *testing.T) {
	t.Parallel()

	if got := NextMs(100, 50*time.Millisecond); got != 200 {
		t.Errorf("expected backoff to double from 100 to 200 on quick failure, got %d", got)
	}
	if got := NextMs(200, time.Second); got != 400 {
		t.Errorf("expected backoff to double from 200 to 400 on quick failure, got %d", got)
	}
}

func TestNextMsCapsAtMax(t *testing.T) {
	t.Parallel()

	if got := NextMs(MaxMs, 0); got != MaxMs {
		t.Errorf("expected backoff to stay capped at MaxMs=%d, got %d", MaxMs, got)
	}
	if got := NextMs(MaxMs-1, 0); got != MaxMs {
		t.Errorf("expected backoff to cap at MaxMs=%d, got %d", MaxMs, got)
	}
}

func TestNextMsResetsAfterLongLivedConnection(t *testing.T) {
	t.Parallel()

	// A connection that survived six hours and then dropped must redial at
	// the initial backoff, not the accumulated maximum.
	if got := NextMs(MaxMs, 6*time.Hour); got != InitialMs {
		t.Errorf("expected backoff to reset to InitialMs=%d after long-lived connection, got %d", InitialMs, got)
	}
	if got := NextMs(MaxMs, ResetAfter); got != InitialMs {
		t.Errorf("expected backoff to reset exactly at ResetAfter, got %d", got)
	}
}

func TestNextMsKeepsDoublingJustUnderThreshold(t *testing.T) {
	t.Parallel()

	if got := NextMs(400, ResetAfter-time.Millisecond); got != 800 {
		t.Errorf("expected backoff to keep doubling just under ResetAfter, got %d", got)
	}
}
