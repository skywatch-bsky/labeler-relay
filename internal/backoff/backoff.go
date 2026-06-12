// pattern: Functional Core

// Package backoff computes redial delays for upstream connection loops.
package backoff

import "time"

const (
	// InitialMs is the delay in milliseconds before the first redial attempt.
	InitialMs = 100
	// MaxMs caps the exponential backoff.
	MaxMs = 30000
	// ResetAfter is how long a connection must survive for the backoff
	// sequence to restart from InitialMs on the next failure.
	ResetAfter = 30 * time.Second
)

// NextMs returns the delay in milliseconds before the upcoming redial.
// prevMs is the delay used for the previous attempt (0 on the first call);
// connectedFor is how long that attempt's connection survived. A connection
// that lived at least ResetAfter restarts the sequence from InitialMs.
func NextMs(prevMs int, connectedFor time.Duration) int {
	if prevMs == 0 || connectedFor >= ResetAfter {
		return InitialMs
	}
	next := prevMs * 2
	if next > MaxMs {
		return MaxMs
	}
	return next
}
