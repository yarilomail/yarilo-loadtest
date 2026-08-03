package stats

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The error column is the first thing anyone reads. Counting the operations cut
// off at the deadline made a clean run report roughly one failure per client,
// so either the reader learns to ignore errors or spends an afternoon chasing a
// server defect that does not exist.
func TestFailuresAfterStoppingAreCancelledNotErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := New()
	c.BindRun(ctx)
	c.Observe("BODY", time.Millisecond, nil)
	cancel()
	c.Observe("BODY", time.Millisecond, errors.New("context deadline exceeded"))

	s := c.Summary()
	if s.Errors != 0 {
		t.Errorf("errors = %d, want 0 — the run ended, the server did not fail", s.Errors)
	}
	if s.Cancelled != 1 {
		t.Errorf("cancelled = %d, want 1 — a cut-off operation must still be visible", s.Cancelled)
	}
	if got := s.Commands["BODY"]; got.Count != 2 || got.Cancelled != 1 || got.Errors != 0 {
		t.Errorf("BODY = %+v, want 2 observed, 1 cancelled, 0 errors", got)
	}
}

// The other half: a failure while the run is live is the server's, and must not
// be swallowed by the same mechanism.
func TestFailuresBeforeStoppingStillCount(t *testing.T) {
	c := New()
	c.Observe("RCPT", time.Millisecond, errors.New("550 no such user"))

	s := c.Summary()
	if s.Errors != 1 {
		t.Errorf("errors = %d, want 1 — a real refusal must be reported", s.Errors)
	}
	if s.Cancelled != 0 {
		t.Errorf("cancelled = %d, want 0", s.Cancelled)
	}
}
