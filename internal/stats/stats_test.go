package stats

import (
	"bytes"
	"context"
	"errors"
	"strings"
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

// A run that reports 4323 errors and no example sends the reader to the server
// logs, which in the case that prompted this had nothing to say — the rejection
// was per-transaction and never surfaced there. The information existed on the
// first run and was thrown away (#22).
func TestSummaryCarriesAVerbatimError(t *testing.T) {
	c := New()
	const first = `lmtp: got "554 5.0.0 transaction failed: smtp: too long a line in input stream", want 250`
	c.Observe("BODY", time.Millisecond, errors.New(first))
	c.Observe("BODY", time.Millisecond, errors.New("a later, different failure"))
	c.Observe("BODY", time.Millisecond, nil)

	s := c.Summary()
	if got := s.Commands["BODY"].FirstError; got != first {
		t.Errorf("FirstError = %q, want the first failure verbatim: %q", got, first)
	}

	var buf bytes.Buffer
	s.WriteTable(&buf)
	if !strings.Contains(buf.String(), "too long a line in input stream") {
		t.Errorf("the table reports counts without the error text:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "first error:") {
		t.Error("the sample is not labelled, so a reader cannot tell what the line is")
	}
}

// A clean run must stay clean: no error line, and nothing captured from the
// operations the deadline cut off.
func TestCleanRunReportsNoSampleError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := New()
	c.BindRun(ctx)
	c.Observe("BODY", time.Millisecond, nil)
	cancel()
	c.Observe("BODY", time.Millisecond, errors.New("context canceled"))

	s := c.Summary()
	if got := s.Commands["BODY"].FirstError; got != "" {
		t.Errorf("a cancelled operation was reported as a sample failure: %q", got)
	}
	var buf bytes.Buffer
	s.WriteTable(&buf)
	if strings.Contains(buf.String(), "first error:") {
		t.Errorf("a clean run printed an error line:\n%s", buf.String())
	}
}
