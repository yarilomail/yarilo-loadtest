package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// The reported shape: a clean run against a healthy server reported almost
// exactly -concurrency failures, one per client caught mid-operation when the
// clock ran out. The number grew with the load, so at 500 clients a perfect run
// claimed 500 failures.
//
// The worker here is the in-flight operation: it is blocked on the server when
// the run ends, and reports the cancellation the way a real driver does — by
// observing it against the command it was executing.
func TestDeadlineDoesNotManufactureErrors(t *testing.T) {
	const concurrency = 8
	c := stats.New()

	err := Run(context.Background(), Options{
		Concurrency: concurrency,
		Duration:    50 * time.Millisecond,
	}, c, func(ctx context.Context, _ int, c *stats.Collector) error {
		<-ctx.Done()
		c.Observe("BODY", time.Millisecond, ctx.Err())
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := c.Summary()
	if s.Errors != 0 {
		t.Errorf("errors = %d, want 0 — a clean run must not report the tool stopping as failures", s.Errors)
	}
	if s.Cancelled != concurrency {
		t.Errorf("cancelled = %d, want %d — the cut-off operations must still be counted somewhere",
			s.Cancelled, concurrency)
	}
}

// A server that genuinely fails must still be reported, in full. This is the
// half a blanket "ignore cancellations" fix would have destroyed.
func TestRealFailuresAreStillReported(t *testing.T) {
	c := stats.New()

	err := Run(context.Background(), Options{
		Concurrency: 1,
		Iterations:  5,
	}, c, func(_ context.Context, _ int, c *stats.Collector) error {
		e := errors.New("550 mailbox unavailable")
		c.Observe("RCPT", time.Millisecond, e)
		return e
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := c.Summary()
	if s.Errors != 5 {
		t.Errorf("errors = %d, want 5 — every refusal the server sent must survive", s.Errors)
	}
	if s.Cancelled != 0 {
		t.Errorf("cancelled = %d, want 0 — this run ended by exhausting its iterations", s.Cancelled)
	}
}

// -iterations ends by running out of work rather than by cancellation, so it
// has nothing to reclassify and must report a clean run as clean.
func TestIterationsRunReportsNothingCancelled(t *testing.T) {
	c := stats.New()

	err := Run(context.Background(), Options{
		Concurrency: 4,
		Iterations:  40,
	}, c, func(_ context.Context, _ int, c *stats.Collector) error {
		c.Observe("BODY", time.Millisecond, nil)
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	s := c.Summary()
	if s.Errors != 0 || s.Cancelled != 0 {
		t.Errorf("errors = %d, cancelled = %d, want 0 and 0", s.Errors, s.Cancelled)
	}
}

// -stop-on-error cancels the run at the first real failure. The clients that
// were mid-operation when it did are collateral, not evidence: reporting them
// as failures is what made the flag unusable for the job it exists for.
func TestStopOnErrorReportsOnlyTheRealFailure(t *testing.T) {
	c := stats.New()

	err := Run(context.Background(), Options{
		Concurrency: 8,
		Duration:    5 * time.Second,
		StopOnError: true,
	}, c, func(ctx context.Context, id int, c *stats.Collector) error {
		if id != 0 {
			<-ctx.Done()
			c.Observe("BODY", time.Millisecond, ctx.Err())
			return ctx.Err()
		}
		e := errors.New("connection reset by peer")
		c.Observe("BODY", time.Millisecond, e)
		return e
	})
	if err == nil {
		t.Fatal("Run returned nil; -stop-on-error must surface the failure that stopped it")
	}

	s := c.Summary()
	if s.Errors != 1 {
		t.Errorf("errors = %d, want 1 — only one client actually failed", s.Errors)
	}
}
