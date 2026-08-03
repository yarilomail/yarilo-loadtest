// Package driver holds what every protocol driver shares: how a run is bounded,
// how load ramps up, and how failures end it.
package driver

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// Options bound a run. A run ends at whichever of Duration or Iterations comes
// first; leaving both unset is refused rather than treated as "forever",
// because an unbounded load run against a shared sandbox is somebody else's
// outage.
type Options struct {
	Addr        string
	Concurrency int
	Duration    time.Duration
	Iterations  int
	// RampUp spreads worker starts over this window. Starting hundreds of
	// connections in the same millisecond measures the accept queue, not the
	// server.
	RampUp time.Duration
	// StopOnError ends the run at the first protocol error. Off by default: a
	// run that stops at the first blip tells you less than one that counts them.
	StopOnError bool
}

// Worker is one concurrent client. It is called repeatedly until the run ends;
// id lets a driver pick which account it acts as.
type Worker func(ctx context.Context, id int, c *stats.Collector) error

// Run executes fn across Concurrency workers under the given bounds.
func Run(ctx context.Context, opts Options, c *stats.Collector, fn Worker) error {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.Duration <= 0 && opts.Iterations <= 0 {
		return fmt.Errorf("driver: set -duration or -iterations; an unbounded run against a shared environment is not a default anyone should get by omission")
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if opts.Duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, opts.Duration)
		defer cancel()
	} else {
		runCtx, cancel = context.WithCancel(ctx)
		defer cancel()
	}

	// Reaching the bound cancels every client mid-operation, and each of those
	// operations then fails with the cancellation. Counting those as protocol
	// errors made a clean run report roughly one failure per client — a number
	// that grows with -concurrency and describes the tool stopping, not the
	// server. The collector holds the same context, so it can tell the two
	// apart at the moment the failure is reported.
	c.BindRun(runCtx)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		firstErr  error
		iterCount int
	)
	stagger := time.Duration(0)
	if opts.RampUp > 0 {
		stagger = opts.RampUp / time.Duration(opts.Concurrency)
	}

	for i := 0; i < opts.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if stagger > 0 {
				select {
				case <-time.After(time.Duration(id) * stagger):
				case <-runCtx.Done():
					return
				}
			}
			for runCtx.Err() == nil {
				mu.Lock()
				if opts.Iterations > 0 && iterCount >= opts.Iterations {
					mu.Unlock()
					return
				}
				iterCount++
				mu.Unlock()

				if err := fn(runCtx, id, c); err != nil {
					// A cancelled run is the bound being reached, not a fault.
					if runCtx.Err() != nil {
						return
					}
					slog.Debug("loadtest: iteration failed", "worker", id, "err", err)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					if opts.StopOnError {
						cancel()
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()

	if opts.StopOnError {
		return firstErr
	}
	return nil
}
