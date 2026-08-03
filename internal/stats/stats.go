// Package stats collects per-command latencies and renders them two ways: a
// table for a human watching a run, and JSON for a job that has to decide
// whether the run passed.
package stats

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Collector accumulates samples per command name.
type Collector struct {
	mu   sync.Mutex
	cmds map[string]*command
	// start is when the run began, so throughput is derived rather than
	// guessed from the duration that was requested.
	start time.Time
}

type command struct {
	// samples holds every latency. A load run is bounded by duration or
	// iterations, so this stays proportional to the work actually done —
	// exact percentiles are worth more here than a fixed-bucket histogram
	// whose buckets would have to be guessed before the first run.
	samples []time.Duration
	errors  int
}

func New() *Collector {
	return &Collector{cmds: make(map[string]*command), start: time.Now()}
}

// Observe records one command. err non-nil counts as a failure and its latency
// is still recorded: a slow failure is a different symptom from a fast one.
func (c *Collector) Observe(name string, d time.Duration, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd, ok := c.cmds[name]
	if !ok {
		cmd = &command{}
		c.cmds[name] = cmd
	}
	cmd.samples = append(cmd.samples, d)
	if err != nil {
		cmd.errors++
	}
}

// Summary is the machine-readable form. A job gates on Errors.
type Summary struct {
	DurationSeconds float64            `json:"durationSeconds"`
	Commands        map[string]CmdStat `json:"commands"`
	Errors          int                `json:"errors"`
}

// CmdStat is one command's outcome. Percentiles rather than a mean alone: a
// mean hides the tail, and the tail is what an operator notices.
type CmdStat struct {
	Count     int     `json:"count"`
	Errors    int     `json:"errors"`
	PerSecond float64 `json:"perSecond"`
	MinMs     float64 `json:"minMs"`
	MedianMs  float64 `json:"medianMs"`
	P95Ms     float64 `json:"p95Ms"`
	P99Ms     float64 `json:"p99Ms"`
	MaxMs     float64 `json:"maxMs"`
}

func (c *Collector) Summary() Summary {
	c.mu.Lock()
	defer c.mu.Unlock()
	elapsed := time.Since(c.start).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	out := Summary{DurationSeconds: elapsed, Commands: make(map[string]CmdStat, len(c.cmds))}
	for name, cmd := range c.cmds {
		sorted := append([]time.Duration(nil), cmd.samples...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		out.Commands[name] = CmdStat{
			Count:     len(sorted),
			Errors:    cmd.errors,
			PerSecond: float64(len(sorted)) / elapsed,
			MinMs:     ms(percentile(sorted, 0)),
			MedianMs:  ms(percentile(sorted, 0.50)),
			P95Ms:     ms(percentile(sorted, 0.95)),
			P99Ms:     ms(percentile(sorted, 0.99)),
			MaxMs:     ms(percentile(sorted, 1)),
		}
		out.Errors += cmd.errors
	}
	return out
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// WriteTable renders the human form, ordered by name so two runs can be
// diffed.
func (s Summary) WriteTable(w io.Writer) {
	names := make([]string, 0, len(s.Commands))
	for name := range s.Commands {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Fprintf(w, "%-16s %8s %8s %9s %9s %9s %9s %9s %9s\n",
		"command", "count", "errors", "ops/s", "min ms", "med ms", "p95 ms", "p99 ms", "max ms")
	fmt.Fprintln(w, strings.Repeat("-", 100))
	for _, name := range names {
		c := s.Commands[name]
		fmt.Fprintf(w, "%-16s %8d %8d %9.1f %9.2f %9.2f %9.2f %9.2f %9.2f\n",
			name, c.Count, c.Errors, c.PerSecond, c.MinMs, c.MedianMs, c.P95Ms, c.P99Ms, c.MaxMs)
	}
	fmt.Fprintf(w, "\nran for %.1fs, %d errors\n", s.DurationSeconds, s.Errors)
}

func (s Summary) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}
