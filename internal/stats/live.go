package stats

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// Live prints a line per interval showing what happened in that interval, the
// way the reference tool does.
//
// A summary at the end tells you a run was bad. A line per second tells you
// when it went bad, which is usually the question: a rate that collapses at
// forty seconds and a rate that was never good look identical in an average.
type Live struct {
	c        *Collector
	w        io.Writer
	interval time.Duration

	mu    sync.Mutex
	prev  map[string]int
	prevE map[string]int
	// columns grows as commands first appear. It used to be frozen at the
	// first line, for readability — which silently dropped every command that
	// had not run yet, and with it their errors: the table reported zero errors
	// through a run whose summary reported eight, and the tool contradicted
	// itself. A new column is appended and the header reprinted, which is the
	// price of the table being true.
	columns []string
	known   map[string]bool
	rows    int
}

func NewLive(c *Collector, w io.Writer, interval time.Duration) *Live {
	if interval <= 0 {
		interval = time.Second
	}
	return &Live{c: c, w: w, interval: interval,
		prev: map[string]int{}, prevE: map[string]int{}, known: map[string]bool{}}
}

// Run prints until done is closed. It is not the summary: the summary reports
// the whole run, this reports each interval.
func (l *Live) Run(done <-chan struct{}) {
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			// One last line, or everything after the final tick is invisible —
			// including a run that failed in its closing second.
			l.tick()
			return
		case <-t.C:
			l.tick()
		}
	}
}

func (l *Live) tick() {
	l.mu.Lock()
	defer l.mu.Unlock()

	s := l.c.Summary()
	if len(s.Commands) == 0 {
		return
	}

	// Errors are summed over everything the collector has seen, not over the
	// printed columns: a command that appears late must not take its failures
	// with it.
	var errs int
	for name, cur := range s.Commands {
		errs += cur.Errors - l.prevE[name]
		l.prevE[name] = cur.Errors
	}

	fresh := false
	for name := range s.Commands {
		if !l.known[name] {
			l.known[name] = true
			l.columns = append(l.columns, name)
			fresh = true
		}
	}
	if fresh {
		sort.Strings(l.columns)
		l.header()
	}

	fmt.Fprintf(l.w, "%6.0fs", s.DurationSeconds)
	for _, name := range l.columns {
		cur := s.Commands[name]
		delta := cur.Count - l.prev[name]
		l.prev[name] = cur.Count
		fmt.Fprintf(l.w, " %7d", delta)
	}
	fmt.Fprintf(l.w, " %7d\n", errs)

	l.rows++
	// The header repeats, because a run long enough to be interesting is longer
	// than a terminal is tall.
	if l.rows%20 == 0 {
		l.header()
	}
}

func (l *Live) header() {
	fmt.Fprintf(l.w, "%6s", "time")
	for _, name := range l.columns {
		fmt.Fprintf(l.w, " %7s", short(name))
	}
	fmt.Fprintf(l.w, " %7s\n", "errors")
}

// short truncates a command name to the column width, the way the reference
// prints Logi / Sele / Fetc: the numbers are what is being read, and a wide
// column pushes them off the line.
func short(name string) string {
	if len(name) <= 7 {
		return name
	}
	return name[:7]
}
