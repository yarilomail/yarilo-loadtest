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
	// columns is fixed after the first line so the table stays readable: a
	// column order that changes as commands first appear cannot be scanned.
	columns []string
	rows    int
}

func NewLive(c *Collector, w io.Writer, interval time.Duration) *Live {
	if interval <= 0 {
		interval = time.Second
	}
	return &Live{c: c, w: w, interval: interval,
		prev: map[string]int{}, prevE: map[string]int{}}
}

// Run prints until ctx is done. It is not the summary: the summary reports the
// whole run, this reports each interval.
func (l *Live) Run(done <-chan struct{}) {
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-done:
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
	if l.columns == nil {
		names := make([]string, 0, len(s.Commands))
		for name := range s.Commands {
			names = append(names, name)
		}
		sort.Strings(names)
		l.columns = names
		l.header()
	}

	fmt.Fprintf(l.w, "%6.0fs", s.DurationSeconds)
	var errs int
	for _, name := range l.columns {
		cur := s.Commands[name]
		delta := cur.Count - l.prev[name]
		l.prev[name] = cur.Count
		errs += cur.Errors - l.prevE[name]
		l.prevE[name] = cur.Errors
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
