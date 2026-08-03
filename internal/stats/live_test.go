package stats_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// buf is mutex-guarded because Live writes from its own goroutine while the
// test reads: the writer a caller supplies has to tolerate that, and os.Stderr
// does.
type buf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *buf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *buf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// Each line reports that interval, not the run so far. A cumulative line would
// answer "was the run good", when the question a live table exists for is
// "when did it stop being good".
func TestLiveReportsPerIntervalDeltas(t *testing.T) {
	c := stats.New()
	var out buf
	live := stats.NewLive(c, &out, 30*time.Millisecond)

	done := make(chan struct{})
	go live.Run(done)

	for i := 0; i < 5; i++ {
		c.Observe("APPEND", time.Millisecond, nil)
	}
	time.Sleep(80 * time.Millisecond)
	for i := 0; i < 3; i++ {
		c.Observe("APPEND", time.Millisecond, nil)
	}
	time.Sleep(80 * time.Millisecond)
	close(done)

	lines := nonEmpty(out.String())
	if len(lines) < 3 {
		t.Fatalf("got %d lines, want a header and at least two intervals:\n%s", len(lines), out.String())
	}
	if !strings.Contains(lines[0], "APPEND") || !strings.Contains(lines[0], "errors") {
		t.Errorf("header does not name the columns: %q", lines[0])
	}
	// The first interval saw 5; a later one must not repeat them.
	if !strings.Contains(lines[1], "5") {
		t.Errorf("first interval does not report its 5 operations: %q", lines[1])
	}
	total := 0
	for _, line := range lines[1:] {
		for _, f := range strings.Fields(line) {
			if n := atoi(f); n > 0 && n < 100 {
				total += n
			}
		}
	}
	if total != 8 {
		t.Errorf("intervals sum to %d, want 8 — the lines are cumulative, not deltas", total)
	}
}

// Errors are their own column: a run at full rate with everything failing looks
// healthy in a throughput number alone.
func TestLiveCountsErrorsPerInterval(t *testing.T) {
	c := stats.New()
	var out buf
	live := stats.NewLive(c, &out, 30*time.Millisecond)
	done := make(chan struct{})
	go live.Run(done)

	c.Observe("FETCH", time.Millisecond, nil)
	c.Observe("FETCH", time.Millisecond, errFake{})
	time.Sleep(80 * time.Millisecond)
	close(done)

	lines := nonEmpty(out.String())
	if len(lines) < 2 {
		t.Fatalf("no interval line:\n%s", out.String())
	}
	fields := strings.Fields(lines[1])
	if fields[len(fields)-1] != "1" {
		t.Errorf("error column = %q, want 1: %q", fields[len(fields)-1], lines[1])
	}
}

type errFake struct{}

func (errFake) Error() string { return "fake" }

func nonEmpty(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}

// A command that first runs after the table has started must appear, and its
// errors must be counted from the moment they happen. Freezing the columns on
// the first line dropped both: a run whose summary reported eight errors showed
// zero in every interval, and the tool contradicted itself.
func TestLiveCountsCommandsThatAppearLate(t *testing.T) {
	c := stats.New()
	var out buf
	live := stats.NewLive(c, &out, 30*time.Millisecond)
	done := make(chan struct{})
	go live.Run(done)

	c.Observe("CONNECT", time.Millisecond, nil)
	time.Sleep(70 * time.Millisecond)

	// A command the first line never saw, failing.
	c.Observe("BODY", time.Millisecond, errFake{})
	time.Sleep(70 * time.Millisecond)
	close(done)
	time.Sleep(30 * time.Millisecond)

	text := out.String()
	if !strings.Contains(text, "BODY") {
		t.Errorf("a command that appeared after the first line is missing from the table:\n%s", text)
	}
	// The column set must include what appeared late; freezing it after the
	// first line is the defect, and it hid the errors entirely rather than
	// delaying them.
	var errs int
	for _, line := range nonEmpty(text) {
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.HasSuffix(fields[0], "s") {
			continue
		}
		errs += atoi(fields[len(fields)-1])
	}
	if errs != 1 {
		t.Errorf("intervals report %d errors, want 1 — the table disagrees with the run:\n%s", errs, text)
	}
}

// Whatever happens after the last tick still gets printed: a run that fails in
// its closing second would otherwise leave no trace in the table.
func TestLivePrintsAFinalLine(t *testing.T) {
	c := stats.New()
	var out buf
	live := stats.NewLive(c, &out, time.Hour) // no tick will ever fire
	done := make(chan struct{})
	go live.Run(done)

	c.Observe("APPEND", time.Millisecond, errFake{})
	close(done)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(out.String(), "APPEND") {
		t.Errorf("nothing was printed for the final interval:\n%s", out.String())
	}
}
