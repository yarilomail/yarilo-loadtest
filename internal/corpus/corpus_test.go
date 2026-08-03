package corpus_test

import (
	"net/mail"
	"strings"
	"sync"
	"testing"

	"github.com/yarilomail/yarilo-loadtest/internal/corpus"
)

// The generated mail has to be mail: a delivery run that measured the server
// rejecting malformed messages would look like throughput.
func TestGeneratedMessagesParse(t *testing.T) {
	specs := []struct {
		name string
		spec corpus.Spec
	}{
		{"plain", corpus.Spec{MinSize: 2048, MaxSize: 2048, Seed: 1}},
		{"with attachments", corpus.Spec{MinSize: 64 << 10, MaxSize: 64 << 10, AttachmentRatio: 1, Seed: 1}},
		{"mixed", corpus.Spec{MinSize: 1024, MaxSize: 32 << 10, AttachmentRatio: 0.5, Seed: 1}},
	}
	for _, tt := range specs {
		t.Run(tt.name, func(t *testing.T) {
			g := corpus.New(tt.spec)
			for i := 0; i < 20; i++ {
				raw := g.Generate("from@example.com", "to@example.com")
				msg, err := mail.ReadMessage(strings.NewReader(string(raw)))
				if err != nil {
					t.Fatalf("generated message %d does not parse: %v", i, err)
				}
				if msg.Header.Get("Message-Id") == "" {
					t.Errorf("message %d has no Message-Id", i)
				}
				if !strings.Contains(string(raw), "\r\n") {
					t.Errorf("message %d does not use CRLF, which is what the wire carries", i)
				}
			}
		})
	}
}

// A body line beginning with a dot would terminate DATA early, silently
// truncating the message the server stores.
func TestGeneratedMessagesNeverStartALineWithADot(t *testing.T) {
	g := corpus.New(corpus.Spec{MinSize: 4096, MaxSize: 64 << 10, AttachmentRatio: 0.5, Seed: 7})
	for i := 0; i < 50; i++ {
		raw := string(g.Generate("a@b.c", "d@e.f"))
		for _, line := range strings.Split(raw, "\r\n") {
			if strings.HasPrefix(line, ".") {
				t.Fatalf("message %d has a line starting with a dot: %q", i, line)
			}
		}
	}
}

// The size spec is the whole point of this driver: a run configured for
// megabyte mail must not quietly deliver kilobytes.
func TestGeneratedSizeFollowsTheSpec(t *testing.T) {
	tests := []struct {
		name            string
		min, max        int
		attachmentRatio float64
	}{
		{"small", 1024, 2048, 0},
		{"large", 256 << 10, 512 << 10, 0},
		{"large with attachment", 256 << 10, 512 << 10, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := corpus.New(corpus.Spec{
				MinSize: tt.min, MaxSize: tt.max, AttachmentRatio: tt.attachmentRatio, Seed: 3,
			})
			for i := 0; i < 20; i++ {
				n := len(g.Generate("a@b.c", "d@e.f"))
				if n < tt.min {
					t.Errorf("message %d is %d bytes, under the %d minimum", i, n, tt.min)
				}
				// Generation stops once the target is reached, so the overshoot
				// is one filler line plus the closing boundary.
				if n > tt.max+2048 {
					t.Errorf("message %d is %d bytes, far over the %d maximum", i, n, tt.max)
				}
			}
		})
	}
}

// A seed makes a before/after comparison a comparison: without it the corpus
// is a second variable.
func TestSeedIsReproducible(t *testing.T) {
	spec := corpus.Spec{MinSize: 4096, MaxSize: 16 << 10, AttachmentRatio: 0.5, Seed: 42}
	a := corpus.New(spec)
	b := corpus.New(spec)
	for i := 0; i < 10; i++ {
		if string(a.Generate("x@y.z", "p@q.r")) != string(b.Generate("x@y.z", "p@q.r")) {
			t.Fatalf("message %d differs between two runs with one seed", i)
		}
	}
}

// The tool calls this from every client at once, and math/rand.Rand is not safe
// for that. The race was live from the first release: single-goroutine tests
// could not see it, and a run without -race would have corrupted the corpus
// silently rather than failing — which for a measuring tool is the worse
// outcome.
func TestGeneratorIsSafeForConcurrentUse(t *testing.T) {
	g := corpus.New(corpus.Spec{MinSize: 2048, MaxSize: 16 << 10, AttachmentRatio: 0.5, Seed: 9})

	const workers, each = 16, 25
	var wg sync.WaitGroup
	ids := make(chan string, workers*each)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				raw := g.Generate("a@b.c", "d@e.f")
				msg, err := mail.ReadMessage(strings.NewReader(string(raw)))
				if err != nil {
					t.Errorf("concurrent generation produced unparseable mail: %v", err)
					return
				}
				ids <- msg.Header.Get("Message-Id")
			}
		}()
	}
	wg.Wait()
	close(ids)

	// Every message must be distinct: a torn sequence counter would repeat one,
	// and a delivery run would then be indexing the same Message-Id twice.
	seen := make(map[string]bool, workers*each)
	for id := range ids {
		if id == "" {
			t.Fatal("a concurrently generated message has no Message-Id")
		}
		if seen[id] {
			t.Fatalf("duplicate Message-Id %q under concurrency", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*each {
		t.Errorf("got %d distinct messages, want %d", len(seen), workers*each)
	}
}
