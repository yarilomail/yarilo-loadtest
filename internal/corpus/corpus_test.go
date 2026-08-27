package corpus_test

import (
	"fmt"
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

// messageIDOf pulls the identifier out of a rendered message.
func messageIDOf(t *testing.T, raw []byte) string {
	t.Helper()
	for _, line := range strings.Split(string(raw), "\r\n") {
		if strings.HasPrefix(line, "Message-Id: ") {
			return strings.TrimPrefix(line, "Message-Id: ")
		}
		if line == "" {
			break
		}
	}
	t.Fatal("rendered message has no Message-Id")
	return ""
}

// Two recipients must never share a Message-ID, even from one seed at one
// sequence position.
//
// They did. The identity was the seed's random draw and the sequence number,
// both of which repeat exactly across runs -- which is what a seed is for -- so
// seeding two ranges of mailboxes with one seed gave every message a twin
// elsewhere, same id and a different To. A client that caches by Message-ID
// then reports the envelope as having changed, which is how this was found:
// 87 of them in a release gate (yarilo-loadtest#25).
func TestTwoRecipientsNeverShareAMessageID(t *testing.T) {
	spec := corpus.Spec{MinSize: 1024, MaxSize: 2048, Seed: 42}
	seen := map[string]string{}
	for _, run := range []string{"first", "second"} {
		g := corpus.New(spec)
		for i := 0; i < 20; i++ {
			// Two separate runs over two ranges of mailboxes, which is how the
			// sandbox is seeded: the same seed, different users.
			to := fmt.Sprintf("u%d@d00001.test", i)
			if run == "second" {
				to = fmt.Sprintf("u%d@d00001.test", i+100)
			}
			id := messageIDOf(t, g.Generate("loadtest@yarilo.invalid", to))
			if prev, ok := seen[id]; ok && prev != to {
				t.Errorf("%s is addressed to both %s and %s", id, prev, to)
			}
			seen[id] = to
		}
	}
}

// And the seed still keeps its promise: one seed and one recipient render the
// same bytes, which is what makes a measurement repeatable.
func TestSeedIsReproduciblePerRecipient(t *testing.T) {
	spec := corpus.Spec{MinSize: 4096, MaxSize: 16 << 10, AttachmentRatio: 0.5, Seed: 42}
	a, b := corpus.New(spec), corpus.New(spec)
	for i := 0; i < 10; i++ {
		to := fmt.Sprintf("u%d@d00001.test", i)
		if string(a.Generate("x@y.z", to)) != string(b.Generate("x@y.z", to)) {
			t.Fatalf("message %d for %s differs between two runs with one seed", i, to)
		}
	}
}
