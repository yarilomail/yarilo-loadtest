package corpus_test

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yarilomail/yarilo-loadtest/internal/corpus"
)

func writeMbox(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.mbox")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// Splitting on the "From " separator is what mbox is; getting it wrong merges
// messages, and a run then delivers one enormous message instead of many.
func TestMboxSplitsMessages(t *testing.T) {
	path := writeMbox(t, "From sender Mon Jan  1 00:00:00 2020\n"+
		"Subject: one\n\nbody one\n"+
		"From sender Mon Jan  1 00:01:00 2020\n"+
		"Subject: two\n\nbody two\n")

	m, err := corpus.LoadMbox(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if m.Len() != 2 {
		t.Fatalf("loaded %d messages, want 2", m.Len())
	}
	first := string(m.Next())
	if !strings.Contains(first, "Subject: one") || strings.Contains(first, "Subject: two") {
		t.Errorf("first message contains the wrong content: %q", first)
	}
}

// The wire carries CRLF. An mbox written on a unix host has bare LFs, and a
// server handed those stores something other than what the file held.
func TestMboxNormalisesLineEndings(t *testing.T) {
	path := writeMbox(t, "From x\nSubject: t\n\nline one\nline two\n")
	m, err := corpus.LoadMbox(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	msg := string(m.Next())
	if strings.Contains(strings.ReplaceAll(msg, "\r\n", ""), "\n") {
		t.Errorf("message still contains a bare LF: %q", msg)
	}
	if !strings.HasSuffix(msg, "\r\n") {
		t.Errorf("message does not end with CRLF: %q", msg)
	}
}

// mbox escapes a body line starting with "From " as ">From ". Replaying it
// unescaped would deliver a message that differs from the one stored.
func TestMboxUnescapesFromLines(t *testing.T) {
	path := writeMbox(t, "From x\nSubject: t\n\n>From here on\nnormal\n")
	m, err := corpus.LoadMbox(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	msg := string(m.Next())
	if !strings.Contains(msg, "From here on") {
		t.Errorf("escaped From line was not restored: %q", msg)
	}
	if strings.Contains(msg, ">From here on") {
		t.Errorf("escape survived into the delivered message: %q", msg)
	}
}

// A corpus with no messages is a mistake worth failing on: a run that silently
// delivered nothing would look like a fast server.
func TestMboxRejectsAnEmptyFile(t *testing.T) {
	if _, err := corpus.LoadMbox(writeMbox(t, "")); err == nil {
		t.Error("an empty mbox was accepted")
	}
}

// Every client draws from one corpus at once.
func TestMboxIsSafeForConcurrentUse(t *testing.T) {
	path := writeMbox(t, "From a\nSubject: 1\n\nbody\nFrom b\nSubject: 2\n\nbody\n")
	m, err := corpus.LoadMbox(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if len(m.Next()) == 0 {
					t.Error("empty message from the corpus")
					return
				}
			}
		}()
	}
	wg.Wait()
}
