package imapdrv_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/corpus"
	"github.com/yarilomail/yarilo-loadtest/internal/imapdrv"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// fakeIMAP answers enough of the protocol to exercise the driver, and records
// what it was asked to do. Testing against a server rather than against the
// driver's own expectations is the only way the literal arithmetic in APPEND
// gets checked at all.
type fakeIMAP struct {
	mu        sync.Mutex
	logins    []string
	selects   []string
	creates   []string
	commands  []string
	appended  map[string]int
	appendBod [][]byte
	// counts is the real per-mailbox message count, so EXISTS reflects what
	// the server was actually told to store — which is what lets a test check
	// that the client's view tracks the server rather than itself.
	counts     map[string]int
	dropAppend bool
}

// issued counts how many times a verb reached the server.
func (f *fakeIMAP) issued(verb string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int
	for _, c := range f.commands {
		if c == verb {
			n++
		}
	}
	return n
}

func newFake() *fakeIMAP {
	return &fakeIMAP{appended: map[string]int{}, counts: map[string]int{}}
}

func (f *fakeIMAP) serve(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() }) //nolint:errcheck
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.handle(conn)
		}
	}()
	return ln.Addr().String()
}

func (f *fakeIMAP) handle(conn net.Conn) {
	defer conn.Close() //nolint:errcheck
	br := bufio.NewReader(conn)
	fmt.Fprint(conn, "* OK fake IMAP ready\r\n")

	var selected string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		tag, rest, _ := strings.Cut(line, " ")
		verb, args, _ := strings.Cut(rest, " ")

		f.mu.Lock()
		f.commands = append(f.commands, strings.ToUpper(verb))
		f.mu.Unlock()

		switch strings.ToUpper(verb) {
		case "LOGIN":
			user, _, _ := strings.Cut(args, " ")
			f.record(&f.logins, unquote(user))
			fmt.Fprintf(conn, "%s OK logged in\r\n", tag)
		case "CREATE":
			f.record(&f.creates, unquote(args))
			fmt.Fprintf(conn, "%s OK created\r\n", tag)
		case "SELECT":
			selected = unquote(args)
			f.record(&f.selects, selected)
			f.mu.Lock()
			n := f.counts[selected]
			f.mu.Unlock()
			fmt.Fprintf(conn, "* %d EXISTS\r\n%s OK [READ-WRITE] selected\r\n", n, tag)
		case "APPEND":
			// {n} is the last token; the body is exactly n bytes after the
			// continuation. Reading fewer or more desynchronises the session,
			// which is the failure this test exists to catch.
			mbox := unquote(strings.Fields(args)[0])
			size := literal(args)
			fmt.Fprint(conn, "+ ready\r\n")
			body := make([]byte, size)
			if _, err := readFull(br, body); err != nil {
				return
			}
			br.ReadString('\n') //nolint:errcheck // trailing CRLF
			f.mu.Lock()
			f.appended[mbox]++
			f.appendBod = append(f.appendBod, append([]byte(nil), body...))
			if !f.dropAppend {
				f.counts[mbox]++
			}
			f.mu.Unlock()
			fmt.Fprintf(conn, "%s OK appended\r\n", tag)
		case "FETCH":
			// A literal in an untagged response: the client must skip exactly
			// these bytes rather than parsing them as lines.
			payload := "Subject: x\r\n\r\nbody in " + selected
			fmt.Fprintf(conn, "* 1 FETCH (BODY[] {%d}\r\n%s)\r\n", len(payload), payload)
			fmt.Fprintf(conn, "%s OK fetched\r\n", tag)
		case "NOOP":
			// A real server reports the mailbox's current size on the next
			// command; without it a client can never learn what the server
			// actually holds.
			f.mu.Lock()
			n := f.counts[selected]
			f.mu.Unlock()
			fmt.Fprintf(conn, "* %d EXISTS\r\n%s OK noop\r\n", n, tag)
		case "LIST":
			fmt.Fprintf(conn, "* LIST () \"/\" INBOX\r\n%s OK listed\r\n", tag)
		case "STATUS":
			fmt.Fprintf(conn, "* STATUS INBOX (MESSAGES 1 UNSEEN 0)\r\n%s OK status\r\n", tag)
		case "EXPUNGE":
			fmt.Fprintf(conn, "%s OK expunged\r\n", tag)
		case "STORE":
			fmt.Fprintf(conn, "* 1 FETCH (FLAGS (\\Seen))\r\n%s OK stored\r\n", tag)
		case "SEARCH":
			fmt.Fprintf(conn, "* SEARCH 1 2\r\n%s OK searched\r\n", tag)
		case "LOGOUT":
			fmt.Fprintf(conn, "* BYE\r\n%s OK logged out\r\n", tag)
			return
		default:
			fmt.Fprintf(conn, "%s BAD unknown\r\n", tag)
		}
	}
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (f *fakeIMAP) record(dst *[]string, v string) {
	f.mu.Lock()
	*dst = append(*dst, v)
	f.mu.Unlock()
}

func (f *fakeIMAP) snapshot(get func(*fakeIMAP) []string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), get(f)...)
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(strings.TrimPrefix(s, `"`), `"`)
	return strings.ReplaceAll(s, `\"`, `"`)
}

func literal(args string) int {
	open := strings.LastIndex(args, "{")
	closeIdx := strings.LastIndex(args, "}")
	if open < 0 || closeIdx < open {
		return 0
	}
	n := 0
	for _, r := range args[open+1 : closeIdx] {
		if r >= '0' && r <= '9' {
			n = n*10 + int(r-'0')
		}
	}
	return n
}

func testDriver(t *testing.T, addr string, cfg imapdrv.Config) *imapdrv.Driver {
	t.Helper()
	cfg.Addr = addr
	cfg.Password = "pw"
	cfg.Timeout = 5 * time.Second
	if cfg.Corpus.MinSize == 0 {
		cfg.Corpus = corpus.Spec{MinSize: 1024, MaxSize: 1024, Seed: 1}
	}
	d, err := imapdrv.New(cfg)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return d
}

// runFor drives one client for a bounded time, which is how the driver is used:
// a client lives for the run, not for one operation.
func runFor(t *testing.T, d *imapdrv.Driver, id int, c *stats.Collector, d2 time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d2)
	defer cancel()
	if err := d.RunClient(ctx, id, c); err != nil {
		t.Fatalf("client %d: %v", id, err)
	}
}

// The point of the rewrite: a client logs in once and keeps working. Logging in
// per operation measures the login path instead of the session resources IMAP
// actually spends — the index handle, the cached view, hibernation.
func TestClientKeepsOneSession(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 1,
		TargetMessages: 5,
		Profile:        imapdrv.Profile{Fetch: 1, Noop: 1},
	})

	c := stats.New()
	runFor(t, d, 0, c, 300*time.Millisecond)

	logins := srv.snapshot(func(f *fakeIMAP) []string { return f.logins })
	if len(logins) != 1 {
		t.Errorf("client logged in %d times over one run, want 1", len(logins))
	}
	summary := c.Summary()
	work := summary.Commands["FETCH"].Count + summary.Commands["NOOP"].Count
	if work < 10 {
		t.Errorf("only %d commands issued on the session — it is not being reused", work)
	}
}

// Without a steady state the mailbox grows all run, so the same operation is
// cheap early and expensive late and no two measurements are comparable.
func TestClientHoldsTheMailboxNearTheTarget(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	const target = 8
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 1,
		TargetMessages: target,
		// Nothing but APPEND is weighted, so only the steady-state rule can
		// stop the mailbox growing without bound.
		Profile: imapdrv.Profile{Append: 1},
	})

	runFor(t, d, 0, stats.New(), 300*time.Millisecond)

	srv.mu.Lock()
	stored := srv.counts["Load1"]
	srv.mu.Unlock()
	if stored < target {
		t.Errorf("mailbox holds %d messages, below the %d target", stored, target)
	}
	// The rule tops up to the target and stops; some overshoot is the profile
	// still choosing APPEND, not the rule failing.
	if stored > target*3 {
		t.Errorf("mailbox grew to %d against a %d target — the steady state is not holding", stored, target)
	}
}

// The check that makes this a test rather than a generator: a server that
// accepts APPEND and stores nothing answers OK to everything, and only the
// message count gives it away.
func TestClientDetectsAServerThatDropsMail(t *testing.T) {
	srv := newFake()
	srv.dropAppend = true // accepts and acknowledges, stores nothing
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 1,
		TargetMessages: 3,
		Profile:        imapdrv.Profile{Append: 1, Noop: 1},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := d.RunClient(ctx, 0, stats.New())
	if err == nil {
		t.Fatal("a server that acknowledged every append and stored nothing was reported as healthy")
	}
	if !strings.Contains(err.Error(), "this client added") {
		t.Errorf("error does not name the mismatch: %v", err)
	}
}

// Commands illegal in the current state are never sent: a driver that ignores
// protocol state is guessing at a server rather than testing one.
func TestNoCommandRunsBeforeSelect(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 2,
		TargetMessages: 2,
		Profile:        imapdrv.Profile{Fetch: 5, Store: 5, Append: 1},
	})
	runFor(t, d, 0, stats.New(), 300*time.Millisecond)

	cmds := srv.snapshot(func(f *fakeIMAP) []string { return f.commands })
	seenSelect := false
	for _, cmd := range cmds {
		switch cmd {
		case "SELECT":
			seenSelect = true
		case "FETCH", "STORE", "APPEND", "EXPUNGE", "SEARCH":
			if !seenSelect {
				t.Fatalf("%s was sent before any SELECT: %v", cmd, cmds)
			}
		}
	}
}

// A profile weight of zero means the command never runs.
func TestZeroWeightCommandsNeverRun(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 1,
		TargetMessages: 2,
		Profile:        imapdrv.Profile{Fetch: 1},
	})
	runFor(t, d, 0, stats.New(), 200*time.Millisecond)

	for _, cmd := range srv.snapshot(func(f *fakeIMAP) []string { return f.commands }) {
		if cmd == "SEARCH" || cmd == "STORE" || cmd == "LIST" {
			t.Fatalf("%s ran with zero weight", cmd)
		}
	}
}

// APPEND is where a hand-rolled IMAP load test usually loses the run: the
// literal size must match the bytes exactly, or the session desynchronises and
// every later command reads somebody else's data.
func TestAppendLiteralMatchesTheBody(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 1,
		TargetMessages: 1000, // keep it appending
		Profile:        imapdrv.Profile{Append: 1},
		Corpus:         corpus.Spec{MinSize: 4096, MaxSize: 64 << 10, AttachmentRatio: 0.5, Seed: 2},
	})
	runFor(t, d, 0, stats.New(), 400*time.Millisecond)

	srv.mu.Lock()
	got := append([][]byte(nil), srv.appendBod...)
	srv.mu.Unlock()
	if len(got) < 5 {
		t.Fatalf("server received %d appends, too few to check", len(got))
	}
	// Byte-for-byte against the same corpus: a literal one short still leaves
	// the session readable, because the server consumes the missing byte as
	// part of the trailing CRLF — so a length check passes while the message
	// stored is not the message sent.
	want := corpus.New(corpus.Spec{MinSize: 4096, MaxSize: 64 << 10, AttachmentRatio: 0.5, Seed: 2})
	for i, body := range got {
		expected := want.Generate("loadtest@yarilo.invalid", "u1@example.com")
		if string(body) != string(expected) {
			t.Fatalf("append %d: server received %d bytes, driver generated %d — the literal and the body disagree",
				i, len(body), len(expected))
		}
	}
}

// Prepare creates the set once, before the clock starts.
func TestPrepareCreatesTheMailboxSet(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users:            []string{"u1@example.com", "u2@example.com"},
		MailboxesPerUser: 3,
		CreateMailboxes:  true,
	})
	if err := d.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	created := srv.snapshot(func(f *fakeIMAP) []string { return f.creates })
	if len(created) != 6 {
		t.Errorf("created %d mailboxes for 2 users x 3, want 6: %v", len(created), created)
	}
}

// The profile spec is what a job carries, so a typo must fail the run rather
// than silently produce a different workload.
func TestParseProfile(t *testing.T) {
	tests := []struct {
		name, spec string
		wantErr    bool
	}{
		{name: "empty is the default", spec: ""},
		{name: "named weights", spec: "append=30,fetch=20,search=5"},
		{name: "spaces", spec: " append = 3 , noop = 1 "},
		{name: "unknown command", spec: "frobnicate=5", wantErr: true},
		{name: "not a pair", spec: "append", wantErr: true},
		{name: "not a number", spec: "append=lots", wantErr: true},
		{name: "all zero", spec: "append=0,fetch=0", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := imapdrv.ParseProfile(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr = %t", err, tt.wantErr)
			}
		})
	}
}

// A read-only run must not write. -msgs=0 used to be silently promoted to 100,
// so a "search only" profile appended a hundred messages per client before it
// searched anything — the run measured the path it was written to exclude, and
// said nothing about having done so.
func TestZeroTargetDisablesTheSteadyState(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 1,
		TargetMessages: 0,
		// The read-only shape: only SEARCH is weighted. If the steady state
		// still ran, it would append regardless of the weights, because it is
		// consulted before the profile is.
		Profile: imapdrv.Profile{Search: 1},
	})

	runFor(t, d, 0, stats.New(), 300*time.Millisecond)

	stored, searches := srv.counts["Load1"], srv.issued("SEARCH")

	if stored != 0 {
		t.Errorf("a run with -msgs=0 stored %d messages; it was asked to read, not write", stored)
	}
	if searches == 0 {
		t.Error("no SEARCH was issued; the run did nothing at all, so the check above proves nothing")
	}
}

// The other half: zero must not be read as "hold nothing, so expunge
// everything". The expunge rule fires above twice the target, and twice zero is
// zero — a run against a mailbox somebody else filled would have emptied it.
func TestZeroTargetDoesNotExpunge(t *testing.T) {
	srv := newFake()
	srv.counts = map[string]int{"Load1": 50}
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com"}, MailboxesPerUser: 1,
		TargetMessages: 0,
		Profile:        imapdrv.Profile{Noop: 1},
	})

	runFor(t, d, 0, stats.New(), 300*time.Millisecond)

	if expunges := srv.issued("EXPUNGE"); expunges > 0 {
		t.Errorf("a run with -msgs=0 issued %d EXPUNGEs against a mailbox it did not fill", expunges)
	}
}
