package imapdrv_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sort"
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
}

func newFake() *fakeIMAP { return &fakeIMAP{appended: map[string]int{}} }

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
			fmt.Fprintf(conn, "* 3 EXISTS\r\n%s OK [READ-WRITE] selected\r\n", tag)
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
			f.mu.Unlock()
			fmt.Fprintf(conn, "%s OK appended\r\n", tag)
		case "FETCH":
			// A literal in an untagged response: the client must skip exactly
			// these bytes rather than parsing them as lines.
			payload := "Subject: x\r\n\r\nbody in " + selected
			fmt.Fprintf(conn, "* 1 FETCH (BODY[] {%d}\r\n%s)\r\n", len(payload), payload)
			fmt.Fprintf(conn, "%s OK fetched\r\n", tag)
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

// The reason this driver exists: churn has to spread over a user's mailboxes,
// not sit in INBOX. A generator that cannot do that cannot produce the one
// condition a per-user dispatcher is worth testing against.
func TestOperationsSpreadOverUsersAndMailboxes(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	users := []string{"u1@example.com", "u2@example.com", "u3@example.com"}
	d := testDriver(t, addr, imapdrv.Config{
		Users: users, MailboxesPerUser: 4, CreateMailboxes: false,
	})

	c := stats.New()
	for i := 0; i < 48; i++ {
		if err := d.Run(context.Background(), i%3, c); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
	}

	gotUsers := distinct(srv.snapshot(func(f *fakeIMAP) []string { return f.logins }))
	if len(gotUsers) != len(users) {
		t.Errorf("reached %d of %d users: %v", len(gotUsers), len(users), gotUsers)
	}
	gotBoxes := distinct(srv.snapshot(func(f *fakeIMAP) []string { return f.selects }))
	if len(gotBoxes) != 4 {
		t.Errorf("reached %d of 4 mailboxes: %v", len(gotBoxes), gotBoxes)
	}
}

// Selection is per operation. Keying it off the client is the defect this
// driver was asked for after the LMTP one had it.
func TestSelectionIsPerOperationNotPerClient(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users: []string{"u1@example.com", "u2@example.com"}, MailboxesPerUser: 3,
	})

	c := stats.New()
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Every operation reports the same client id: if selection followed
			// the client, one user and one mailbox would be all we saw.
			if err := d.Run(context.Background(), 0, c); err != nil {
				t.Errorf("operation: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := distinct(srv.snapshot(func(f *fakeIMAP) []string { return f.logins })); len(got) != 2 {
		t.Errorf("one client id reached %d users, want 2: %v", len(got), got)
	}
	if got := distinct(srv.snapshot(func(f *fakeIMAP) []string { return f.selects })); len(got) != 3 {
		t.Errorf("one client id reached %d mailboxes, want 3: %v", len(got), got)
	}
}

// APPEND is where a hand-rolled IMAP load test usually loses the run: the
// literal size must match the bytes exactly, or the session desynchronises and
// every later command reads somebody else's data.
func TestAppendLiteralMatchesTheBody(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users:            []string{"u1@example.com"},
		MailboxesPerUser: 1,
		Ops:              imapdrv.OpMix{Append: 1},
		Corpus:           corpus.Spec{MinSize: 4096, MaxSize: 64 << 10, AttachmentRatio: 0.5, Seed: 2},
	})

	c := stats.New()
	for i := 0; i < 12; i++ {
		if err := d.Run(context.Background(), 0, c); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	srv.mu.Lock()
	got := append([][]byte(nil), srv.appendBod...)
	srv.mu.Unlock()
	if len(got) != 12 {
		t.Fatalf("server received %d appends, want 12", len(got))
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
	if summary := c.Summary(); summary.Errors != 0 {
		t.Errorf("%d errors on a healthy server", summary.Errors)
	}
}

// The mix is deterministic so two runs with the same flags issue the same
// operations: a comparison should differ by the server, not by the generator.
func TestOperationMixIsHonoured(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users:            []string{"u1@example.com"},
		MailboxesPerUser: 1,
		Ops:              imapdrv.OpMix{Append: 1, Fetch: 1, Store: 1, Search: 1},
	})

	c := stats.New()
	for i := 0; i < 16; i++ {
		if err := d.Run(context.Background(), 0, c); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	counts := map[string]int{}
	for _, cmd := range srv.snapshot(func(f *fakeIMAP) []string { return f.commands }) {
		counts[cmd]++
	}
	for _, want := range []string{"APPEND", "FETCH", "STORE", "SEARCH"} {
		if counts[want] != 4 {
			t.Errorf("%s ran %d times over 16 operations with an even mix, want 4", want, counts[want])
		}
	}
}

// An operation excluded from the mix must never run.
func TestZeroWeightOperationsNeverRun(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, imapdrv.Config{
		Users:            []string{"u1@example.com"},
		MailboxesPerUser: 1,
		Ops:              imapdrv.OpMix{Fetch: 1},
	})
	c := stats.New()
	for i := 0; i < 8; i++ {
		if err := d.Run(context.Background(), 0, c); err != nil {
			t.Fatalf("operation %d: %v", i, err)
		}
	}
	for _, cmd := range srv.snapshot(func(f *fakeIMAP) []string { return f.commands }) {
		if cmd == "APPEND" || cmd == "STORE" || cmd == "SEARCH" {
			t.Fatalf("%s ran with zero weight", cmd)
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

func distinct(in []string) []string {
	seen := map[string]bool{}
	for _, v := range in {
		seen[v] = true
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
