package pop3drv_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/pop3drv"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// fakePOP3 is enough of a server to answer the driver, and it records what it
// was asked so a test can assert on the session rather than on the driver's own
// bookkeeping.
type fakePOP3 struct {
	mu       sync.Mutex
	commands []string
	users    []string
	// count is what STAT reports.
	count int
	// body is what RETR returns, verbatim, before the terminating dot.
	body string
	// rejectPass answers -ERR to PASS, the way a server refuses an account.
	rejectPass bool
}

func newFake() *fakePOP3 {
	return &fakePOP3{count: 5, body: "Subject: x\r\n\r\nbody\r\n"}
}

func (f *fakePOP3) issued(verb string) int {
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

func (f *fakePOP3) sawUsers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.users...)
}

func (f *fakePOP3) serve(t *testing.T) string {
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

func (f *fakePOP3) handle(conn net.Conn) {
	defer conn.Close() //nolint:errcheck
	fmt.Fprint(conn, "+OK yarilo-loadtest fake\r\n")

	buf := make([]byte, 4096)
	var pending string
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		pending += string(buf[:n])
		for {
			idx := strings.Index(pending, "\r\n")
			if idx < 0 {
				break
			}
			line := pending[:idx]
			pending = pending[idx+2:]
			verb, rest, _ := strings.Cut(line, " ")
			verb = strings.ToUpper(verb)

			f.mu.Lock()
			f.commands = append(f.commands, verb)
			if verb == "USER" {
				f.users = append(f.users, rest)
			}
			count, body, reject := f.count, f.body, f.rejectPass
			f.mu.Unlock()

			switch verb {
			case "USER":
				fmt.Fprint(conn, "+OK\r\n")
			case "PASS":
				if reject {
					fmt.Fprint(conn, "-ERR authentication failed\r\n")
					return
				}
				fmt.Fprint(conn, "+OK logged in\r\n")
			case "STAT":
				fmt.Fprintf(conn, "+OK %d %d\r\n", count, count*100)
			case "LIST", "UIDL":
				fmt.Fprint(conn, "+OK listing follows\r\n")
				for i := 1; i <= count; i++ {
					fmt.Fprintf(conn, "%d %s\r\n", i, strconv.Itoa(i*100))
				}
				fmt.Fprint(conn, ".\r\n")
			case "RETR":
				fmt.Fprintf(conn, "+OK %d octets\r\n%s.\r\n", len(body), body)
			case "DELE":
				fmt.Fprint(conn, "+OK deleted\r\n")
			case "QUIT":
				fmt.Fprint(conn, "+OK bye\r\n")
				return
			default:
				fmt.Fprint(conn, "-ERR unknown\r\n")
			}
		}
	}
}

func testDriver(t *testing.T, addr string, cfg pop3drv.Config) *pop3drv.Driver {
	t.Helper()
	cfg.Addr = addr
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	d, err := pop3drv.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

// A session is the unit here, and it has to be a whole one: authenticate,
// survey, retrieve, quit. A driver that stops at the greeting would report a
// throughput number that describes the accept queue.
func TestSessionRunsTheWholeSequence(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, pop3drv.Config{
		Users: []string{"u1@example.com"}, Password: "pw",
		MessagesPerSession: 2,
	})

	c := stats.New()
	if err := d.Session(context.Background(), 0, c); err != nil {
		t.Fatalf("Session: %v", err)
	}

	for _, verb := range []string{"USER", "PASS", "STAT", "LIST", "QUIT"} {
		if srv.issued(verb) != 1 {
			t.Errorf("%s issued %d times, want 1", verb, srv.issued(verb))
		}
	}
	// Every command is timed separately: RETR against QUIT in one bucket is a
	// histogram whose median describes neither.
	for _, name := range []string{"connect", "greeting", "USER", "PASS", "STAT", "LIST", "RETR", "QUIT"} {
		if _, ok := c.Summary().Commands[name]; !ok {
			t.Errorf("%s was not observed; it cannot be read from the summary", name)
		}
	}
}

// RETR is bounded, or its cost grows with the mailbox and the same operation is
// cheap at the start of a run and expensive at the end.
func TestRetrievalIsBoundedPerSession(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maildrop int
		perSess  int
		want     int
	}{
		{"fewer in the maildrop than asked for", 3, 10, 3},
		{"more in the maildrop than asked for", 50, 4, 4},
		{"survey only", 50, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFake()
			srv.count = tc.maildrop
			addr := srv.serve(t)
			d := testDriver(t, addr, pop3drv.Config{
				Users: []string{"u1@example.com"}, Password: "pw",
				MessagesPerSession: tc.perSess,
			})
			if err := d.Session(context.Background(), 0, stats.New()); err != nil {
				t.Fatalf("Session: %v", err)
			}
			if got := srv.issued("RETR"); got != tc.want {
				t.Errorf("RETR issued %d times, want %d", got, tc.want)
			}
		})
	}
}

// The protocol detail a hand-rolled POP3 client loses a run on. A body line of
// a single dot arrives byte-stuffed as "..", and the terminator is a line
// holding only a dot. A client that ends the message on any line starting with
// one stops mid-message, and the remainder is then read as the reply to the
// next command — so the session does not fail here, it fails later, somewhere
// unrelated.
func TestDotStuffedBodyDoesNotEndTheMessageEarly(t *testing.T) {
	srv := newFake()
	srv.count = 1
	// ".." is a stuffed "."; "..leading" is a stuffed ".leading".
	srv.body = "Subject: quoting\r\n\r\nbefore\r\n..\r\n..leading dot\r\nafter\r\n"
	addr := srv.serve(t)
	d := testDriver(t, addr, pop3drv.Config{
		Users: []string{"u1@example.com"}, Password: "pw",
		MessagesPerSession: 1,
	})

	if err := d.Session(context.Background(), 0, stats.New()); err != nil {
		t.Fatalf("Session: %v — the stuffed body was read as the end of the message", err)
	}
	// QUIT proves the connection was still in a usable state afterwards: had
	// the body ended early, its remaining lines would have been consumed as
	// QUIT's reply.
	if srv.issued("QUIT") != 1 {
		t.Error("QUIT never completed; the connection was left mid-message")
	}
}

// Deleting happens only when the config asks for it.
//
// Named for what it checks. It was TestDeleteIsOffByDefault, which promised
// something it could not see: the default lives in main.go, so flipping it left
// this green (#17). The default is guarded in main_test.go; this guards the
// driver's half — that it obeys the config it is given.
func TestDeleteObeysTheConfig(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, pop3drv.Config{
		Users: []string{"u1@example.com"}, Password: "pw",
		MessagesPerSession: 3,
	})
	if err := d.Session(context.Background(), 0, stats.New()); err != nil {
		t.Fatalf("Session: %v", err)
	}
	if got := srv.issued("DELE"); got != 0 {
		t.Errorf("DELE issued %d times with Delete unset — the run consumed the corpus", got)
	}
}

func TestDeleteIssuesOnePerRetrieval(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	d := testDriver(t, addr, pop3drv.Config{
		Users: []string{"u1@example.com"}, Password: "pw",
		MessagesPerSession: 3, Delete: true,
	})
	if err := d.Session(context.Background(), 0, stats.New()); err != nil {
		t.Fatalf("Session: %v", err)
	}
	if got := srv.issued("DELE"); got != 3 {
		t.Errorf("DELE issued %d times for 3 retrievals, want 3", got)
	}
}

// Users advance per session, not per client. Keying the user off the client id
// pins each one to a single mailbox, so a run touches as many mailboxes as it
// has clients however many are configured — and every number taken from it
// describes a handful of mailboxes under many times the intended load. That is
// a defect this project has already shipped once, in the LMTP driver.
func TestUsersAdvancePerSessionNotPerClient(t *testing.T) {
	srv := newFake()
	addr := srv.serve(t)
	users := []string{"u1@example.com", "u2@example.com", "u3@example.com", "u4@example.com"}
	d := testDriver(t, addr, pop3drv.Config{Users: users, Password: "pw"})

	// One client, four sessions: all four users must be reached.
	for i := 0; i < len(users); i++ {
		if err := d.Session(context.Background(), 0, stats.New()); err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
	}

	seen := map[string]bool{}
	for _, u := range srv.sawUsers() {
		seen[u] = true
	}
	if len(seen) != len(users) {
		t.Errorf("one client reached %d of %d users: %v", len(seen), len(users), srv.sawUsers())
	}
}

// A refused login is the server declining, and must be counted rather than
// swallowed: a run against a mistyped password would otherwise report a healthy
// server doing no work.
func TestRejectedLoginIsReported(t *testing.T) {
	srv := newFake()
	srv.rejectPass = true
	addr := srv.serve(t)
	d := testDriver(t, addr, pop3drv.Config{
		Users: []string{"u1@example.com"}, Password: "wrong",
	})

	c := stats.New()
	err := d.Session(context.Background(), 0, c)
	if err == nil {
		t.Fatal("a refused login was reported as a successful session")
	}
	if !strings.Contains(err.Error(), "-ERR") {
		t.Errorf("error does not carry the server's refusal: %v", err)
	}
	if got := c.Summary().Commands["PASS"].Errors; got != 1 {
		t.Errorf("PASS errors = %d, want 1", got)
	}
}

func TestNewRejectsAnEmptyUserSet(t *testing.T) {
	if _, err := pop3drv.New(pop3drv.Config{}); err == nil {
		t.Error("a run with no users was accepted; it would connect and have nobody to log in as")
	}
}
