package lmtp_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/corpus"
	"github.com/yarilomail/yarilo-loadtest/internal/lmtp"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// fakeLMTP is a server that speaks the protocol properly — multi-line LHLO
// included — and records what it received, so the driver is tested against the
// wire rather than against itself.
type fakeLMTP struct {
	mu         sync.Mutex
	deliveries []delivery
	// bodyDelay is held before the post-transfer replies, standing in for the
	// work a real server does there — write the message, update the index, run
	// the full-text pass. Without it the fake answers in microseconds and the
	// transfer cannot be distinguished from the handshake by timing at all.
	bodyDelay time.Duration
}

type delivery struct {
	sender     string
	recipients []string
	body       string
}

func (f *fakeLMTP) serve(t *testing.T) string {
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

func (f *fakeLMTP) handle(conn net.Conn) {
	defer conn.Close() //nolint:errcheck
	br := bufio.NewReader(conn)
	w := func(s string) { fmt.Fprint(conn, s) }

	w("220 fake LMTP ready\r\n")
	var d delivery
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		verb, rest, _ := strings.Cut(line, " ")
		switch strings.ToUpper(verb) {
		case "LHLO":
			// Multi-line, as a real server answers: a driver that stops at the
			// first line would misread the rest as later replies.
			w("250-fake greets you\r\n250-PIPELINING\r\n250-8BITMIME\r\n250 ENHANCEDSTATUSCODES\r\n")
		case "MAIL":
			d = delivery{sender: addr(rest)}
			w("250 2.1.0 OK\r\n")
		case "RCPT":
			d.recipients = append(d.recipients, addr(rest))
			w("250 2.1.5 OK\r\n")
		case "DATA":
			w("354 go ahead\r\n")
			var body strings.Builder
			for {
				bl, err := br.ReadString('\n')
				if err != nil {
					return
				}
				if bl == ".\r\n" {
					break
				}
				// A real server removes the escaping the client added, so the
				// fake has to as well: otherwise a test comparing what arrived
				// against what was sent would be comparing the wire form with
				// the message and would call correct stuffing a corruption.
				body.WriteString(strings.TrimPrefix(bl, "."))
			}
			d.body = body.String()
			f.mu.Lock()
			f.deliveries = append(f.deliveries, d)
			delay := f.bodyDelay
			f.mu.Unlock()
			time.Sleep(delay)
			// One reply per recipient, per RFC 2033.
			for range d.recipients {
				w("250 2.0.0 delivered\r\n")
			}
		case "QUIT":
			w("221 2.0.0 bye\r\n")
			return
		default:
			w("500 5.5.2 unknown\r\n")
		}
	}
}

func addr(s string) string {
	_, rest, ok := strings.Cut(s, ":")
	if !ok {
		return s
	}
	return strings.Trim(strings.TrimSpace(rest), "<>")
}

func (f *fakeLMTP) got() []delivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]delivery(nil), f.deliveries...)
}

// The whole conversation, against a server that answers LHLO on four lines.
func TestDeliverSpeaksTheProtocol(t *testing.T) {
	srv := &fakeLMTP{}
	addr := srv.serve(t)

	d := lmtp.New(lmtp.Config{
		Addr:       addr,
		Recipients: []string{"u1@example.com"},
		Sender:     "sender@example.com",
		Corpus:     corpus.Spec{MinSize: 2048, MaxSize: 2048, Seed: 1},
		Timeout:    5 * time.Second,
	})
	c := stats.New()
	if err := d.Deliver(context.Background(), 0, c); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	got := srv.got()
	if len(got) != 1 {
		t.Fatalf("server saw %d deliveries, want 1", len(got))
	}
	if got[0].sender != "sender@example.com" {
		t.Errorf("sender = %q", got[0].sender)
	}
	if len(got[0].recipients) != 1 || got[0].recipients[0] != "u1@example.com" {
		t.Errorf("recipients = %v", got[0].recipients)
	}
	if !strings.Contains(got[0].body, "Message-Id:") {
		t.Error("the server received no Message-Id — the body did not arrive intact")
	}
	// Every command is timed, and DATA is the one that carries the delivery
	// cost the run exists to measure.
	if _, ok := c.Summary().Commands["DATA"]; !ok {
		t.Error("DATA was not timed")
	}
}

// One DATA answered once per recipient: reading fewer replies would leave them
// in the buffer to be misread as the answer to QUIT.
func TestDeliverReadsOneReplyPerRecipient(t *testing.T) {
	srv := &fakeLMTP{}
	addr := srv.serve(t)

	d := lmtp.New(lmtp.Config{
		Addr:                 addr,
		Recipients:           []string{"u1@example.com", "u2@example.com", "u3@example.com"},
		RecipientsPerMessage: 3,
		Corpus:               corpus.Spec{MinSize: 1024, MaxSize: 1024, Seed: 2},
		Timeout:              5 * time.Second,
	})
	if err := d.Deliver(context.Background(), 0, stats.New()); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	got := srv.got()
	if len(got) != 1 || len(got[0].recipients) != 3 {
		t.Fatalf("server saw %+v, want one delivery to three recipients", got)
	}
}

// The recipient is chosen per delivery, not per client. Keying it off the
// client pinned each one to a single mailbox, so a run reached as many
// mailboxes as it had clients however many were configured — and every
// measurement taken from it described a few mailboxes under many times the
// intended load.
func TestDeliverySpreadsOverTheRecipientSetNotTheClientCount(t *testing.T) {
	srv := &fakeLMTP{}
	addr := srv.serve(t)

	const recipients, clients, deliveries = 12, 3, 36
	rcpts := make([]string, 0, recipients)
	for i := 1; i <= recipients; i++ {
		rcpts = append(rcpts, fmt.Sprintf("u%d@example.com", i))
	}
	d := lmtp.New(lmtp.Config{
		Addr:       addr,
		Recipients: rcpts,
		Corpus:     corpus.Spec{MinSize: 1024, MaxSize: 1024, Seed: 3},
		Timeout:    5 * time.Second,
	})

	var wg sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			if err := d.Deliver(context.Background(), worker%clients, stats.New()); err != nil {
				t.Errorf("delivery: %v", err)
			}
		}(i)
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, del := range srv.got() {
		seen[del.recipients[0]] = true
	}
	if len(seen) != recipients {
		t.Errorf("%d deliveries from %d clients reached %d of %d mailboxes: %v",
			deliveries, clients, len(seen), recipients, keys(seen))
	}
}

// Fan-out walks the set too: a run with several recipients per message must not
// revisit the same first few.
func TestFanOutWalksTheWholeSet(t *testing.T) {
	srv := &fakeLMTP{}
	addr := srv.serve(t)

	rcpts := make([]string, 0, 9)
	for i := 1; i <= 9; i++ {
		rcpts = append(rcpts, fmt.Sprintf("u%d@example.com", i))
	}
	d := lmtp.New(lmtp.Config{
		Addr:                 addr,
		Recipients:           rcpts,
		RecipientsPerMessage: 3,
		Corpus:               corpus.Spec{MinSize: 512, MaxSize: 512, Seed: 5},
		Timeout:              5 * time.Second,
	})
	for i := 0; i < 3; i++ {
		if err := d.Deliver(context.Background(), 0, stats.New()); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	seen := map[string]bool{}
	for _, del := range srv.got() {
		for _, r := range del.recipients {
			seen[r] = true
		}
	}
	if len(seen) != 9 {
		t.Errorf("three 3-recipient deliveries reached %d of 9 mailboxes: %v", len(seen), keys(seen))
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// A server that refuses must surface as an error rather than a silent success:
// a load run that counts refusals as throughput is worse than no run.
func TestDeliverReportsARefusal(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() }) //nolint:errcheck
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close() //nolint:errcheck
		fmt.Fprint(conn, "220 ready\r\n")
		br := bufio.NewReader(conn)
		br.ReadString('\n') //nolint:errcheck // LHLO
		fmt.Fprint(conn, "421 4.7.0 go away\r\n")
	}()

	d := lmtp.New(lmtp.Config{
		Addr:       ln.Addr().String(),
		Recipients: []string{"u1@example.com"},
		Corpus:     corpus.Spec{MinSize: 512, MaxSize: 512, Seed: 4},
		Timeout:    5 * time.Second,
	})
	c := stats.New()
	if err := d.Deliver(context.Background(), 0, c); err == nil {
		t.Fatal("a refused LHLO was reported as success")
	}
	if c.Summary().Errors == 0 {
		t.Error("the refusal was not counted as an error")
	}
}

// The handshake and the transfer are separate observations. Recording both as
// "DATA" counted every delivery twice and put two unrelated things in one
// histogram: the 354 reply takes microseconds, the transfer hundreds of
// milliseconds, so the median described neither and a failure could not be
// attributed to either.
func TestDeliveryRecordsHandshakeAndBodySeparately(t *testing.T) {
	// The delay is what makes this deterministic. Against a fake that answers
	// instantly, both observations are microseconds and their order is noise —
	// the assertion below failed about one run in ten while claiming to be
	// about conflation, which is a test that reports a defect it did not find.
	const serverWork = 20 * time.Millisecond
	srv := &fakeLMTP{bodyDelay: serverWork}
	addr := srv.serve(t)

	d := lmtp.New(lmtp.Config{
		Addr:       addr,
		Recipients: []string{"u1@example.com"},
		Corpus:     corpus.Spec{MinSize: 1024, MaxSize: 1024, Seed: 11},
		Timeout:    5 * time.Second,
	})
	c := stats.New()
	const deliveries = 5
	for i := 0; i < deliveries; i++ {
		if err := d.Deliver(context.Background(), 0, c); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}

	cmds := c.Summary().Commands
	// One of each per delivery: the count of DATA must match the count of the
	// commands that surround it, or ops/s for deliveries is inflated.
	for _, name := range []string{"MAIL", "RCPT", "DATA", "BODY"} {
		if cmds[name].Count != deliveries {
			t.Errorf("%s counted %d times over %d deliveries", name, cmds[name].Count, deliveries)
		}
	}
	// And they are not the same measurement. The server's work lands entirely
	// in BODY: DATA is the 354 that precedes it, so a driver that timed them
	// together would put this delay in both.
	const ms = float64(serverWork / time.Millisecond)
	if cmds["BODY"].MedianMs < ms {
		t.Errorf("BODY median %.1fms is below the %.0fms the server spent — the transfer is not being measured",
			cmds["BODY"].MedianMs, ms)
	}
	if cmds["DATA"].MedianMs >= ms {
		t.Errorf("DATA median %.1fms includes the %.0fms of server work — the handshake and the transfer are still conflated",
			cmds["DATA"].MedianMs, ms)
	}
}

// Replay has to put on the wire exactly what was loaded. The generated corpus
// and a replayed one take the same path from DATA onwards, so a failure that
// only appears with -mbox is a difference in the bytes — and nothing here
// compared them until a replay run failed 100% of its deliveries (#20).
func TestReplayedMessageArrivesByteForByte(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corpus.mbox")

	// Two messages, unix line endings, and the two shapes mbox has to undo:
	// a body line that mbox escaped as ">From ", and a line that is a single
	// dot, which the wire reserves as the terminator.
	const file = "From sender Mon Jan  1 00:00:00 2020\n" +
		"From: gen@es.test\n" +
		"To: u1@d00001.test\n" +
		"Subject: corpus es 0\n" +
		"\n" +
		"hola que tal\n" +
		">From here it continues\n" +
		".\n" +
		"final line\n" +
		"\n" +
		"From sender Mon Jan  1 00:01:00 2020\n" +
		"From: gen@es.test\n" +
		"Subject: corpus es 1\n" +
		"\n" +
		"segundo mensaje\n"
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}

	src, err := corpus.LoadMbox(path)
	if err != nil {
		t.Fatalf("LoadMbox: %v", err)
	}
	if src.Len() != 2 {
		t.Fatalf("loaded %d messages, want 2", src.Len())
	}

	srv := &fakeLMTP{}
	addr := srv.serve(t)
	d := lmtp.New(lmtp.Config{
		Addr:       addr,
		Recipients: []string{"u1@example.com"},
		Source:     src,
		Timeout:    5 * time.Second,
	})

	c := stats.New()
	for i := 0; i < 2; i++ {
		if err := d.Deliver(context.Background(), 0, c); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if got := c.Summary().Errors; got != 0 {
		t.Fatalf("%d errors replaying a corpus the loader accepted", got)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.deliveries) != 2 {
		t.Fatalf("server received %d messages, want 2", len(srv.deliveries))
	}
	for i, want := range [][]byte{src.Next(), src.Next()} {
		got := srv.deliveries[i].body
		if got != string(want) {
			t.Errorf("message %d differs from what was loaded:\n got %q\nwant %q", i, got, string(want))
		}
	}
}
