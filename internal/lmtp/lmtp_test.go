package lmtp_test

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
	"github.com/yarilomail/yarilo-loadtest/internal/lmtp"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// fakeLMTP is a server that speaks the protocol properly — multi-line LHLO
// included — and records what it received, so the driver is tested against the
// wire rather than against itself.
type fakeLMTP struct {
	mu         sync.Mutex
	deliveries []delivery
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
				body.WriteString(bl)
			}
			d.body = body.String()
			f.mu.Lock()
			f.deliveries = append(f.deliveries, d)
			f.mu.Unlock()
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

// Workers walk the recipient list rather than all hammering one mailbox —
// otherwise the run measures contention on that mailbox, not delivery.
func TestDeliverySpreadsAcrossRecipients(t *testing.T) {
	srv := &fakeLMTP{}
	addr := srv.serve(t)

	d := lmtp.New(lmtp.Config{
		Addr:       addr,
		Recipients: []string{"u1@example.com", "u2@example.com", "u3@example.com"},
		Corpus:     corpus.Spec{MinSize: 1024, MaxSize: 1024, Seed: 3},
		Timeout:    5 * time.Second,
	})
	for worker := 0; worker < 3; worker++ {
		if err := d.Deliver(context.Background(), worker, stats.New()); err != nil {
			t.Fatalf("worker %d: %v", worker, err)
		}
	}
	seen := map[string]bool{}
	for _, del := range srv.got() {
		seen[del.recipients[0]] = true
	}
	if len(seen) != 3 {
		t.Errorf("three workers delivered to %d distinct recipients: %v", len(seen), seen)
	}
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
