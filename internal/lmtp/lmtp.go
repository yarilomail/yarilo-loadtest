// Package lmtp is the delivery-storm driver: it feeds mail in at the same door
// a real MTA uses, so the measurement covers everything delivery triggers —
// storage write, index update and the full-text index pass behind it.
package lmtp

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/corpus"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// Config describes one delivery run.
type Config struct {
	Addr string
	// Recipients are delivered to in turn. Several per connection is the
	// fan-out a real MTA produces when one message goes to many local users.
	Recipients []string
	// RecipientsPerMessage is how many RCPT TO lines one DATA serves.
	RecipientsPerMessage int
	Sender               string
	Corpus               corpus.Spec
	// Source overrides the generated corpus, for replaying real mail.
	Source corpus.Source
	// Timeout bounds one command, so a wedged server ends the run instead of
	// hanging it.
	Timeout time.Duration
}

// Driver delivers messages over LMTP.
type Driver struct {
	cfg Config
	gen corpus.Source
	// seq advances once per delivery, not once per client. Keying the
	// recipient off the client instead pinned each one to a single mailbox, so
	// a run touched as many mailboxes as it had clients however many were
	// configured — and every measurement taken from it described a handful of
	// mailboxes under many times the intended load.
	seq atomic.Uint64
}

func New(cfg Config) *Driver {
	if cfg.RecipientsPerMessage <= 0 {
		cfg.RecipientsPerMessage = 1
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Sender == "" {
		cfg.Sender = "loadtest@yarilo.invalid"
	}
	src := cfg.Source
	if src == nil {
		src = corpus.New(cfg.Corpus)
	}
	return &Driver{cfg: cfg, gen: src}
}

// Deliver runs one delivery: connect, LHLO, MAIL, RCPT×N, DATA, QUIT.
//
// Each command is timed separately. DATA is the one that matters — the server
// answers it only after the message is written and indexed, so its latency is
// the delivery cost an operator actually experiences.
func (d *Driver) Deliver(ctx context.Context, _ int, c *stats.Collector) error {
	dialer := net.Dialer{Timeout: d.cfg.Timeout}
	t0 := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", d.cfg.Addr)
	c.Observe("connect", time.Since(t0), err)
	if err != nil {
		return fmt.Errorf("lmtp: dial %s: %w", d.cfg.Addr, err)
	}
	defer conn.Close() //nolint:errcheck

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(d.cfg.Timeout))
	}
	s := &session{conn: conn, br: bufio.NewReader(conn), c: c}

	if err := s.expect("greeting", "220"); err != nil {
		return err
	}
	if err := s.cmd("LHLO", "LHLO yarilo-loadtest", "250"); err != nil {
		return err
	}
	if err := s.cmd("MAIL", "MAIL FROM:<"+d.cfg.Sender+">", "250"); err != nil {
		return err
	}

	rcpts := d.nextRecipients()
	for _, rcpt := range rcpts {
		if err := s.cmd("RCPT", "RCPT TO:<"+rcpt+">", "250"); err != nil {
			return err
		}
	}
	if err := s.cmd("DATA", "DATA", "354"); err != nil {
		return err
	}

	msg := d.gen.Generate(d.cfg.Sender, rcpts[0])
	t0 = time.Now()
	err = s.body(msg, len(rcpts))
	// BODY, not DATA. Both used to be recorded under one name, which counted
	// every delivery twice and — worse — put two unrelated observations in one
	// histogram: the 354 handshake takes microseconds, the transfer takes
	// hundreds of milliseconds, so the result was bimodal by construction and
	// its median described neither. A failure was equally unattributable: the
	// server refusing DATA and the transfer failing looked the same.
	//
	// One DATA yields one reply per recipient (RFC 2033), so this covers the
	// whole delivery rather than being divided by a fan-out the server did not
	// parallelise.
	c.Observe("BODY", time.Since(t0), err)
	if err != nil {
		return err
	}
	return s.cmd("QUIT", "QUIT", "221")
}

// nextRecipients takes the next slice of the configured set, round-robin over
// deliveries. Concurrency controls how many deliveries are in flight and
// nothing about which mailboxes they reach: -recipients means "spread across
// these", and a tool whose flag reads that way has to do it.
func (d *Driver) nextRecipients() []string {
	n := d.cfg.RecipientsPerMessage
	if n > len(d.cfg.Recipients) {
		n = len(d.cfg.Recipients)
	}
	// One counter increment per delivery, taking n consecutive addresses, so a
	// fan-out run still walks the whole set rather than revisiting the first n.
	start := int(d.seq.Add(uint64(n))) - n
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		idx := (start + i) % len(d.cfg.Recipients)
		if idx < 0 {
			idx += len(d.cfg.Recipients)
		}
		out = append(out, d.cfg.Recipients[idx])
	}
	return out
}

// stuffDots escapes the lines a message may not send as they are: RFC 5321
// §4.5.2 reserves a line holding a single dot as the end of the data, so any
// body line beginning with one is sent with an extra dot and the server removes
// it again.
//
// The generated corpus never produces such a line, which is why this was left
// out and why nothing noticed. A replayed message can contain anything — and
// one containing a bare dot did not merely arrive corrupted: it ended the
// transfer early, and everything after it was read as commands. That is why the
// failure showed up as a refusal on a later command rather than on the body
// (#20).
func stuffDots(msg []byte) []byte {
	if !needsStuffing(msg) {
		return msg
	}
	out := make([]byte, 0, len(msg)+16)
	atLineStart := true
	for i := 0; i < len(msg); i++ {
		if atLineStart && msg[i] == '.' {
			out = append(out, '.')
		}
		out = append(out, msg[i])
		atLineStart = msg[i] == '\n'
	}
	return out
}

// terminator ends the data, adding the CRLF the message lacks.
func terminator(msg []byte) []byte {
	if bytes.HasSuffix(msg, []byte("\r\n")) {
		return []byte(".\r\n")
	}
	return []byte("\r\n.\r\n")
}

// needsStuffing keeps the common case allocation-free: almost no message has a
// line starting with a dot, and copying every body to discover that would put
// an allocation the size of the corpus in the hot path.
func needsStuffing(msg []byte) bool {
	if len(msg) > 0 && msg[0] == '.' {
		return true
	}
	return bytes.Contains(msg, []byte("\n."))
}

type session struct {
	conn net.Conn
	br   *bufio.Reader
	c    *stats.Collector
}

func (s *session) cmd(name, line, wantPrefix string) error {
	t0 := time.Now()
	err := s.send(line)
	if err == nil {
		err = s.readReply(wantPrefix)
	}
	s.c.Observe(name, time.Since(t0), err)
	return err
}

func (s *session) send(line string) error {
	if _, err := s.conn.Write([]byte(line + "\r\n")); err != nil {
		return fmt.Errorf("lmtp: write %q: %w", line, err)
	}
	return nil
}

func (s *session) expect(name, wantPrefix string) error {
	t0 := time.Now()
	err := s.readReply(wantPrefix)
	s.c.Observe(name, time.Since(t0), err)
	return err
}

// readReply consumes one reply, including a multi-line one: LHLO answers with
// a capability list, and stopping at the first line would leave the rest in the
// buffer to be misread as the answer to the next command.
func (s *session) readReply(wantPrefix string) error {
	for {
		line, err := s.br.ReadString('\n')
		if err != nil {
			return fmt.Errorf("lmtp: read: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if len(line) < 4 {
			return fmt.Errorf("lmtp: short reply %q", line)
		}
		if !strings.HasPrefix(line, wantPrefix) {
			return fmt.Errorf("lmtp: got %q, want %s", line, wantPrefix)
		}
		if line[3] == ' ' {
			return nil
		}
		// line[3] == '-': another line follows.
	}
}

// body writes the message and reads one reply per recipient.
func (s *session) body(msg []byte, recipients int) error {
	if _, err := s.conn.Write(stuffDots(msg)); err != nil {
		return fmt.Errorf("lmtp: write body: %w", err)
	}
	// The terminator is a line of its own, so it needs a CRLF before it only
	// when the message does not already end with one. Writing both
	// unconditionally appends an empty line to every message — which delivers
	// fine and quietly makes the stored message differ from the one that was
	// replayed, defeating the purpose of replaying it.
	if _, err := s.conn.Write(terminator(msg)); err != nil {
		return fmt.Errorf("lmtp: write terminator: %w", err)
	}
	for i := 0; i < recipients; i++ {
		if err := s.readReply("250"); err != nil {
			return fmt.Errorf("lmtp: recipient %d: %w", i+1, err)
		}
	}
	return nil
}
