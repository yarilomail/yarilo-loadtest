// Package lmtp is the delivery-storm driver: it feeds mail in at the same door
// a real MTA uses, so the measurement covers everything delivery triggers —
// storage write, index update and the full-text index pass behind it.
package lmtp

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
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
	// Timeout bounds one command, so a wedged server ends the run instead of
	// hanging it.
	Timeout time.Duration
}

// Driver delivers messages over LMTP.
type Driver struct {
	cfg Config
	gen *corpus.Generator
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
	return &Driver{cfg: cfg, gen: corpus.New(cfg.Corpus)}
}

// Deliver runs one delivery: connect, LHLO, MAIL, RCPT×N, DATA, QUIT.
//
// Each command is timed separately. DATA is the one that matters — the server
// answers it only after the message is written and indexed, so its latency is
// the delivery cost an operator actually experiences.
func (d *Driver) Deliver(ctx context.Context, worker int, c *stats.Collector) error {
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

	rcpts := d.recipientsFor(worker)
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
	// One DATA yields one reply per recipient (RFC 2033), so the timing is
	// attributed to the whole delivery rather than divided by a fan-out the
	// server did not parallelise.
	c.Observe("DATA", time.Since(t0), err)
	if err != nil {
		return err
	}
	return s.cmd("QUIT", "QUIT", "221")
}

// recipientsFor picks this worker's recipients, walking the list so a run
// spreads over accounts instead of hammering one mailbox — one mailbox would
// measure lock contention on it rather than delivery throughput.
func (d *Driver) recipientsFor(worker int) []string {
	n := d.cfg.RecipientsPerMessage
	if n > len(d.cfg.Recipients) {
		n = len(d.cfg.Recipients)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, d.cfg.Recipients[(worker+i)%len(d.cfg.Recipients)])
	}
	return out
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
	if _, err := s.conn.Write(msg); err != nil {
		return fmt.Errorf("lmtp: write body: %w", err)
	}
	// Dot-stuffing is unnecessary for generated mail — the corpus never emits
	// a line starting with a dot — but the terminator must be on its own line
	// even when the body already ended with CRLF.
	if _, err := s.conn.Write([]byte("\r\n.\r\n")); err != nil {
		return fmt.Errorf("lmtp: write terminator: %w", err)
	}
	for i := 0; i < recipients; i++ {
		if err := s.readReply("250"); err != nil {
			return fmt.Errorf("lmtp: recipient %d: %w", i+1, err)
		}
	}
	return nil
}
