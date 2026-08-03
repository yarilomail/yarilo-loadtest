// Package pop3drv is the POP3 driver: a full client session per iteration —
// connect, authenticate, survey the maildrop, retrieve, quit.
//
// A session per iteration rather than the persistent connections the IMAP
// driver holds, and that is a protocol difference rather than a shortcut. A
// POP3 server locks the maildrop for the duration of a session (RFC 1939 §3),
// so a client that stays connected keeps every other client for that user
// locked out. A generator that held its sessions open would measure its own
// lock contention and report it as the server's.
package pop3drv

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// Config describes a POP3 run.
type Config struct {
	Addr     string
	Users    []string
	Password string
	TLS      bool
	Insecure bool
	Timeout  time.Duration

	// MessagesPerSession bounds how many messages one session retrieves.
	//
	// Bounded because a real client retrieves what is new, while an unbounded
	// RETR loop walks the whole maildrop — so its cost grows with the mailbox
	// and the same operation is cheap at the start of a run and expensive at
	// the end. Zero means survey only: STAT and LIST, no retrieval.
	MessagesPerSession int

	// Delete issues DELE for what it retrieved, so the session ends the way a
	// real client's does.
	//
	// Off by default, and the default is the important part: a load run that
	// deletes empties the mailboxes every other run measures against. Turn it
	// on to measure the expunge-on-QUIT path, knowing it consumes the corpus.
	Delete bool

	// UIDL asks for the unique-id listing, which is what a client that keeps
	// mail on the server uses to work out what it has not seen. It is a
	// separate cost from LIST on some backends.
	UIDL bool
}

// Driver runs POP3 sessions.
type Driver struct {
	cfg Config
	// seq advances once per session rather than once per client, so the users a
	// run touches are a property of the run and not of how many clients it
	// happened to start.
	seq atomic.Uint64
}

func New(cfg Config) (*Driver, error) {
	if len(cfg.Users) == 0 {
		return nil, fmt.Errorf("pop3: no users configured")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MessagesPerSession < 0 {
		return nil, fmt.Errorf("pop3: -retr must not be negative")
	}
	return &Driver{cfg: cfg}, nil
}

// Session runs one complete POP3 session.
//
// Each command is timed separately. RETR is the one that matters: the server
// answers it only after reading the message out of the store, so its latency is
// what a user waiting for mail actually experiences.
func (d *Driver) Session(ctx context.Context, _ int, c *stats.Collector) error {
	user := d.cfg.Users[int(d.seq.Add(1)-1)%len(d.cfg.Users)]

	t0 := time.Now()
	cl, err := dial(ctx, d.cfg)
	c.Observe("connect", time.Since(t0), err)
	if err != nil {
		return err
	}
	defer cl.close()

	if err := cl.greeting(c); err != nil {
		return err
	}
	if err := cl.cmd(c, "USER", "USER "+user); err != nil {
		return err
	}
	if err := cl.cmd(c, "PASS", "PASS "+d.cfg.Password); err != nil {
		return err
	}

	count, err := cl.stat(c)
	if err != nil {
		return err
	}
	if _, err := cl.multiline(c, "LIST", "LIST"); err != nil {
		return err
	}
	if d.cfg.UIDL {
		if _, err := cl.multiline(c, "UIDL", "UIDL"); err != nil {
			return err
		}
	}

	// Newest first, the way a client fetching what arrived since last time
	// works through the maildrop.
	retrieved := 0
	for n := count; n > 0 && retrieved < d.cfg.MessagesPerSession; n-- {
		if err := cl.retr(c, n); err != nil {
			return err
		}
		retrieved++
		if d.cfg.Delete {
			if err := cl.cmd(c, "DELE", "DELE "+strconv.Itoa(n)); err != nil {
				return err
			}
		}
	}

	// QUIT is timed on its own because on a session that deleted anything it is
	// not a formality: the maildrop's update phase — the actual expunge —
	// happens here, and lumping it in with the rest hides it.
	return cl.cmd(c, "QUIT", "QUIT")
}

// client speaks as much POP3 as the driver needs.
type client struct {
	conn net.Conn
	br   *bufio.Reader
}

func dial(ctx context.Context, cfg Config) (*client, error) {
	d := net.Dialer{Timeout: cfg.Timeout}
	var (
		conn net.Conn
		err  error
	)
	if cfg.TLS {
		host, _, sErr := net.SplitHostPort(cfg.Addr)
		if sErr != nil {
			host = cfg.Addr
		}
		conn, err = tls.DialWithDialer(&d, "tcp", cfg.Addr, &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: cfg.Insecure, //nolint:gosec // opt-in, for sandbox certs
			MinVersion:         tls.VersionTLS12,
		})
	} else {
		conn, err = d.DialContext(ctx, "tcp", cfg.Addr)
	}
	if err != nil {
		return nil, fmt.Errorf("pop3: dial %s: %w", cfg.Addr, err)
	}
	deadline := time.Now().Add(cfg.Timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	return &client{conn: conn, br: bufio.NewReaderSize(conn, 64<<10)}, nil
}

func (c *client) close() { c.conn.Close() } //nolint:errcheck

// greeting reads the banner. Anything but +OK means the server will not serve
// this connection, and saying so beats failing on the next command with an
// error that describes the wrong thing.
func (c *client) greeting(col *stats.Collector) error {
	t0 := time.Now()
	_, err := c.readStatus()
	col.Observe("greeting", time.Since(t0), err)
	return err
}

// cmd sends a single-line command and reads its status reply.
func (c *client) cmd(col *stats.Collector, name, line string) error {
	t0 := time.Now()
	err := c.send(line)
	if err == nil {
		_, err = c.readStatus()
	}
	col.Observe(name, time.Since(t0), err)
	return err
}

// stat reads the maildrop size from STAT, which is what tells the session how
// many messages there are to retrieve.
func (c *client) stat(col *stats.Collector) (int, error) {
	t0 := time.Now()
	err := c.send("STAT")
	var rest string
	if err == nil {
		rest, err = c.readStatus()
	}
	col.Observe("STAT", time.Since(t0), err)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 {
		return 0, fmt.Errorf("pop3: STAT answered %q, want a message count", rest)
	}
	n, cerr := strconv.Atoi(fields[0])
	if cerr != nil {
		return 0, fmt.Errorf("pop3: STAT count %q: %w", fields[0], cerr)
	}
	return n, nil
}

// retr retrieves one message and discards it. The bytes are read rather than
// skipped: a driver that stops reading mid-message leaves the connection in a
// state the next command would fail from, and would report the server's
// response time as the time to send a request nobody finished.
func (c *client) retr(col *stats.Collector, n int) error {
	t0 := time.Now()
	err := c.send("RETR " + strconv.Itoa(n))
	if err == nil {
		if _, err = c.readStatus(); err == nil {
			_, err = c.readMultiline()
		}
	}
	col.Observe("RETR", time.Since(t0), err)
	return err
}

// multiline issues a command whose reply is a status line followed by lines
// terminated by a lone dot, and returns how many lines it held.
func (c *client) multiline(col *stats.Collector, name, line string) (int, error) {
	t0 := time.Now()
	var n int
	err := c.send(line)
	if err == nil {
		if _, err = c.readStatus(); err == nil {
			n, err = c.readMultiline()
		}
	}
	col.Observe(name, time.Since(t0), err)
	return n, err
}

func (c *client) send(line string) error {
	if _, err := c.conn.Write([]byte(line + "\r\n")); err != nil {
		return fmt.Errorf("pop3: write %q: %w", line, err)
	}
	return nil
}

// readStatus reads one status line and returns whatever followed +OK.
func (c *client) readStatus() (string, error) {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("pop3: read: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	switch {
	case strings.HasPrefix(line, "+OK"):
		return strings.TrimSpace(strings.TrimPrefix(line, "+OK")), nil
	case strings.HasPrefix(line, "-ERR"):
		return "", fmt.Errorf("pop3: %s", line)
	default:
		return "", fmt.Errorf("pop3: unexpected reply %q", line)
	}
}

// readMultiline consumes lines until the terminating dot, counting them.
//
// The terminator is a line holding *only* a dot. Content lines that begin with
// one arrive byte-stuffed as "..", so an equality check passes them through
// while a prefix check would end the message early — on any mail quoting a line
// that starts with a full stop, and the remainder of that message would then be
// read as the reply to the next command (RFC 1939 §3).
func (c *client) readMultiline() (int, error) {
	var n int
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n, fmt.Errorf("pop3: reply ended without its terminating dot")
			}
			return n, fmt.Errorf("pop3: read multiline: %w", err)
		}
		if strings.TrimRight(line, "\r\n") == "." {
			return n, nil
		}
		n++
	}
}
