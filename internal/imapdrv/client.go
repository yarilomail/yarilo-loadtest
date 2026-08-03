// Package imapdrv is the IMAP driver: a client that owns enough of the
// protocol to generate realistic churn, and a mix of operations over a
// configurable set of mailboxes per user.
//
// The mailbox set is the point. A generator that works only in INBOX cannot
// produce the one condition worth testing on a server that dispatches index
// work per user: several mailboxes of one user wanting indexing at once.
package imapdrv

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// client speaks as much IMAP as the driver needs. It is deliberately not a
// general-purpose library: a load generator that hides protocol detail behind
// an abstraction cannot be trusted to say what it actually sent.
type client struct {
	conn net.Conn
	br   *bufio.Reader
	tag  atomic.Uint64
	// selected is the mailbox this connection has open, so the driver can skip
	// a redundant SELECT without tracking it itself.
	selected string
}

func dial(addr string, useTLS, insecure bool, timeout time.Duration) (*client, error) {
	d := net.Dialer{Timeout: timeout}
	var (
		conn net.Conn
		err  error
	)
	if useTLS {
		host, _, sErr := net.SplitHostPort(addr)
		if sErr != nil {
			host = addr
		}
		conn, err = tls.DialWithDialer(&d, "tcp", addr, &tls.Config{
			ServerName:         host,
			InsecureSkipVerify: insecure, //nolint:gosec // opt-in, for sandbox certs
			MinVersion:         tls.VersionTLS12,
		})
	} else {
		conn, err = d.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("imap: dial %s: %w", addr, err)
	}
	c := &client{conn: conn, br: bufio.NewReaderSize(conn, 64<<10)}
	if err := c.deadline(timeout); err != nil {
		return nil, err
	}
	// The greeting is untagged; anything but OK means the server will not serve
	// this connection and the run should say so rather than time out later.
	line, err := c.br.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("imap: greeting: %w", err)
	}
	if !strings.HasPrefix(line, "* OK") && !strings.HasPrefix(line, "* PREAUTH") {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("imap: greeting refused: %s", strings.TrimSpace(line))
	}
	return c, nil
}

func (c *client) deadline(d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if err := c.conn.SetDeadline(time.Now().Add(d)); err != nil {
		return fmt.Errorf("imap: deadline: %w", err)
	}
	return nil
}

func (c *client) close() {
	c.conn.Close() //nolint:errcheck
}

func (c *client) nextTag() string {
	return fmt.Sprintf("a%03d", c.tag.Add(1))
}

// do sends one command and consumes responses until its tagged completion.
// Untagged lines are returned, because some of them are the point of the
// command — SEARCH results, FETCH data.
func (c *client) do(cmd string) ([]string, error) {
	tag := c.nextTag()
	if _, err := fmt.Fprintf(c.conn, "%s %s\r\n", tag, cmd); err != nil {
		return nil, fmt.Errorf("imap: write %q: %w", cmd, err)
	}
	return c.readUntilTagged(tag, cmd)
}

func (c *client) readUntilTagged(tag, cmd string) ([]string, error) {
	var untagged []string
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			return untagged, fmt.Errorf("imap: read after %q: %w", cmd, err)
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, tag+" "):
			rest := line[len(tag)+1:]
			if strings.HasPrefix(rest, "OK") {
				return untagged, nil
			}
			// NO and BAD are the server declining; a load run must count these
			// rather than treat them as data.
			return untagged, fmt.Errorf("imap: %s: %s", cmd, rest)
		case strings.HasPrefix(line, "* "):
			untagged = append(untagged, line[2:])
			// A literal in an untagged response — FETCH BODY[] — is followed by
			// exactly that many bytes, which are not lines and must not be
			// parsed as such.
			if n, ok := literalSize(line); ok {
				if err := c.discard(n); err != nil {
					return untagged, err
				}
			}
		case strings.HasPrefix(line, "+ "):
			// A continuation outside APPEND means the server wants data the
			// driver did not intend to send; saying so beats hanging.
			return untagged, fmt.Errorf("imap: unexpected continuation after %q", cmd)
		}
	}
}

// literalSize reads the {n} suffix that announces a literal.
func literalSize(line string) (int, bool) {
	if !strings.HasSuffix(line, "}") {
		return 0, false
	}
	open := strings.LastIndex(line, "{")
	if open < 0 {
		return 0, false
	}
	spec := line[open+1 : len(line)-1]
	spec = strings.TrimSuffix(spec, "+") // literal8 / non-synchronising
	n, err := strconv.Atoi(spec)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// discard consumes n bytes plus the CRLF that follows the literal's last line.
func (c *client) discard(n int) error {
	if _, err := c.br.Discard(n); err != nil {
		return fmt.Errorf("imap: discard literal: %w", err)
	}
	return nil
}

func (c *client) login(user, pass string) error {
	_, err := c.do(fmt.Sprintf("LOGIN %s %s", quoted(user), quoted(pass)))
	return err
}

// create makes a mailbox, treating "already exists" as success: a load run
// starts against whatever state the last one left.
func (c *client) create(mailbox string) error {
	_, err := c.do("CREATE " + quoted(mailbox))
	if err != nil && strings.Contains(strings.ToUpper(err.Error()), "ALREADYEXISTS") {
		return nil
	}
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return nil
	}
	return err
}

func (c *client) selectMailbox(mailbox string) error {
	if c.selected == mailbox {
		return nil
	}
	if _, err := c.do("SELECT " + quoted(mailbox)); err != nil {
		c.selected = ""
		return err
	}
	c.selected = mailbox
	return nil
}

// appendMessage writes a message with a synchronising literal: the server
// answers "+" before the bytes may be sent. Getting this arithmetic wrong is
// the usual way a hand-rolled IMAP load test loses a run, so the size is taken
// from the payload rather than computed alongside it.
func (c *client) appendMessage(mailbox string, body []byte) error {
	tag := c.nextTag()
	cmd := fmt.Sprintf("%s APPEND %s {%d}\r\n", tag, quoted(mailbox), len(body))
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("imap: write APPEND: %w", err)
	}
	line, err := c.br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("imap: APPEND continuation: %w", err)
	}
	if !strings.HasPrefix(line, "+") {
		return fmt.Errorf("imap: APPEND refused: %s", strings.TrimSpace(line))
	}
	if _, err := c.conn.Write(body); err != nil {
		return fmt.Errorf("imap: write body: %w", err)
	}
	if _, err := c.conn.Write([]byte("\r\n")); err != nil {
		return fmt.Errorf("imap: terminate literal: %w", err)
	}
	_, err = c.readUntilTagged(tag, "APPEND")
	return err
}

// quoted renders an IMAP quoted string. Mailbox names come from configuration,
// but a name with a quote or a backslash would otherwise change the meaning of
// the command rather than being rejected.
func quoted(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		if r == '"' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
}
