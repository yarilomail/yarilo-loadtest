package imapdrv

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/corpus"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

// Config describes an IMAP run.
type Config struct {
	Addr     string
	Users    []string
	Password string
	TLS      bool
	Insecure bool
	Timeout  time.Duration

	// Mailboxes is the per-user set the run works over. Empty means INBOX
	// alone, which is what a generator that cannot spread looks like — and the
	// reason this option exists.
	Mailboxes []string
	// MailboxesPerUser generates Load1 … LoadN in place of an explicit list, so
	// a run can be scaled without editing one.
	MailboxesPerUser int
	// CreateMailboxes makes the set before the run. A mailbox that already
	// exists is not an error.
	CreateMailboxes bool

	// Ops is the operation mix, by weight. Zero-weight operations never run.
	Ops    OpMix
	Corpus corpus.Spec
}

// OpMix weights the operations. APPEND and STORE are what queue index work,
// SEARCH is what reads it back — a mix without all three measures half the
// system.
type OpMix struct {
	Append int
	Fetch  int
	Store  int
	Search int
}

func (m OpMix) total() int { return m.Append + m.Fetch + m.Store + m.Search }

// Driver runs the IMAP mix.
type Driver struct {
	cfg       Config
	gen       *corpus.Generator
	mailboxes []string
	// seq advances once per operation. Selection is per operation, not per
	// client: keying it off the client would pin each one to a single user and
	// mailbox, which is the defect this driver was asked for in the first place.
	seq atomic.Uint64
}

func New(cfg Config) (*Driver, error) {
	if len(cfg.Users) == 0 {
		return nil, fmt.Errorf("imap: no users configured")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Ops.total() == 0 {
		cfg.Ops = OpMix{Append: 3, Fetch: 3, Store: 2, Search: 2}
	}
	d := &Driver{cfg: cfg, gen: corpus.New(cfg.Corpus)}

	switch {
	case len(cfg.Mailboxes) > 0:
		d.mailboxes = cfg.Mailboxes
	case cfg.MailboxesPerUser > 0:
		for i := 1; i <= cfg.MailboxesPerUser; i++ {
			d.mailboxes = append(d.mailboxes, fmt.Sprintf("Load%d", i))
		}
	default:
		d.mailboxes = []string{"INBOX"}
	}
	return d, nil
}

// Mailboxes reports the resolved set, for the run's own logging.
func (d *Driver) Mailboxes() []string { return d.mailboxes }

// Prepare creates the mailbox set for every user. Done once before the run so
// the measured operations are not half mailbox creation.
func (d *Driver) Prepare(ctx context.Context) error {
	if !d.cfg.CreateMailboxes {
		return nil
	}
	for _, user := range d.cfg.Users {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c, err := dial(d.cfg.Addr, d.cfg.TLS, d.cfg.Insecure, d.cfg.Timeout)
		if err != nil {
			return err
		}
		if err := c.login(user, d.cfg.Password); err != nil {
			c.close()
			return fmt.Errorf("imap: login %s: %w", user, err)
		}
		for _, mbox := range d.mailboxes {
			if strings.EqualFold(mbox, "INBOX") {
				continue
			}
			if err := c.create(mbox); err != nil {
				c.close()
				return fmt.Errorf("imap: create %s for %s: %w", mbox, user, err)
			}
		}
		c.close()
	}
	return nil
}

// Run performs one operation: connect, log in, act, disconnect.
//
// A connection per operation is deliberate for now. It costs a login per
// operation, which the stats show separately, and it keeps the mailbox
// selection honest — a long-lived connection would tempt the driver to reuse
// whatever it had selected, which is how load ends up concentrated on one
// mailbox without anybody deciding that.
func (d *Driver) Run(ctx context.Context, _ int, c *stats.Collector) error {
	n := d.seq.Add(1) - 1
	user := d.cfg.Users[int(n)%len(d.cfg.Users)]
	mailbox := d.mailboxes[int(n/uint64(len(d.cfg.Users)))%len(d.mailboxes)]
	op := d.pickOp(n)

	t0 := time.Now()
	cl, err := dial(d.cfg.Addr, d.cfg.TLS, d.cfg.Insecure, d.cfg.Timeout)
	c.Observe("connect", time.Since(t0), err)
	if err != nil {
		return err
	}
	defer cl.close()

	t0 = time.Now()
	err = cl.login(user, d.cfg.Password)
	c.Observe("LOGIN", time.Since(t0), err)
	if err != nil {
		return err
	}

	t0 = time.Now()
	err = cl.selectMailbox(mailbox)
	c.Observe("SELECT", time.Since(t0), err)
	if err != nil {
		return err
	}

	if err := d.runOp(cl, op, user, mailbox, c); err != nil {
		return err
	}

	t0 = time.Now()
	_, err = cl.do("LOGOUT")
	c.Observe("LOGOUT", time.Since(t0), err)
	return err
}

// op names one operation of the mix.
type op string

const (
	opAppend op = "APPEND"
	opFetch  op = "FETCH"
	opStore  op = "STORE"
	opSearch op = "SEARCH"
)

// pickOp walks the weighted mix deterministically. Deterministic rather than
// random so two runs with the same flags issue the same operations in the same
// proportion — a comparison should differ by what changed on the server, not by
// what the generator felt like doing.
func (d *Driver) pickOp(n uint64) op {
	m := d.cfg.Ops
	slot := int(n % uint64(m.total()))
	switch {
	case slot < m.Append:
		return opAppend
	case slot < m.Append+m.Fetch:
		return opFetch
	case slot < m.Append+m.Fetch+m.Store:
		return opStore
	default:
		return opSearch
	}
}

func (d *Driver) runOp(cl *client, o op, user, mailbox string, c *stats.Collector) error {
	t0 := time.Now()
	var err error
	switch o {
	case opAppend:
		err = cl.appendMessage(mailbox, d.gen.Generate("loadtest@yarilo.invalid", user))
	case opFetch:
		// Whole bodies, not headers: this is the path whose cost is unmeasured
		// and which decides where message prefetching belongs.
		_, err = cl.do("FETCH 1:* BODY.PEEK[]")
	case opStore:
		// Flag churn is what queues index work without adding messages.
		_, err = cl.do(`STORE 1:* +FLAGS.SILENT (\Seen)`)
	case opSearch:
		// A term the corpus actually contains, so the search does the work of
		// matching rather than the work of finding nothing.
		_, err = cl.do("SEARCH TEXT throughput")
	}
	c.Observe(string(o), time.Since(t0), err)
	// An empty mailbox answers FETCH and STORE with a NO; that is the mailbox
	// being empty, not the server failing, and counting it as an error would
	// make every fresh run look broken.
	if err != nil && isEmptyMailbox(err) {
		return nil
	}
	return err
}

func isEmptyMailbox(err error) bool {
	msg := strings.ToUpper(err.Error())
	return strings.Contains(msg, "NO SUCH MESSAGE") ||
		strings.Contains(msg, "INVALID MESSAGE SET") ||
		strings.Contains(msg, "NO MESSAGES")
}
