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

	// Mailboxes is the per-user set the run works over. A generator confined to
	// INBOX cannot produce the condition a per-user dispatcher is worth testing
	// against: several mailboxes of one user wanting work at once.
	Mailboxes        []string
	MailboxesPerUser int
	CreateMailboxes  bool

	// TargetMessages is the message count each client keeps its mailbox near.
	// Without it a run's mailboxes grow all the way through, so an operation at
	// the end costs more than the same operation at the start and the numbers
	// are not comparable with each other, let alone between runs.
	TargetMessages int

	Profile Profile
	Corpus  corpus.Spec
}

// Driver runs persistent IMAP sessions.
type Driver struct {
	cfg       Config
	gen       *corpus.Generator
	mailboxes []string
	weights   []weighted
	// seq numbers operations across all sessions, so the command mix and the
	// mailbox rotation are properties of the run rather than of one client.
	seq atomic.Uint64
}

func New(cfg Config) (*Driver, error) {
	if len(cfg.Users) == 0 {
		return nil, fmt.Errorf("imap: no users configured")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.TargetMessages <= 0 {
		cfg.TargetMessages = 100
	}
	if cfg.Profile == (Profile{}) {
		cfg.Profile = DefaultProfile()
	}
	d := &Driver{cfg: cfg, gen: corpus.New(cfg.Corpus), weights: cfg.Profile.weights()}
	if len(d.weights) == 0 {
		return nil, fmt.Errorf("imap: every command has zero weight")
	}

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

func (d *Driver) Mailboxes() []string { return d.mailboxes }

// Prepare creates the mailbox set for every user, before the clock starts: a
// run whose first operations are CREATE measures setup.
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

// RunClient is one persistent client: it logs in once and issues commands over
// the same connection until the run ends, reconnecting only when the profile
// says LOGOUT or the connection fails.
//
// This is the shape imaptest uses, and the reason is not tidiness. IMAP spends
// its resources per session — the index handle, the cached mailbox view,
// hibernation — and a generator that reconnects per operation measures the
// login path instead of any of that.
func (d *Driver) RunClient(ctx context.Context, id int, c *stats.Collector) error {
	s := &session{}
	defer func() {
		if s.cl != nil {
			s.cl.close()
		}
	}()

	for ctx.Err() == nil {
		if s.state == stateDisconnected {
			if err := d.connect(ctx, s, id, c); err != nil {
				return err
			}
		}
		if err := d.step(ctx, s, c); err != nil {
			// A failed command drops the session rather than being retried on
			// it: after an error the connection's state is not known, and
			// continuing on a connection whose state is a guess produces
			// failures that belong to the generator.
			if s.cl != nil {
				s.cl.close()
				s.cl = nil
			}
			s.state = stateDisconnected
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}

func (d *Driver) connect(ctx context.Context, s *session, id int, c *stats.Collector) error {
	// Each client owns one user for its lifetime — that is what a user is: a
	// person with a session, not a label attached to individual commands. The
	// spread comes from having more clients than the run has users, and from
	// mailbox rotation within the session.
	s.user = d.cfg.Users[id%len(d.cfg.Users)]

	t0 := time.Now()
	cl, err := dial(d.cfg.Addr, d.cfg.TLS, d.cfg.Insecure, d.cfg.Timeout)
	c.Observe("connect", time.Since(t0), err)
	if err != nil {
		return err
	}
	s.cl = cl

	t0 = time.Now()
	err = cl.login(s.user, d.cfg.Password)
	c.Observe("LOGIN", time.Since(t0), err)
	if err != nil {
		cl.close()
		s.cl = nil
		return fmt.Errorf("imap: login %s: %w", s.user, err)
	}
	s.state = stateAuthenticated
	s.view = view{}
	_ = ctx
	return nil
}

// step issues one command, choosing it from the profile and skipping any the
// current state forbids.
func (d *Driver) step(ctx context.Context, s *session, c *stats.Collector) error {
	n := d.seq.Add(1) - 1

	// A session that has nothing open must open something before the profile
	// gets a say: the alternative is a client that spends the run rejecting its
	// own choices.
	if !s.selected() {
		return d.doSelect(s, n, c)
	}
	// The steady state comes before the profile too. Without it the mailbox
	// grows for the whole run and every later operation costs more than the
	// same operation did earlier.
	// The client's own appends count towards the target even before the server
	// has reported them: otherwise a server that only sends EXISTS on the next
	// command would be appended to without bound in between.
	held := s.view.exists + s.view.appended
	if held < d.cfg.TargetMessages {
		return d.run(s, cmdAppend, c)
	}
	if held > d.cfg.TargetMessages*2 {
		return d.run(s, cmdExpunge, c)
	}

	cmd := pick(d.weights, n)
	for i := 0; !s.legal(cmd) && i < len(d.weights); i++ {
		cmd = pick(d.weights, n+uint64(i)+1)
	}
	if !s.legal(cmd) {
		cmd = cmdNoop
	}
	_ = ctx
	return d.run(s, cmd, c)
}

func (d *Driver) doSelect(s *session, n uint64, c *stats.Collector) error {
	mbox := d.mailboxes[int(n)%len(d.mailboxes)]
	t0 := time.Now()
	lines, err := s.cl.do("SELECT " + quoted(mbox))
	c.Observe(string(cmdSelect), time.Since(t0), err)
	if err != nil {
		return err
	}
	s.state = stateSelected
	s.view = view{mailbox: mbox}
	s.view.applyUntagged(lines)
	return s.checkMismatch()
}

// run issues one command and folds its untagged responses into the client's
// view of the mailbox.
func (d *Driver) run(s *session, cmd command, c *stats.Collector) error {
	t0 := time.Now()
	var (
		lines []string
		err   error
	)
	switch cmd {
	case cmdList:
		lines, err = s.cl.do(`LIST "" "*"`)
	case cmdStatus:
		lines, err = s.cl.do("STATUS " + quoted(s.view.mailbox) + " (MESSAGES UNSEEN)")
	case cmdSelect:
		c.Observe(string(cmdSelect), 0, nil) // counted inside doSelect
		return d.doSelect(s, d.seq.Add(1)-1, c)
	case cmdFetch:
		// Metadata for the whole mailbox: what a client does when it opens a
		// folder.
		lines, err = s.cl.do(d.rangeCmd(s, "FETCH", "(UID FLAGS RFC822.SIZE)"))
	case cmdFetch2:
		// Whole bodies for a bounded window. Bounded because an unbounded body
		// fetch grows with the run, and because this is the path whose cost
		// decides where message prefetching belongs.
		lines, err = s.cl.do(d.rangeCmd(s, "FETCH", "BODY.PEEK[]"))
	case cmdStore:
		lines, err = s.cl.do(d.rangeCmd(s, "STORE", `+FLAGS.SILENT (\Seen)`))
	case cmdDelete:
		// Marking, not removing: EXPUNGE is a separate command in the profile,
		// so the two can be weighted independently as they are on a real server.
		lines, err = s.cl.do(d.rangeCmd(s, "STORE", `+FLAGS.SILENT (\Deleted)`))
	case cmdExpunge:
		lines, err = s.cl.do("EXPUNGE")
	case cmdSearch:
		lines, err = s.cl.do("SEARCH TEXT throughput")
	case cmdNoop:
		lines, err = s.cl.do("NOOP")
	case cmdAppend:
		err = s.cl.appendMessage(s.view.mailbox, d.gen.Generate("loadtest@yarilo.invalid", s.user))
		if err == nil {
			// Only what this client did. exists comes from the server alone —
			// incrementing it here would compare the client's bookkeeping
			// against itself, which is a check that can never fail.
			s.view.appended++
		}
	case cmdLogout:
		lines, err = s.cl.do("LOGOUT")
		if err == nil {
			s.cl.close()
			s.cl = nil
			s.state = stateDisconnected
		}
	}
	c.Observe(string(cmd), time.Since(t0), err)
	if err != nil {
		return err
	}
	s.commands++
	s.view.applyUntagged(lines)
	// The check that makes this a test rather than a generator: a server that
	// accepted appends and stored nothing answers every command with OK, and
	// only the count gives it away.
	return s.checkMismatch()
}

// rangeCmd bounds a sequence-set command to the newest messages. "1:*" was the
// obvious form and the wrong one: its cost grows with the mailbox, so the same
// operation is cheap at the start of a run and expensive at the end, and no two
// measurements from one run are comparable.
func (d *Driver) rangeCmd(s *session, verb, args string) string {
	const window = 20
	from := s.view.exists - window + 1
	if from < 1 {
		from = 1
	}
	to := s.view.exists
	if to < 1 {
		to = 1
	}
	return fmt.Sprintf("%s %d:%d %s", verb, from, to, args)
}
