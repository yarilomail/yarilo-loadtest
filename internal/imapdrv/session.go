package imapdrv

import (
	"fmt"
	"strconv"
	"strings"
)

// state is where a connection is in the protocol. Commands are legal per state,
// and a driver that ignores that is not testing a server so much as guessing at
// one: FETCH before SELECT is a client bug, and a server answering it would be
// the finding, not the load.
type state int

const (
	stateDisconnected state = iota
	stateAuthenticated
	stateSelected
)

// view is what the client believes the selected mailbox contains.
//
// Tracking it is the difference between generating load and testing a server.
// A run that accepts any reply beginning with OK cannot tell a working server
// from one that answers plausibly and stores nothing; comparing EXISTS against
// what this client appended and expunged catches exactly that.
type view struct {
	mailbox string
	// exists is the server's last reported message count.
	exists int
	// baseline is the count the server reported when this client last heard
	// from it, and appended counts what this client has added since. The
	// comparison happens only when the server speaks again: comparing at any
	// other moment asks the server about messages it has not been told about
	// yet, and would call every healthy run a failure.
	baseline int
	appended int
	// mismatch is set when a report contradicts what this client put there.
	mismatch error
}

// applyUntagged folds untagged responses into the view. EXISTS and EXPUNGE are
// the two that move the count; the rest are data the caller may want but the
// view does not.
func (v *view) applyUntagged(lines []string) {
	for _, line := range lines {
		num, rest, ok := splitNum(line)
		if !ok {
			continue
		}
		switch {
		case strings.HasPrefix(rest, "EXISTS"):
			v.report(num)
		case strings.HasPrefix(rest, "EXPUNGE"):
			// An expunge shifts every later sequence number down by one, which
			// is why a client that tracks state has to see them: acting on a
			// stale sequence number is how a test corrupts its own mailbox.
			if v.exists > 0 {
				v.exists--
			}
			if v.baseline > 0 {
				v.baseline--
			}
		}
	}
}

// report takes a server-stated message count and checks it against what this
// client has added since the last one.
//
// Only a floor is asserted: these mailboxes are shared, so more messages than
// expected is unremarkable. Fewer cannot be explained by another client, and is
// the shape a server that acknowledges appends and stores nothing takes — it
// answers OK to everything, and only the count gives it away.
func (v *view) report(n int) {
	if n < v.baseline+v.appended {
		v.mismatch = fmt.Errorf("imap: %s reports %d messages, this client added %d to the %d it last saw",
			v.mailbox, n, v.appended, v.baseline)
	}
	v.exists = n
	v.baseline = n
	v.appended = 0
}

// splitNum parses "12 EXISTS" into (12, "EXISTS").
func splitNum(line string) (int, string, bool) {
	numStr, rest, ok := strings.Cut(line, " ")
	if !ok {
		return 0, "", false
	}
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, "", false
	}
	return n, rest, true
}

// session is one persistent client: a connection, its protocol state, and its
// view of the mailbox it has open.
//
// imaptest's clients are long-lived, and that is not incidental. IMAP is a
// session protocol: the per-session index handle, the cached mailbox view and
// hibernation are what a server spends resources on, and a generator that
// reconnects per operation exercises the login path instead of any of it.
type session struct {
	cl    *client
	user  string
	state state
	view  view
	// commands is this session's own count, for the per-second table.
	commands int
}

func (s *session) selected() bool { return s.state == stateSelected }

// legal reports whether cmd may run in the current state.
func (s *session) legal(c command) bool {
	switch c {
	case cmdList, cmdStatus, cmdSelect:
		return s.state >= stateAuthenticated
	case cmdFetch, cmdFetch2, cmdStore, cmdDelete, cmdExpunge, cmdSearch, cmdNoop:
		return s.state == stateSelected
	case cmdAppend:
		// APPEND does not require a selected mailbox, but this driver always
		// appends to the mailbox it has open, so the counts it tracks stay
		// meaningful.
		return s.state == stateSelected
	case cmdLogout:
		return s.state >= stateAuthenticated
	default:
		return false
	}
}

// checkMismatch returns and clears any contradiction the server reported.
func (s *session) checkMismatch() error {
	err := s.view.mismatch
	s.view.mismatch = nil
	return err
}
