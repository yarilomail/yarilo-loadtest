package imapdrv

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// command is one IMAP operation the profile can weight. The set follows the
// columns imaptest prints — Logi List Stat Sele Fetc Fet2 Stor Dele Expu Appe
// Logo — so a run here can be read against one there.
type command string

const (
	cmdList    command = "LIST"
	cmdStatus  command = "STATUS"
	cmdSelect  command = "SELECT"
	cmdFetch   command = "FETCH"
	cmdFetch2  command = "FETCH2"
	cmdStore   command = "STORE"
	cmdDelete  command = "DELETE"
	cmdExpunge command = "EXPUNGE"
	cmdSearch  command = "SEARCH"
	cmdNoop    command = "NOOP"
	cmdAppend  command = "APPEND"
	cmdLogout  command = "LOGOUT"
)

// Profile weights the commands, as percentages of a client's activity. Zero
// means the command never runs, which is how a run narrows to one path — the
// `select=0 logout=0 search=100` shape.
//
// These are relative weights rather than probabilities that must total 100:
// requiring them to sum forces every change to be paired with a compensating
// one, and a load profile is edited far more often than it is designed.
type Profile struct {
	List    int
	Status  int
	Select  int
	Fetch   int
	Fetch2  int
	Store   int
	Delete  int
	Expunge int
	Search  int
	Noop    int
	Append  int
	Logout  int
}

// DefaultProfile is a mixed workload: mail arrives, is read, is flagged, and
// occasionally removed, with the mailbox reopened and listed as a client would.
func DefaultProfile() Profile {
	return Profile{
		List: 5, Status: 5, Select: 10,
		Fetch: 30, Fetch2: 10, Store: 15,
		Delete: 5, Expunge: 5, Search: 5, Noop: 5,
		Append: 30, Logout: 2,
	}
}

// weights renders the profile as a stable, ordered list. Ordered because the
// selection walks it deterministically: two runs with one profile must issue
// the same proportions, or a comparison measures the generator.
func (p Profile) weights() []weighted {
	all := []weighted{
		{cmdList, p.List}, {cmdStatus, p.Status}, {cmdSelect, p.Select},
		{cmdFetch, p.Fetch}, {cmdFetch2, p.Fetch2}, {cmdStore, p.Store},
		{cmdDelete, p.Delete}, {cmdExpunge, p.Expunge}, {cmdSearch, p.Search},
		{cmdNoop, p.Noop}, {cmdAppend, p.Append}, {cmdLogout, p.Logout},
	}
	out := all[:0]
	for _, w := range all {
		if w.weight > 0 {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].cmd < out[j].cmd })
	return out
}

type weighted struct {
	cmd    command
	weight int
}

// total is the sum of the enabled weights.
func total(ws []weighted) int {
	n := 0
	for _, w := range ws {
		n += w.weight
	}
	return n
}

// pick maps a counter onto the weighted set. Deterministic: the nth choice of a
// profile is always the same command, so two runs differ by the server rather
// than by chance.
func pick(ws []weighted, n uint64) command {
	sum := total(ws)
	if sum == 0 {
		return cmdNoop
	}
	slot := int(n % uint64(sum))
	for _, w := range ws {
		if slot < w.weight {
			return w.cmd
		}
		slot -= w.weight
	}
	return ws[len(ws)-1].cmd
}

// ParseProfile reads a weight list like "append=30,fetch=30,store=15". An empty
// string is the default mix.
//
// Weights are named rather than positional so a run's intent is readable in the
// job that launched it: "search=100,append=0" says what it is doing, where a
// tuple of numbers would need the source to interpret.
func ParseProfile(spec string) (Profile, error) {
	if strings.TrimSpace(spec) == "" {
		return DefaultProfile(), nil
	}
	p := Profile{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, value, ok := strings.Cut(part, "=")
		if !ok {
			return p, fmt.Errorf("imap profile: %q is not name=weight", part)
		}
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || n < 0 {
			return p, fmt.Errorf("imap profile: %q has no usable weight", part)
		}
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "list":
			p.List = n
		case "status":
			p.Status = n
		case "select":
			p.Select = n
		case "fetch":
			p.Fetch = n
		case "fetch2":
			p.Fetch2 = n
		case "store":
			p.Store = n
		case "delete":
			p.Delete = n
		case "expunge":
			p.Expunge = n
		case "search":
			p.Search = n
		case "noop":
			p.Noop = n
		case "append":
			p.Append = n
		case "logout":
			p.Logout = n
		default:
			return p, fmt.Errorf("imap profile: unknown command %q", name)
		}
	}
	if p == (Profile{}) {
		return p, fmt.Errorf("imap profile: every weight is zero")
	}
	return p, nil
}
