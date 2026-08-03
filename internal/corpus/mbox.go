package corpus

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
)

// Mbox replays messages from an mbox file instead of generating them.
//
// It exists for comparability: a run against the same corpus the reference tool
// was given is the only way two sets of numbers describe the same work.
// Generated mail is better for choosing sizes deliberately; real mail is better
// for saying "this server is slower than that one".
type Mbox struct {
	messages [][]byte
	seq      atomic.Uint64
}

// LoadMbox reads an mbox file. Messages are split on the "From " line at the
// start of a line, which is what mbox is; the ">From " escaping the format uses
// inside bodies is undone, or every replayed message would differ from the one
// that was stored.
func LoadMbox(path string) (*Mbox, error) {
	f, err := os.Open(path) //nolint:gosec // an operator-supplied corpus path
	if err != nil {
		return nil, fmt.Errorf("corpus: open mbox: %w", err)
	}
	defer f.Close() //nolint:errcheck

	m := &Mbox{}
	var cur bytes.Buffer
	br := bufio.NewReaderSize(f, 1<<20)
	atStart := true
	for {
		line, rerr := br.ReadString('\n')
		if line != "" {
			switch {
			case strings.HasPrefix(line, "From ") && atStart:
				if cur.Len() > 0 {
					m.add(&cur)
				}
				// The separator itself is not part of the message.
			case strings.HasPrefix(line, ">From "):
				cur.WriteString(line[1:])
			default:
				cur.WriteString(line)
			}
			atStart = strings.HasSuffix(line, "\n")
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, fmt.Errorf("corpus: read mbox: %w", rerr)
		}
	}
	if cur.Len() > 0 {
		m.add(&cur)
	}
	if len(m.messages) == 0 {
		return nil, fmt.Errorf("corpus: %s contains no messages", path)
	}
	return m, nil
}

// add normalises line endings to CRLF, which is what the wire carries: an mbox
// written on a unix host has bare LFs, and a server given those either rejects
// the message or stores something different from what the file held.
func (m *Mbox) add(buf *bytes.Buffer) {
	body := buf.Bytes()
	out := make([]byte, 0, len(body)+len(body)/20)
	for i := 0; i < len(body); i++ {
		if body[i] == '\n' && (i == 0 || body[i-1] != '\r') {
			out = append(out, '\r', '\n')
			continue
		}
		out = append(out, body[i])
	}
	out = bytes.TrimRight(out, "\r\n")
	m.messages = append(m.messages, append(out, '\r', '\n'))
	buf.Reset()
}

// Len reports how many messages were loaded.
func (m *Mbox) Len() int { return len(m.messages) }

// Generate returns the next message, cycling, so an Mbox is a Source. The
// arguments are ignored: a replayed message keeps the headers it was stored
// with, which is the point of replaying it.
func (m *Mbox) Generate(_, _ string) []byte { return m.Next() }

// Next returns the next message, cycling. Safe for concurrent use: every client
// draws from the same corpus at once.
func (m *Mbox) Next() []byte {
	n := m.seq.Add(1) - 1
	return m.messages[int(n)%len(m.messages)]
}
