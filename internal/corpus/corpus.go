// Package corpus builds the messages a driver delivers.
//
// It exists because the size distribution is the point, not an incidental
// detail: measurements taken against 400-byte synthetic mail answered the
// wrong question about where indexing spends its time, and the ratio between
// reading a message and parsing it inverts once attachments are involved.
package corpus

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Spec describes the messages to generate. Sizes are in bytes.
type Spec struct {
	// MinSize and MaxSize bound the whole message. A run picks uniformly
	// between them, so one run can cover a spread rather than a single point.
	MinSize int
	MaxSize int
	// AttachmentRatio is the fraction of messages carrying an attachment,
	// 0 to 1. An attachment is base64-encoded, which is what makes a large
	// message expensive to parse as well as to read.
	AttachmentRatio float64
	// Seed makes a run reproducible: two runs with one seed generate the same
	// corpus, so a before/after comparison is not confounded by the mail.
	Seed int64
}

// Source is where a driver gets messages. Two implementations: a generator,
// for choosing sizes deliberately, and an mbox replay, for running the same
// corpus a reference run was given.
type Source interface {
	Generate(from, to string) []byte
}

// Generator produces messages to Spec.
//
// Safe for concurrent use: a load driver calls this from every client at once,
// and math/rand.Rand is not safe for that. Without the lock the RNG state tears
// under -race — and, worse, silently under a normal run, which for a tool whose
// output is a measurement means a corpus nobody can describe.
//
// The lock is not a bottleneck: generating a message is orders of magnitude
// cheaper than delivering it.
type Generator struct {
	spec Spec
	// base is the Date header's starting point. With a seed it is fixed, so a
	// seeded corpus is byte-for-byte reproducible: a Date taken from the clock
	// made two runs with one seed differ in content while matching in length,
	// which is the kind of difference a comparison silently absorbs.
	base time.Time

	mu  sync.Mutex
	rnd *rand.Rand
	seq int
}

func New(spec Spec) *Generator {
	if spec.MinSize <= 0 {
		spec.MinSize = 2 << 10
	}
	if spec.MaxSize < spec.MinSize {
		spec.MaxSize = spec.MinSize
	}
	seed := spec.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	base := time.Now()
	if spec.Seed != 0 {
		// A fixed point, not the clock: the seed's promise is that two runs
		// generate the same mail.
		base = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Generator{
		spec: spec,
		base: base,
		rnd:  rand.New(rand.NewSource(seed)), //nolint:gosec // corpus shape, not secrecy
	}
}

// Next renders one message as RFC 5322 bytes with CRLF line endings, which is
// what a delivery agent receives on the wire.
func (g *Generator) Generate(from, to string) []byte {
	// Everything random is drawn under the lock; rendering happens outside it,
	// since that is the part that costs anything.
	g.mu.Lock()
	g.seq++
	seq := g.seq
	size := g.spec.MinSize
	if g.spec.MaxSize > g.spec.MinSize {
		size += g.rnd.Intn(g.spec.MaxSize - g.spec.MinSize)
	}
	withAttachment := g.rnd.Float64() < g.spec.AttachmentRatio
	msgID := g.rnd.Int63()
	g.mu.Unlock()

	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", to)
	fmt.Fprintf(&b, "Subject: loadtest message %d\r\n", seq)
	fmt.Fprintf(&b, "Message-Id: <lt-%d-%d@yarilo-loadtest>\r\n", msgID, seq)
	fmt.Fprintf(&b, "Date: %s\r\n", g.base.Add(time.Duration(seq)*time.Second).Format(time.RFC1123Z))
	b.WriteString("MIME-Version: 1.0\r\n")

	if !withAttachment {
		b.WriteString("Content-Type: text/plain; charset=utf-8\r\n\r\n")
		writeFiller(&b, size)
		return b.Bytes()
	}

	const boundary = "yarilo-loadtest-boundary"
	fmt.Fprintf(&b, "Content-Type: multipart/mixed; boundary=%s\r\n\r\n", boundary)
	fmt.Fprintf(&b, "--%s\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n", boundary)
	writeFiller(&b, b.Len()+512)
	fmt.Fprintf(&b, "\r\n--%s\r\n", boundary)
	b.WriteString("Content-Type: application/octet-stream; name=payload.bin\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n")
	b.WriteString("Content-Disposition: attachment; filename=payload.bin\r\n\r\n")
	// base64 in 76-column lines, as a real MUA emits: the decoder cost is part
	// of what we are measuring.
	writeBase64Filler(&b, size-b.Len())
	fmt.Fprintf(&b, "\r\n--%s--\r\n", boundary)
	return b.Bytes()
}

// writeFiller pads until the buffer reaches target bytes. The target is
// absolute, not a remainder: passing "how much is left" once made every message
// short by the size of its own headers, which a size-driven load run cannot
// afford to get wrong quietly.
//
// The filler is word-like rather than one repeated character — a tokeniser's
// cost depends on how many distinct terms it sees, and a megabyte of "aaaa"
// would flatter it.
func writeFiller(b *bytes.Buffer, target int) {
	if b.Len() >= target {
		return
	}
	words := []string{"delivery", "mailbox", "index", "search", "message",
		"attachment", "throughput", "latency", "checkpoint", "tokenise"}
	line := 0
	for b.Len() < target {
		b.WriteString(words[line%len(words)])
		b.WriteByte(' ')
		line++
		if line%10 == 0 {
			b.WriteString("\r\n")
		}
	}
	b.WriteString("\r\n")
}

func writeBase64Filler(b *bytes.Buffer, n int) {
	if n <= 0 {
		return
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var line strings.Builder
	written := 0
	for written < n {
		line.Reset()
		for i := 0; i < 76 && written < n; i++ {
			line.WriteByte(alphabet[(written+i)%len(alphabet)])
			written++
		}
		b.WriteString(line.String())
		b.WriteString("\r\n")
	}
}
