// yarilo-loadtest generates protocol load against a yarilo deployment and
// reports per-command latencies.
//
// It is a client, not part of the server: it ships as its own image so a run
// can be pointed at any deployment without the server carrying a load
// generator inside it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yarilomail/yarilo-loadtest/internal/corpus"
	"github.com/yarilomail/yarilo-loadtest/internal/driver"
	"github.com/yarilomail/yarilo-loadtest/internal/imapdrv"
	"github.com/yarilomail/yarilo-loadtest/internal/lmtp"
	"github.com/yarilomail/yarilo-loadtest/internal/stats"
)

var (
	flagProtocol = flag.String("protocol", "", "protocol driver: lmtp | imap")
	flagAddr     = flag.String("addr", "", "host:port of the target listener")

	flagConcurrency = flag.Int("concurrency", 10, "concurrent clients")
	flagDuration    = flag.Duration("duration", 0, "run for this long (set this or -iterations)")
	flagIterations  = flag.Int("iterations", 0, "stop after this many operations (set this or -duration)")
	flagRampUp      = flag.Duration("ramp-up", 2*time.Second, "spread client starts over this window")
	flagStopOnError = flag.Bool("stop-on-error", false, "end the run at the first protocol error")
	flagTimeout     = flag.Duration("timeout", 30*time.Second, "per-command timeout")

	flagRecipients = flag.String("recipients", "", "comma-separated LMTP recipients, or user@domain:N to expand u1..uN")
	flagSender     = flag.String("sender", "loadtest@yarilo.invalid", "envelope sender")
	flagRcptPerMsg = flag.Int("recipients-per-message", 1, "RCPT TO lines per delivery")

	flagMinSize    = flag.Int("min-size", 2048, "smallest generated message, bytes")
	flagMaxSize    = flag.Int("max-size", 8192, "largest generated message, bytes")
	flagAttachRate = flag.Float64("attachment-ratio", 0, "fraction of messages carrying a base64 attachment, 0..1")
	flagSeed       = flag.Int64("seed", 0, "corpus seed; the same seed generates the same mail")

	flagUsers      = flag.String("users", "", "comma-separated IMAP users, or user@domain:N to expand u1..uN")
	flagPassword   = flag.String("password", "", "password for every user")
	flagTLS        = flag.Bool("tls", false, "connect with implicit TLS (IMAPS)")
	flagInsecure   = flag.Bool("insecure", false, "skip TLS certificate verification")
	flagMailboxes  = flag.String("mailboxes", "", "comma-separated mailbox names per user")
	flagMailboxesN = flag.Int("mailboxes-per-user", 0, "generate Load1..LoadN mailboxes per user instead of a list")
	flagCreate     = flag.Bool("create-mailboxes", true, "create the mailbox set before the run")
	flagMsgs       = flag.Int("msgs", 100, "message count each client keeps its mailbox near")
	flagProfileStr = flag.String("profile", "",
		"command weights, e.g. 'append=30,fetch=30,store=15,search=5'; empty uses the default mix")

	flagMbox     = flag.String("mbox", "", "replay messages from this mbox file instead of generating them")
	flagInterval = flag.Duration("interval", time.Second, "how often to print the live table; 0 disables it")

	flagJSON = flag.Bool("json", false, "emit the summary as JSON instead of a table")
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: levelFromEnv(),
	})))

	if *flagAddr == "" {
		fail("-addr is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	src, serr := messageSource()
	if serr != nil {
		fail("%v", serr)
	}

	c := stats.New()
	// The live table goes to stderr so -json on stdout stays machine-readable:
	// a run watched by a human and a run parsed by a job are the same run.
	done := make(chan struct{})
	if *flagInterval > 0 && !*flagJSON {
		go stats.NewLive(c, os.Stderr, *flagInterval).Run(done)
	}

	var err error
	switch strings.ToLower(*flagProtocol) {
	case "lmtp":
		err = runLMTP(ctx, c, src)
	case "imap":
		err = runIMAP(ctx, c, src)
	case "":
		fail("-protocol is required (lmtp | imap)")
	default:
		fail("unknown protocol %q", *flagProtocol)
	}

	close(done)
	summary := c.Summary()
	if *flagJSON {
		if werr := summary.WriteJSON(os.Stdout); werr != nil {
			fail("writing summary: %v", werr)
		}
	} else {
		summary.WriteTable(os.Stdout)
	}

	// A run that produced protocol errors exits non-zero so a Job can gate on
	// it without parsing the output.
	if err != nil || summary.Errors > 0 {
		if err != nil {
			slog.Error("loadtest: run failed", "err", err)
		}
		os.Exit(1)
	}
}

func runLMTP(ctx context.Context, c *stats.Collector, src corpus.Source) error {
	rcpts := expandRecipients(*flagRecipients)
	if len(rcpts) == 0 {
		fail("-recipients is required for the lmtp driver")
	}
	d := lmtp.New(lmtp.Config{
		Addr:                 *flagAddr,
		Recipients:           rcpts,
		RecipientsPerMessage: *flagRcptPerMsg,
		Sender:               *flagSender,
		Timeout:              *flagTimeout,
		Source:               src,
		Corpus: corpus.Spec{
			MinSize:         *flagMinSize,
			MaxSize:         *flagMaxSize,
			AttachmentRatio: *flagAttachRate,
			Seed:            *flagSeed,
		},
	})
	slog.Info("loadtest: starting", "protocol", "lmtp", "addr", *flagAddr,
		"recipients", len(rcpts), "concurrency", *flagConcurrency,
		"min_size", *flagMinSize, "max_size", *flagMaxSize, "attachment_ratio", *flagAttachRate)

	return driver.Run(ctx, driver.Options{
		Addr:        *flagAddr,
		Concurrency: *flagConcurrency,
		Duration:    *flagDuration,
		Iterations:  *flagIterations,
		RampUp:      *flagRampUp,
		StopOnError: *flagStopOnError,
	}, c, func(ctx context.Context, id int, c *stats.Collector) error {
		return d.Deliver(ctx, id, c)
	})
}

// messageSource picks the corpus: a replayed mbox when one is given, generated
// mail otherwise. Replay is for comparing against a reference run on the same
// corpus; generation is for choosing sizes deliberately.
func messageSource() (corpus.Source, error) {
	if *flagMbox == "" {
		return nil, nil
	}
	m, err := corpus.LoadMbox(*flagMbox)
	if err != nil {
		return nil, err
	}
	slog.Info("loadtest: corpus loaded", "mbox", *flagMbox, "messages", m.Len())
	return m, nil
}

func runIMAP(ctx context.Context, c *stats.Collector, src corpus.Source) error {
	users := expandRecipients(*flagUsers)
	if len(users) == 0 {
		fail("-users is required for the imap driver")
	}
	profile, perr := imapdrv.ParseProfile(*flagProfileStr)
	if perr != nil {
		fail("%v", perr)
	}
	d, err := imapdrv.New(imapdrv.Config{
		Addr:             *flagAddr,
		Users:            users,
		Password:         *flagPassword,
		TLS:              *flagTLS,
		Insecure:         *flagInsecure,
		Timeout:          *flagTimeout,
		Mailboxes:        splitList(*flagMailboxes),
		MailboxesPerUser: *flagMailboxesN,
		CreateMailboxes:  *flagCreate,
		TargetMessages:   *flagMsgs,
		Profile:          profile,
		Corpus: corpus.Spec{
			MinSize:         *flagMinSize,
			MaxSize:         *flagMaxSize,
			AttachmentRatio: *flagAttachRate,
			Seed:            *flagSeed,
		},
	})
	if err != nil {
		fail("%v", err)
	}
	slog.Info("loadtest: starting", "protocol", "imap", "addr", *flagAddr,
		"users", len(users), "mailboxes_per_user", len(d.Mailboxes()),
		"clients", *flagConcurrency, "target_messages", *flagMsgs, "tls", *flagTLS)

	// Mailbox creation happens before the clock starts: a run whose first
	// operations are CREATE measures setup, not steady state.
	if err := d.Prepare(ctx); err != nil {
		return err
	}
	return driver.Run(ctx, driver.Options{
		Addr:        *flagAddr,
		Concurrency: *flagConcurrency,
		Duration:    *flagDuration,
		Iterations:  *flagIterations,
		RampUp:      *flagRampUp,
		StopOnError: *flagStopOnError,
	}, c, d.RunClient)
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// expandRecipients accepts an explicit list or the "u@domain:N" shorthand that
// matches how the sandbox names its accounts (u1..uN@domain), so a 150-user
// run does not need a 150-entry command line.
func expandRecipients(spec string) []string {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		addr, count, ok := splitCount(part)
		if !ok {
			out = append(out, part)
			continue
		}
		local, domain, found := strings.Cut(addr, "@")
		if !found {
			out = append(out, part)
			continue
		}
		for i := 1; i <= count; i++ {
			out = append(out, fmt.Sprintf("%s%d@%s", local, i, domain))
		}
	}
	return out
}

// splitCount parses "u@example.com:150" into ("u@example.com", 150).
func splitCount(s string) (addr string, count int, ok bool) {
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return "", 0, false
	}
	n := 0
	for _, r := range s[idx+1:] {
		if r < '0' || r > '9' {
			return "", 0, false
		}
		n = n*10 + int(r-'0')
	}
	if n == 0 {
		return "", 0, false
	}
	return s[:idx], n, true
}

func levelFromEnv() slog.Level {
	if strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug") {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "yarilo-loadtest: "+format+"\n", args...)
	os.Exit(2)
}
