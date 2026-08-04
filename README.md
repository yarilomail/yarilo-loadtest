# yarilo-loadtest

[![CI](https://github.com/yarilomail/yarilo-loadtest/actions/workflows/ci.yml/badge.svg)](https://github.com/yarilomail/yarilo-loadtest/actions/workflows/ci.yml) [![Trivy](https://github.com/yarilomail/yarilo-loadtest/actions/workflows/trivy.yml/badge.svg)](https://github.com/yarilomail/yarilo-loadtest/actions/workflows/trivy.yml) [![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](https://go.dev/) [![Platform](https://img.shields.io/badge/platform-linux%2Famd64-blue)](https://github.com/yarilomail/yarilo-loadtest) [![License: AGPL v3](https://img.shields.io/badge/license-AGPLv3-blue.svg)](LICENSE) ![Status: alpha](https://img.shields.io/badge/status-alpha-red)

Load generator for [yarilo](https://github.com/yarilomail/yarilo): protocol
drivers with per-command latency statistics.

It is a **client**, deliberately outside the server repository and image. A load
generator has no business shipping inside a mail server, and keeping it separate
means a run can be pointed at any deployment without the server carrying it.

## Why it exists

Measurements taken with a synthetic corpus answer synthetic questions. The
indexing work in yarilo needed to know where an index pass spends its time —
reading a message, or parsing it — and the first numbers came from mail
averaging 393 bytes. At that size parsing dominates fivefold; with real
attachments the ratio inverts. Two decisions were waiting on numbers that no
existing tool could produce: how many index workers to run, and whether to
overlap reads at all.

So the message corpus is configurable by size and attachment ratio, and seeded,
so a before/after comparison compares one thing.

## Status

| Driver | State |
|:---|:---|
| LMTP | implemented — delivery storm, configurable sizes and fan-out |
| IMAP | implemented — APPEND/FETCH/STORE/SEARCH over a configurable mailbox set |
| POP3 | implemented — full sessions, bounded retrieval, optional DELE |
| JMAP | implemented — session discovery, Mailbox/get, chained Email/query+get |

## Usage

```sh
yarilo-loadtest -protocol lmtp \
  -addr yarilo-lmtp:24 \
  -recipients 'u@d00001.test:150' \
  -concurrency 20 \
  -duration 5m \
  -min-size 4096 -max-size 2097152 -attachment-ratio 0.3 \
  -seed 42
```

`-recipients` takes an explicit comma-separated list, or the `user@domain:N`
shorthand which expands to `user1@domain … userN@domain` — matching how the
sandbox names accounts, so a 150-user run does not need a 150-entry argument.

A recipient is chosen **per delivery**, round-robin over the set. `-concurrency`
controls how many deliveries are in flight and nothing about which mailboxes
they reach: a run with 150 recipients touches 150 mailboxes whether it uses 4
clients or 40.

With `-seed`, one seed produces one corpus byte for byte — including the `Date`
header, which is derived from the seed rather than the clock. Which client sends
which message is still decided by scheduling. A before/after comparison
therefore compares the same mail, not the same order.

A run is bounded by `-duration` or `-iterations`. Setting neither is refused:
an unbounded load run against a shared environment is somebody else's outage.

### IMAP

```sh
yarilo-loadtest -protocol imap \
  -addr yarilo-imap:143 \
  -users 'u@d00001.test:150' -password secret \
  -concurrency 500 -msgs 5000 -duration 90s \
  -mailboxes-per-user 10 \
  -profile 'append=30,fetch=30,fetch2=10,store=15,search=5,expunge=5,logout=2'
```

**Clients are persistent sessions.** A client logs in once and issues commands
over the same connection until the run ends or the profile chooses LOGOUT. That
is not tidiness: IMAP spends its resources per session — the index handle, the
cached mailbox view, hibernation — and a generator that reconnects per operation
measures the login path instead of any of them.

**`-msgs` is a steady state, not a total.** Each client keeps its mailbox near
that many messages, appending below it and expunging above. Without it the
mailboxes grow all the way through a run, so the same operation costs more at
the end than at the start and no two measurements are comparable — with each
other or with a later run.

`-msgs 0` turns the steady state off, which is what a read-only run needs: a
`-profile search=100` run that has to append a hundred messages first is
measuring the path it was written to exclude.

**Commands respect protocol state.** FETCH before SELECT is never sent; a
generator that ignores state is guessing at a server rather than testing one.

**The client tracks what it believes the mailbox holds**, and compares it
against the server's `EXISTS` whenever the server reports one. A server that
acknowledges every APPEND and stores nothing answers `OK` to everything — only
the count gives it away. The comparison is a floor, since these mailboxes are
shared: more messages than expected is unremarkable, fewer cannot be explained
by another client.

**The mailbox set is configurable** — named (`-mailboxes=INBOX,Archive,Work`) or
generated (`-mailboxes-per-user=10` → `Load1 … Load10`), created before the
clock starts. A generator confined to INBOX cannot produce the condition worth
testing against a server that dispatches index work per user: several mailboxes
of one user wanting work at once.

**The profile weights commands** the same way the reference tool does, over the
same set: `list`, `status`, `select`, `fetch`, `fetch2`, `store`, `delete`,
`expunge`, `search`, `noop`, `append`, `logout`. `fetch` is metadata, `fetch2`
whole bodies. Weights are relative, so one can be changed without rebalancing
the rest, and selection is deterministic: two runs with one profile issue the
same proportions, so a comparison differs by the server rather than by chance.

### Corpus

Two sources, for two different questions.

**Generated** (default) — sizes and attachment ratio are yours to choose, and a
seed makes the corpus reproducible byte for byte. Use it when the question is
"how does cost scale with message size", which is what the size flags exist for.

**Replayed** — `-mbox=/path/to/test.mbox` delivers the messages in an mbox file,
cycling. Use it when the question is "is this server slower than that one":
comparing two runs means comparing the same work, and the same corpus is the
only way to be sure of that. Line endings are normalised to CRLF and the mbox
`>From ` escaping is undone, so a replayed message is the one that was stored.

**The corpus is checked at load, not at delivery.** SMTP carries at most 1000
octets per line including the CRLF (RFC 5321 §4.5.3.1.6), and a server given a
longer one rejects the **whole transaction** — so one unwrapped line makes every
delivery in the run fail, and the server says only "too long a line in input
stream": no message number, no line number, no length. The loader refuses such a
file and names all three. Real mail wraps; a corpus assembled by a script often
does not.

### POP3

```sh
yarilo-loadtest -protocol pop3 \
  -addr yarilo-pop3-login:110 \
  -users u1@d00001.test:150 -password 'secret' \
  -concurrency 16 -duration 3m -retr 10
```

**A session per iteration, not a persistent connection.** That is a protocol
difference rather than a shortcut: a POP3 server locks the maildrop for the
length of a session (RFC 1939 §3), so a client that stays connected keeps every
other client for that user locked out. A generator that held its sessions open
would measure its own lock contention and report it as the server's.

Each session is the whole sequence — connect, `USER`, `PASS`, `STAT`, `LIST`,
optionally `UIDL`, then `RETR` up to `-retr` messages, newest first, and `QUIT`.
Every command is timed on its own, because `RETR` and `QUIT` in one bucket give
a median that describes neither.

**`-retr` is bounded**, for the same reason the IMAP driver bounds its fetch
window: an unbounded `RETR` loop walks the whole maildrop, so its cost grows
with the mailbox and the same operation is cheap at the start of a run and
expensive at the end. `-retr 0` surveys without retrieving.

**`-delete` is off by default and consumes the corpus when on.** It issues
`DELE` for what it retrieved, so the maildrop is expunged at `QUIT` — which is
the point if you are measuring that path, and destroys the mailboxes every
other run measures against if you are not. The run logs a warning when it is
enabled.

### JMAP

```sh
yarilo-loadtest -protocol jmap \
  -addr https://mail.example.com \
  -users u1@d00001.test:150 -password 'secret' \
  -concurrency 16 -duration 3m -window 20 -bodies
```

`-addr` is a base URL here, not `host:port`: JMAP is HTTP, and **the API
endpoint is read out of the session resource** rather than assembled. A driver
that hard-codes `/jmap/api/` is testing its own guess and would keep passing
against a server that moved it.

**The session is fetched once per client.** It is what a real client discovers
at startup and never repeats; re-fetching it per operation measures discovery.
The HTTP connection is kept alive for the same reason — it is the only thing a
JMAP client holds open, since credentials ride on every request instead.

**`Email/query` and `Email/get` travel as one request**, joined by a
back-reference (RFC 8620 §3.7): `#ids` tells the server to feed the query's
result into the get without the client seeing it. Saving that round trip is what
JMAP is for, so issuing them separately would measure a client nobody writes and
report the server as slower than it is.

**A method failure under HTTP 200 is an error.** JMAP answers 200 for a request
whose methods all failed, so a driver that stops at the status code reports a
server refusing everything as a healthy one.

`-window` bounds the query, `-bodies` asks for body values rather than metadata
alone — on the server, the difference between an index lookup and going to the
message store — and `-threads` adds a `Thread/get` for what the query returned.

### Live output

A line per interval on stderr while the run is going, with a column per command
and one for errors:

```
  time   APPEND    FETCH    STORE   SEARCH  errors
    1s       28      31       14        6       0
    2s       27      33       15        5       0
    3s        4       6        2        1      12
```

Each line reports **that interval**, not the run so far. A summary tells you a
run was bad; this tells you when it went bad, which is usually the question — a
rate that collapses at forty seconds and a rate that was never good average to
the same number.

It goes to stderr, so `-json` on stdout stays machine-readable. `-interval 0`
turns it off.

### Output

A table by default, `-json` for a job that has to decide whether the run passed:

```
command             count   errors    cancel     ops/s    min ms    med ms    p95 ms    p99 ms    max ms
--------------------------------------------------------------------------------------------------
BODY                 2410        0         8      40.2    102.11    155.42    294.10    411.88    902.30
DATA                 2410        0         0      40.2      0.09      0.11      0.32      0.71      2.40
RCPT                 2410        0         0      40.2      0.21      0.44      1.02      2.11      8.40
...

ran for 120.0s, 0 errors, 8 cancelled at the deadline
```

`errors` means the server did something wrong. `cancel` counts the operations
the run cut off when `-duration` expired — one per client that was mid-command
at the time, so a clean run has roughly `-concurrency` of them and **zero**
errors. Counting those as failures is what an earlier version did, and it made
every clean run report failures that scaled with the load; `-stop-on-error`
could not be used for the job it exists for, because the noise it fired on was
its own.

`DATA` is the command and its `354` reply; `BODY` is the transfer and the
per-recipient replies that follow it. They are separate rows because they are
separate measurements — microseconds against hundreds of milliseconds — and one
row holding both is bimodal by construction, with a median that describes
neither and a failure that cannot be attributed to either.

Percentiles rather than a mean alone: a mean hides the tail, and the tail is
what an operator notices.

**Exit status is the gate.** Non-zero when any protocol error occurred, so a
Kubernetes Job fails without anything parsing the output.

## Flags

| Flag | Default | Meaning |
|:---|:---|:---|
| `-protocol` | — | driver to run (`lmtp`) |
| `-addr` | — | `host:port` of the target listener |
| `-concurrency` | 10 | concurrent clients |
| `-duration` | — | run for this long |
| `-iterations` | — | stop after this many operations |
| `-ramp-up` | 2s | spread client starts over this window |
| `-stop-on-error` | false | end at the first protocol error |
| `-timeout` | 30s | per-command timeout |
| `-recipients` | — | LMTP recipients, list or `user@domain:N` |
| `-sender` | `loadtest@yarilo.invalid` | envelope sender |
| `-recipients-per-message` | 1 | `RCPT TO` lines per delivery |
| `-min-size` / `-max-size` | 2048 / 8192 | message size bounds, bytes |
| `-attachment-ratio` | 0 | fraction carrying a base64 attachment |
| `-seed` | time-based | corpus seed; one seed generates one corpus |
| `-users` | — | IMAP users, list or `user@domain:N` |
| `-password` | — | password for every user |
| `-tls` / `-insecure` | false | implicit TLS; skip certificate verification |
| `-window` | 20 | JMAP: messages one `Email/query` returns |
| `-bodies` | false | JMAP: fetch body values, not metadata alone |
| `-threads` | false | JMAP: also `Thread/get` the returned threads |
| `-retr` | 10 | POP3: messages one session retrieves; `0` surveys without retrieving |
| `-delete` | false | POP3: `DELE` what was retrieved — consumes the corpus |
| `-uidl` | false | POP3: ask for the unique-id listing |
| `-msgs` | 100 | messages each client keeps its mailbox near; `0` disables the steady state for read-only runs |
| `-profile` | — | command weights, e.g. `append=30,fetch=20` |
| `-mailboxes` | — | explicit per-user mailbox names |
| `-mailboxes-per-user` | 0 | generate `Load1 … LoadN` instead of a list |
| `-create-mailboxes` | true | create the set before the run |
| `-mbox` | — | replay messages from an mbox file instead of generating them |
| `-interval` | 1s | live table interval; 0 disables |
| `-json` | false | machine-readable summary |

Ramp-up is not cosmetic: starting hundreds of connections in the same
millisecond measures the accept queue rather than the server.

## Tests

Every test here was written for a defect that had already happened, and five of
them have since turned out to check something adjacent to the property their
name promised. So:

**Mutate the thing the name promises, not the thing the code does.** A test
called `TestDeleteIsOffByDefault` must fail when the default is flipped — it is
not enough that it fails when the driver ignores its config. Those are different
claims, and the first is the one that keeps a load run from emptying a sandbox.

Concretely, before a test is finished: reintroduce the defect and watch it fail.
If it passes, the test is describing something else, however true that something
is. The ones that caught real damage this way are marked in their comments.

Flag defaults that are safety rather than preference — `-delete`, `-retr`,
`-duration`, `-concurrency` — are guarded in `main_test.go`, because a
driver-level test cannot see them.

## Releases

`VERSION` is the release trigger, the part `helm/Chart.yaml` `appVersion` plays
in the server repo. Bump it in the same PR as the change it ships, and merging
produces a git tag, a GitHub release with generated notes, and a semver image
tag. Leave it alone and main is built and pushed as `latest` plus the commit
sha, with no release.

Images are published to `ghcr.io/yarilomail/yarilo-loadtest`. Docker Hub is
deliberately not a target yet: nothing outside the cluster consumes this image,
and a second registry is a second thing to keep in sync for no reader.

## Licence

AGPL-3.0, matching the server.
