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
| POP3 | planned |
| JMAP | planned |

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

With `-seed`, one seed produces one corpus: the same set of messages, though
which client sends which is decided by scheduling. A before/after comparison
therefore compares the same mail, not the same order.

A run is bounded by `-duration` or `-iterations`. Setting neither is refused:
an unbounded load run against a shared environment is somebody else's outage.

### IMAP

```sh
yarilo-loadtest -protocol imap \
  -addr yarilo-imap:143 \
  -users 'u@d00001.test:150' -password secret \
  -mailboxes-per-user 10 \
  -concurrency 20 -duration 5m \
  -op-append 3 -op-fetch 3 -op-store 2 -op-search 2
```

The **mailbox set is the point**. A generator that works only in INBOX cannot
produce the condition worth testing against a server that dispatches index work
per user: several mailboxes of one user wanting indexing at once. Either name
them (`-mailboxes=INBOX,Archive,Work`) or generate them
(`-mailboxes-per-user=10` → `Load1 … Load10`), and they are created before the
clock starts so the run measures operations rather than setup.

User and mailbox are chosen **per operation**, like the LMTP recipient.
`-concurrency` controls how many operations are in flight and nothing about
where they land.

The mix is deterministic: with even weights, 16 operations are 4 of each. Two
runs with the same flags issue the same operations, so a comparison differs by
the server rather than by the generator.

### Output

A table by default, `-json` for a job that has to decide whether the run passed:

```
command             count   errors     ops/s    min ms    med ms    p95 ms    p99 ms    max ms
------------------------------------------------------------------------------------------
DATA                 4820        0      80.3      3.11      8.42     24.10     41.88     92.30
RCPT                 4820        0      80.3      0.21      0.44      1.02      2.11      8.40
...
```

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
| `-mailboxes` | — | explicit per-user mailbox names |
| `-mailboxes-per-user` | 0 | generate `Load1 … LoadN` instead of a list |
| `-create-mailboxes` | true | create the set before the run |
| `-op-append` / `-op-fetch` / `-op-store` / `-op-search` | 3/3/2/2 | operation mix weights |
| `-json` | false | machine-readable summary |

Ramp-up is not cosmetic: starting hundreds of connections in the same
millisecond measures the accept queue rather than the server.

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
