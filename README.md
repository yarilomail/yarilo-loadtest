# yarilo-loadtest

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
| IMAP | planned |
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

A run is bounded by `-duration` or `-iterations`. Setting neither is refused:
an unbounded load run against a shared environment is somebody else's outage.

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
| `-json` | false | machine-readable summary |

Ramp-up is not cosmetic: starting hundreds of connections in the same
millisecond measures the accept queue rather than the server.

## Licence

AGPL-3.0, matching the server.
