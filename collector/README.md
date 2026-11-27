---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How to install, configure, run and test miroir-collector, what it reports after each run, and how a source is added.
topics:
  - "[[Device collector]]"
  - "[[Warehouse]]"
created: 2026-11-20T14:36
---

# miroir-collector

A small program for a personal machine. It reads sources that exist only
there, withholds secrets, and delivers what is new to a Miroir connection's
receiving URL. It needs Python 3.12 or later and three libraries (urllib3,
pydantic and the Kingfisher secret scanner), and imports no server code.
Why it is built this way: decisions 030 to 039 in
[`docs/miroir/decisions.md`](../docs/miroir/decisions.md). How it fits with the
other systems: the handbook's
[Device collector](https://github.com/h-wb/handbook/blob/main/Systems/Device%20collector.md).
What Miroir does with what it sends: [Connections and runs](../docs/miroir/runs.md).

## Run it

Install a release. Releases are tags named `collector-vX.Y.Z` on this
repository ([how they are made](../docs/releases.md)).

With [mise](https://mise.jdx.dev), as one pinned tool:

```toml
[tools]
"pipx:git+https://github.com/h-wb/miroir#subdirectory=collector" = "collector-vX.Y.Z"
```

Or with uv alone:

```bash
uv tool install "git+https://github.com/h-wb/miroir@collector-vX.Y.Z#subdirectory=collector"
miroir-collector --version
```

Moving to another release is changing that tag (`--force` with uv). The collector
carries on from its saved state.

## Commands

```bash
miroir-collector run     --config collector.json [--source ID] [--max-batches N] [--max-media-files N]
miroir-collector collect --config collector.json --source ID --out DIR
miroir-collector resend  --config collector.json --source ID --from DATE [--to DATE]
                     [--media | --out DIR] [--max-batches N]
miroir-collector status  --config collector.json
```

- `run` is one pass, then it exits: deliver a pending batch if there is one,
  otherwise collect, spool and deliver. The saved position moves only after
  Miroir answers 200 or 201. Have the operating system's scheduler start it
  about hourly.
- `collect` writes the batches to a folder and sends nothing, to inspect what
  would leave the machine.
- `resend` collects a date range again, using the current masking rules and
  leaving the saved positions and spool unchanged. `--from` is inclusive;
  `--to` includes the whole local day for a bare date, excludes an exact
  instant, and defaults to now. `--out` writes batches instead of posting
  them and needs no token. `--media` also sends ScreenMemory files in the
  range, up to the saved frame cursor.
- `status` shows each source's position and the last row, media and resend outcomes.

None of them prints source content. A line for a failed pass names the step
(`collect`, `deliver`, `links`, `put`, `confirm`) and a reason, for example
`screen_macbook failed step=deliver reason=network {...}`, and the same fields
are in `status`.

## Reports

After each `run` of a source, once rows and media are done (delivered, nothing
new, or failed), the collector tells Miroir how the run went. The report goes
to the connection's own address, with `/receive` replaced by `/report`, and the
same delivery token. It holds what `status` already shows: each pass's outcome,
counts, step, reason and HTTP status, plus the run's start and end times, the
collector's version, the source's name, whether a batch is waiting in the
spool, and how many earlier runs could not be reported. Nothing from the
source, no token and no address. Miroir takes at most 40 entries in a map of
counts (such as Apple Messages' `masked` or `blanked`), so a larger map is
reported as its 39 largest entries and `other`, their rest summed; `status`
and the log line keep it whole.

Reporting never affects a run: a report that cannot be sent leaves the
outcome, the exit code, the saved positions and the spool exactly as they
were, and is not retried. `collect`, `status`, `resend` and a run that found
the source already running send nothing, nor does a source without a token or
whose `receive_url` does not end in `/receive`. Miroir's answer is ignored.

`status` shows the last report's fate as `"report"`: `sent`, `failed` (counted
in `report.json` and carried by the next report), `unsupported` (a Miroir
without reports, answering 401, 404 or 405; not counted, as a bad token shows
in the row delivery anyway) or `skipped`.

## Configuration

When upgrading an existing installation, rename the configuration keys to
`sources` and `source` as shown below, but keep the existing `state_dir` value
and entry IDs to resume from the same state.

```json
{
  "state_dir": "~/Library/Application Support/miroir-collector",
  "sources": [
    {
      "id": "screen_macbook",
      "source": "screenmemory",
      "receive_url": "https://miroir.example/api/connections/screen_macbook/receive",
      "token_env": "MIROIR_TOKEN_SCREEN_MACBOOK",
      "options": {"data_dir": "~/ScreenMemoryData", "since": "2025-01-01"}
    }
  ]
}
```

`receive_url` must be `https`. The token is the connection's receive-only
delivery token, read from the named environment variable; it can deliver to its
own connection and nothing else. Give each machine and source a connection of its own, and
its token from the connection's **Settings → Data delivery**. Each delivery
requests an import, so start a long history by hand with a larger
`--max-batches`.

Every source accepts an optional `since`, where it starts when it has no saved
position: use `YYYY-MM-DD` for local
midnight or `YYYY-MM-DDTHH:MM:SS` with `Z` or a UTC offset for an exact instant.
Omit it to start at the beginning. Starting recent and resending older months
later gets a connection live quickly. Zen origins and bookmarks are not filtered
by `since` and are not included in a resend. Each source's other options are on
its page, linked under [Sources](#sources).

## Masking

Text is masked before it is written to a batch, by the collector's fixed rules and
by the Kingfisher scanner's catalogue of provider credentials. The scanner is
always on. It starts on the first text that needs it (about four seconds), so
`status` and a pass with nothing new do not pay for it. To add your own
patterns, name rule files at the top level of the configuration:

```json
"masking": { "rules": ["~/.config/miroir-collector/rules.yml"] }
```

The rule format, the order of the rules and what each one catches are in
[docs/sources/screenmemory.md](../docs/sources/screenmemory.md). If the scanner
cannot start, the pass fails with `reason=masking` and nothing is collected.

## Media

Set `"media": true` in a source's options to upload its files. Media requires a
URL ending in `/receive`. The collector asks Miroir for upload links, sends each
file with an HTTP PUT, and asks Miroir to confirm. It holds no storage credential.

- `screenmemory`: uploads screenshots from `<data_dir>/screenshots` (or
  `media_dir`). It never uploads a withheld frame or runs ahead of delivered rows.
- `apple_messages`: uploads only files already on disk inside the resolved
  Messages attachments folder, after Miroir has acknowledged their keys and
  links to in-scope messages. `resend --media` filters these files by inode
  change time, not by message date. See the
  [source options](../docs/sources/apple-messages.md).

`run --max-media-files N` sets the pass limit (default 2000); files sharing the
same position stay together. Links are requested for 25 files at a
time and used at once, four uploads in parallel, so a link never outlives its
use. The machine must reach the storage address Miroir signs links for, not
only Miroir.

## Sources

| Source | Local data | What it sends and withholds |
|---|---|---|
| `screenmemory` | ScreenMemory's `text.sqlite` and `usage.sqlite` | [docs/sources/screenmemory.md](../docs/sources/screenmemory.md) |
| `apple_messages` | Apple Messages' `chat.db` | [docs/sources/apple-messages.md](../docs/sources/apple-messages.md) |
| `zen_history` | Zen or Firefox `places.sqlite` (read from a copy) | [docs/sources/browser-history.md](../docs/sources/browser-history.md) |

## Layout

| Module | Job |
|---|---|
| `cli.py` | Arguments, printing, exit code |
| `runner.py` | The passes for one source: run, preview, resend, status |
| `config.py` | The configuration file as typed values |
| `state.py` | The state folder: cursor, spool, media position, status, unreported runs, lock |
| `transport.py` | Every request the collector sends, through one connection pool, reports included |
| `media.py` | Upload a page of files in groups |
| `batch.py` | One batch file: header, records, counted trailer |
| `sources/base.py` | The contract every source implements |
| `sources/<source>.py` | One source: its options, cursor, source folders and rows |
| `masking.py` | What masks secrets in text: the fixed rules, joined with the scanner's findings |
| `scanner.py` | The Kingfisher scanner: started on first use, closed on failure |
| `dates.py` | Parse local dates and offset-qualified instants |
| `errors.py` | The failure a pass reports: a step and a reason |

Lower modules do not import higher ones, only the registry imports a source,
and `tests/test_boundaries.py` holds both rules.

### Adding a source

Write one module in `sources/`: a model of its options, a model of its
cursor, and a `Source` subclass that names its batch format, lists the
folders it reads from and writes a batch with `BatchWriter`. Add it to the
registry in `sources/__init__.py`. Nothing else in the collector changes. The
connector that reads the batch lives on the server, with a contract test
between the two.

## Tests

From the repository root, in the server's environment, which has both
libraries:

```bash
(cd collector && uv run --project .. --locked python -m unittest discover -s tests -t .)
uv run --locked ruff check collector
uv run --locked mypy --strict --config-file collector/pyproject.toml collector/miroir_collector
```

The server's suite runs the collector's tests too, and holds the contract test that
checks a collected batch against the connector that reads it.
