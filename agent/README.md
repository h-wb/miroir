# miroir-agent

A small program for a personal machine. It reads sources that exist only
there, withholds secrets, and delivers what is new to a Miroir connection's
receiving URL. It needs Python 3.12 or later and three libraries (urllib3,
pydantic and the Kingfisher secret scanner), and imports no server code.
Design: decisions 030, 031, 035 and 036 in
[`docs/miroir/decisions.md`](../docs/miroir/decisions.md).

## Run it

Install it as a command, from this folder of the repository at a pinned commit
or tag:

```bash
uv tool install "git+https://github.com/h-wb/pipelines@<ref>#subdirectory=agent"
miroir-agent --version
```

Add `--force` to move an installed agent to another commit. It carries on from
its saved state.

## Commands

```bash
miroir-agent run     --config agent.json [--collector ID] [--max-batches N]
miroir-agent collect --config agent.json --collector ID --out DIR
miroir-agent resend  --config agent.json --collector ID --from DATE [--to DATE]
                     [--media | --out DIR] [--max-batches N]
miroir-agent status  --config agent.json
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
- `status` shows each collector's position and the last row, media and resend outcomes.

None of them prints source content. A line for a failed pass names the step
(`collect`, `deliver`, `links`, `put`, `confirm`) and a reason, for example
`screen_macbook failed step=deliver reason=network {...}`, and the same fields
are in `status`.

## Configuration

```json
{
  "state_dir": "~/Library/Application Support/miroir-agent",
  "collectors": [
    {
      "id": "screen_macbook",
      "collector": "screenmemory",
      "receive_url": "https://miroir.example/api/connections/screen_macbook/receive",
      "token_env": "MIROIR_TOKEN_SCREEN_MACBOOK",
      "options": {"data_dir": "~/ScreenMemoryData", "since": "2025-01-01"}
    }
  ]
}
```

`receive_url` must be `https`. The token is the connection's receive-only
delivery token, read from the named environment variable; it can deliver to its
own connection and nothing else.

Both collectors accept an optional `since`: use `YYYY-MM-DD` for local
midnight or `YYYY-MM-DDTHH:MM:SS` with `Z` or a UTC offset for an exact instant.
Omit it to start at the beginning. Zen origins and bookmarks are not filtered
by `since` and are not included in a resend.

## Masking

Text is masked before it is written to a batch, by the agent's fixed rules and
by the Kingfisher scanner's catalogue of provider credentials. The scanner is
always on. It starts on the first text that needs it (about four seconds), so
`status` and a pass with nothing new do not pay for it. To add your own
patterns, name rule files at the top level of the configuration:

```json
"masking": { "rules": ["~/.config/miroir-agent/rules.yml"] }
```

The rule format, the order of the rules and what each one catches are in
[docs/sources/screenmemory.md](../docs/sources/screenmemory.md). If the scanner
cannot start, the pass fails with `reason=masking` and nothing is collected.

## Media

A collector can also upload the files its rows point at. For `screenmemory`, set
`"media": true` in its options (and `media_dir` if the screenshots are not in
`<data_dir>/screenshots`). After the rows are delivered, the agent asks Miroir
for upload links, sends each file with an HTTP PUT, and asks Miroir to confirm.
It never uploads the file of a frame it withheld, never runs ahead of delivered
rows, and holds no storage credential. `run --max-media-files N` bounds how many
frames one pass examines (default 2000). Links are requested for 25 files at a
time and used at once, four uploads in parallel, so a link never outlives its
use. The machine must reach the storage address Miroir signs links for, not
only Miroir.

## Collectors

| Collector | Source | What it sends and withholds |
|---|---|---|
| `screenmemory` | ScreenMemory's `text.sqlite` and `usage.sqlite` | [docs/sources/screenmemory.md](../docs/sources/screenmemory.md) |
| `zen_history` | Zen or Firefox `places.sqlite` (read from a copy) | [docs/sources/browser-history.md](../docs/sources/browser-history.md) |

## Layout

| Module | Job |
|---|---|
| `cli.py` | Arguments, printing, exit code |
| `runner.py` | The passes for one collector: run, preview, resend, status |
| `config.py` | The configuration file as typed values |
| `state.py` | The state folder: cursor, spool, media position, status, lock |
| `transport.py` | Every request the agent sends, through one connection pool |
| `media.py` | Upload a page of files in groups |
| `batch.py` | One batch file: header, records, counted trailer |
| `collectors/base.py` | What a collector is: the contract every source implements |
| `collectors/<source>.py` | One source: its options, cursor, source folders and rows |
| `masking.py` | What masks secrets in text: the fixed rules, joined with the scanner's findings |
| `scanner.py` | The Kingfisher scanner: started on first use, closed on failure |
| `dates.py` | Parse local dates and offset-qualified instants |
| `errors.py` | The failure a pass reports: a step and a reason |

Lower modules do not import higher ones, only the registry imports a collector,
and `tests/test_boundaries.py` holds both rules.

### Adding a collector

Write one module in `collectors/`: a model of its options, a model of its
cursor, and a `Collector` subclass that names its batch format, lists the
folders it reads from and writes a batch with `BatchWriter`. Add it to the
registry in `collectors/__init__.py`. Nothing else in the agent changes. The
connector that reads the batch lives on the server, with a contract test
between the two.

## Tests

From the repository root, in the server's environment, which has both
libraries:

```bash
(cd agent && uv run --project .. --locked python -m unittest discover -s tests -t .)
uv run --locked ruff check agent
uv run --locked mypy --strict --config-file agent/pyproject.toml agent/miroir_agent
```

The server's suite runs the agent's tests too, and holds the contract test that
checks a collected batch against the connector that reads it.
