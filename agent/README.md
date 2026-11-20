# miroir-agent

A small program for a personal machine. It reads sources that exist only
there, withholds secrets, and delivers what is new to a Miroir connection's
receiving URL. It uses only the Python standard library (3.10 or later) and
imports no server code. Design: decisions 030 and 031 in
[`docs/miroir/decisions.md`](../docs/miroir/decisions.md).

## Run it

From a checkout, with nothing installed:

```bash
cd agent && python3 -m miroir_agent --version
```

Or as an installed command, from this folder of the repository at a pinned
commit or tag:

```bash
uvx --from "git+https://github.com/h-wb/pipelines@<ref>#subdirectory=agent" miroir-agent --version
```

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

None of them prints source content.

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

## Media

A collector can also upload the files its rows point at. For `screenmemory`, set
`"media": true` in its options (and `media_dir` if the screenshots are not in
`<data_dir>/screenshots`). After the rows are delivered, the agent asks Miroir
for upload links, sends each file with an HTTP PUT, and asks Miroir to confirm.
It never uploads the file of a frame it withheld, never runs ahead of delivered
rows, and holds no storage credential. `run --max-media-files N` bounds how many
frames one pass examines (default 2000). Links are requested for 25 files at a
time and used at once, four uploads in parallel, so a link never outlives its
use. A failure names the step (`collect`, `links`, `put`, `confirm`), a reason
and the HTTP status, in the printed line and in `status`. The machine must reach the storage
address Miroir signs links for, not only Miroir.

## Collectors

| Collector | Source | What it sends and withholds |
|---|---|---|
| `screenmemory` | ScreenMemory's `text.sqlite` and `usage.sqlite` | [docs/sources/screenmemory.md](../docs/sources/screenmemory.md) |
| `zen_history` | Zen or Firefox `places.sqlite` (read from a copy) | [docs/sources/browser-history.md](../docs/sources/browser-history.md) |

## Layout

| Module | Job |
|---|---|
| `cli.py` | Commands and exit codes; the pass over collectors |
| `config.py` | Load and validate the configuration |
| `dates.py` | Parse local dates and offset-qualified instants |
| `collectors/` | One module per source: read new rows, write one batch |
| `masking.py` | The fixed rules that mask secrets in text |
| `state.py` | Saved position, the spool, status and the lock |
| `deliver.py` | The HTTP POST |
| `media.py` | Upload links, the PUT and the confirmation |

## Tests

```bash
cd agent && python3 -S -m unittest discover -s tests -t .
```

`-S` disables site packages, which proves nothing beyond the standard library
is needed. The server's suite runs this too, and holds the contract test that
checks a collected batch against the connector that reads it.
