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
miroir-agent status  --config agent.json
```

- `run` is one pass, then it exits: deliver a pending batch if there is one,
  otherwise collect, spool and deliver. The saved position moves only after
  Miroir answers 200 or 201. Have the operating system's scheduler start it
  about hourly.
- `collect` writes the batches to a folder and sends nothing, to inspect what
  would leave the machine.
- `status` shows each collector's position and last outcome.

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
      "options": {"data_dir": "~/ScreenMemoryData", "cutoff_ts": 0}
    }
  ]
}
```

`receive_url` must be `https`. The token is the connection's receive-only
delivery token, read from the named environment variable; it can deliver to its
own connection and nothing else.

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
| `collectors/` | One module per source: read new rows, write one batch |
| `masking.py` | The fixed rules that mask secrets in text |
| `state.py` | Saved position, the spool, status and the lock |
| `deliver.py` | The HTTP POST |

## Tests

```bash
cd agent && python3 -S -m unittest discover -s tests -t .
```

`-S` disables site packages, which proves nothing beyond the standard library
is needed. The server's suite runs this too, and holds the contract test that
checks a collected batch against the connector that reads it.
