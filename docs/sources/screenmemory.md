# ScreenMemory

What was on the Mac's screen: one row per screenshot with its recognised text, and
which app was in front. Collected on the Mac by the Miroir agent (decision 030).

- **Status**: connector and collector built 2026-11-20; not deployed, no connection
  configured yet
- **Code**: connector [`src/integrations/screenmemory/`](../../src/integrations/screenmemory/),
  collector [`agent/miroir_agent/collectors/screenmemory.py`](../../agent/miroir_agent/collectors/screenmemory.py)
- **Tables**: `frames`, `app_usage`, in the connection's dataset
- **dbt**: none yet
- **Dashboard**: none yet

## Where the data comes from

[ScreenMemory](https://screenmemory.app) saves a screenshot every few seconds and
reads its text on the Mac. In its data folder:

- `text.sqlite`, table `ocrresults`: `imagetimestamp` (Unix seconds), `imagepath`
  (relative to `screenshots/`), `imagetext`.
- `usage.sqlite`, table `usage`: `timestamp`, `identifier` (the front app's bundle
  id). Its `name` column is unusable.
- `screenshots/`: the image files. Miroir never parses them; `frames.image_path`
  points at them, and the agent can upload them as media (decision 033).

ScreenMemory documents none of this, so a new version can change it. The collector
checks the tables and columns it needs and fails with a fixed message if they are
gone.

## What the collector sends

A gzip JSON Lines file per batch (`screenmemory_batch`, version 1): a header, `frame`
and `app_usage` records, and a trailer with the record counts and what was withheld.
The connector rejects a file whose trailer is missing or whose counts do not match,
so a truncated file never loads.

Rows are sent once they are older than `settle_seconds` (default 300), newer than
the cursor, at most `max_rows` per table per batch. A long history therefore goes
out as several batches, each its own import.

## What never leaves the Mac

Screen text holds whatever was displayed. The collector applies fixed rules before
it writes the file; no model is involved, and the same input always gives the same
output.

- **Whole frames withheld** (the row is kept with `text` null and a reason in
  `withheld`): the front app is a password manager (`excluded_app`), or the text
  contains a password-manager address such as a web vault or the browser's own
  password page (`marker`). Both lists can be extended in the agent's configuration.
- **Spans masked** as `[masked:<rule>]`, counted per frame in `masked`:
  - `private_key`: private key blocks;
  - `token`: known key and token shapes (GitHub, GitLab, AWS, Slack, Google, npm,
    Stripe, Tailscale, age, JSON web tokens, bearer values, `sk-` keys);
  - `assignment`: the value after a name such as `password`, `token`, `secret` or
    `api_key`, also inside a longer name like `POSTGRES_PASSWORD`;
  - `random`: long mixed-case strings with digits. Hex-only strings (commit and
    image digests) are left readable;
  - `card`: card numbers that pass their check digit.

The rules go by shape. A password typed as an ordinary word, or a short code, is not
caught, so treat these tables as the most sensitive data in the warehouse.

Screenshots cannot be masked. With `media` on, the agent uploads the image of
each frame it sent, and never the image of a frame it withheld. A screenshot can
still show a secret that the text rules masked, so the bucket is private and a
file is only reachable through a logged-in view link.

To see what a batch would contain before anything is sent:

```bash
miroir-agent collect --config agent.json --collector <id> --out /tmp/batches
```

## Tables

| Table | Key | Columns |
|---|---|---|
| `frames` | `image_path` | `ts`, `captured_at` (UTC), `image_path`, `text`, `withheld`, `masked` |
| `app_usage` | `ts`, `bundle_id` | `ts`, `observed_at` (UTC), `bundle_id` |

Both merge on their key, so a batch delivered twice changes nothing. A frame joins
its app on the exact timestamp.

## Not covered yet

- No dbt models: sessions, hours per app and a cleaned text for summaries are the
  next step.
- Screenshots are uploaded only when the collector's `media` option is true, and
  only for frames after the media cursor. An existing archive is brought in by
  copying the `screenshots/` folder into the bucket under
  `<prefix>/<connection>/screenshots/`; the agent then finds those files present.
- History before the collector's `since` is not sent by normal runs. Load it later
  with `miroir-agent resend --from ... --to ...`, with `--media` for the screenshots.
