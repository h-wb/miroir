---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
summary: This reference explains how ScreenMemory text, app usage, and screenshots are collected, masked, and represented in the warehouse.
topics:
  - "[[Screen activity]]"
  - "[[Device collector]]"
  - "[[Warehouse]]"
created: 2026-11-20T09:57
---

# ScreenMemory

What was on the Mac's screen: one row per screenshot with its recognised text, and
which app was in front. Collected on the Mac by the Miroir collector (decision 030).

- **Status**: the connection `screen_macbook`, sent hourly by the collector since
  2026-11-20, with screenshots as media
- **Code**: connector [`src/integrations/screenmemory/`](../../src/integrations/screenmemory/),
  source [`collector/miroir_collector/sources/screenmemory.py`](../../collector/miroir_collector/sources/screenmemory.py)
- **Tables**: `frames`, `app_usage`, in the connection's dataset
- **dbt**: the screen lane of the `timeline` analysis (`screen_frames`, `events`,
  `days`, `apps`; decision 040). Thumbnails and `media_objects` come from the
  derive run
- **Dashboard**: none; the timeline interface reads it

## Where the data comes from

[ScreenMemory](https://screenmemory.app) saves a screenshot every few seconds and
reads its text on the Mac. In its data folder:

- `text.sqlite`, table `ocrresults`: `imagetimestamp` (Unix seconds), `imagepath`
  (relative to `screenshots/`), `imagetext`.
- `usage.sqlite`, table `usage`: `timestamp`, `identifier` (the front app's bundle
  id). Its `name` column is unusable.
- `screenshots/`: the image files. Miroir never parses them; `frames.image_path`
  points at them, and the collector can upload them as media (decision 033).

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
  password page (`marker`). Both lists can be extended in the collector's configuration.
- **Spans masked** as `[masked:<rule>]`, counted per frame in `masked`. The
  collector's own rules, in this order:
  - `private_key`: private key blocks;
  - `token`: known key and token shapes (GitHub, GitLab, AWS, Slack, Google, npm,
    Stripe, Tailscale, age, JSON web tokens, bearer values, `sk-` keys);
  - `assignment`: the value after a name such as `password`, `token`, `secret` or
    `api_key`, also inside a longer name like `POSTGRES_PASSWORD`;
  - `random`: long mixed-case strings with digits. Hex-only strings (commit and
    image digests) are left readable;
  - `card`: card numbers that pass their check digit.
- **Spans the scanner adds**, as `[masked:scanner]`: credentials of known
  providers from the Kingfisher scanner's catalogue of about 500 rules, and
  anything matched by your own rule files (decision 036). It runs on the same
  original text, and whatever either it or the rules above find is masked. Where
  both find the same span, it keeps the rule's name, so `scanner` counts what the
  catalogue adds.
- **A text the scanner could not check** (it failed, took over five seconds, or
  the text holds NUL characters) is replaced whole by `[masked:unscanned]` and
  counted as `unscanned`. If the scanner cannot start at all, nothing is
  collected and the pass fails with `reason=masking`.

The rules go by shape. A password typed as an ordinary word, or a short code, is not
caught, so treat these tables as the most sensitive data in the warehouse.

### Your own rules

Name rule files in the collector's configuration, beside `state_dir`:

```json
"masking": { "rules": ["~/.config/miroir-collector/rules.yml"] }
```

A rule file is in the scanner's format. Each rule is a pattern whose first
group is the part to mask, and its `id` must start with `custom.`:

```yaml
rules:
  - name: Example internal token
    id: custom.example-internal
    pattern: |
      (?x)
      \b
      (
        exm_[A-Za-z0-9]{24}
      )
      \b
    min_entropy: 3.0
    confidence: medium
```

A new or changed rule applies to what is collected from then on. To apply it to
text already sent, run `resend` over the dates concerned.

Screenshots cannot be masked. With `media` on, the collector uploads the image of
each frame it sent, and never the image of a frame it withheld. A screenshot can
still show a secret that the text rules masked, so the bucket is private and a
file is only reachable through a logged-in view link.

To see what a batch would contain before anything is sent:

```bash
miroir-collector collect --config collector.json --source <id> --out /tmp/batches
```

## Tables

| Table | Key | Columns |
|---|---|---|
| `frames` | `image_path` | `ts`, `captured_at` (UTC), `image_path`, `text`, `withheld`, `masked` |
| `app_usage` | `ts`, `bundle_id` | `ts`, `observed_at` (UTC), `bundle_id` |

Both merge on their key, so a batch delivered twice changes nothing. A frame joins
its app on the exact timestamp.

## Not covered yet

- No cleaned text for summaries yet. Sessions and time per app are in the
  `timeline` analysis.
- Screenshots are uploaded only when the source's `media` option is true, and
  only for frames after the media cursor. An existing archive is brought in by
  copying the `screenshots/` folder into the bucket under
  `<prefix>/<connection>/screenshots/`; the collector then finds those files present.
- History before the source's `since` is not sent by normal runs. Load it later
  with `miroir-collector resend --from ... --to ...`, with `--media` for the screenshots.
