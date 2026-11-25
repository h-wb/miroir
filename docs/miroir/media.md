---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How Miroir keeps the files rows point at, signs links to them, makes thumbnails and records them in media_objects, and how earlier files are backfilled.
topics:
  - "[[Warehouse]]"
  - "[[Device collector]]"
created: 2026-11-24T23:30
---

# Media

Some rows point at a file: a screen frame at its screenshot, a message at its
attachment. Miroir keeps those files in an S3-compatible bucket and never parses
them, so they are not artifacts (decision 033). A connector declares its media
kinds: a name, the table and column holding the key, the allowed extensions.
Today these are `screenmemory` `screenshots` (`frames.image_path`) and
`apple_messages` `attachments` (`attachments.media_key`).

## Storage and links

Media is off until the configuration has a `media` section:

```json
"media": {
  "endpoint": "http://garage.example:3900",
  "link_endpoint": "https://s3.example",
  "region": "garage",
  "bucket": "miroir-media",
  "prefix": ""
}
```

| Key | Default | Meaning |
| --- | --- | --- |
| `endpoint` | | What the server and workers call |
| `link_endpoint` | `endpoint` | The address written into signed links; devices and browsers must reach it |
| `region` | `garage` | The bucket's region |
| `bucket`, `prefix` | `prefix` empty | Where objects go |
| `upload_ttl_seconds` | 900 (60–3600) | How long an upload link lasts |
| `view_ttl_seconds` | 300 (30–3600) | How long a view link lasts |
| `derive_delay_seconds` | 300 (30–3600) | How long a derive run waits for more confirmations |
| `derive_batch` | 500 (1–5000) | Files of one kind per derive run, about 2 to 3 minutes of the connection's deployment |

The key comes from `MIROIR_MEDIA_ACCESS_KEY_ID` and
`MIROIR_MEDIA_SECRET_ACCESS_KEY`; the server refuses to start with the section
and without either. It needs read and write on that bucket and nothing else. An
object is named `prefix / connection / kind / key`, the key being the value the
row carries. Core signs links through the `ObjectStore` protocol
(`src/core/media.py`); `src/adapters/s3.py` implements it.

| Route | Who | What |
| --- | --- | --- |
| `POST /api/connections/{id}/media/uploads` | The connection's delivery token | For a list of `{key, size}`: `present`, an upload link, or `rejected` |
| `POST /api/connections/{id}/media/confirm` | The connection's delivery token | Whether each object is now `stored` |
| `GET /media/{connection}/{kind}/{key}` | A logged-in administrator | A redirect to a short view link; with `size`, the bytes ([images through the server](timeline.md#images-through-the-server)) |

A device holds no storage credential: it asks for links and uses them at once
([the collector's side](../../collector/README.md#media)). The bucket is the
record of what is stored: the server asks the bucket, not a table. An archive can
therefore be copied into the bucket under the right names with any S3 tool, and
its files are found present.

## Thumbnails and `media_objects`

For a kind that names extensions to make thumbnails of, so far only
`screenshots`, the worker makes two WebP copies of each stored image, 360 and
1280 pixels wide, and records every stored file of the kind in the warehouse
(decision 040). A kind that names none, such as `attachments` today (HOMEL-6), is not
derived: no thumbnails and no records.

- **Names.** `prefix / connection / screenshots.w360 / <key>.webp` and
  `… / screenshots.w1280 / <key>.webp`. An image narrower than a size keeps its
  width. A thumbnail's kind is not a kind: it can be neither read nor uploaded by
  name.
- **When.** When the server answers `stored` to a confirmation (or `present` to
  an upload request), it queues the file in the metadata store (`media_pending`)
  before answering and asks for a `derive` operation on the connection's own
  deployment, due `derive_delay_seconds` later. Confirmations before it starts
  join it, so a collector confirming thousands of files in a pass causes one run.
  A run handles at most `derive_batch` files of each kind and, if more wait and
  it made progress, schedules the next at once; loads of the connection take
  turns with it. Without Prefect the run starts at once and empties the queue
  itself. An overdue run is joined only while Prefect says it is scheduled,
  pending or running; if Prefect lost it, the next confirmation asks for a new
  one.
- **Decoding.** The web server never decodes an image, and neither does the
  worker's own process. A child process does (`src/core/thumbnails.py`, imported
  by nothing), one file at a time, with no credentials in its environment, a
  1 GiB memory limit and 30 seconds per file. On Linux the worker makes itself
  undumpable first, so the child cannot read its environment through `/proc`;
  the child still has the network and the mounted volumes. Each fresh child first
  makes thumbnails of a tiny built-in image for every decoder its kinds use (JPEG
  and PNG for screenshots) and of the costliest image the bounds accept: about
  1.4 s and 530 MB, once per run and again after each lost file. If that check
  fails, the run stops as failed ("The image decoder could not start or failed
  its check") and the files stay queued, uncharged.
- **Records.** One row per stored file in `media_objects`, in the connection's
  own dataset, written by a dlt pipeline of its own (`<pipeline_name>_media`),
  merged on `(connection_id, kind, key)`. Rows are written every 100 files; a run
  that dies loses at most that much finished work.

| Column | Meaning |
| --- | --- |
| `connection_id`, `kind`, `key` | Which file |
| `size` | Bytes stored |
| `content_type` | From the key's extension, as when it was uploaded |
| `stored_at` | When the server confirmed it; for a backfilled file, the bucket's modification time |
| `thumbnail_w360`, `thumbnail_w1280` | Whether that size exists |
| `thumbnail_status` | `made`, or why there is none: `unsupported` (a type without thumbnails: video, HEIC, PDF), `too_large` (over the kind's size, 40 million pixels, 16,384 on a side, a side 16 times the other, or the decoder's memory), `unreadable` (truncated, corrupt or not what its name says), `failed` (its decode did not finish three times while the decoder worked) |
| `derived_at`, `derive_version` | When the row was written, and by which version of the thumbnail code |

A file that has a row is not read again unless queued again, whatever its status.
A row says the file was in the bucket when the run reached it, and it stays if
the object is deleted later. No row does not mean not stored: the file may be
queued, its kind may not be derived, it may predate thumbnails and not have been backfilled, or it was
gone when the run reached it. A file replaced by another with the same key and
size is not derived again.

A derive is not a load: the connection's status, last sync, history and analyses
ignore it, and a settings edit does not block it. The connection page shows one
line for it: files waiting, and the last derive run with its counts
(`Media: made 480, unsupported 3; 0 waiting`).

Without the bucket key in its environment, a derive run is closed as blocked
("Media storage is not configured where this runs") and everything else runs as
before. A key given to every flow run reaches every connector's runs, which can
then read and write the whole bucket. A flow run needs about 1 GiB of memory for
a derive run (the check measured 530 MB for the decoder, 711 MB with the
worker); give it 1.5 GiB. With less, every derive run stops as "decoder
unavailable" without charging files. On macOS the decoder's memory limit is not
enforced; the shape bounds still apply.

## Backfill

Files stored before thumbnails existed are found by walking the bucket under
`prefix / connection / kind /`:

```bash
# Where flow runs can be requested (needs the deployment section); prints the operation ID.
uv run --locked pipelines-miroir media-backfill screen screenshots --limit 10000
```

`--limit` (default 10,000, at most 1,000,000) is how many stored files this run
walks; the next run continues after the last, and the run's detail says
`walk continues` or `walk finished`. Running it again is safe: files already
derived are recognised from `media_objects` without being read, and a finished
walk starts again from the beginning. A backfill always asks for a run of its
own, so it also unsticks a queue whose run Prefect lost. Measured on a
development machine: 0.16 to 0.19 s per JPEG screenshot, up to 0.37 s per large
PNG.

`--retry` also queues the kind's files recorded as `failed`, and
`--retry failed,unreadable` those recorded as `unreadable` too, to be derived
again whatever their rows say: the repair after a decoder incident without a new
`DERIVE_VERSION`. A plain backfill leaves those rows alone.

For development only, `--duckdb FILE --metadata sqlite:///FILE` runs it in the
CLI's own process; it refuses a `--metadata` that is not a local SQLite file, so
it can never drain the installation's queue or move its walk.
