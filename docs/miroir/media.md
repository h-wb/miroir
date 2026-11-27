---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-26
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
| `derive_batch` | 500 (1–5000) | Files of one kind per derive run, about 2 to 3 minutes of the media deployment |

The key comes from `MIROIR_MEDIA_ACCESS_KEY_ID` and
`MIROIR_MEDIA_SECRET_ACCESS_KEY`; the server refuses to start with the section
and without either. It needs read and write on that bucket and nothing else. An
object is named `prefix / connection / kind / key`, the key being the value the
row carries. Media lives in `src/media/` (the
[package split](../decisions/2026-11-25-package-split.md), steps 4 and 5) and
imports only the kernel: the
`ObjectStore` protocol (`store.py`), which `s3.py` implements and nothing else
reaches the bucket by; names and content types (`names.py`); upload links and
confirmations (`uploads.py`); view links and the bytes copied to a browser
(`links.py`); the `media` section (`config.py`); kinds (`kinds.py`) and profiles
(`profiles.py`); the derive queue (`queue.py`); and what it reads of a
connection (`target.py`), which the control plane builds from its own
configuration and hands in.

| Route | Who | What |
| --- | --- | --- |
| `POST /api/connections/{id}/media/uploads` | The connection's delivery token | For a list of `{key, size}`: `present`, an upload link, or `rejected` |
| `POST /api/connections/{id}/media/confirm` | The connection's delivery token | Whether each object is now `stored` |
| `GET /media/{connection}/{kind}/{key}` | A logged-in administrator | A redirect to a short view link; with `size`, the bytes ([images through the server](timeline.md#images-through-the-server)) |

Media has no routes of its own. The two the collector calls are the control
plane's, which checks the delivery token and calls `uploads.py`. The view route
is the viewer's (`src/viewer/media.py`, step 7): after the login it asks the
views that declare a `Withhold` for the file's kind (`src/analyses/views.py`)
whether a row hides the file, then calls `links.py` with the `media` section,
the connection's kinds and the object store its settings give it.

A device holds no storage credential: it asks for links and uses them at once
([the collector's side](../../collector/README.md#media)). The bucket is the
record of what is stored: the server asks the bucket, not a table. An archive can
therefore be copied into the bucket under the right names with any S3 tool, and
its files are found present.

## Thumbnails and `media_objects`

For a kind that names a profile, `screenshots` and `attachments`, the worker
makes two WebP copies of each stored image, 360 and 1280 pixels wide, and
records every stored file of the kind in the warehouse (decision 040). A kind
that names none is not derived: no thumbnails and no records.

A profile (`src/media/profiles.py`) is data: the decoder of each extension, the
widths, the output format (WebP, the only one the decoder writes) and the
largest thumbnail it can make (16 MB). Each width is a `thumbnail_w<width>`
column of `media_objects`, so a new width is a new column beside the others; a
test holds every profile to the columns there are.

| Kind | Profile | Thumbnails of | Read up to |
| --- | --- | --- | --- |
| `screenshots` | `screenshot` | `.jpeg`, `.jpg`, `.png` | 20 MB, the kind's size |
| `attachments` | `image` | `.heic`, `.heif`, `.jpeg`, `.jpg`, `.png`, `.gif`, `.webp`, `.tiff`, `.tif`, `.bmp` | 50 MB; the kind accepts 5,000 MB |

Every other file of the kind (a HEIC screenshot; an attachment's video, audio,
PDF or other document) gets a row with `unsupported` and is not read. A file over the size read is recorded
`too_large` from the size it was stored with, without being read or decoded
(`MediaKind.derive_max_bytes`).

- **Names.** `prefix / connection / <kind>.w360 / <key>.webp` and
  `… / <kind>.w1280 / <key>.webp`. An image narrower than a size keeps its
  width. A thumbnail's kind is not a kind: it can be neither read nor uploaded by
  name.
- **When.** When the server answers `stored` to a confirmation (or `present` to
  an upload request), it queues the file in the metadata store (`media_pending`,
  whose definition and repository are media's, `src/media/queue.py`) before
  answering and asks for a `derive` operation on the Miroir's media deployment
  ([Prefect](runs.md#prefect)), due `derive_delay_seconds` later. Confirmations
  before it starts join it, so a collector confirming thousands of files in a
  pass causes one run.
  A run handles at most `derive_batch` files of each kind and, if more wait and
  it made progress, schedules the next at once; loads of the connection take
  turns with it by lock. Without Prefect the run starts at once and empties the queue
  itself. An overdue run is joined only while Prefect says it is scheduled,
  pending or running; if Prefect lost it, the next confirmation asks for a new
  one.
- **Decoding.** The web server never decodes an image, and neither does the
  worker's own process. A child process does (`src/media/decoder.py`, run as
  `python -m src.media.decoder` and imported by nothing), one file at a time, with no credentials in its environment, a
  1 GiB memory limit and 30 seconds per file. On Linux the worker makes itself
  undumpable first, so the child cannot read its environment through `/proc`;
  the child still has the network and the mounted volumes. Each fresh child first
  makes thumbnails of a tiny built-in image for every decoder its kinds' profiles use
  (JPEG and PNG for screenshots; HEIF, GIF, WebP, TIFF and BMP too for attachments)
  and of the costliest image the bounds accept: about
  1.4 s and 530 MB, once per run and again after each lost file. If that check
  fails, the run stops as failed ("The image decoder could not start or failed
  its check") and the files stay queued, uncharged. A file is read only by the
  decoder of its key's extension, whatever its bytes claim: JPEG bytes named
  `.png` are `unreadable`. EXIF orientation is applied and only the first frame
  of an animation is used; no EXIF or XMP is written into a thumbnail, so a
  photo's place is not kept. HEIC and HEIF are read by pillow-heif (libheif),
  registered in the child only, on one thread, primary image only: libheif
  applies the file's own rotation (`irot`, `imir`) and the EXIF orientation is
  then ignored, so a photo is turned once.
- **Records.** One row per stored file in `media_objects`, in the connection's
  own dataset, written by a dlt pipeline of its own (`<pipeline_name>_media`,
  `MediaWriter` in `src/media/derive.py`, which the runner builds with the
  connection's own lock, held around each write),
  merged on `(connection_id, kind, key)`. Rows are written every 100 files; a run
  that dies loses at most that much finished work.

| Column | Meaning |
| --- | --- |
| `connection_id`, `kind`, `key` | Which file |
| `size` | Bytes stored |
| `content_type` | From the key's extension, as when it was uploaded |
| `stored_at` | When the server confirmed it; for a backfilled file, the bucket's modification time |
| `thumbnail_w360`, `thumbnail_w1280` | Whether that size exists |
| `thumbnail_status` | `made`, or why there is none: `unsupported` (a type without thumbnails: a HEIC screenshot, video, audio, PDF and other documents), `too_large` (over the size read for the kind, above, 40 million pixels, 16,384 on a side, a side 16 times the other, or the decoder's memory), `unreadable` (truncated, corrupt or not what its name says), `failed` (its decode did not finish three times while the decoder worked) |
| `derived_at`, `derive_version` | When the row was written, and by which version of the thumbnail code |

A file that has a row is not read again unless queued again, whatever its status.
A row says the file was in the bucket when the run reached it, and it stays if
the object is deleted later. No row does not mean not stored: the file may be
queued, its kind may not be derived, it may predate thumbnails and not have been backfilled, or it was
gone when the run reached it. A file replaced by another with the same key and
size is not derived again.

### In the models

A model reads these records only through media's dbt macro `media_ref(role,
kind, key_column)` (`dbt/macros/media.sql`): the files of `kind` that the input
linked as `role` points at through `key_column`. It returns the pieces a model
puts in its own query: `kind`, the kind as a literal for a `media_kind` column;
`records`, the role's connection's records of that kind (the dataset could hold
another connection's), for a CTE named `media`; and `record`, the first record
of a key among them whose load completed. A record's `thumbnail_status` is the
row's `media_status`. The timeline's `screen_frames`, `messages`,
`timeline_screen` and `timeline_messages` use it; a catalog test checks that each
call names a kind its role's connector declares, and that no model joins
`media_objects` otherwise.

A derive is not a load: the connection's status, last sync, history and analyses
ignore it, and a settings edit does not block it. The connection page shows one
line for it: files waiting, and the last derive run with its counts
(`Media: made 480, unsupported 3; 0 waiting`).

Without the bucket key in its environment, a derive run is closed as blocked
("Media storage is not configured where this runs") and everything else runs as
before. Derive runs run on the media deployment alone, so only its Jobs need the
key: the work pool's template gives it to them through `media_job_variables`
([Configuration](configuration.md#execution-links-and-media)), and syncs,
imports and builds do not hold it. A flow run needs about 1 GiB of memory for
a derive run (the check measured 530 MB for the decoder, 711 MB with the
worker); give it 1.5 GiB. With less, every derive run stops as "decoder
unavailable" without charging files. On macOS the decoder's memory limit is not
enforced; the shape bounds still apply.

## Backfill

Files stored before thumbnails existed are found by walking the bucket under
`prefix / connection / kind /`:

```bash
# Where flow runs can be requested (needs the deployment section); prints the operation ID.
uv run --locked miroir media-backfill screen screenshots --limit 10000
# Apple Messages attachments of a connection named `messages`.
uv run --locked miroir media-backfill messages attachments --limit 10000
```

`--limit` (default 10,000, at most 1,000,000) is how many stored files this run
walks; the next run continues after the last, and the run's detail says
`walk continues` or `walk finished`. Running it again is safe: files already
derived are recognised from `media_objects` without being read, and a finished
walk starts again from the beginning. A backfill always asks for a run of its
own, so it also unsticks a queue whose run Prefect lost. Measured on a
development machine: 0.16 to 0.19 s per JPEG screenshot, up to 0.37 s per large
PNG for a whole run. Decoding alone, on generated 12-megapixel photos (4032 ×
3024): 0.08 to 0.11 s per JPEG, 0.22 to 0.25 s per PNG, 0.34 to 0.43 s per
HEIC (tiled in 512-pixel squares as an iPhone writes them, or not), about 1 s
for a 40-megapixel HEIC at a peak of 310 to 390 MB; a video or document costs
no decode.

`--retry` also queues the kind's files recorded as `failed`, and
`--retry failed,unreadable` those recorded as `unreadable` too, to be derived
again whatever their rows say: the repair after a decoder incident without a new
`DERIVE_VERSION`. A plain backfill leaves those rows alone.

For development only, `--duckdb FILE --metadata sqlite:///FILE` runs it in the
CLI's own process; it refuses a `--metadata` that is not a local SQLite file, so
it can never drain the installation's queue or move its walk.
