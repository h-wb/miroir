---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-26
summary: How Health Auto Export files reach their connection, which tables they load, and how historical and ongoing exports are kept apart.
topics:
  - "[[Health data]]"
  - "[[Warehouse]]"
created: 2026-09-24T22:01
---

# Apple Health

Health metrics (steps, heart rate, energy, sleep, body), workouts, state of mind
and medications from the phone and the watch, and every other collection the
export holds (symptoms, ECG readings and so on) as raw JSON.

- **Integration**: `apple_health`, fed on each export through a connection's
  receiving URL. The connections configured today are listed in the interface and by
  `pipelines-miroir connections`.
- **Code**: [`src/integrations/apple_health/`](../../src/integrations/apple_health/)
- **Raw tables**: `metrics`, `workouts`, `state_of_mind`, `medications`,
  `other_records`, `other_collections`
- **Packages**: `health` (`daily_summary`, `workout_summary`), `crossovers`

## Where the data comes from

The phone app Health Auto Export posts each JSON export to the connection's
receiving URL, with the connection's delivery token
([how to set it up](../miroir/runs.md#the-receiving-url)). Miroir stores the file
as an artifact and imports it (decisions 017, 021). A file can also be uploaded
in the interface or left in a watched folder. Only Health Auto Export's JSON is
read; Apple's own XML export is not.

## Extraction

Each file is imported once by content. Its `data` object splits into:

| Table | Key |
| --- | --- |
| `metrics` (one row per data point; `metric_name`, `units`) | `(metric_name, date, source)`; a missing `source` becomes `"unknown"` |
| `workouts` | `id` |
| `state_of_mind` (from `stateOfMind`) | `id` |
| `medications` | `(displayText, start)` |
| `other_records` (each object of any other collection; `collection`, `item` as JSON, `file_sha256`, `role`) | `(collection, record_key)`; `record_key` is the SHA-256 of the object's canonical JSON |
| `other_collections` (per file and role: `entries`, `loaded`, `unreadable` in each other collection) | `(file_sha256, role, collection)` |

Nothing in `data` is dropped (decision 008 amendment). A collection without a
table of its own is loaded as it is, one row per object, with no schema invented
for it; an entry that is not an object is counted as `unreadable` in
`other_collections`, not loaded. The same object in two exports, or a replay,
merges on its key. An object that changes between exports gets a new row, since
its key is the object's hash, and the old row stays, as in any raw table: models
that read `other_records` dedupe. A file is taken as an export only if `data`
holds at least one collection Health Auto Export documents: the four above,
`symptoms`, `ecg`, `heartRateNotifications` or `cycleTracking`. An export with
only one of those is accepted, and unknown collections beside it still load.

Each import has a `role`. `historical` files (a one-off bulk export) contribute
only rows dated before the connection's `cutoff` setting; `ongoing` files, the
default and what the receiving URL always uses, only rows from the cutoff on. The
bulk export and the automatic ones therefore never overlap. In `other_records`
an object is dated by its `start` or `date`; an object with neither is loaded by
both roles and merges on its key.

## Re-importing stored exports

Exports are kept as artifacts, so a parser change reaches old data by importing
them again. The parser version is 2 since `other_records`, and an export
imported under version 1 is loaded again by a plain import, without replay:

- **Watched folder**: the next sync re-imports the files still in the folder,
  one load per file.
- **Receiving URL**: an export that arrived only this way is re-imported one by
  one, with **Import** on the connection's page, or
  `POST /api/connections/{id}/artifacts/{artifact}/import`
  ([API](../miroir/api.md)). There is no bulk path.
- **Command line**: the `--duckdb` command loads into a local DuckDB file only,
  never the warehouse:

  ```bash
  uv run --locked pipelines-miroir artifacts health
  uv run --locked pipelines-miroir import health <artifact-id> --duckdb /tmp/mirror.duckdb [--replay]
  ```

The server, the worker and the flow-run image must run the same version. An
import records the parser version it was requested with, and one run by an
image with another version is refused ("Parser version changed"), so an export
that arrives while they differ stays unloaded until it is imported again by
hand.

A replay merges on the keys above: it adds no duplicate and removes nothing an
older parser loaded. An export whose stored copy retention removed is refused
until the same bytes arrive again.

## Settings and credentials

`cutoff`, a date. No source credential; the delivery token is managed on the
connection's **Settings → Data delivery**.

## Quirks

- Exports between 2026-06-07 and 2026-09-22 never arrived: the n8n path used
  before Miroir silently blocked its writes (decision 009). That gap stays.
- File names only need to be unique: the merge keys dedupe overlapping exports.
- Energy and distance units are assumed, not read from the export.
