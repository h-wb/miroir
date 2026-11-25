---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How Health Auto Export files reach their connection, which tables they load, and how historical and ongoing exports are kept apart.
topics:
  - "[[Health data]]"
  - "[[Warehouse]]"
created: 2026-09-24T22:01
---

# Apple Health

Health metrics (steps, heart rate, energy, sleep, body), workouts, state of mind
and medications from the phone and the watch.

- **Integration**: `apple_health`, fed on each export through a connection's
  receiving URL. The connections configured today are listed in the interface and by
  `pipelines-miroir connections`.
- **Code**: [`src/integrations/apple_health/`](../../src/integrations/apple_health/)
- **Raw tables**: `metrics`, `workouts`, `state_of_mind`, `medications`
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

Each import has a `role`. `historical` files (a one-off bulk export) contribute
only rows dated before the connection's `cutoff` setting; `ongoing` files, the
default and what the receiving URL always uses, only rows from the cutoff on. The
bulk export and the automatic ones therefore never overlap.

## Settings and credentials

`cutoff`, a date. No source credential; the delivery token is managed on the
connection's **Settings → Data delivery**.

## Quirks

- Exports between 2026-06-07 and 2026-09-22 never arrived: the n8n path used
  before Miroir silently blocked its writes (decision 009). That gap stays.
- File names only need to be unique: the merge keys dedupe overlapping exports.
- Energy and distance units are assumed, not read from the export.
