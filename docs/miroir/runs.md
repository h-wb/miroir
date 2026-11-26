---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How data reaches a connection, how one load runs and is recorded, how Prefect runs it, how analyses are built after it, and what the metadata schema holds.
topics:
  - "[[Warehouse]]"
  - "[[Job runner]]"
created: 2026-11-24T23:30
---

# Connections and runs

## How data arrives

A connector declares its inputs (decision 021): a fetch, file formats, or both.

- A fetch pulls from the source's API with the connection's credentials, on the
  connection's schedule or on request.
- A file reaches the connection through one of three transports, all in core:
  an upload in the interface, the connection's receiving URL, or a watched
  folder. Each stores the file as an artifact, then imports it.

An artifact is named by its SHA-256, read-only, and scoped to its connection:
identical bytes on two connections are two artifacts. Sending the same bytes
again returns the existing artifact; sending them with different import options
is refused. An invalid file is refused before anything is stored (decision 008).

### The receiving URL

`POST /api/connections/{id}/receive` takes the raw file as the body, with the
connection's delivery token as a bearer token, stores it and requests an import
(decisions 017, 032). `?format=<id>` picks a format when the connector has
several; otherwise the first is used. It always imports with the format's
default options (for Health, `role=ongoing`). The answer holds `artifact_id`,
`new` and `operation_id`, or `already_imported` for a file already loaded with
the current parser and options. 200 or 201 acknowledges receipt and the request,
not the import's outcome. Bodies are capped at 512 MiB. When Prefect cannot be
reached or refuses the request, the answer is 503 (`OrchestratorUnavailable`),
so a sender retries; sending the same file again is always safe.

A delivery token is made on the connection's **Settings → Data delivery**,
shown once and stored as a hash; viewing or retrying setup never replaces an
active one. It can only deliver to its own connection (rows,
media, collector reports); it cannot read history, edit configuration, import
older files or replay. Replacing it invalidates the old one at once; removing
the connection revokes it.

Senders: the phone's export app posts each Health Auto Export JSON with a REST
API automation (JSON format, the URL and `Authorization: Bearer <token>` shown
during setup, no query or multipart wrapping); the
[collector](../../collector/README.md) posts batches from a device. A sender
must reach the URL: an installation behind an internal gateway needs the home
network or the VPN.

### A watched folder

A folder is connection configuration ([its keys](configuration.md#connections)).
Each sync scans it, registers new content (identical bytes map to the existing
artifact), imports each pending file as its own import operation linked to the
sync, and builds each linked analysis once at the end. A scan that finds nothing
new loads and builds nothing. A file that stays in the folder is not copied or
dated again by later scans (decision 024). Only the newest `keep_last` copies are
kept; older imported copies are deleted with their receipts and history kept,
and replaying one is refused until the same bytes arrive again by upload or the
receiving URL.

### Imports

An import parses exactly one artifact, under the connection's lock, with its
format's parser version and the import's options. An artifact already imported
with the same parser version and options is not loaded again unless replayed;
replay is merge-safe, since rows keep their source keys. A failed import never
counts as imported. A connector bumps its parser version when a change alters
the rows a file produces.

For Health, `role=historical` contributes rows dated before the connection's
`cutoff`, `role=ongoing` (the default) rows from the cutoff on, so a one-off
bulk export and the automatic ones never overlap.

## How one load runs

Every path, the interface, the API, the CLI, a Prefect schedule, calls
`run_connection` in `src/control/runner.py` (decisions 005, 006):

1. Register the operation, or adopt the requested one; a Prefect retry adds an
   attempt to its flow run's operation. Without the metadata store nothing loads.
2. Take the connection's lock, keyed by its pipeline name: a Postgres advisory
   lock, or `flock` beside a SQLite store. Both are released when the process
   dies. A queued request waits for it; requests for one connection run one
   after another.
3. Resolve this connection's credentials only, then load with dlt: a fetch, the
   watched folder's new files, or one artifact.
4. Record the outcome. If that write fails, the load is not reported as failed.
5. Build each analysis that links the connection, as its own `models` operation.

An attempt records ingestion (`running`, `succeeded`, `failed`, `blocked`) and
transformation (`pending`, `succeeded`, `failed`, `skipped`, `not_run`) apart,
with dlt load IDs and an error with credential values replaced by `***`. A build
failure leaves the load succeeded and the analysis stale.

The connection page shows each attempt; **Activity** holds receipts, uploads,
imports and history, **Settings** holds settings, credentials, data delivery,
the watched folder and removal (decision 018). Guided setup for a new connection
offers its delivery methods, prepares that connection's deployment (and the
Miroir's media deployment), and for
a sender waits for the first automatic delivery and its import. A manual upload
never counts as proof that a sender is connected.

### On the command line

These run here, into a local DuckDB file, never into the warehouse, and skip
dbt, whose models target Postgres:

```bash
uv run --locked pipelines-miroir --config miroir.json run music --duckdb /tmp/mirror.duckdb
uv run --locked pipelines-miroir submit health ~/Downloads/export.json --option role=historical
uv run --locked pipelines-miroir import health <artifact-id> --duckdb /tmp/mirror.duckdb [--replay]
uv run --locked pipelines-miroir artifacts health
uv run --locked pipelines-miroir status
```

`run` loads one connection; dlt's working state goes beside the file
(`<name>.dlt/`). `submit` stores and registers a file without loading it
(`--format` and `--option NAME=VALUE` set its format and options;
`--artifacts`, default `$MIROIR_ARTIFACT_DIR`). `import` loads a registered
artifact and prints `already_imported` when there is nothing to do. `artifacts`
lists each file with every import, its parser version and options. `status`
shows per connection the latest attempt with its outcomes and load IDs, the last
successful load, and how many requests have not started. A `running` attempt is
`verified: false` until its flow records an outcome or `refresh` closes it.
`execute <operation>` runs one request; the server uses it as its local executor.

## Prefect

Prefect is the one orchestrator (decisions 012, 013, 029). With a `deployment`
section, each connection gets one deployment of the flow `sync_connection`
(`src/pipelines/miroir.py`), named `sync-connection/<miroir>-<connection>`, with
a concurrency limit of 1, its schedule, and the section's pool, image and job
variables. Miroir decides what a load is; Prefect only runs it.

While some connection has a media kind with thumbnails, the Miroir also gets one
media deployment, `sync-connection/<miroir>--media` (decision 033 amendment):
the same flow, pool and image, a concurrency limit of 1, no schedule, and the
tag `miroir:media`. Every derive run (thumbnails, backfill walks) of every
connection runs there, with its connection as a parameter; everything else runs
on the connection's deployment. Its job variables are the section's
`job_variables` with `media_job_variables` merged over them (top-level keys
replace; the image is always the connections'), so the work pool's
template can give the bucket key to its Jobs alone. A derive and a load of one
connection take turns by the advisory locks, not by Prefect: the derive's writer
waits for the connection's lock inside its Job (up to an hour), and derives of
one connection hold `<pipeline>-media`. Derives of different connections queue
behind each other in Prefect. Preparing a connection also creates or updates the
media deployment.

```bash
uv run --locked pipelines-miroir deployments plan    # creates, updates, removals; hand edits show as drift
uv run --locked pipelines-miroir deployments apply   # or the Apply deployments button
uv run --locked pipelines-miroir refresh
```

With a pool configured, the server submits each request as a flow run,
idempotent on the operation ID; without one it runs `execute` subprocesses
against a local DuckDB file (`serve --duckdb`). `refresh` closes attempts whose
flow run ended without an outcome (crashed becomes `lost`, cancelled
`cancelled`), and requests that got no run within an hour (`blocked`,
`NeverStarted`). A flow run that Prefect still shows as running after its pod
died before recording an attempt is not closed by `refresh`: cancel it in
Prefect.

On a cluster each flow run is its own Kubernetes Job from the pinned image. The
image is built in CI from `uv.lock` (Python 3.12, `PIPELINES_CODE_REVISION`
baked in). The Job needs the configuration and artifacts at the configured
paths, the warehouse and metadata credentials and the master key; the media
deployment's Jobs alone also need the bucket key. How the installation provides
them is in
[home-ops](https://github.com/h-wb/home-ops/blob/main/kubernetes/apps/main/automation/miroir/README.md).
The repo's own tasks, `mise run miroir:deploy` and `mise run miroir:refresh`,
run `deployments apply` and `refresh` from a checkout whose gitignored
`miroir.json` matches the installation's.

## Analysis builds

Each successful Postgres load builds every analysis that links its connection,
once, as a child `models` operation; a folder sync builds after all its files.
A failed build leaves the load succeeded and other analyses unaffected. A
rebuild requested alone resolves no credentials and leaves load times as they
were.

- A build waits for its connection's lock, then for the Prefect global
  concurrency limit `dbt` (one build at a time; it must exist on the Prefect
  server). Holding both, it reads the configuration again: if the analysis's
  settings changed meanwhile, it ends `blocked`, and the load shows its
  transformation as skipped with the reason. A connection edit alone does not
  stop a build, since what a build applies is in the analysis's receipt.
- A package with a minimum build interval (decision 040; timeline 5 minutes,
  messages 10) builds at most that often: a load joins the scheduled build bound
  to its own flow run instead of building. Two loads deciding at once do not
  both build.
- An analysis is `current` only after a successful build with its current plan,
  on data at least as recent as every linked input's last load. A load that
  arrives during a build leaves it `stale`, as does a failed or interrupted
  build, since dbt may have replaced some outputs. Other states: `running`,
  `no_data`.
- **Rebuild analysis** runs on one linked connection's deployment; **Rebuild from
  scratch** also rebuilds incremental tables (dbt `--full-refresh`).

What packages exist and what they build: [Analyses](analyses.md).

## The metadata store

SQLAlchemy Core over SQLite (development, tests) or PostgreSQL, schema
`miroir_meta`, in a database of its own apart from the warehouse (decision 015).
It holds operations and attempts, the artifact registry and imports, encrypted
credentials, hashed delivery tokens, receipt times, collector reports, the media
queue and backfill walks. `schema_info` holds the version, now 8
(`SCHEMA_VERSION` in `src/control/operations.py`); startup migrates an older store
and refuses a newer one.

An upgrade of the schema is one-way. Release the server, the worker and the
flow-run image together; drain Miroir runs and back up the configuration and the
metadata first, since going back means restoring that backup.

| Version | Added |
| --- | --- |
| 4 | Hashed delivery tokens; first and latest receipt times |
| 5 | Import options and folder provenance |
| 6 | Analysis IDs on build operations, earlier builds backfilled under their connection IDs |
| 7 | `collector_reports` and `collector_summaries` |
| 8 | `media_pending` and `media_backfills` |

Versions 2 and 3 added operation columns and an attempt's `failure_kind`.
