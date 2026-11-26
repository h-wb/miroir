---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-26
summary: Which part of the code does what, who may import whom, and where a new thing goes.
topics:
  - "[[Warehouse]]"
  - "[[Device collector]]"
created: 2026-11-20T14:36
---

# Architecture: parts and responsibilities

Each part of Miroir has one job. The import rules below are enforced by
`tests/test_boundaries.py`, so this page and the code cannot drift apart
silently. Decision 031 records why; the
[package split](decisions/2026-11-25-package-split.md) is moving the code
towards its parts, one step at a time, and this page says where it is now.

## The parts

| Part | Its job | It must not |
|---|---|---|
| `src/kernel/` | The shared vocabulary: `models.py` (`StrictModel`, `Identifier`, `DatabaseIdentifier`, `SecretKey`) and `errors.py` (`OperationError`, the product error the interface and the CLI show, and its bases `Unavailable` and `Timeout`) | Import anything but the standard library and pydantic |
| `src/integrations/` (connectors) | Know one source: what it can read (a fetch, file formats) and how to turn it into tables | Know how a file arrived, when anything runs, or where it is stored |
| `src/analyses/` | Catalog of analysis packages (dbt models and dashboards) and the inputs they need | Load data |
| `src/media/` | The files rows point at: the object store and its S3 client, names, upload and view links, the `media` section, media kinds and profiles, the derive queue's tables and repository, the derive run and its decoder | Have routes, or know about HTTP, the scheduler, the viewer or the connector registry |
| `src/core/` | Configuration, receiving and storing files, opening a connection's inputs, running and recording loads, secrets | Know about HTTP, the command line or the scheduler |
| `src/viewer/` | Reading the warehouse and showing it (decision 040): the reader and its read-only sessions, the timeline's API, Day view, search and conversation pages, their templates and static files, and `ViewerSettings`, what it reads of the installation | Import the control plane, web, the connectors, dlt, prefect, Pillow or boto3, or read the configuration file |
| `src/pipelines/` | The flow entry points the scheduler runs | Hold product logic; they call core |
| `src/adapters/` | Talk to the scheduler (Prefect): plan and apply deployments, submit, observe | Decide what a load is |
| `src/web/`, `src/cli/` | Transport and presentation; both call the same core services | Contain logic of their own |
| `collector/` (`miroir-collector`) | On a personal machine: read a local source, withhold secrets, deliver to a receiving URL, remember what was acknowledged | Parse or load anything, or import any server code |

## Who may import whom

| Part | May import, besides itself |
|---|---|
| `kernel` | Nothing: the standard library and pydantic only |
| `integrations` | `kernel`, and `media.kinds` (to declare a kind) |
| `analyses` | `kernel` only |
| `media` | `kernel` only |
| `core` | `kernel`, `integrations`, `analyses`, `media` |
| `pipelines` | `kernel`, `core`, `media` (the flow builds the worker's bucket client) |
| `adapters` | `kernel`, `core`, `pipelines` |
| `viewer` | `kernel`, `media` |
| `web` | `kernel`, `core`, `integrations`, `media`, and named parts of `viewer` (below) |
| `cli` | `kernel`, `core`, `integrations`, `adapters`, `web`, `media`, `viewer` |

Connectors and analyses take their base types and errors from the kernel, never
from `core`.

Media never imports the control plane (package split, step 5). The control
plane builds what media reads of a connection from its own configuration
(`MediaConnection` and `MediaTarget`, in `src/media/target.py`) and hands it
in, with the connection's lock for the derive run's writer. Media owns the
derive queue's tables and their repository (`src/media/queue.py`); the
metadata store holds them in its database, its metadata and its one migration
sequence (`src.core.operations`), and reaches them as `store.media_queue`.
Media never imports fastapi, starlette or prefect.

The viewer (package split, step 6) is the server's only access to the
warehouse, read-only (decision 040). It reads nothing of the configuration: it
is handed a `ViewerSettings` (`src/viewer/settings.py`), the reader database
opened from `MIROIR_TIMELINE_DATABASE*` and a function that gives the
configuration as the viewer reads it when it answers: the timeline analyses
with their schema (named as the build names it, by `analysis_plan`), time zone,
linked inputs and whether the build grants the reader role, and each
connection's media kinds. `serve` builds it (`src/cli/main.py`, through
`src/web/viewer.py`); `create_app` builds the same when given only the reader
database. Its templates and static files are its own (`src/viewer/templates/`,
`src/viewer/static/`): the app's template loader reads both folders, its pages
extend `base.html` by name, and `/static/` serves both folders at the same
addresses as before. A test imports every viewer module in a fresh interpreter
and checks that none of dlt, prefect, Pillow or boto3 loaded.

Web imports the viewer only by the names `tests/test_boundaries.py` lists as
debt (`_WEB_VIEWER_DEBT`), each with the step that removes it; a name no longer
imported fails the test until it is struck off:

- Step 7: `src/web/app.py` serves the `/media` view route and asks the viewer's
  withheld check (`media_withheld`, `TimelineTimeout`, `TimelineUnavailable`).
- Step 8: `src/web/app.py` mounts the viewer's routers, adds its template
  folder (`TEMPLATES_DIR`), links the header to it (`timeline_source`) and
  takes `TimelineDatabase` and `ViewerSettings`; `src/web/viewer.py` builds the
  settings from the configuration, and moves to the composition root.

Also: nothing under `src/` imports the collector, and a connector's `formats.py` and
`contracts.py` never import dlt at module level, so files can be validated
without the loading stack.
Media is never decoded in the web process: only `src/media/decoder.py` imports
an image library (Pillow, and pillow-heif for HEIC), and nothing in `src`
imports it: the derive run starts it as a child process
(`python -m src.media.decoder`). Only `src/media/s3.py` imports boto3. Only
the runner imports the derive module (`src/media/derive.py`), inside a derive
run. A test imports the server's and media's modules, then the derive module,
and checks that no image library loaded. The collector imports the Python standard library,
urllib3, pydantic and a pinned secret scanner, and nothing else; its own parts have import rules too, held
by `collector/tests/test_boundaries.py` (decision 035).

A new folder under `src/` fails the boundary test until it is given a rule.

## Where data moves

How data gets in (a fetch, or a delivered file by upload, receiving URL or
watched folder), how media skips the import path, and how it is read are the
[flows](miroir/flows.md). In code terms: every transport is in `core`; a sender
such as the collector is a client of the receiving URL, not a transport of its
own; media are never parsed, so they are not artifacts and never go through a
connector's format, and core reaches the bucket only through media's
`ObjectStore` protocol (`media/store.py`), implemented by `media/s3.py`.

## Where a new thing goes

- A new service with an API: a connector with a fetch.
- A new export file: a file format on a connector.
- A source that exists only on a device: a source in `collector/`, and a file
  format on a connector for what it sends. A contract test ties the two.
- Files that rows point at: a media kind on the connector, naming a profile in
  `media/profiles.py` if thumbnails are made, and a media function on the
  source.
- A new way to schedule or run, or a new place to store: an adapter.
- A new screen or command: `web` or `cli`, calling a core service; a new view
  of the warehouse: `viewer`, reading what it needs from its settings.
