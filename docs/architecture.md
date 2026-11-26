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
| `src/analyses/` | Catalog of analysis packages (dbt models and dashboards) and the inputs they need; what the viewer shows of them, as data (`views.py`: `Withhold`, where rows hide their files; `Lane`, each source of the timeline's `events`, with the plugin names the viewer implements; the tables' contract version) | Load data |
| `src/media/` | The files rows point at: the object store and its S3 client, names, upload and view links, the `media` section, media kinds and profiles, the derive queue's tables and repository, the derive run and its decoder | Have routes, or know about HTTP, the scheduler, the viewer or the connector registry |
| `src/control/` | Configuration, receiving and storing files, opening a connection's inputs, running and recording loads, secrets | Know about HTTP, the command line or the scheduler |
| `src/viewer/` | Reading the warehouse and showing it (decision 040): the reader and its read-only sessions, the timeline's API, Day view, search and conversation pages, the `/media` view route and its withheld check, their templates and static files, and `ViewerSettings`, what it reads of the installation | Import the control plane, web, the connectors, dlt, prefect, Pillow or boto3, or read the configuration file |
| `src/pipelines/` | The flow entry points the scheduler runs | Hold product logic; they call control |
| `src/adapters/` | Talk to the scheduler (Prefect): plan and apply deployments, submit, observe | Decide what a load is |
| `src/web/`, `src/cli/` | Transport and presentation; both call the same control services. `src/web/app.py` is the control plane's admin part (its API and pages); `src/cli/compose.py` (`serve`) and `src/cli/view.py` (`view`) are the web server's composition roots | Contain logic of their own; web never imports the viewer |
| `src/web/shell/` (the web shell) | One HTTP surface both the admin part and the viewer mount: the token and session checks, `/login` and `/logout`, the security headers with a content security policy per mounted part, `/static/`, the template loader with `base.html`, `login.html` and `error.html`, and the header's navigation, to which each part adds its links | Import anything of Miroir but the kernel |
| `collector/` (`miroir-collector`) | On a personal machine: read a local source, withhold secrets, deliver to a receiving URL, remember what was acknowledged | Parse or load anything, or import any server code |

## Who may import whom

| Part | May import, besides itself |
|---|---|
| `kernel` | Nothing: the standard library and pydantic only |
| `integrations` | `kernel`, and `media.kinds` (to declare a kind) |
| `analyses` | `kernel` only |
| `media` | `kernel` only |
| `control` | `kernel`, `integrations`, `analyses`, `media` |
| `pipelines` | `kernel`, `control`, `media` (the flow builds the worker's bucket client) |
| `adapters` | `kernel`, `control`, `pipelines` |
| `viewer` | `kernel`, `media`, `analyses.views` |
| `web` | `kernel`, `control`, `integrations`, `analyses`, `media`, the shell |
| `web.shell` | `kernel` only (and fastapi, starlette, jinja2) |
| `cli` | `kernel`, `control`, `integrations`, `analyses`, `adapters`, `web` (the shell included), `media`, `viewer` |

Connectors and analyses take their base types and errors from the kernel, never
from `control`.

Media never imports the control plane (package split, step 5). The control
plane builds what media reads of a connection from its own configuration
(`MediaConnection` and `MediaTarget`, in `src/media/target.py`) and hands it
in, with the connection's lock for the derive run's writer. Media owns the
derive queue's tables and their repository (`src/media/queue.py`); the
metadata store holds them in its database, its metadata and its one migration
sequence (`src.control.operations`), and reaches them as `store.media_queue`.
Media never imports fastapi, starlette or prefect.

The viewer (package split, step 6) is the server's only access to the
warehouse, read-only (decision 040). It reads nothing of the configuration: it
is handed a `ViewerSettings` (`src/viewer/settings.py`), the reader database
opened from `MIROIR_TIMELINE_DATABASE*` and a function that gives the
configuration as the viewer reads it when it answers: the analyses of a package
that declares lanes (the timeline's) with their schema (named as the build names
it, by `analysis_plan`), time zone, linked inputs, lanes and whether the build
grants the reader role (the analysis's own `reader_role` parameter, for any
package that declares views), each connection's media kinds, the `media` section, and the analyses whose package declares a
`Withhold` (`src/analyses/views.py`); and the media object store `serve` built.
`serve` and `view` build it (`src/cli/main.py`, through `src/cli/view.py`);
`web_app` builds the same when given only the reader database. The `/media` view route is
the viewer's (`src/viewer/media.py`, step 7): it asks those declarations whether
a row hides the file, then calls media's view side (`src/media/links.py`); the
control plane's service has no viewing reads, and the collector's media routes
stay in the control plane. Its templates and static files are its own (`src/viewer/templates/`,
`src/viewer/static/`): the shell's template loader reads them after its own and
the admin part's, its pages extend `base.html` by name, and its static files are
served under a prefix of their own, `/static/viewer/` (step 10), so a gateway
can route them. A test imports every viewer module in a fresh interpreter
and checks that none of dlt, prefect, Pillow or boto3 loaded.

The viewer names no source and no media kind (step 11): the Day view, its
totals, the calendar, search and the API's sources come from the lanes and
`events`, and every link to a file from its row's connection and kind. Plugins,
the scrubber's frames and the thread's conversations, are its own code, chosen
by the plugin name a lane declares. It reads the tables' version from the
package's `contract` relation and refuses a major it does not know. In dbt,
media's `media_ref` (`dbt/macros/media.sql`) is the only join on
`media_objects`, and `events` takes its sources from the lanes the build passes.

The web shell (`src/web/shell/`, package split, step 8) is the one HTTP
surface; neither the admin part nor the viewer imports the other. A part is
mounted as a `Part`: its template and static folders, its header links, the
paths it answers and the sources it adds to the content security policy there,
the requests that pass without the login because they check a token of their
own, and its notices. `serve` composes them in `src/cli/compose.py`
(`web_app`): a shell, the admin part (`mount_admin`, `src/web/app.py`), then the
viewer (`mount_viewer`, `src/cli/view.py`), which hands over plain values from
`src/viewer/part.py` (its routers, `PATHS`, `POLICY`, `STATIC_URL` and links)
and imports nothing of the shell. The login and the headers wrap every part's own
middleware. `create_app` is the admin part alone in a shell of its own; tests
that need both parts build them through `web_app`. The viewer's
`img-src 'self' blob:` is on its own paths only (`/timeline`, `/api/timeline`,
`/media`); every other response has `img-src 'self'`.

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

## Processes

One image runs as up to two processes behind one hostname, routed by path at
the gateway ([package split](decisions/2026-11-25-package-split.md#runtime),
step 10). `python -m src.cli` stays the module.

| Command | Mounts | Holds | Never loads or reads |
|---|---|---|---|
| `serve` | The admin part and the viewer, as before | Everything below, together | |
| `serve --without-viewer` | The admin part, composed by `admin_app` (`src/cli/compose.py`) | The metadata store, `MIROIR_SECRET_KEY`, Prefect, the media key for upload links and confirms, `MIROIR_API_TOKEN`, `MIROIR_METRICS_TOKEN` | `MIROIR_TIMELINE_DATABASE*`, the reader; of the viewer it imports only `src/viewer/navigation.py` |
| `view` | The viewer, composed by `viewer_app` (`src/cli/view.py`), and `/healthz` | The configuration file, read only; `MIROIR_TIMELINE_DATABASE*`; the media key (`MIROIR_MEDIA_*`, read-only in the cluster); `MIROIR_API_TOKEN` | The metadata store, `MIROIR_SECRET_KEY`, Prefect, the control plane's service, runner and operations, dlt, Pillow, pillow-heif, a connector's source; it writes nothing |

Reading the configuration loads the connector registry and pydantic, never a
connector's dlt source, so `view` reads the configuration file as `serve` does
and builds its `ViewerSettings` the same way. `tests/test_entry_points.py` runs
each command in a fresh interpreter up to the moment it would serve and checks
what it loaded.

Both headers carry the same links: the admin pages' are data in
`src/web/navigation.py`, which `view` mounts without the admin part; the
viewer's are declared in `src/viewer/navigation.py`, which needs no reader:
`view` asks it over its settings, `serve --without-viewer` over the
configuration alone. Both answer `/login` and `/logout`; the session cookie is
an HMAC of the same token, so a login on one is a login on both. `serve`'s login
lands on `/`, `view`'s on `/timeline` when it is reached directly. Behind the
gateway `/login` is `serve`'s, so a login lands on `/`.

The paths each process answers, for the gateway's routes. A prefix is itself
and every path below it (`/timeline` and `/timeline/2026-11-20`, not
`/timelinex`):

| Process | Answers | Route to it |
|---|---|---|
| `view` | `/timeline`, `/api/timeline`, `/media` (the `GET /media/{connection}/{kind}/{key}` view route), `/static/viewer`; also `/login`, `/logout`, `/healthz` and the shell's `/static` files, for its own pages | `/timeline`, `/api/timeline`, `/media`, `/static/viewer` |
| `serve --without-viewer` | Everything else, including `/login`, `/logout`, `/healthz`, `/static`, `/metrics` and the collector's routes (`/api/connections/{id}/receive`, `/report`, `/media/uploads`, `/media/confirm`, all under `/api/connections`) | Every other path |

The collector's media routes are under `/api/connections/`, not `/media/`, so
the `/media` prefix is the viewer's alone. Each process answers 404 on the
other's paths.

## Where data moves

How data gets in (a fetch, or a delivered file by upload, receiving URL or
watched folder), how media skips the import path, and how it is read are the
[flows](miroir/flows.md). In code terms: every transport is in `control`; a sender
such as the collector is a client of the receiving URL, not a transport of its
own; media are never parsed, so they are not artifacts and never go through a
connector's format, and control reaches the bucket only through media's
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
- A new screen or command: `web` or `cli`, calling a control service; a new view
  of the warehouse: `viewer`, reading what it needs from its settings.
