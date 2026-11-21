# Architecture: parts and responsibilities

Each part of Miroir has one job. The import rules below are enforced by
`tests/test_boundaries.py`, so this page and the code cannot drift apart
silently. Decision 031 records why.

## The parts

| Part | Its job | It must not |
|---|---|---|
| `src/integrations/` (connectors) | Know one source: what it can read (a fetch, file formats) and how to turn it into tables | Know how a file arrived, when anything runs, or where it is stored |
| `src/analyses/` | Catalog of analysis packages (dbt models and dashboards) and the inputs they need | Load data |
| `src/core/` | Configuration, receiving and storing files, opening a connection's inputs, running and recording loads, secrets | Know about HTTP, the command line or the scheduler |
| `src/pipelines/` | The flow entry points the scheduler runs | Hold product logic; they call core |
| `src/adapters/` | Talk to the scheduler (Prefect): plan and apply deployments, submit, observe | Decide what a load is |
| `src/web/`, `src/cli/` | Transport and presentation; both call the same core services | Contain logic of their own |
| `collector/` (`miroir-collector`) | On a personal machine: read a local source, withhold secrets, deliver to a receiving URL, remember what was acknowledged | Parse or load anything, or import any server code |

## Who may import whom

| Part | May import, besides itself |
|---|---|
| `integrations` | `src.core.models` only |
| `analyses` | `src.core.models` only |
| `core` | `integrations`, `analyses` |
| `pipelines` | `core` |
| `adapters` | `core`, `pipelines` |
| `web` | `core`, `integrations` |
| `cli` | `core`, `integrations`, `adapters`, `web` |

Also: nothing under `src/` imports the collector, and a connector's `formats.py` and
`contracts.py` never import dlt at module level, so files can be validated
without the loading stack. `src/core/timeline.py`, the server's read-only access
to the warehouse (decision 040), imports only `src.core.config` and
`src.core.operations` of the server's code, and only `web` and `cli` import it.
Nothing in `web` or the timeline module imports an image library: media is never
decoded in the web process. The collector imports the Python standard library,
urllib3, pydantic and a pinned secret scanner, and nothing else; its own parts have import rules too, held
by `collector/tests/test_boundaries.py` (decision 035).

A new folder under `src/` fails the boundary test until it is given a rule.

## How data gets in

There are two ways, and a connector declares which it supports (decision 021):

1. **Fetch.** Miroir pulls from a service's API with the connection's
   credentials, on a schedule.
2. **A delivered file.** Someone or something hands Miroir a file. It is
   validated, stored as an artifact, then imported. A file reaches Miroir
   through one of three transports, all owned by core:
   - **upload**, by a person in the web interface;
   - **the receiving URL**, when a program pushes it;
   - **a watched folder**, when Miroir picks up a file something left there.

Programs that push to the receiving URL are *senders*. They live outside the
server: the Health Auto Export app is one, and the collector is another, for sources
that exist only on a device. The collector is therefore not a fourth transport. It
is a client of one that already exists. After each run it also reports, with the
same token, what happened on the device (fixed words and counts, never data), so
Miroir can tell a quiet source from a broken one ([observability](observability.md),
decision 039).

## Media

Some rows point at a file: a frame at its screenshot. Those files are media
(decision 033). They are never parsed, so they are not artifacts and do not go
through a connector's format. A connector declares which column holds the key;
core signs upload and view links against an object store it reaches through the
`ObjectStore` protocol; the S3 implementation is an adapter. A device uploads
with the links and never holds a storage credential.

## Where a new thing goes

- A new service with an API: a connector with a fetch.
- A new export file: a file format on a connector.
- A source that exists only on a device: a source in `collector/`, and a file
  format on a connector for what it sends. A contract test ties the two.
- Files that rows point at: a media kind on the connector, and a media function
  on the source.
- A new way to schedule or run, or a new place to store: an adapter.
- A new screen or command: `web` or `cli`, calling a core service.
