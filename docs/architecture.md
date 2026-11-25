---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: Which part of the code does what, who may import whom, and where a new thing goes.
topics:
  - "[[Warehouse]]"
  - "[[Device collector]]"
created: 2026-11-20T14:36
---

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
| `pipelines` | `core`, and `adapters.s3` (the flow builds the worker's bucket client) |
| `adapters` | `core`, `pipelines` |
| `web` | `core`, `integrations` |
| `cli` | `core`, `integrations`, `adapters`, `web` |

Also: nothing under `src/` imports the collector, and a connector's `formats.py` and
`contracts.py` never import dlt at module level, so files can be validated
without the loading stack. `src/core/timeline.py`, the server's read-only access
to the warehouse (decision 040), imports only `src.core.config`,
`src.core.operations` and `src.core.analysis` (to name the timeline's schema as
the build does) of the server's code, and only `web` and `cli` import it.
Nothing in `web` or the timeline module imports an image library: media is never
decoded in the web process. Only `src/core/thumbnails.py` imports one (Pillow,
and pillow-heif for HEIC), and nothing in `src` imports it: the derive run
starts it as a child process. Only
the runner imports the derive module, inside a derive run. A test imports the
server's modules and the derive module and checks that no image library loaded. The collector imports the Python standard library,
urllib3, pydantic and a pinned secret scanner, and nothing else; its own parts have import rules too, held
by `collector/tests/test_boundaries.py` (decision 035).

A new folder under `src/` fails the boundary test until it is given a rule.

## Where data moves

How data gets in (a fetch, or a delivered file by upload, receiving URL or
watched folder), how media skips the import path, and how it is read are the
[flows](miroir/flows.md). In code terms: every transport is in `core`; a sender
such as the collector is a client of the receiving URL, not a transport of its
own; media are never parsed, so they are not artifacts and never go through a
connector's format, and core reaches the bucket only through the `ObjectStore`
protocol, implemented by `adapters/s3.py`.

## Where a new thing goes

- A new service with an API: a connector with a fetch.
- A new export file: a file format on a connector.
- A source that exists only on a device: a source in `collector/`, and a file
  format on a connector for what it sends. A contract test ties the two.
- Files that rows point at: a media kind on the connector, and a media function
  on the source.
- A new way to schedule or run, or a new place to store: an adapter.
- A new screen or command: `web` or `cli`, calling a core service.
