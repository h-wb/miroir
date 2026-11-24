---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
summary: This index points to warehouse architecture, source contracts, and instructions for adding sources.
topics:
  - "[[Warehouse]]"
  - "[[Device collector]]"
created: 2026-09-24T22:01
---

# Pipelines documentation

Personal data warehouse: every service I use, mirrored into one Postgres so it
outlives the service's own retention and can be joined across sources.

Miroir development: [implementation guide and decision log](miroir/README.md).
Roadmap: [Miroir implementation plan](product-plan.md).

```
 source service ──(API / export / file drop)──▶ dlt source ──▶ Postgres  <source>_data   (raw)
                                                                   │
                                                dbt (each linked analysis, after each load)
                                                                   ▼
                                                  miroir_<analysis>_… schemas ──▶ Metabase
```

- **Extract raw, transform in dbt.** dlt sources yield records as the service
  returns them (dlt flattens nested JSON into columns / child tables). Anything
  that interprets the data (who is "me", bots, units, time zones, dedup across
  sources) lives in `dbt/`.
- **Merge, don't append.** Every resource has a primary key and
  `write_disposition="merge"`, so re-fetching a window is harmless. Incremental
  cursors re-read a small overlap to catch late updates.
- **The warehouse keeps what services forget.** Merges never delete, so rows a
  service expires (GitHub's 90-day event feed, Firefox's history trimming) stay.

Which part of the code does what, who may import whom, and the ways data gets
in: [architecture](architecture.md).

How the collector is versioned and released: [releases](releases.md).

## Sources

On 2026-11-11, the four sources listed first below moved to Miroir connections
bound to their original raw schemas, and the legacy per-source flows were retired (decision 025).

| Source | Data | Miroir connection, schedule (UTC) | Raw schema | dbt | Dashboards |
|---|---|---|---|---|---|
| [ListenBrainz](sources/listenbrainz.md) | every listen (Spotify, Plex, Last.fm history) | `listenbrainz`, daily 00:00 | `listenbrainz_data` | `music`, `crossovers` | Music (2), crossovers |
| [GitHub](sources/github.md) | commits, PRs, issues, comments, releases, stars, events, contribution calendar | `github`, daily 01:00 | `github_data` | `github` | GitHub (30) |
| [Apple Health](sources/apple-health.md) | metrics, workouts, state of mind, medications | `health`, on each export (receiving URL) | `apple_health_data` | `health`, `crossovers` | Health (24), crossovers |
| [Bike Share Toronto](sources/bikeshare.md) | every rental | `bikeshare`, daily 00:00 | `bikeshare_data` | `biking`, `crossovers` | Biking (3), Rides (26) |
| [Browser history](sources/browser-history.md) | Zen (Firefox) visits, time on page, typing | legacy load retired; supports snapshots and `zen_history` collector batches | the connection's dataset | `browser`, `timeline` | Browser (31, historical reference) |
| [ScreenMemory](sources/screenmemory.md) | screen text per screenshot, front app | supported by the Miroir collector | the connection's dataset | `timeline` | timeline interface |
| [Apple Messages](sources/apple-messages.md) | iMessage, SMS and RCS rows | supported by the Miroir collector | the connection's dataset | `messages` | – |
| Dawarich | location history | in progress, not deployed | `dawarich_data` | – | – |

Crossover dashboards (collection "Crossovers"): Music & Heart (25), Rides (26),
Late nights (27), Correlations (28), Year in review (29). Each Miroir load builds
the analyses linked to its connection into their own `miroir_<analysis>_…`
schemas, and the dashboards read those (since 2026-11-22, decision 042). The dbt
column and the source pages name model groups (`music`, `crossovers`, …); an
analysis builds each group into its own `miroir_<analysis>_…_<group>` schema.

## How a run works

- **Miroir** connections run as Prefect flow runs on the `miroir-k8s` pool, each
  in its own Kubernetes Job from the pinned image; their deployments come from
  the Miroir configuration (see [Miroir](miroir/README.md)).
- **Prefect**'s process worker (`local-pool`) runs flows from other repositories;
  this repository no longer deploys to it (decision 042).
- **Warehouse**: CNPG cluster `postgres-metabase` (automation namespace), database
  `metabase`. Only reachable in-cluster; deliver one-off files through a Miroir
  connection by upload or receiving URL (see [operations](operations.md)).
- **File drops**: Miroir flow runs mount the NAS share `Documents/data` at
  `/miroir/data`. The same folder is `Hugo/Documents/data` in Nextcloud, so
  anything synced there from a laptop
  (browser snapshots, exports) can feed a Miroir watched folder.
- **Metabase** reads the warehouse as database id 2; dashboards are built with
  the `mb` CLI (query-builder cards, date + group-by filters).

## Secrets and config

- Every credential lives in the Proton Pass **`Automation General`** vault: source
  tokens (`listenbrainz`, `bikeshare`, `github-pipelines`; the Miroir connections
  hold their own copies in Miroir's encrypted store) and operator copies
  (`prefect`, `postgres-metabase`), which mirror BWS/CNPG until operator tasks
  read those sources directly. References map them to env vars in
  [`fnox.toml`](../fnox.toml). The `miroir` installation item is still
  unprovisioned; its metadata password and existing encryption key must be
  supplied before infrastructure development/deployment tasks can use it.
  Mise injects only the keys listed in each task's `secrets = [...]` grant, including warehouse and
  Prefect credentials only where needed.
  Run `mise run pass:login` first (macOS Keychain or Paseo's `PROTON_PASS_TOKEN`).
  `mise secrets ls` lists names and grants without fetching values; ad-hoc
  commands use `mise x --secrets KEY -- <command>`.
- Shared non-secret config is in [`mise.toml`](../mise.toml); machine overrides
  go in gitignored `mise.local.toml`. No repo mise environment is selected, so
  the machine keeps its dotfiles layers (including pod/Paseo settings).
- In-cluster hosts select the tracked [`fnox.cluster.toml`](../fnox.cluster.toml)
  profile with the following in gitignored `mise.local.toml`. It changes only
  `PREFECT_API_URL` to the internal Kubernetes service; other secrets still come
  from Proton Pass. Without this selection, the default profile uses the vault URL.
  Do not also set `PREFECT_API_URL` under mise `[env]`: it conflicts with the
  task secret grant.

  ```toml
  [secrets.fnox]
  profile = "cluster"
  ```

## Adding a source

1. A connector package in `src/integrations/<name>/` (settings, credentials,
   check, resources, inputs, dlt source: extract only, raw records, merge keys,
   incremental cursor with an overlap), listed in `registry.py` (decision 021).
2. dbt: raw tables in `dbt/models/staging/_sources.yml`, `stg_<source>__*` views,
   marts in a `dbt/models/<topic>/` schema, and an analysis package in
   `src/analyses/catalog.py`.
3. Create the connection in the Miroir interface (credentials go to its
   encrypted store), then sync it once.
4. A page in `docs/sources/` from the template below, and a row in the table above.

Source page template: status & schedule · where the data comes from (and any
code outside this repo) · extraction (resources, keys, cursors) · tables → dbt →
dashboards · secrets & config · backfills and history · quirks.

See also: [operations runbook](operations.md), and
[observability](observability.md): what Miroir knows about each connection, its
Prometheus metrics and example alerts.
