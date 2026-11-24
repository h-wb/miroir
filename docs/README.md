---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: The index of this repo's pages, one row per source with its connector and collector source, and how secrets reach a checkout.
topics:
  - "[[Warehouse]]"
  - "[[Device collector]]"
created: 2026-09-24T22:01
---

# Documentation

Miroir keeps every source I use in one Postgres, so it outlives each service's
own retention and can be joined across sources. Why, and how it fits with the
other systems: the handbook's
[Warehouse](https://github.com/h-wb/handbook/blob/main/Systems/Warehouse.md) and
[Device collector](https://github.com/h-wb/handbook/blob/main/Systems/Device%20collector.md).
What is deployed and how to operate it:
[home-ops](https://github.com/h-wb/home-ops/blob/main/kubernetes/apps/main/automation/miroir/README.md).

## Pages

| Page | What it holds |
| --- | --- |
| [Miroir](miroir/README.md) | What it is, the words, the rules, the commands, development; the reference pages beside it |
| [Architecture](architecture.md) | Which part of the code does what, who may import whom, where a new thing goes |
| [Decisions](miroir/decisions.md) | Why it is built this way, numbered |
| [Writing an integration](miroir/integrations.md) | How a connector is written and tested |
| [Observability](observability.md) | How Miroir tells a quiet source from a broken one; metrics and alert rules |
| [Operations](operations.md) | Commands and failures seen, from a checkout |
| [Releases](releases.md) | How the collector is versioned and released |
| [The collector](../collector/README.md) | Install, configure, run and test `miroir-collector` |
| [Sources](sources/) | One page per source, below |

## Sources

One row per connector in `src/integrations/registry.py`. A collector source is a
module in `collector/miroir_collector/sources/`.

| Source | Connector, input | Collector source | Package | Handbook |
| --- | --- | --- | --- | --- |
| [ListenBrainz](sources/listenbrainz.md) | `listenbrainz`, fetch | | `music`, `crossovers` | [Music listening](https://github.com/h-wb/handbook/blob/main/Systems/Music%20listening.md) |
| [GitHub](sources/github.md) | `github`, fetch | | `github` | [Code activity](https://github.com/h-wb/handbook/blob/main/Systems/Code%20activity.md) |
| [Bike Share](sources/bikeshare.md) | `bikeshare`, fetch | | `biking`, `crossovers` | [Bike rides](https://github.com/h-wb/handbook/blob/main/Systems/Bike%20rides.md) |
| [Apple Health](sources/apple-health.md) | `apple_health`, Health Auto Export JSON | | `health`, `crossovers` | [Health data](https://github.com/h-wb/handbook/blob/main/Systems/Health%20data.md) |
| [Browser history](sources/browser-history.md) | `browser_history`, Places snapshot or collector batches | `zen_history` | `browser`, `timeline` | [Browser history](https://github.com/h-wb/handbook/blob/main/Systems/Browser%20history.md) |
| [ScreenMemory](sources/screenmemory.md) | `screenmemory`, collector batches; media `screenshots` | `screenmemory` | `timeline` | [Screen activity](https://github.com/h-wb/handbook/blob/main/Systems/Screen%20activity.md) |
| [Apple Messages](sources/apple-messages.md) | `apple_messages`, collector batches; media `attachments` | `apple_messages` | `messages` | [Messages](https://github.com/h-wb/handbook/blob/main/Systems/Messages.md) |

Location is not loaded: the location tracker keeps that record, and there is no
connector for it. Which connections the installation configures, and how each
arrives, is in home-ops.

A source page covers: status, where the data comes from, extraction (resources,
keys, cursors) or what the collector sends and withholds, tables, models and
dashboards, credentials and settings, history, quirks.

### Adding a source

1. A connector package in `src/integrations/<name>/`, listed in `registry.py`:
   [Writing an integration](miroir/integrations.md). A device-only source also
   gets a collector source ([the collector](../collector/README.md#adding-a-source)).
2. dbt: the raw tables in `dbt/models/staging/_sources.yml`, `stg_<source>__*`
   views, models in `dbt/models/<group>/`, and a package in
   `src/analyses/catalog.py`.
3. A page in `sources/` and a row above, and the handbook's system page for it.
4. In the installation: create the connection in the interface, then sync it once.

## Secrets and local configuration

- Every credential a task needs is a Proton Pass reference in the
  `Automation General` vault, mapped to an environment variable in
  [`fnox.toml`](../fnox.toml): source tokens, and operator copies of the
  cluster's Prefect, warehouse and Miroir credentials, kept until tasks read
  those sources directly. Connections hold their own credentials in Miroir's
  encrypted store ([Configuration](miroir/configuration.md#credentials)).
- `mise run pass:login` opens the repo's session (the macOS Keychain, or a pod's
  `PROTON_PASS_TOKEN`); it extends the dotfiles' global `proton-login` task. Each
  task gets only the keys its `secrets = [...]` grant lists.
- Shared non-secret settings are in [`mise.toml`](../mise.toml); machine
  overrides go in the gitignored `mise.local.toml`. The repo selects no mise
  environment, so a machine keeps its own layers.
- Inside the cluster, select the tracked [`fnox.cluster.toml`](../fnox.cluster.toml)
  profile in `mise.local.toml`. It only points `PREFECT_API_URL` at the in-cluster
  service; do not also set that variable under `[env]`, where it conflicts with
  the task grant.

  ```toml
  [secrets.fnox]
  profile = "cluster"
  ```
