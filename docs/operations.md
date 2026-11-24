---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How to add a device source or an analysis, the commands a checkout can run against the installation, what to look at when something looks wrong, and the failures seen so far.
topics:
  - "[[Warehouse]]"
  - "[[Job runner]]"
created: 2026-09-24T22:01
---

# Operations

Day to day Miroir is operated from its interface: each connection's page shows
its last delivery, last load and collector report, each analysis's page its last
build, and both can sync, rebuild and rebuild from scratch. This page is the rest:
the steps that change what Miroir reads, the commands a checkout of this repo
can run, and what to look at when something looks wrong. Rolling out a new image
is in
[home-ops](https://github.com/h-wb/home-ops/blob/main/kubernetes/apps/main/automation/miroir/README.md),
since it changes what the cluster runs.

## Add a source on a device

1. Create the connection in the interface, one per device and source; its
   setup offers automatic delivery and shows the delivery token once.
2. Make the token readable on the device under the environment variable the
   collector's `token_env` names. The settings of the installation's devices are
   in dotfiles.
3. Add the source to the collector's configuration with the connection's
   receiving URL ([the collector](../collector/README.md#configuration)).
4. Run `miroir-collector run` once by hand with a larger `--max-batches` for the
   history, since each delivery requests an import; then let the device's
   scheduler carry on.

## Add an analysis

Add it from the overview's **Add an analysis**, or under `analyses` in the
configuration, with its package, its inputs, a time zone and its parameters
([Analyses](miroir/analyses.md)). Build it from its page. Then sync the database
schema in Metabase so the new tables show.

## From a checkout

Every task below needs `mise run pass:login` first and gets only its granted
secrets.

```bash
mise run miroir:deploy                                    # deployments apply from the local miroir.json
mise run miroir:refresh                                   # close runs that ended without an outcome
mise run prefect -- deployment ls
mise run prefect -- flow-run ls --flow-name sync-connection --limit 5
mise run prefect -- flow-run logs <flow-run-id>
```

`miroir:deploy` and `miroir:refresh` read the checkout's gitignored
`miroir.json`, which must match the installation's copy; flow runs read the
installation's path (`MIROIR_PREFECT_CONFIG_PATH`). Run
`pipelines-miroir deployments plan` first to see what apply would change.

## Builds

Every load builds the analyses linked to its connection, into their own schemas
([Analyses](miroir/analyses.md)); a failed build leaves the load succeeded and
the analysis stale. Builds take the Prefect global concurrency limit `dbt`, one
at a time. It lives in the Prefect server; recreate it after a rebuild of that
server, or builds queue forever:

```bash
mise run prefect -- gcl create dbt --limit 1
```

Loads have no full refresh. Never drop a raw table: staging views depend on it
and Postgres refuses the `DROP TABLE`. An analysis can be rebuilt from scratch.

The warehouse is reachable only inside the cluster. Load a one-off file through
a connection, by upload or its receiving URL: flow runs have the credentials.

## When something looks wrong

| Symptom | Look at |
| --- | --- |
| A source stopped arriving | The connection's page: last delivery, last collector report ([observability](observability.md)); then the collector's log on the device |
| A load failed | The attempt on the connection's page, and its run in Prefect; `refresh` closes runs that ended without an outcome |
| A dashboard is stale | The analysis's page: a failed build leaves the load succeeded and the analysis stale |
| Builds are requested but never run | The `dbt` concurrency limit, above, must exist |
| A device cannot deliver | It cannot reach the server; the batch waits in its spool and goes with the next run |

## Failures seen so far

| Symptom | Cause | Fix |
| --- | --- | --- |
| `JSONDecodeError: Expecting value` while extracting ListenBrainz | ListenBrainz served its HTML "Verifying your browser" page after heavy traffic | Wait until `…/listens?count=1` answers `application/json` again; the connector already paces its pages |
| `DependentObjectsStillExist: cannot drop table` | Views depend on the table | Do not drop it |
| 403 from a public URL of the cluster, from inside the cluster | The CDN in front blocks non-browser user agents | Call the in-cluster service, with the public `Host` header if the app checks it |
| A timeline build failed with "no empty local buffer available" (PostgreSQL 18) | Large reads staged in a temporary table | Fixed: rows are staged in an unlogged table (decision 040, amendment of 2026-11-23) |
