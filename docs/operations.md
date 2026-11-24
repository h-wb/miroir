---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: The commands to look at and reconcile the installation from a checkout, and the failures seen so far with their fixes.
topics:
  - "[[Warehouse]]"
  - "[[Job runner]]"
created: 2026-09-24T22:01
---

# Operations

Day to day the installation is operated from its interface; rolling out an
image, adding a device source and the symptoms to look for are in
[home-ops](https://github.com/h-wb/home-ops/blob/main/kubernetes/apps/main/automation/miroir/README.md#operating-it).
This page is what a checkout of this repo can do. Every task below needs
`mise run pass:login` first and gets only its granted secrets.

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

## Failures seen so far

| Symptom | Cause | Fix |
| --- | --- | --- |
| `JSONDecodeError: Expecting value` while extracting ListenBrainz | ListenBrainz served its HTML "Verifying your browser" page after heavy traffic | Wait until `…/listens?count=1` answers `application/json` again; the connector already paces its pages |
| `DependentObjectsStillExist: cannot drop table` | Views depend on the table | Do not drop it |
| 403 from a public URL of the cluster, from inside the cluster | The CDN in front blocks non-browser user agents | Call the in-cluster service, with the public `Host` header if the app checks it |
| Builds requested but never run | The `dbt` concurrency limit is missing | Create it, above |
| A timeline build failed with "no empty local buffer available" (PostgreSQL 18) | Large reads staged in a temporary table | Fixed: rows are staged in an unlogged table (decision 040, amendment of 2026-11-23) |
