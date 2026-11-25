---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: Every key of Miroir's configuration file, the environment variables it reads, and how connection credentials are stored.
topics:
  - "[[Warehouse]]"
created: 2026-11-24T23:30
---

# Configuration

Miroir reads one JSON file, `miroir.json`, validated by `src/core/config.py`
(decision 001). It is the desired state: the interface, the API and the CLI
all edit it, and each write checks the file's revision first, so two editors
cannot overwrite each other (a stale edit is refused as a conflict). Unknown
fields, inline credential values and type coercion are refused, and errors
never echo a value. The installation's file is not in git; a synthetic one is
[`examples/miroir.json`](../../examples/miroir.json).

```json
{
  "schema_version": 1,
  "id": "personal",
  "connections": [],
  "analyses": [],
  "links": {"prefect_ui": "https://prefect.example", "metabase": "https://metabase.example"},
  "deployment": {"work_pool": "miroir-k8s", "image": "ghcr.io/h-wb/pipelines:sha-<full-commit>"},
  "media": {"endpoint": "http://garage.example:3900", "bucket": "miroir-media"}
}
```

IDs (the Miroir's, connections', analyses') start with a lowercase letter and
hold lowercase letters, digits and `_`, at most 24 characters. The
configuration's revision is a hash of the normalised settings, references and
bindings, not of what is deployed.

One warehouse per Miroir. Configurable destinations, machine-local overlays and
stored revision snapshots are not in the schema; each would come with a
schema change of its own.

## Connections

```json
{
  "id": "music",
  "integration": "listenbrainz",
  "settings": {"username": "example_user", "start_date": "2020-01-01T00:00:00Z"},
  "credentials": {"access_token": {"provider": "miroir"}},
  "binding": {"pipeline_name": "listenbrainz", "dataset_name": "listenbrainz_data"},
  "resources": ["listens"],
  "folder": {"path": "/miroir/data/browser", "pattern": "*.sqlite.gz", "options": {}, "keep_last": 5},
  "schedule": {"cron": "0 3 * * *", "timezone": "UTC"}
}
```

| Key | Holds |
| --- | --- |
| `integration`, `settings` | The connector, and settings its model validates (`integrations` lists each schema) |
| `credentials` | A reference per credential field, below |
| `binding` | Optional. dlt pipeline and dataset names. Without it they are generated: `ws_<length of the Miroir ID>_<Miroir ID>_<connection ID>` (decision 002; `ws_` predates the rename to Miroir). A binding only names existing state; it adopts, resets and changes nothing. Pipeline and dataset names are unique within a Miroir |
| `resources` | Optional. A nonempty subset of the connector's fetched resources; absent or null means all, including resources added later. Deselecting keeps rows and cursors. File imports always parse every resource |
| `folder` | Optional. A watched folder ([how it is read](runs.md#a-watched-folder)). `pattern` and `format` default to the connector's first format's, `options` to its defaults, `keep_last` to 5 (1 to 1000, null keeps every copy). A `settings.data_dir` from before decision 021 is read as a folder |
| `schedule` | Optional. `cron` and an IANA `timezone` (default `UTC`), validated with croniter and zoneinfo (decision 013) |

## Analyses

```json
{"id": "together", "package": "crossovers",
 "inputs": {"health": "health", "music": "music"},
 "timezone": "Europe/Paris", "parameters": {}, "dashboards": {"Correlations": 1}}
```

`inputs` maps each of the package's input roles to a connection; `timezone`
(default `UTC`) sets local days and hours; `parameters` are typed by the
package; `dashboards` names Metabase dashboard IDs for links. What each package
needs: [Analyses](analyses.md).

A file without an `analyses` key is upgraded on read: each connection gets its
integration's recommended analysis under the connection's ID, keeping its old
analysis settings and dashboards, its output schemas and its build history. The
next write saves the new shape. `"analyses": []` means no analyses.

## Execution, links and media

`deployment` turns on Prefect (decisions 013, 016, 029): `work_pool` is the pool
flow runs go to; `image` and `job_variables` are optional and become the
deployment's job variables. `media_job_variables`, optional, is merged over
`job_variables` for the media deployment alone (its top-level keys replace
those of `job_variables`; `image` is always the connections'), the one derive runs use
(decision 033 amendment): a reference the work pool's template resolves, such as
`{"env_from": [{"secretRef": {"name": "miroir-media"}}]}`, never the key itself,
since Prefect stores job variables. On a cluster, pin an image by `sha-` tag or digest:
a registry mirror can serve stale mutable tags. Without the section, requests
run as local subprocesses. How deployments are planned: [Prefect](runs.md#prefect).

`links.prefect_ui` and `links.metabase` give the interface its links to runs and
dashboards.

`media` turns on media storage. Its keys (`endpoint`, `link_endpoint`,
`region`, `bucket`, `prefix`, the link lifetimes and the derive settings) are
described in [Media](media.md).

## The environment

| Variable | For |
| --- | --- |
| `MIROIR_API_TOKEN` | The administrator's token, for the interface and the API. `serve` needs it |
| `MIROIR_SECRET_KEY` | The master key that encrypts stored credentials; `"<new>,<old>"` during a rotation. Not needed to receive files when no credential must be decrypted |
| `MIROIR_METADATA_URL`, or `MIROIR_METADATA__HOST`, `__USERNAME`, `__PASSWORD`, `__PORT`, `__DATABASE` | The metadata store. The URL wins; otherwise it is composed from the parts (port 5432 and the username as database by default) with the psycopg2 driver |
| `MIROIR_ARTIFACT_DIR` | Where artifacts are kept (default `.miroir/artifacts`) |
| `MIROIR_PREFECT_CONFIG_PATH` | Where flow runs read the configuration, when it differs from the writer's path |
| `DESTINATION__POSTGRES__CREDENTIALS__*` | The warehouse, for flow runs and dbt |
| `MIROIR_MEDIA_ACCESS_KEY_ID`, `MIROIR_MEDIA_SECRET_ACCESS_KEY` | The bucket key, on the server and in derive runs (the media deployment's); required by the server when `media` is set |
| `MIROIR_TIMELINE_DATABASE_URL`, or `MIROIR_TIMELINE_DATABASE__*` | The timeline's read-only access to the warehouse ([Timeline](timeline.md)) |
| `MIROIR_METRICS_TOKEN` | Turns on `/metrics`; must differ from the API token ([observability](../observability.md)) |
| `FORWARDED_ALLOW_IPS` | Uvicorn's trusted proxies, so receiving URLs use HTTPS and cookies are Secure behind a TLS proxy |

Installation secrets (the master key, database roles, Prefect and Metabase
logins) come from whoever deploys; Miroir does not store them.

## Credentials

A credential reference is `{"provider": "miroir"}` for Miroir's own encrypted
store, or `{"provider": "env", "key": "NAME"}` or
`{"provider": "fnox", "key": "NAME"}` for a value managed elsewhere (decision
011). Stored values are encrypted with Fernet under `MIROIR_SECRET_KEY` in the
metadata store, and resolved only inside the run that needs them.

```bash
export MIROIR_SECRET_KEY="$(uv run --locked pipelines-miroir generate-key)"   # back it up apart from the database
uv run --locked pipelines-miroir set-credential music access_token            # prompts, or reads stdin
uv run --locked pipelines-miroir check music
```

In the interface, **Add a connection** asks for settings and credentials and
tests them. Credentials are write-only: no page, response, log or file shows
them again; a connection page shows "set · updated …" with Replace and Test. A
connection's status turns `rejected` when a check or a run is refused (HTTP 401
or 403) and recovers after a passing check or a successful load.

To rotate the key: set `MIROIR_SECRET_KEY="<new>,<old>"`, run
`pipelines-miroir rotate-secrets`, then drop the old key.
