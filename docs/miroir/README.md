# Miroir development

Miroir provides a web interface, HTTP API, and CLI over shared connection,
artifact, and execution services. The server and isolated Prefect Kubernetes
runtime are deployed; ListenBrainz has completed an end-to-end sync. Legacy
flows continue alongside Miroir until each source is explicitly cut over.

- [Roadmap](../product-plan.md)
- [Baseline and migration constraints](baseline.md)
- [Design decisions](decisions.md)
- [Writing an integration](integrations.md)
- [Implementation log and next steps](implementation-log.md)
- [Synthetic example configuration](../../examples/miroir.json)

## Development launchers

```bash
mise run dev                 # http://127.0.0.1:8765
mise run dev -- --port 8766  # optional port override
```

This starts the current checkout with its own empty configuration, SQLite
metadata, DuckDB warehouse, artifacts and encryption key under gitignored
`.miroir/dev/`. The generated UI login token is in `.miroir/dev/api-token`; read
that file locally and paste it into the login page. The key and token are reused
after restarts, and their files are mode 600 in a mode-700 directory. No Proton
login or fnox secret fetch is needed. Inherited Miroir, Prefect and Postgres
destination settings are ignored; the local configuration cannot have a Prefect
deployment. Add connections through the UI as needed. Restart after code edits.

To run the UI checkout against the **existing installation** instead:

```bash
mise run dev:infra
# Or select the installation's mounted paths explicitly:
mise run dev:infra -- --config /miroir/data/miroir/miroir.json --artifacts /miroir/data/miroir/artifacts
```

Every host uses the same `fnox.toml` map, in the `Automation General` vault. The two
Miroir installation fields remain provisioning prerequisites.

The task authenticates through the shared `proton-login` template and receives
only `MIROIR_METADATA__PASSWORD`, `MIROIR_SECRET_KEY`, `PREFECT_API_URL`, and
`PREFECT_API_AUTH_STRING`. Both live in the `Automation General` vault. The `miroir`
installation item is still missing. Populate its `metadata-password` from the
existing CNPG application secret and its `secret-key` from the existing BWS
installation key before using this mode. The Homelab/Miroir website login is
a different credential. These remain operator copies of BWS/CNPG credentials. The warehouse password and source tokens
are not needed by this server task: deployed flow Jobs get their own credentials.

Infrastructure mode requires the existing PostgreSQL metadata settings from
`mise.toml`/`mise.local.toml`, an existing config with `deployment.work_pool`, and
the shared artifact directory. The default config and artifacts paths above
match the deployed server and Jobs. Mount the installation files with that same
layout on the development host. The draft general Paseo pod currently mounts
only `/mnt/tank/shared` at `/data` read-only, so it **does not yet supply this
Miroir data mount**. Mount provisioning is separate from starting this script.
On macOS, also configure reachable database/Prefect endpoints. Do not use an
empty replacement config or a private artifact copy: jobs use the installation's
shared files. If a config copy is used, keep it synchronized with the worker's
config; `MIROIR_PREFECT_CONFIG_PATH` selects the worker-visible path (default
`/miroir/data/miroir/miroir.json`). Missing prerequisites stop startup before a
database connection, with no fallback to a local installation.

This is live installation access: startup runs the normal metadata migrations,
UI edits affect the shared database/configuration, and operations submit to the
configured Prefect pool. Jobs run the deployment's image, **not the checkout's
Python source**; test flow-code changes with the separate deployment workflow.
The development server generates its own UI token in `.miroir/dev-infra/api-token`
and does not request the deployed server's login token.

In Paseo, start service script `dev` or `infra` and open its assigned URL. Both
delegate to these mise tasks using Paseo's assigned host/port. Worktree setup
only installs dependencies; neither server starts automatically.

## Try the first slice

From the repo root:

```bash
uv run --locked pipelines-miroir integrations
uv run --locked pipelines-miroir --config examples/miroir.json validate
uv run --locked pipelines-miroir --config examples/miroir.json connections
```

Output is JSON. Exit status is 0 for success and 2 for an invalid/unreadable
configuration. The default path is `miroir.json` in the current directory;
that root-level local file is gitignored. Pass `--config` before the subcommand.
`integrations`, `validate`, and `connections` never fetch credentials, connect to
services, start a pipeline, or write configuration. The Python module entry point
is `python -m src.cli`.

`integrations` lists connector metadata: settings schemas, credentials, resources,
and **inputs** (decision 021). An input is either a **fetch** (an API pull with the
connection's credentials) or a **file format** (with its own import options).
ListenBrainz, GitHub (personal access token for the configured login; the token's
account is checked), and Bike Share Toronto (mobile-app member ID and authorization
token) fetch. Apple Health reads Health Auto Export JSON (option `role`:
`ongoing`/`historical`); browser history reads Firefox/Zen Places snapshots, plain or
gzip, or the incremental batches the local collector sends; ScreenMemory reads the
collector's batches too (see below). Native
Apple Health XML is not supported. Each connector package under
`src/integrations/<name>/` owns its settings, credentials, check, resources, inputs,
and dlt sources; the registry only lists them. The legacy `src/sources/` and per-source
flows were retired after the 2026-11-11 cutovers.

Files arrive through three core transports, all of which register an artifact and
then import it: upload, the connection's receiving URL, and a **watched folder**. A
folder is connection configuration, not a source setting:

```json
"folder": {"path": "/miroir/data/browser", "pattern": "*.sqlite.gz",
           "options": {}, "keep_last": 5}
```

`pattern` defaults to the format's, `options` to its defaults, and `format` to the
connector's first. Each sync scans the folder, registers new content (identical
bytes map to their existing artifact), imports each pending file as its own import
operation linked to the sync, and builds each linked analysis once. A scan that finds
nothing new loads and builds nothing. Folder files keep the latest `keep_last`
stored copies (`null` keeps all); older imported copies are removed, their receipts
and history kept, and replaying them is refused until the same bytes are sent again
by upload or the receiving URL. A file that stays in the folder is not re-copied or
re-dated by later scans (decision 024). A pre-021 `settings.data_dir` is read as a
folder.

### Running a connection and reading its status

```bash
uv run --locked pipelines-miroir --config miroir.json run music --duckdb /tmp/mirror.duckdb
uv run --locked pipelines-miroir --config miroir.json status
```

`run` resolves the connection's credentials (`miroir`, `env`, or `fnox`), loads into the given
**local DuckDB file only**, and records an operation/attempt. It never writes to the
production warehouse, and it skips dbt (models target Postgres). dlt working state
goes next to the DuckDB file (`<name>.dlt/`). Exit status 1 means the run failed;
the redacted cause is on the attempt record.

`status` shows, per connection: the latest ingestion attempt (origin, ingestion and
transformation outcome, load IDs, redacted error), the last successful ingestion,
and how many ingestion operations have not started. `analyses` lists each configured
analysis, its linked inputs, build time, and `current`, `stale`, `running`, or
`no_data` status. A `running` attempt is marked
`verified: false` until the flow records its outcome or `refresh` (below) closes
it from the orchestrator's state.

Both commands use the metadata store at `--metadata` (a SQLAlchemy URL), defaulting
to `$MIROIR_METADATA_URL` or `sqlite:///.miroir/metadata.sqlite` (gitignored).

### Importing an export file

```bash
uv run --locked pipelines-miroir submit health ~/Downloads/export.json --option role=historical
uv run --locked pipelines-miroir import health <artifact-id> --duckdb /tmp/mirror.duckdb
uv run --locked pipelines-miroir artifacts health
```

`submit` validates the file's format, copies it into artifact storage
(`--artifacts`, default `$MIROIR_ARTIFACT_DIR` or `.miroir/artifacts`, named
by SHA-256 and read-only), and registers it. It does not load anything. Invalid
files are rejected before anything is stored. Submitting identical bytes again
returns the existing artifact; submitting them with different import options is
refused.

`--option NAME=VALUE` sets the format's import options (`--format` picks a format
when a connector has several). For Apple Health, `role=historical` contributes rows
dated before the connection's `cutoff`, `role=ongoing` (the default) rows from the
cutoff onwards.

`import` parses exactly that artifact, with no modification-time cursor, under the
connection lock, into a local DuckDB file. An artifact already imported with the
same parser version and options prints `already_imported` and loads nothing;
`--replay` loads it again (merge-safe: rows keep their source keys). A failed
import never counts as imported. `artifacts` lists each artifact with every import
attempt, its parser version, and options.

### Credentials

Connection secrets (decision 011) are stored encrypted by the miroir under a
master key that you provide:

```bash
export MIROIR_SECRET_KEY="$(uv run --locked pipelines-miroir generate-key)"
# Back this key up separately from the database: stored secrets need it.
```

Add a connection in the web interface (**Add a connection**: settings plus
credentials, then an automatic test), or from the CLI by editing the configuration
with `{"provider": "miroir"}` and storing the value:

```bash
uv run --locked pipelines-miroir set-credential music access_token   # prompts, or reads stdin
uv run --locked pipelines-miroir check music
```

Credentials are write-only: no page, API response, log, or file shows them again.
Connection pages show "set · updated …", Replace, and Test. The status turns
`rejected` when a check or a run is refused (HTTP 401/403) and recovers after a
passing check or a successful load. External references still work, for values
managed elsewhere: `{"provider": "fnox", "key": "LISTENBRAINZ__ACCESS_TOKEN"}` or
`{"provider": "env", "key": "..."}`.

Key rotation: set `MIROIR_SECRET_KEY="<new>,<old>"`, run
`pipelines-miroir rotate-secrets`, then drop the old key.

### Schedules and the Prefect orchestrator

A connection can declare a `schedule` (decision 013), validated with `croniter`
and `zoneinfo`:

```json
"schedule": {"cron": "0 3 * * *", "timezone": "Europe/Paris"}
```

Prefect is the single orchestrator, on a cluster or on one machine (`prefect
server` plus a process worker). The miroir only reconciles with it. Execution
settings live in the configuration (decision 016) — a `deployment` section —
so every writer (the interface, the CLI) plans the same deployments:

```json
"deployment": {
  "work_pool": "miroir-k8s",
  "image": "ghcr.io/h-wb/pipelines:sha-<full-commit>",
  "job_variables": {"namespace": "automation"}
}
```

`image` and `job_variables` are optional. The section is what enables the Prefect
integration and is the only source of these settings; they map onto the
deployment's `work_pool_name` and `job_variables` in Prefect, with `image` kept
inside the job variables. On the cluster, the deployment section should carry a
digest-pinned image — the cluster's registry mirror serves stale mutable tags.

```bash
uv run --locked pipelines-miroir deployments plan
uv run --locked pipelines-miroir deployments apply   # or: the Apply deployments button
uv run --locked pipelines-miroir refresh
```

Where flow runs find the configuration is an environment concern
(`MIROIR_PREFECT_CONFIG_PATH`, default the local file path) because it differs
between the operator's machine and the Job's mount.

Each connection gets one deployment of `sync_connection`
(`sync-connection/<miroir>-<connection>`) with a concurrency limit of 1.
`plan` reports creates, updates, and removals against what Prefect has — hand
edits in Prefect show as drift; `apply` performs them. With the pool configured,
`serve` submits requested syncs as Prefect flow runs (idempotent on the operation
ID) instead of subprocesses. `refresh` closes attempts whose flow run ended
without recording an outcome: crashed becomes `lost`, cancelled `cancelled`. It
also closes requests that never got a run after an hour (no flow run, no
attempt) as `blocked` with `NeverStarted`, so they stop showing as requested.

On the cluster this is deployed (decisions 012, 015): the image
`ghcr.io/h-wb/pipelines` is built in CI from `uv.lock` (Python 3.12, Prefect
matching the server; `PIPELINES_CODE_REVISION` baked in), a Kubernetes work pool
`miroir-k8s` gives each flow run its own Job with the NFS data export mounted
at `/miroir/data` (configuration and artifacts live under
`/miroir/data/miroir/`), and a dedicated worker runs it. Nothing secret lives
in Prefect, and no database password is copied anywhere: the miroir-k8s base
job template in home-ops injects the warehouse credentials from the
CNPG-generated `postgres-metabase-app` secret, and the metadata credentials from
the `postgres-miroir-app` secret of Miroir's own CNPG cluster — the miroir
composes its metadata URL from the
`MIROIR_METADATA__{HOST,PORT,DATABASE,USERNAME,PASSWORD}` parts
(`MIROIR_METADATA_URL` still wins when set). The only other value a flow run
needs is the miroir master key, in the `miroir-secret` Secret, owned by the
Bitwarden `miroir` item through the ExternalSecret. Reconcile from
a checkout of the repo (its gitignored `miroir.json` must match the NFS copy):

```bash
mise run miroir:deploy     # deployments apply (plan first to review drift)
mise run miroir:refresh    # close unverified runs
```

The tasks get the metadata password from the ProtonPass item
`Automation General/miroir/metadata-password` (the app-user password of the
postgres-miroir cluster — create the item from the cluster's
`postgres-miroir-app` secret after bootstrap); the other
`MIROIR_METADATA__*` parts are non-secret defaults in `mise.toml`.

Operator mise tasks require the installed dotfiles' global `proton-login`
task template. This repo's `pass:login` extends it with session name `dlt`;
tasks that request fnox secrets depend on login. The shared implementation
selects macOS Keychain service `pass-cli-dlt` or the pod's `PROTON_PASS_TOKEN`,
refreshes rotated/expired sessions, and clears the named cache on missing or
rejected credentials. It does not alter token grants or deployed flow credentials.

Installation secrets (the master key, Postgres roles, Prefect and Metabase
credentials) are environment variables provided by whoever deploys. They are not
stored by the miroir.

### Web interface and HTTP API

```bash
export MIROIR_API_TOKEN="$(openssl rand -hex 24)"
export MIROIR_SECRET_KEY="$(uv run --locked pipelines-miroir generate-key)"
uv run --locked pipelines-miroir --config miroir.json serve --duckdb /tmp/miroir.duckdb
# http://127.0.0.1:8765 — log in with the token. A missing miroir.json is created empty.
```

Without a configured work pool, requests run as local subprocesses and need
`--duckdb` (a local DuckDB file). With `deployment.work_pool` in the configuration,
they are submitted as Prefect flow runs instead.

Behind a TLS-terminating proxy, set Uvicorn's `FORWARDED_ALLOW_IPS` to the trusted
proxy addresses/networks. The cluster trusts its ingress pod network; this makes
generated receiving URLs use HTTPS and login cookies carry Secure. Do not rely on
the localhost-only default when the proxy runs in another pod.

On the cluster the server is deployed (home-ops `automation/miroir`): the
`miroir` service at `https://miroir.<domain>` behind envoy-internal, on the
same image as the flow runs, with the metadata store in postgres-miroir and
artifacts/configuration on the NFS export. It waits for the `miroir-secret`
Secret — place it by hand until the Bitwarden `miroir` item exists:

```bash
kubectl -n automation create secret generic miroir-secret \
  --from-literal=MIROIR_SECRET_KEY="$(uv run --locked pipelines-miroir generate-key)" \
  --from-literal=MIROIR_API_TOKEN="$(openssl rand -hex 24)"
```

The overview lists connections and analyses separately. Connections own source
settings, credentials, delivery, ingestion, and schedules. Analyses link connections
to packages and own interpretation, build status, output relations, and dashboard
links (decision 022). Configuration edits detect conflicting saves.

### Analyses and explicit input links

Creating a connection creates its recommended analysis. **Add an analysis** on the
overview selects a package and explicitly links each input role to a compatible
connection. Multiple analyses may read the same connection. Open an analysis to
change its inputs, timezone, or typed parameters (including GitHub author emails),
rebuild it, inspect its outputs and build history, or remove its configuration.
Removing an analysis keeps its tables and history. A connection cannot be removed
while another analysis links it; its own default analysis is removed with it.

Analyses are top-level configuration entries. For example, with connections named
`health` and `music`:

```json
"analyses": [
  {"id": "music", "package": "music", "inputs": {"music": "music"},
   "timezone": "America/Toronto", "dashboards": {"Music": 32}},
  {"id": "together", "package": "crossovers",
   "inputs": {"health": "health", "music": "music"},
   "timezone": "America/Toronto"}
]
```

Packages are `music`, `health`, `github`, `biking`, `browser`, `messages`,
`crossovers`, and `timeline`. The first six require one input. Crossovers requires Health and
optionally reads music and biking. The timeline's inputs are all optional, and at
least one must be linked (see *The timeline package* below). Unlinked inputs are empty and their measures are unknown (null),
including yearly counts and late-listening categories. They never fall back to
legacy datasets or another account. A linked input with no successful load is shown
on the analysis page. Health dates keep the exported local date; the analysis's
IANA timezone controls local music and biking days/hours.

Each successful Postgres load builds every analysis linking that connection once,
as a separate child `models` operation. Folder syncs build after all files load;
a scan with no new files builds nothing. A failed analysis leaves ingestion
successful and does not prevent other analyses from building. Model-only rebuilds
resolve no source credentials and leave ingestion times unchanged. **Rebuild
analysis** uses one linked connection's Prefect deployment; **Rebuild from
scratch** also rebuilds incremental tables from scratch. Local DuckDB execution
skips Postgres models.

A build waits for its connection's lock and then for the dbt slot. Once it holds
each, a build run by Prefect or by `execute` reads the configuration again. If
the analysis's settings changed meanwhile, it ends as blocked, and a load whose
build was blocked this way shows its transformation as skipped, with the reason.
A connection edit alone does not stop a build, since what a build applies is in
the analysis's receipt. A requested rebuild or a scheduled build is still
blocked when its connection changed before it started, as any request is. With a minimum build interval
(decision 040), a load joins only a scheduled build bound to its own flow run.
Two loads deciding at once do not both build: the one that builds is registered
before the other decides.

An analysis is current only after a successful build with the current plan receipt,
using data at least as recent as every linked input's last successful load. A load
arriving during a build conservatively leaves it stale until another build. Failed
or interrupted builds also make it stale, since dbt may have replaced some outputs.

```bash
uv run --locked pipelines-miroir --config miroir.json analyses
uv run --locked pipelines-miroir --config miroir.json models together
uv run --locked pipelines-miroir --config miroir.json models together --full-refresh
```

Each analysis reads its inputs' actual raw bindings and writes isolated
`miroir_<analysis>_<identity>_<group>` schemas. **Model outputs** lists exact relations.
Even a legacy raw binding leaves legacy reporting schemas and dashboards intact.
Packages select their models and ancestors through dbt; they do not maintain a
second SQL dependency graph. Absent optional raw fields/resources are typed nulls
or empty inputs; present tables missing required identity/time fields fail clearly.

`links.metabase` and each analysis's `dashboards` provide dashboard links;
`links.prefect_ui` provides run links. Miroir does not yet reconcile dashboard assets.
[`examples/analysis-dashboards.json`](../../examples/analysis-dashboards.json)
contains SQL/chart seeds using logical `{health}`/`{music}` schema placeholders.
Resolve them against the analysis's output schema, create new Metabase assets, and
store their IDs on that analysis. Managed dashboard ownership/conflicts and portable
installation/restore remain later milestone-6 slices.

A configuration without an `analyses` key upgrades on read: each connection gets its
recommended analysis under its connection ID, carrying its old `analysis` settings
and `dashboards`. Output namespaces, source revisions, and existing build receipts
stay stable. Service edits write the upgraded shape. An explicit `"analyses": []`
means no analyses; connection-level analysis fields are invalid once the list exists.

Requests run as `pipelines-miroir execute <operation>` subprocesses against
the same local DuckDB file; with a `deployment` section in the configuration they
are submitted as Prefect flow runs instead. Requests for one connection queue behind each other.

The JSON API takes `Authorization: Bearer $MIROIR_API_TOKEN`:

| Method and path | Purpose |
| --- | --- |
| `GET /api/overview`, `GET /api/connections/{id}` | Status, history, files |
| `POST /api/connections/{id}/sync` | Request a sync (202, operation ID) |
| `GET /api/packages`, `GET /api/analyses`, `GET /api/analyses/{id}` | Package inputs/parameters; analysis status, inputs, outputs, and build history |
| `POST /api/analyses` with `If-Match: <file_revision>` | Add `{id, package, inputs, timezone}` |
| `PUT /api/analyses/{id}` with `If-Match: <file_revision>` | Update inputs, timezone, parameters, or dashboard links |
| `DELETE /api/analyses/{id}` with `If-Match: <file_revision>` | Remove configuration; keep tables and history |
| `POST /api/analyses/{id}/models` | Rebuild without ingestion (202, operation ID); requires a successful input load; `?full_refresh=true` rebuilds incremental tables from scratch |
| `POST /api/connections/{id}/artifacts?import=true&name=…&<option>=…` | Store the raw request body as a file, optionally import it |
| `POST /api/connections/{id}/artifacts/{artifact}/import?replay=false` | Request an import |
| `POST /api/connections/{id}/delivery-token`, `DELETE /api/connections/{id}/delivery-token` | Create/replace (value shown once) or revoke a receive-only token; requires administrator auth |
| `POST /api/connections/{id}/receive` | Receive an ongoing export and request an import; requires that connection's delivery bearer token |
| `GET /api/config`, `PUT /api/config` with `If-Match: <file_revision>` | Read or replace the configuration |
| `GET /api/integrations` | Integrations with settings schemas and credential fields |
| `POST /api/connections` | Add a connection: `{"integration", "id", "settings", "credentials"}`; returns the check result |
| `PUT /api/connections/{id}/settings` with `If-Match: <file_revision>` | Patch non-secret settings; keeps other connection configuration |
| `PUT /api/connections/{id}/resources` with `If-Match: <file_revision>` | Save `{"resources": ["commits", "stars"]}`; null selects all; keeps data and cursors |
| `POST /api/connections/{id}/prepare` | Prepare execution for this connection only |
| `PUT /api/connections/{id}/credentials/{name}` | Replace a credential: `{"value": "..."}` (204) |
| `POST /api/connections/{id}/check`, `DELETE /api/connections/{id}` | Test or remove a connection |
| `POST /api/refresh`, `GET /api/deployments/plan` | Reconcile with Prefect (needs the `deployment` section) |

### Uploading and automatically receiving Apple Health exports

Add an **Apple Health (Health Auto Export)** connection, then choose how data
arrives: **Upload files** or **Health Auto Export**. **Advanced delivery methods**
offers **Watch a server folder**. Enter a connection ID and cutoff date; only the
folder method asks for a server path. Source credentials are not required.

Continue into first-data setup. Upload setup offers a file picker immediately.
Automatic setup offers **Enable automatic delivery**, which prepares execution
for this connection and generates its receiving token. Follow the sender steps,
then choose **I've configured my sender — check delivery** to wait for the first
automatic export and its import result. Viewing or retrying setup never replaces
an active token. Manual uploads do not count as proof that a sender has connected.
ListenBrainz follows the same journey with account settings, source credentials,
and a first-sync action. Execution actions prepare only their connection, so there
is no separate global Apply deployments step for first use.

On an existing connection, **Activity** contains receipts, uploads, imports, and
history. **Settings** contains account/parser settings, source credentials,
**Data delivery**, and removal. Non-secret edits use the same conflict checks as
the configuration editor. Uploads remain available alongside automatic delivery;
server folder scanning can be added or removed independently. A receiving-only
check describes readiness, never proof of phone connectivity.

On the connection page, **Upload an export** accepts a Health Auto Export JSON
file and defaults to importing it immediately. Historical files contribute dates
before the cutoff; ongoing files contribute dates from the cutoff onwards.

For automatic delivery, copy the URL and Authorization header shown during setup.
For an existing connection, open **Settings → Data delivery → Set up automatic
delivery**. In Health Auto Export, create a
REST API automation with JSON format and these settings:

```http
POST https://miroir.example/api/connections/health/receive
Content-Type: application/json
Authorization: Bearer <connection-delivery-token>
```

The body is the raw JSON export. No query parameters or multipart wrapping are
needed; `?format=<id>` picks one of the connector's formats when it has several,
and without it the first is used (decision 032). This endpoint always receives
ongoing data and requests an import. Its
response contains `artifact_id`, `new`, and `operation_id` (or `already_imported`
for an export already loaded with the current parser and options). A 201/200
acknowledges receipt and submission, not completion of ingestion. Files and
History show the import outcome; the page also shows when the last export was
received, through either upload or automatic delivery. Uploads are capped at
512 MiB. Identical exports reuse the retained artifact; an already successful
import is not repeated.

Delivery tokens only authorize their own connection's receive endpoint. They
cannot read history, edit configuration, import historical files, or replay data.
Tokens are shown once, stored only as hashes, and can be replaced or revoked on
the connection's **Settings → Data delivery**. Replacing a token immediately invalidates the previous one;
removing a connection revokes its token. The installation-wide API token remains
for administration and the older artifact API. Receiving files does not require
`MIROIR_SECRET_KEY` when no source credentials need decrypting.

The phone must be able to reach the URL. This installation uses an internal
gateway, so use the home network or VPN. n8n can use the same HTTP endpoint as an
interim sender without installing the CLI or sharing the artifact directory.
Confirm a real receipt and successful import before disabling the old n8n workflow
or changing legacy dataset bindings (decision 009). The CLI's `submit` only
registers a file; its `import` command targets local DuckDB.

Metadata schema 6 includes hashed delivery tokens and separate first/latest receipt
times (version 4), file options and folder provenance (version 5), and analysis IDs
on build operations (version 6). Earlier build records are backfilled under their
connection IDs so upgraded default analyses retain their history and freshness.
Release the server, worker, and flow-run image together: older runtimes refuse
schema 6. Before rollout, drain active Miroir runs and back up configuration and
metadata. Downgrading requires restoring that matching backup; old runtimes cannot
read the migrated database. Production connector cutover remains separate.

The Prefect flow `src/pipelines/miroir.py:sync_connection` uses the same runner
against Postgres and builds analyses that link the loaded connection. Miroir manages its deployments through plan/apply; the repository has no other
Prefect deployments (decision 042).

Configuration version 1 contains a miroir ID, connections, analyses, and optional
deployment settings and service links. Each connection has an integration, validated
settings, credential references, and optional explicit pipeline/dataset binding,
resource selection (`resources`: a subset of declared resources; unselected resources
keep their stored state and rows), watched folder, and schedule. Analyses own input
links, timezone, typed parameters, and dashboard links.
Fetched resources can be selected during setup or in connection settings. The API
also accepts `resources` when creating a connection. Omission/null keeps the
existing all-resources default; an explicit selection must be nonempty. Deselecting
keeps stored rows and sync progress, which analyses may still read. The file import
path continues to parse all resources in an artifact.
Credentials use `provider: "miroir"` for the built-in encrypted store without a
key, or `provider: "env"` / `"fnox"` with a `key` naming the external value. Inline
credential fields and unknown settings are rejected. The configuration's content
revision is a hash of normalized settings/references/bindings, not a deployed revision.

Generated names distinguish accounts and miroirs. Existing connections can be
mapped explicitly, for example:

```json
"binding": {
  "pipeline_name": "listenbrainz",
  "dataset_name": "listenbrainz_data"
}
```

This only describes a binding. It does not adopt, reset, or modify any live state.
The validator rejects duplicate connection IDs, pipeline names, and dataset names
within a miroir. The first design assumes one warehouse per miroir.

Configurable destinations, machine-local overlays, and stored immutable revision
snapshots remain outside this schema. Unsupported fields fail validation rather than being ignored.
Add each through a documented schema change when its operation is implemented.

### Sending device data with the local collector

Some sources exist only on a personal machine. The collector in `collector/` (the
`miroir-collector` package, decisions 030, 031 and 035) reads them there and posts
what is new to a connection's receiving URL. It installs on its own, with two
small libraries and none of the server's; [its README](../../collector/README.md)
covers installing it.

```bash
miroir-collector run     --config collector.json [--source ID] [--max-batches N]
miroir-collector collect --config collector.json --source ID --out DIR
miroir-collector resend  --config collector.json --source ID --from DATE [--to DATE]
miroir-collector status  --config collector.json
```

`run` is one pass: deliver a pending batch if there is one, otherwise collect, write
the batch to a spool and deliver it. The cursor moves only after Miroir answers 200
or 201; anything else leaves the batch in the spool for the next pass. `collect`
writes the batches to a folder and sends nothing, to inspect what would leave the
machine. `resend` collects a date range again and delivers it without moving the
saved position (decision 034): use it after improving a masking rule, to repair a
gap, or to load older history later. `status` shows each source's cursor and
last outcome. None of them prints source content.

A source's optional `since` (a date, or an instant with a UTC offset) is where
it starts when it has no saved position. Starting recent and re-sending older
months later gets a connection live quickly.

```json
{
  "state_dir": "~/Library/Application Support/miroir-collector",
  "sources": [
    {
      "id": "screen_macbook",
      "source": "screenmemory",
      "receive_url": "https://miroir.example/api/connections/screen_macbook/receive",
      "token_env": "MIROIR_TOKEN_SCREEN_MACBOOK",
      "options": {"data_dir": "~/ScreenMemoryData", "since": "2025-01-01"}
    }
  ]
}
```

To set one up: add the connection in Miroir (one per machine and source), create its
delivery token under **Settings → Data delivery**, put the token in the environment
variable the configuration names, and have the operating system's scheduler start
`run` about hourly. `receive_url` must be `https`. The token can only deliver to
its own connection. Each delivery requests an import, so a first pass over a long
history is best started by hand with a larger `--max-batches`.

Sources today: `screenmemory` ([what it sends and what it withholds](../sources/screenmemory.md)),
`zen_history` ([browser history](../sources/browser-history.md#through-the-collector-decision-032)),
and `apple_messages` ([Apple Messages rows](../sources/apple-messages.md)).
The collector adds the source's format to the receiving URL itself.

### The messages package

The `messages` package reads one Apple Messages connection (decision 041) and
builds six tables in its `…_messages` schema. Creating an Apple Messages
connection adds it. Local days and hours are in the analysis's timezone.

The tables hold no message text, only lengths and flags. Names come from the
connection's `contacts` when the collector sends them (`names`); without them a
person is their number or address.

- **conversations**: one row per conversation. A conversation is the chat's own
  identifier, so one person over iMessage, SMS and RCS, or the same chat seen from
  two Macs, is one row. `conversation_key`, `display_name` (a group's title, else
  its participants' names; a direct conversation's contact name, else its
  identifier), `is_group`, `participants`, `first_message_at`,
  `last_message_at`, `messages_sent`, `messages_received`, `attachments`,
  `services` (for example `SMS, iMessage`).
- **messages**: one row per message: not reactions, group events, calls or poll
  votes. `guid`, `conversation_key` (null for the few messages in no chat),
  `is_group`, `sent_at`, local `day`, `hour` and `weekday` (1 is Monday),
  `is_from_me`, `sender_handle` and `sender_name` (null for my own, and when
  Apple stores no handle), `service`,
  `has_text` and `text_length` (null when the text could not be read),
  `attachments`, `is_thread_reply`, `is_edited`, `is_unsent`, `is_audio`, and
  `reply_minutes`: in a direct conversation, for the first message after one from
  the other side, the minutes since that message, null beyond 24 hours.
- **reactions**: one row per reaction that stands, a removed one cancelled.
  `guid`, `conversation_key`, `sent_at`, `day`, `hour`, `kind` (`love`, `like`,
  `dislike`, `laugh`, `emphasis`, `question`, `emoji`, `sticker`), `is_from_me`,
  `sender_handle`, `sender_name`, `target_guid` (the message it reacts to) and
  `target_part` (which part of it, 0 for the first).
- **participants**: one row per conversation and person other than me: everyone
  listed in its chats and everyone who sent in it. `conversation_key`, `handle`,
  `display_name`, `has_contact_name`, `messages_sent`, `first_message_at`,
  `last_message_at`.
- **attachments**: one row per attachment of a message. `attachment_guid`,
  `message_guid`, `conversation_key`, `sent_at`, `day`, `is_from_me`, `kind`
  (`image`, `video`, `audio`, `document`, `other`, from the mime type or the
  extension), `mime_type`, `extension`, `bytes`, `has_media_key` (whether the file
  has a name to be stored under). Link previews are left out.
- **daily**: one row per local day from the first message to the last, quiet days
  included. `day`, `weekday`, `messages_sent`, `messages_received`,
  `conversations_active`, `attachments`, `reactions`.

The models are built only for an analysis. They are rebuilt in full after every
load, in about four seconds for 200,000 messages.

### Media: files that rows point at

Some rows point at a file, such as a screen frame at its screenshot. Miroir keeps
those files in an S3-compatible bucket and never parses them (decision 033). It is
off until the configuration has a `media` section:

```json
"media": {
  "endpoint": "http://garage.storage.svc:3900",
  "link_endpoint": "https://s3.example.org",
  "region": "garage",
  "bucket": "miroir-media",
  "prefix": ""
}
```

`endpoint` is what the server itself calls. `link_endpoint` is the address written
into signed links, which devices and browsers must reach; it defaults to
`endpoint`. `upload_ttl_seconds` (default 900) and `view_ttl_seconds` (default 300)
set how long links last. The credentials come from the server's environment,
`MIROIR_MEDIA_ACCESS_KEY_ID` and `MIROIR_MEDIA_SECRET_ACCESS_KEY`; the server
refuses to start if the section is there and either is missing. The key needs read
and write on that bucket and nothing else.

An object is named `prefix / connection / kind / key`, where the key is the value
the row carries (for `screenmemory`: kind `screenshots`, key `frames.image_path`).

| Route | Who | What |
|---|---|---|
| `POST /api/connections/{id}/media/uploads` | the connection's delivery token | For a list of `{key, size}`: `present`, an upload link, or `rejected` |
| `POST /api/connections/{id}/media/confirm` | the connection's delivery token | Whether each object is now `stored` |
| `GET /media/{id}/{kind}/{key}` | a logged-in administrator | Redirects to a link valid for a few minutes |

There is no table of uploads: the server asks the bucket. An archive that already
exists can therefore be copied into the bucket under the right names with any S3
tool, and its files are found present.

To show a frame's screenshot in a dashboard, build the view address from the row:
`https://<miroir>/media/<connection>/screenshots/<image_path>`. It works in a
browser that is logged in to Miroir.

### Timeline: reading the warehouse

The timeline (decision 040) is a page of the server that reads the warehouse. So
far it only checks that it can. It is off until the server's environment names a
read-only connection:

- `MIROIR_TIMELINE_DATABASE_URL`, a `postgresql://` URL; or
- `MIROIR_TIMELINE_DATABASE__HOST`, `__USERNAME` and `__PASSWORD` (required) and
  `__PORT` and `__DATABASE` (default 5432 and the username), composed the way the
  metadata URL is. The URL wins when both are set.

The role should be able to select from the timeline's tables and nothing else, and
should have `statement_timeout = '5s'` and `default_transaction_read_only = on`.
The server sets both on every session it opens anyway, whatever the role or the
URL say, so one slow search cannot hold a connection and a mistaken grant cannot
lead to a write.

| Route | What |
|---|---|
| `GET /timeline` | For a logged-in administrator: whether the timeline is configured and its database reachable and read-only |
| `GET /api/timeline/health` | The same as JSON; 200 when ready, 503 when not configured, unreachable or writable |

The server starts without the warehouse: a database that is down only shows here.

### The timeline package

The `timeline` analysis package builds the tables the timeline reads. Its inputs
are `screen` (a ScreenMemory connection) and `browser` (a browser history
connection), both optional; link one or both:

```json
{"id": "timeline", "package": "timeline",
 "inputs": {"screen": "screen", "browser": "browser"},
 "timezone": "America/Toronto",
 "parameters": {"reader_role": "miroir_timeline"}}
```

It writes these relations to `miroir_<analysis>_<identity>_timeline`:

| Relation | What |
|---|---|
| `screen_frames` | One row per frame: `captured_at`, `bundle_id` (the app usage row of the same second, or null), `text` (null when withheld), `withheld`, `masked`, `image_path`. Screen text is indexed only here: search with `to_tsvector('simple', text)`, the indexed expression. |
| `events` | One row per event, the same columns for every source: `source`, `connection`, `event_id`, `started_at`, `ended_at`, `kind`, `title`, `body`, `media_kind`, `media_key`, `group_key`, `payload`, `search`. The screen gives one row per app session (`kind` `app_session`, `group_key` the bundle, `media_key` the first frame), not one per frame; the browser one per visit (`group_key` the site). `search` weights the title over the body, `simple` configuration. |
| `days` | Per local day, in the analysis's timezone, and source: `events` that started that day and, for the screen, `frames`. |
| `apps` | Bundle identifier to `name` and `colour`, for a list of common apps and every app seen. Others are named from their identifier, with a palette colour. |

An app session is a run of frames of one app with no pause over five minutes.
Frames without an app neither start nor end one. Redirects, reloads, embeds and
framed links are not visits. Columns whose names start with `_` are the builds'
own bookkeeping.

**Builds.** It builds at most every five minutes, however many loads arrive
(decision 040), and each build is incremental. It reads only rows of loads that
dlt lists as complete in `_dlt_loads`, in the first build too, and remembers the
latest completion time it has read. Each build reads the loads completed after
that time, less ten minutes: a load that completes after a later one (a retried
load, say) is still read. A load that never completes is never read.

- A resent frame replaces its row. An app usage row that arrives after its frame
  gives the frame its app and moves the sessions around it.
- A session still open at the last build grows. A page's new title reaches its
  visits.
- A frame or visit that moves to another time leaves its old session and day. For
  frames, this is not expected from ScreenMemory. For visits, it happens when
  Places reuses the IDs of cleared visits. The old times stay on the row until
  every model that recounts them has read it, so a build that fails part way
  loses none.
- The result is what a first build over the same rows gives.

`timeline_screen` and `timeline_browser` are the two sources' own models and are
not meant to be read.

**Unlinking or replacing an input.** Every row records the connection and raw
dataset it came from. When the analysis no longer links that input, or links
another connection or dataset in its place, the next build removes that input's
rows from every table and reads the current input from its start. No button is
needed.

**Rebuild from scratch.** A requested rebuild can rebuild the incremental tables
from scratch (dbt `--full-refresh`): **Rebuild from scratch** on the analysis
page, `models <analysis> --full-refresh` on the command line, or
`POST /api/analyses/{id}/models?full_refresh=true`. The operation records it.
Use it after a change to these models or to the known apps: old rows otherwise
keep what they were built with.

**Raw-table indexes.** Builds create indexes on the raw tables they look rows up
by when those are missing: load, second and image path for frames; page for
visits; load ID and completion time in dlt's list of loads.
- Each index is built once, outside the model's transaction, and committed at
  once. Writes to that raw table wait while it is built, so the first build over
  a large table holds loads for that long.
- An index left invalid by an interrupted build is rebuilt.
- Creating an index needs the build's role to own the raw table. A table owned by
  another role is not indexed, and builds read it correctly but more slowly.

On a test machine with a million frames, an ordinary build took under half a
second per model. A million frames arriving at once took about 50 seconds; a
first build over them took about 15. An ordinary build reads no table in full
except `days` (one row per day and source) and dlt's own small bookkeeping
tables.

**Read access.** `reader_role` names the role the server reads the timeline with
(the installation uses `miroir_timeline`). Each build leaves the four relations
readable by the owner and that role only, with `SELECT`, and grants `USAGE` on
their schema, so nothing has to be granted by hand. The two source models are
left readable by the owner alone.
- The role must exist before the build that names it, or the build fails.
- With `reader_role` empty, nothing is granted, and the next build takes back the
  `SELECT` an earlier build gave. It leaves the schema's `USAGE`.
- Make it a dedicated role, with no privileges of its own beyond logging in. Do
  not grant it other roles, or default privileges, in the warehouse. The builds
  clear such grants from the timeline's tables, but not from the raw tables.

Limits:

- Load completion times come from the clocks of the workers that loaded them. A
  load recorded as completing more than ten minutes before one already read is
  read only by a rebuild from scratch.
- A build that reads a raw table while a load is still rewriting a row can miss
  that row's old version, for example a frame's app or a visit's page. Nothing is
  locked against ingestion: the next build, once that load is complete, corrects it.
- If more than 5,000 loads are waiting, a build reads every completed load once.
- A change spread over more than 50 separate stretches of the screen history is
  recomputed as one span, from the first to the last.
- Where the clocks go back at midnight, a day starts at the later of the two
  midnights. The hour before it counts on the previous day.
- The timeline is built only for a Miroir analysis; dbt run without one leaves
  these models out.

### Web pages and the content security policy

Every response carries `Content-Security-Policy: default-src 'self'; img-src
'self' blob:; ...` and `Referrer-Policy: same-origin` (no address of a Miroir page reaches another
host). The policy refuses inline
styles and scripts, so templates load them from `src/web/static/` (served at
`/static/`, without the login): `miroir.css`, `miroir.js` and the vendored
`htmx.min.js`, whose version and SHA-256 are in `VENDORED.md`. A template with a
`style` attribute, a `<style>` element or an `on…=` handler fails
`tests/test_web_static.py`.

## Validate in an isolated environment

Use a task-specific virtualenv so this does not change a shared worker environment:

```bash
export UV_PROJECT_ENVIRONMENT="$(mktemp -d /tmp/miroir-validation.XXXXXX)"
uv sync --locked --python 3.12 --no-dev --group test
RUNTIME__DLTHUB_TELEMETRY=false "$UV_PROJECT_ENVIRONMENT/bin/python" -m unittest discover -v
"$UV_PROJECT_ENVIRONMENT/bin/pipelines-miroir" --config examples/miroir.json validate
```

The test group adds DuckDB for synthetic source compatibility checks. HTTP calls
from the API source are intercepted; account tokens are fake and destinations are
temporary files. The checks exercise real dlt extraction/loading, independent
account namespaces, repeat imports, and restoration of committed cursor state into
a fresh local working directory. `tests/test_artifacts.py` covers explicit imports: invalid, duplicate, and
overlapping files, changed contents under an unchanged modification time, failed,
interrupted, and crash-window imports, parser version changes, and the metadata
schema upgrade. `tests/test_operations.py` drives the shared runner the
same way: retries, request adoption, same-connection locking, dbt failure, redaction,
metadata outage after a load, and restart persistence. None of this proves
Postgres-specific behavior (including advisory locks), Prefect worker execution,
artifact replay, or crash recovery of pending packages.

For lint and type checks after `uv sync --locked --dev`:

```bash
uv run --locked ruff check src/core src/integrations src/analyses src/cli src/pipelines/miroir.py tests
uv run --locked ruff format --check src/core src/integrations src/analyses src/cli src/pipelines/miroir.py tests
uv run --locked mypy --follow-imports=silent src/core src/integrations src/analyses src/cli src/pipelines/miroir.py
```

## How to continue this PR

For every slice, append a dated entry to the implementation log describing the
problem, changed behavior, validation commands/results, limitations, and next step.
Record architectural choices in the numbered decision log; supersede old decisions
explicitly rather than silently erasing their rationale. Update roadmap status
when its completion criteria are actually met. Keep production cutover a distinct,
reviewable step with its own baseline comparison and recovery procedure.
