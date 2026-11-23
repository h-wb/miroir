# Workspace design decisions

## 001 — Versioned JSON configuration with strict typed validation

Status: accepted for the first slice, 2026-10-28.

Use one explicit JSON file, `schema_version: 1`, and Pydantic 2 models. JSON is
available on all supported Python versions and is straightforward for a later API
or configuration editor. TOML offers comments but needs a reader dependency on
Python 3.10; YAML has a larger parsing surface. Neither is necessary for this slice.
Revisit format ergonomics before building write/edit operations, with an explicit
migration if the persisted format changes.

Reject unknown fields and type coercion; validate integration settings against the
registry. Credentials are named references only. Diagnostics omit input values,
unknown field names, and user-supplied credential names so malformed configuration
cannot print a pasted token. CLI summaries omit account settings and reference keys.

The initial reader has no implicit environment overrides, file merging, secret
lookup, persistence, or network access. A future resolver can implement env/fnox
references inside a connection's execution boundary. Schedules/destinations/overlays
will be added with their concrete operations, not guessed in advance.

## 002 — Explicit identities and opt-in legacy bindings

Status: accepted for the first slice, 2026-10-28.

Workspace and connection IDs use lowercase letters, digits, and underscores, up to
24 characters. Defaults derive `ws_<workspace-ID-length>_<workspace-ID>_<connection-ID>`
for both dlt pipeline and dataset names. The length prefix prevents ambiguous names
such as workspace `a_b` plus connection `c` versus workspace `a` plus connection `b_c`.
Names remain within Postgres's 63-byte identifier limit. They are not derived from
usernames or tokens, so credential changes need not change state identity.

An explicit binding preserves legacy pipeline/dataset names. Validation rejects
colliding pipeline or dataset names within the workspace, including collisions
between explicit and generated bindings. The first product has one destination per
workspace; cross-workspace ownership of manually supplied bindings remains a future
execution-time check. Configuration validation does not authorize adopting live data.

A canonical content hash identifies normalized desired configuration. Object key
order, whitespace, and connection list order do not affect the hash; settings,
reference changes, and bindings do. Runtime revision storage and deeply immutable
execution snapshots are not implemented by the reader's frozen model objects.

## 003 — Small bundled registry and lazy source adapters

Status: accepted for the first slice, 2026-10-28.

Start with ListenBrainz and Apple Health in an explicit registry. Definitions expose
settings schemas, credential names, resources/keys, input modes, and underlying
capabilities. Do not load arbitrary modules from configuration or implement a plugin
installer yet. The CLI must be usable without importing dlt or Prefect.

`src/integrations/sources.py` adapts a validated connection to the existing dlt source
with explicit settings and already-resolved credentials. It does not mutate process
environment variables or resolve secrets. Keep existing source/flow implementations
intact to preserve production behavior. Execution, destination bindings, concurrency,
and credential resolution belong to the next layer.

Apple Health's current directory scanner remains a compatibility path. Explicit
artifact import will use a dedicated parser with origin/cutoff metadata and will
bypass modification-time scanning. (Implemented by decision 008.) Native XML and generic CSV are separate format
implementations, not implied by the integration's presence in the catalog.

## 004 — Lock application dependencies; validate outside the worker

Status: local validation accepted; production runtime selection pending, 2026-10-28.

Commit `uv.lock` and use `uv sync --locked` in a separate Python 3.12 environment for
validation. Keep the package's Prefect compatibility range while locking this tested
application environment. Do not install or upgrade dependencies in the shared worker
as part of development. The observed worker image uses Python 3.12 / Prefect 3.9.0.

Use a small test group for DuckDB-backed synthetic integration tests and stdlib
unittest for the suite. Add Pydantic as an explicit application dependency instead
of relying on Prefect's transitive dependency. Remove the obsolete `dlt[workspace]`
extra, which the resolved dlt 1.31.0 does not provide; retain marimo separately.

A local venv is evidence of dependency/source compatibility, not an actual Prefect
worker isolation solution. Validate the process/container execution mechanism,
mounts, secret injection, and restart behavior separately before production changes.

## 005 — Operation and attempt records in a small SQLAlchemy metadata store

Status: accepted for the execution slice, 2026-10-28.

An **operation** is one requested or observed sync of one connection; an **attempt**
is one execution of it. A Prefect flow run maps to exactly one operation (unique
`backend_run_id`), so Prefect retries become attempts 2, 3, … rather than new
operations. A CLI-requested operation is created first and adopted by the flow run
that receives its `operation_id`; adopting it from another connection or binding it
to a second backend run is rejected. Schedules and n8n triggers pass no operation
ID and register their own on entry, so every execution path is visible.

Each attempt records ingestion (`running|succeeded|failed|blocked`) and
transformation (`pending|succeeded|failed|skipped|not_run`) separately, plus dlt
load IDs and a redacted error (credential values replaced with `***`, length
capped). Status is a projection of these records; Prefect keeps execution logs and
dlt keeps cursors. Nothing here copies dlt state.

Store: SQLAlchemy Core, one code path for SQLite (local/tests) and PostgreSQL
(schema `workspace_meta` in the existing instance). `schema_info` holds a version;
`migrate()` creates version 1 and refuses unknown versions. Real migrations
(Alembic or hand-written) come with the first schema change. SQLAlchemy is now an
explicit dependency; it was previously only transitive through Prefect.

Not yet: per-resource partial-load tracking beyond dlt's own package resume.
Reconciling `running` attempts and submitting requested operations to Prefect
are now built (decision 013).

## 006 — Runner ordering: register, lock, load, record, model

Status: accepted for the execution slice, 2026-10-28.

`src/core/runner.py:run_connection` is the only execution path:

1. Register or adopt the operation. If the metadata store is unavailable, nothing
   loads: new executions require registration.
2. Take the connection lock (keyed by dlt pipeline name). If held, record a
   `blocked` attempt and raise `ConnectionBusy`; do not queue or wait. PostgreSQL
   uses a session advisory lock; SQLite uses `flock` on a file beside the database.
   Both release when the holding process dies. Legacy flows do not take this lock,
   so a workspace connection bound to a legacy pipeline name must not run alongside
   its legacy deployment. Cutover disables the legacy deployment first.
3. Resolve credentials for this connection only, then run dlt.
4. Record load success. If that write fails, log it and return `recorded=False`;
   never report a committed load as failed.
5. Run dbt only when the connection writes the dataset the dbt source reads
   (`ModelBinding` in the registry); otherwise `skipped` with the reason. A dbt
   failure is recorded and leaves ingestion `succeeded` and analysis `stale`. This
   replaces the legacy `refresh_models` behavior of logging and moving on.

The dbt selector stays `source:<name>+`, matching the legacy flows, so it still
includes crossover models that need other sources. Narrowing to self-contained
models is milestone 2 work tied to the dbt project, not to the runner.

## 007 — Prefect entry point exists but is not deployed

Status: accepted, 2026-10-28; deployment blocked on runtime decisions.

`src/pipelines/workspace.py:sync_connection` wraps the runner with Postgres, dbt,
and the flow run ID. It is deliberately absent from `prefect.yaml`. Deploying it
requires, in order: a decision on worker isolation (home-ops change or existing
work-pool facility), worker-side credential resolution (fnox on the worker or
explicit injection; `resolve_credentials` supports both `env` and `fnox`), a
workspace configuration and `WORKSPACE_METADATA_URL` on the worker, and an isolated
Postgres schema check. `run` in the CLI targets local DuckDB only until then.

## 008 — Content-addressed artifacts and explicit, versioned imports

Status: accepted for the import slice, 2026-10-28.

**Receipt and import are separate records.** An artifact records bytes received
for one connection: SHA-256, size, input format, role, original name, storage path,
retention (`keep`), and received time. Identity is (workspace, connection, SHA-256):
identical bytes from two accounts are two artifacts. Imports are operations
(`kind` `import` or `replay`) carrying `artifact_id`, `parser_version`, and
`options`, executed by the shared runner under the connection lock.

**Storage.** Files are copied to a temporary name in the destination directory,
fsynced, and renamed to `<root>/<workspace>/<connection>/<sha256>` with mode 0440.
An interrupted copy is deleted and never registered. Validation runs before the
copy, so invalid files are neither stored nor recorded. Import re-hashes the
stored copy and refuses a missing or changed file. Local disk only for now.

**Role is metadata, not a path.** For Apple Health, `historical`/`ongoing` replace
the `/manual/` path test, with the cutoff still taken from connection settings.
Both paths share `select_rows` in `src/sources/apple_health.py`, so the rule is
defined once. The artifact source has no incremental cursor and does not touch the
legacy scanner's `filesystem` state, so explicit imports and the compatibility
scanner can coexist on one connection.

**When an artifact counts as imported.** It counts as imported when an attempt
succeeded with the same parser version and the same options (role and cutoff).
Bumping `PARSER_VERSIONS` or changing the cutoff makes the next plain `import` load
it again. `--replay` always loads. Replays are merge-only: they cannot remove rows
an older parser produced wrongly. That needs the scoped reconciliation the plan
describes, not yet built. If the load committed but its completion was not recorded
(crash window), the artifact is not marked imported and the next import reloads it;
merge keys make that safe for the current Apple Health resources.

**Schema version 2** adds `artifacts` and the three operation columns. `migrate()`
upgrades version 1 in place (create missing tables, `ALTER TABLE ... ADD COLUMN`,
bump the version) in one transaction, and refuses versions it does not know.

Not yet: archive formats and resource limits, retention/deletion operations, a
Prefect entry point for imports, migrating the n8n drop folder to `submit`,
transformation-only retries, and native Apple Health XML or other formats.

## 009 — n8n is a temporary receiver for Health Auto Export

Status: accepted, 2026-10-28.

Today n8n has one job. Workflow "Apple Health → Files: Ingest Exports" receives
Health Auto Export's webhook POSTs, writes each body unchanged to the NAS share
mounted on the worker, then starts `load_apple_health` through the Prefect API. It
exists only because nothing else in the stack accepts HTTP uploads.

The workspace HTTP API (milestone 4) replaces it with an upload endpoint for the
connection's artifacts. That endpoint is the same `submit` (and optional import)
path as the CLI and web form, so phone pushes become one more acquisition path for
the artifact inbox. This removes a service from the health data path and the
shared-folder write access. Every receipt is also recorded, so a silent stop like
the `N8N_RESTRICT_FILE_ACCESS_TO` separator bug (no writes from 2026-06-07 to
2026-09-22) shows up as "no export received since …" instead of going unnoticed.

Until the workspace server is deployed, the existing workflow and legacy
deployment keep running unchanged. Cutover: point Health Auto Export at the
workspace endpoint, confirm receipts and imports, then disable the n8n workflow
and the legacy Apple Health deployment. Do not run both against the same
connection's dataset (decision 006). n8n is not a dependency of the workspace
design; other automations in n8n are out of scope.

## 010 — Small server-rendered interface over shared services

Status: accepted for milestone 4, 2026-10-28.

**Stack.** FastAPI, Jinja2 templates, plain HTML forms, and a few lines of CSS. No
JavaScript build and no client framework. Every web dependency was already in the
lockfile through Prefect; they are now declared explicitly. A richer frontend can
replace the templates later, because the JSON API exposes the same operations.

**One service layer.** `src/core/service.py:WorkspaceService` is what the HTTP API
and web pages call: overview, connection detail, request sync, submit file,
request import, read/save configuration. Execution goes through an `Executor`:
`SubprocessExecutor` (server: one `pipelines-workspace execute` process per
operation, so a crash affects one operation) and `InlineExecutor` (tests). A
Prefect executor that creates flow runs of `sync_connection` replaces the
subprocess one when the runtime decisions land (decision 007).

**Request, then execute.** The runner now splits `request_sync`/`request_import`
(record the operation, visible as requested) from `execute_operation` (run one
attempt). `run_connection` and `import_artifact` remain as request-plus-execute.
Requested executions wait up to an hour for the connection lock instead of being
blocked. A demo push of two exports at once showed the second one blocked and
never retried before this change.

**Configuration identity.**
- Edit conflicts compare a SHA-256 of the exact file bytes (`file_revision`),
  under a lock, with atomic replacement. Any change by someone else, including
  formatting, is a conflict. An invalid file can still be repaired.
- Each connection has its own revision (its normalized configuration plus the
  workspace ID). Operations record it. A request runs only if its connection is
  unchanged; otherwise its attempt is `blocked`. Edits to one connection do not
  block another's queue.
- "Saved vs applied" compares the connection's revision with that of its latest
  executed attempt (`applied`, `saved_not_applied`, `never_run`).
- `links` (Prefect UI, Metabase) and per-connection `dashboards` are optional,
  additive schema fields. They do not affect execution revisions.

**Auth.** Single user, one token from `WORKSPACE_API_TOKEN` (16+ characters,
required to start). API clients send it as a bearer token. Browsers exchange it
once for an `HttpOnly`, `SameSite=Strict` cookie holding an HMAC derived from the
token, never the token itself. Cookie-authenticated POSTs with a foreign `Origin`
are refused. Redirect notices are fixed codes, so nothing from the URL is reflected.
Jinja autoescaping covers user-supplied names. The server binds to 127.0.0.1 by
default; put TLS in front of it before exposing it.

**Analysis states.** `not_modelled` is new: the connection's dataset is not one
the dbt models read. Previously this showed as `stale`, which implied it would
catch up.

Not yet: a structured config editor (JSON textarea for now), multi-user accounts,
exercising the flow against this installation (expired credentials, outages) on
the test setup. The Prefect executor and reconciliation are now built
(decision 013).

## 011 — Connection secrets: encrypted in the workspace, pluggable backends

Status: accepted, 2026-10-28. Replaces the idea, discussed but never built, of
using fnox as the product's secret store.

This follows the established pattern of comparable tools. n8n
(`N8N_ENCRYPTION_KEY`) and Airflow (`fernet_key`, plus pluggable secrets backends)
store connection secrets encrypted in their own database under one master key and
offer external managers as alternatives. Airbyte stores only references in
connector configuration. The goal is that another person can add a connection by
pasting a token, as in those tools, without running a secret manager.

**Two kinds of secret.**
- *Connection secrets* (a person's ListenBrainz or GitHub token) are entered in
  the UI or CLI and stored by the workspace. Only the job that runs the
  connection decrypts them, just before creating the source.
- *Installation secrets* (the master key, Postgres roles, Prefect and Metabase
  API credentials) are provided by whoever deploys, as environment variables from
  a Kubernetes Secret, a `.env` file, or fnox/mise locally (twelve-factor). The
  workspace never stores these.

**Built-in store.** A `secrets` table in the metadata database holds one ciphertext
per (workspace, connection, credential name), encrypted with `cryptography`'s
Fernet (AES-128-CBC with HMAC-SHA256, the scheme Airflow uses). No custom
cryptography. The key comes from `WORKSPACE_SECRET_KEY`; a comma-separated list
enables rotation (first key encrypts, all decrypt; `rotate-secrets` re-encrypts).
The key never goes in the database or the configuration file. Back it up
separately: without it, stored secrets are unrecoverable.

**References, not values.** Configuration holds `{"provider": "workspace"}` for a
built-in secret, whose identity is implied by connection and credential name.
Secrets therefore cannot be shared across connections by mistake. `env` and
`fnox` references with explicit keys remain supported as external backends.
Vault/OpenBao, Kubernetes, or cloud secret managers can be added behind the same
interface (the Airflow secrets-backend model). Nothing returns a stored value:
UI and API show only "set / updated at".

**Integrations declare credentials** as named fields with a label and help text.
They are not `SecretStr` fields in the settings model, as first sketched. Settings
are persisted to the configuration file; keeping credentials in a separate
structure makes "never persisted there" structural rather than a filtering rule.
Resolved values are passed in memory only.

**Check and health.** An integration may declare a check: ListenBrainz calls
`/1/validate-token` and confirms the account name, and Apple Health confirms its
folder is readable. A failed check, or an HTTP 401/403 during a run, makes the
connection's credential status `rejected` until a later check or run succeeds.
This is a status of its own, with a "Replace" action, instead of a generic
failed run.

**Lifecycle.** Adding a connection validates the configuration, stores its
secrets, then saves the configuration (revision-checked). If the save fails, the
just-stored secrets are removed. Removing a connection deletes its configuration
entry and its secrets. Loaded data, artifacts, and history are kept; purging them
is a separate operation, per the plan.

**Each component gets only what it needs.** The server and worker get the master
key and metadata database. dlt gets a Postgres writer role, dbt a transformer
role, and Metabase a read-only role. Only the worker running a connection
decrypts that connection's secrets.

Not yet: OAuth flows (consent, refresh-token rotation), external backends beyond
env/fnox, and per-user key separation for a hosted multi-user version.

## 012 — Keep Prefect; run each flow in its own Kubernetes Job from a versioned image

Status: accepted, 2026-10-28. Settles runtime question 1 of decision 007.

**Prefect stays** as the cluster orchestrator. It is deployed, Python-native,
works with dlt, and its UI is the "execution details" page the workspace links
to. Dagster's asset, freshness, and lineage model overlaps the workspace itself
(it would duplicate or replace the product layer). Airflow is heavier.
Kestra and Windmill are less Python-native. Bare CronJobs would mean rebuilding
retries, run history, and logs. The failures so far (a dependency conflict that
broke the worker, the server's SQLAlchemy pin) come from installing code into one
shared worker environment. Changing orchestrator would not fix them; isolation
does.

Prefect is also the single-machine orchestrator (decision 013): a `prefect server`
on SQLite plus a process worker, bundled for installation. The workspace does not
build its own scheduler.

**Runtime.** Observed: one long-lived process worker (`local-pool`) that clones
`main` and `uv pip install`s into its own environment on each run. All flows
share its 6 Gi limit, and it has no permissions to create Jobs. Replacement:

- One image, `ghcr.io/h-wb/pipelines`, built in CI from `uv.lock` (Python 3.12,
  Prefect matching the server), following the `sento` image workflow. The same
  image runs flow runs, the Kubernetes worker, and later the workspace server.
  Nothing is installed at run time. The image digest identifies code and
  dependencies (`PIPELINES_CODE_REVISION` is set at build time).
- A Kubernetes work pool (`workspace-k8s`). Each flow run is its own Job with its
  own resource limits; a crash affects one run.
- home-ops: a second worker controller running the image
  (`prefect worker start --type kubernetes`), a namespaced Role to manage Jobs,
  Pods, and logs, an ExternalSecret (Bitwarden Secrets Manager, the cluster's
  existing source) with the installation secrets from decision 011, and the
  existing NFS export mounted for artifacts.
- The legacy deployments stay on `local-pool` until their connection is cut over;
  both pools run side by side.

## 013 — Prefect is the orchestrator; the workspace only reconciles with it

Status: accepted, 2026-10-28. An earlier draft (same day, never built) also
specified a workspace-owned "local backend" with its own cron loop for
installations without Prefect. That draft was dropped because it reinvents what
Prefect already does on one machine: a SQLite-backed `prefect server` and a
process worker give the same scheduling, retries, run history, and logs as the
cluster. One orchestrator in every installation means less code and one
behaviour to reason about.

The workspace owns the **desired** state: connections and their optional
`schedule` (`{"cron": "...", "timezone": "..."}`, not part of the execution
revision). Prefect owns **execution**. The glue does three things, all through
Prefect's own APIs:

1. **Deployments from configuration.** Each connection gets one deployment of
   `sync_connection` (`sync-connection/<workspace>-<connection>`). Its parameters
   (`connection_id`, config path), schedule, work pool, image, and a concurrency
   limit of 1 (Prefect queues overlapping runs of one connection) all come from
   the workspace. `plan` lists creates, updates, and removals against what Prefect
   has, so hand edits in Prefect show as drift. `apply` performs them. A
   deployment per connection, rather than one deployment with per-schedule
   parameters (also supported by Prefect 3.9), keeps per-connection pause,
   concurrency, and run history in the Prefect UI.
2. **Submit.** A request becomes `run_deployment(..., timeout=0,
   idempotency_key=<operation ID>)`, so retrying an ambiguous submission returns
   the same flow run. The flow run ID is bound to the operation; the flow adopts
   it on entry (decision 005). Scheduled runs register their own operation.
3. **Observe.** Attempts still `running` (and submitted operations with no attempt)
   are compared with their flow run's state. Crashed becomes `lost` and cancelled
   becomes `cancelled`, both kept apart from `failed`. Pending or running is left
   alone.

The subprocess executor remains for development and tests only (manual requests,
no schedules). It is not a product mode.

## 014 — The product is Miroir; "workspace" is retired

Status: accepted, 2026-10-29.

Until now "workspace" was both the working title of the product ("the personal
data workspace") and the name of everything inside it: the configuration unit,
the environment variables, the metadata schema, the CLI. The product is
**Miroir** — one installation mirrors one person's data — and the old name is
gone entirely rather than half-kept:

- Python (`MiroirConfig`, `MiroirService`, `load_miroir`, `miroir_id`, …),
  the CLI binary (`pipelines-miroir`, config file `miroir.json`, local state
  in `.miroir/`), the environment (`MIROIR_*`), the metadata schema
  (`miroir_meta`), the built-in secrets provider value (`{"provider":
  "miroir"}`), the Prefect work pool (`miroir-k8s`), and the cluster
  resources in home-ops.
- One unit of configuration is a miroir: one `miroir.json`, one owner, its
  connections inside. The name no longer means two things.

Earlier entries in this log keep the wording of their time; paths moved with
`docs/workspace/` → `docs/miroir/`.

## 015 — The metadata store gets its own Postgres, not Metabase's

Status: accepted, 2026-10-29.

The metadata store is Miroir's control plane (operations, attempts, artifact
registry, connection checks, encrypted connection secrets). It ran in
postgres-metabase because that instance already existed, but that couples a BI
database's lifecycle to the pipeline bookkeeping and widens the blast radius of
either. Following home-ops' standard pattern, Miroir gets its own CloudNativePG
cluster — `postgres-miroir` in the automation namespace, bootstrapped with the
`cnpg/initdb` component, backed up to Garage via Barman like every other
cluster — and the job template reads its credentials from the generated
`postgres-miroir-app` secret.

The **warehouse data** (dlt datasets, dbt models, Metabase dashboards) stays on
postgres-metabase; moving it would be a data migration with dashboard
re-pointing and buys nothing. The destination credentials keep coming from
`postgres-metabase-app`.

`MIROIR_METADATA__*` (host, port, database, username, password) compose the
metadata URL, so the database password still exists nowhere outside CNPG.

## 016 — Execution settings live in the configuration, not the writer's environment

Status: accepted, 2026-10-29.

Until now the deployment job's work pool, image, and job variables came from
each writer's environment (`MIROIR_PREFECT_POOL`, `MIROIR_IMAGE`,
`MIROIR_PREFECT_JOB_VARIABLES`). With two writers — the interface and the CLI —
that made desired state writer-dependent: disagreeing environments would apply
different deployments and fight over drift forever.

The configuration now carries a `deployment` section (`work_pool` required;
`image` and `job_variables` optional), authoritative per field. The
environment variables remain as fallback defaults for fields the section omits
and for a first setup, and either source enables the Prefect integration. Like
schedules, the section is not part of a connection's execution revision.
Consequences: any writer produces the same plan; apply is writer-independent;
and on the cluster the image pin (a digest, never a mutable tag — the
registry mirror serves those stale) is one config edit instead of three
environment pins. `MIROIR_PREFECT_CONFIG_PATH` stays an environment concern:
where flow runs find the configuration genuinely differs between the operator's
machine and the Job's mount.

## 017 — File delivery is independent of parsing; connection-scoped receive tokens

Status: accepted, 2026-10-29.

An Apple Health connection accepts Health Auto Export JSON without a server folder.
Browser uploads and automatic HTTP delivery enter the same retained-artifact and
import pipeline. `data_dir` becomes optional; when present it enables the existing
directory scanner as an advanced compatibility mode. Checks for a receiving-only
connection describe readiness to accept exports, not successful access to a phone.
Existing folder configurations and their execution revisions remain valid.

The connection page offers file upload and automatic-delivery setup together.
`POST /api/connections/{id}/receive` accepts raw JSON, retains it as an ongoing
artifact, and requests an import. Receipt acknowledges storage/submission, not
completed ingestion. Identical exports retain the existing duplicate/import rules.
The existing administrator artifact API remains available for historical imports.

Each connection can have one receive-only bearer token: generated randomly, shown
once on creation/replacement, stored only as a SHA-256 digest in metadata schema
version 4. It authorizes only that connection's receive endpoint, never configuration,
history, credentials, replay, or another connection. Replacing/revoking it invalidates
the previous token; removing the connection revokes it. No installation master key
is needed to receive files. Setup instructions show the endpoint and headers and
explain that the phone must be able to reach Miroir. Native Apple XML, arbitrary
JSON webhooks, and automatic discovery of input formats remain separate work.
Version 4 also preserves first and latest receipt times separately, so repeated
identical exports update delivery health without changing artifact identity or
re-importing successful data.

## 018 — Delivery-aware onboarding; connection activity and settings have separate homes

Status: accepted, 2026-10-29. Refines decision 017's presentation.

Adding a connection selects an integration and a delivery method, collects the
settings/credentials needed for that method, then guides the user to first data.
API credentials grant Miroir access to a source; receive tokens grant a sender
access to one connection. They remain different services and are explained in
their respective setup steps. Uploads remain available alongside automatic delivery.

Integrations declare delivery methods and supported file-format presentation in
the registry. Method-specific settings (such as a server folder) are separate
from interpretation/account settings in forms. The selected method is navigation,
not an exclusive persisted connection mode: configured credentials, receive tokens,
and optional folder settings determine which methods are active. Existing config
files and execution revisions are preserved; no schema migration is needed.

The connection's activity page contains current status, incoming files, imports,
and run history. Settings contains account/parser settings, source credentials,
data-delivery methods, token management, and removal. Settings edits validate and
save through the existing revision-checked configuration service; unrelated
connections, bindings, schedules, and credentials are preserved.

Setup prepares execution for only its connection, using plan/apply with connection
identities on create/update changes. It never applies another connection's drift
or deletes deployments. Sender setup is retryable when execution is unavailable;
existing tokens are not silently replaced by a retry or by viewing a page. First
receipt and first successful import are separate evidence, and readiness to receive
is never shown as proof that a phone is connected. One-time token responses stay
uncacheable and never refresh automatically.

## 019 — Analysis follows a connection's binding; source-local builds are independent

Status: accepted, 2026-10-29.

Real Health Auto Export delivery is verified. Its generated raw dataset must feed
analysis without rebinding it to a legacy dataset. Integration metadata declares
a source-local dbt selector and reporting relations. The core resolves a plan with
the actual source binding, stable output namespace derived from miroir/connection
identity, and analysis settings. The dbt adapter executes that plan; transports
and the runner do not infer SQL dependencies.

Health and music build their models and ancestors, never every descendant of a
raw source. Existing crossover models require health, music, and bike-share data;
they stay on the legacy path until those inputs can be explicitly bound together.
A second account is never silently combined with the first. Legacy dbt invocations
retain their source/output schemas. Miroir writes isolated schemas even when
reading an explicit legacy raw binding, preserving existing dashboards.

dlt infers columns/tables from received data. Staging projects typed nulls for
absent optional fields and empty typed inputs for absent resources. Present tables
missing required identity/time fields fail clearly. Missing metrics remain null;
an absent input does not establish zero activity. Health keeps exported local
dates; music uses the connection's analysis timezone (UTC by default).

Successful imports/syncs rebuild source-local analysis. An explicit model rebuild
runs under the same connection lock without loading data or resolving source
credentials, with independent operation history. Freshness requires a successful
build after the last load using the current configuration and analysis-plan version.
Ingestion success survives model failure. Activity presents build status, output
relations, rebuild, and configured Metabase links. Analysis settings stay separate
from source settings. Dashboard packaging/reconciliation and explicit cross-source
input selection remain subsequent work (product-plan milestone 6).

Source execution revisions exclude analysis settings, preserving existing source
operation identities. The analysis receipt captures timezone/plan changes instead.
Queued rebuilds pin that receipt and refuse changed analysis settings; source
loads build the analysis plan resolved when they execute. Model-only operations
never mark changed source settings as applied. A failed/interrupted build makes
analysis stale even without a newer load, because dbt may have replaced only some
relations before failing.

## 020 — Migrate connectors through explicit capabilities and owned lifecycle hooks

Status: accepted, 2026-10-29. Extends 003, 017–019 for milestone 5.

The API/file/reference workflow is verified end to end. GitHub, Bike Share Toronto,
 and Firefox/Zen snapshots now exercise the contracts before analysis packaging.
Each integration module owns typed source settings, credentials, checks, resource
semantics, delivery metadata, and lazy execution hooks. The bundled registry only
collects definitions. Shared runners/transports dispatch through those hooks;
there are no integration-name/settings-type branches in product services.

File-format definitions own validation, parser version, import options, and the
artifact opener. Delivery produces an artifact; parsers consume that exact artifact.
Execution opens sources in a context so temporary SQLite copies and resources are
closed after success or failure. Browser snapshots merge profile-local identities
into a connection's namespace; artifact imports read all snapshot rows, including
older history, instead of applying a folder scanner's cursor. Gzip and plain SQLite
are two encodings of the supported Firefox Places snapshot format, not arbitrary
browser databases. Validation is bounded and SQLite is opened read-only.

Connections may select declared resources; unselected resources keep their stored
state/data. Resource declarations distinguish retained events, entities, and snapshots.
No destructive replacement or upstream-deletion propagation is implied. GitHub API
access checks that the token's account matches the configured login. Legacy GitHub
export seeding (including bundled Git repositories and truncate/reseed behavior)
stays an explicit legacy operation; ordinary sync never invokes it. Bike Share's new
adapter uses merge/pagination with explicit TLS settings; existing entry points
retain their bindings and compatibility defaults.

Source-local dbt definitions belong in an independent analysis catalog. Integrations
recommend an analysis by logical ID; the catalog owns selectors, versions, outputs,
and typed analysis parameters. GitHub author-email matching is an analysis setting,
not an ingestion credential or a process-global environment override. Existing
health/music output identities and source revisions are preserved. Migrated models
accept actual raw bindings and absent optional resources/fields; unknown data is
not silently presented as measured zero. Cross-source packages remain a later slice.

Validation uses isolated destinations and actual source/model executions. Production
cutover remains separate: inventory mappings, drain writers, preserve state/backups,
compare existing history, then change schedules/triggers. Adding a definition or
binding alone never adopts a legacy deployment, resets its cursor, or disables it.

## 021 — Connectors declare inputs; core owns file delivery

Status: accepted, 2026-10-29. Refines 017, 018, 020. Requires metadata schema 5.

A connector declares what it can read, a connection chooses how data arrives, and
core owns every transport. A connector definition has identity settings (facts
about the account or source: a username, a cutoff; never paths), credentials,
resource declarations, an optional analysis recommendation, a check, and a list of
**inputs**:

- a **fetch**: pulls from an API with the connection's credentials, incremental
  through dlt state, selectable by resource, schedulable;
- zero or more **file formats**: an ID, parser version, validator, typed
  **import options** (per-file choices declared by the format), a parser over one
  stored artifact, and the descriptive metadata senders need. Connection settings
  that change how every file is read (Health's cutoff) are recorded with each
  import, as before.

Capabilities, delivery choices, and setup methods are derived from the inputs;
connectors no longer declare `input_modes`, capability strings, or delivery
methods. A connector may have both a fetch and formats (GitHub's API plus its
account-export archive, planned).

Files reach Miroir through three core transports, all of which register an
artifact first (validated, content-addressed, deduplicated) and then import it:
upload, the connection's receiving URL, and a **watched folder**. A folder is
connection configuration (`folder: {path, pattern, format, options, keep_last}`),
not a source setting. A sync scans it, registers new content, and imports each
pending artifact as its own import operation (linked to the sync as its parent),
then builds the analysis once. A scan that finds nothing new loads nothing and
does not mark the analysis stale. The per-connector folder readers, Health's
`manual/` layout convention, and the incremental-folder vs full-artifact split
for browser snapshots are removed: a file is read the same way however it
arrived. Browser snapshots are therefore always read in full and merged.

The built-in historical/ongoing **role** is replaced by format-declared import
options, stored with the artifact. Health keeps `role` as its option, so existing
import receipts still match. Identical bytes resubmitted with different options
are still refused. Folder files take the folder's configured options; received
files take the format's defaults.

Folder-delivered artifacts are retained per connection, by default the latest
five; older ones that were imported have their stored copy removed (the receipt
and import history stay, replay is refused). Uploaded and received files are
kept. A legacy `settings.data_dir` is read as a folder with the format's default
pattern, so existing configurations keep working.

Cutover of the legacy GitHub, Bike Share, and browser loads binds each connection
to the legacy pipeline/dataset (history, `export_*` tables, and dlt cursors carry
over), pauses the legacy schedule before the first Miroir run, compares counts
and cursors, and only then removes the legacy deployment.

## 022 — Analyses are configured instances of packages with linked inputs

Status: accepted, 2026-10-30. Milestone 6, first slice. Refines 019 and 020.
Requires metadata schema 6.

A **package** (the analysis catalog) declares a dbt selector, output relations, a
version, typed parameters, and **inputs**: named roles, each accepting certain
integrations through one dbt source, required or optional. Single-source packages
(music, health, GitHub, biking, browser) have one required input; the cross-source
package combines health (required) with music and biking (optional).

An **analysis** is configuration: a top-level `analyses` list whose entries name a
package, link each role to a connection, and carry their timezone, parameters, and
dashboard links. This is the only analysis concept; connections no longer carry
analysis settings. Creating a connection adds its package's default analysis.
A configuration without an `analyses` key is upgraded on read: each connection
whose integration recommends a package becomes an analysis with the connection's
ID and its former analysis settings and dashboards. Output namespaces derive from
the miroir and analysis IDs exactly as they derived from the connection ID before,
so existing relations and dashboards keep resolving.

Linking is explicit; nothing is inferred from which connections happen to exist.
An unlinked optional input resolves to a schema that never exists, so its models
are empty, and cross-source measures that depend on it are null (unknown), not
zero; it never falls back to a legacy source schema. A linked connection's
integration must be one the role accepts, and a connection cannot be removed while
an analysis links it.

Every build is its own `models` operation with an `analysis_id`. After a
connection loads, each analysis linking it builds once (a child of the load); a
rebuild requested in the interface runs through one of its input connections'
deployments. Analysis status compares the latest successful build with the
current plan receipt and with the latest successful load of any linked input.
Builds are serialized by the existing dbt concurrency limit. Freshness compares
input load times with the start of a build, so an input loading during that build
conservatively requires another build. For migrated combined load/build attempts,
the recorded ingestion time is the safe boundary. Inline child builds inherit their
parent's backend run for observation without duplicating its unique submission ID.
Unlinked sources are explicitly excluded from raw-table discovery, even if a table
exists at the placeholder schema.

## 023 — Explicit local and infrastructure development launchers

Status: accepted, 2026-11-03. Development tooling; no runtime schema change.

`mise run dev` serves the checkout with workspace-local configuration, SQLite
metadata, DuckDB data, artifacts and generated credentials under `.miroir/dev/`.
It requests no fnox secrets and ignores inherited Miroir/Prefect installation
settings. Its configuration cannot select a Prefect deployment.

`mise run dev:infra` serves the checkout against the existing installation. It
depends on the shared Proton login and requests only the metadata password,
installation encryption key, and Prefect URL/auth. It requires an existing
configuration with a deployment and the shared artifact directory; it must not
create an empty installation or fall back to local execution. Its UI login token
is local to the development server, independent of the deployed server's token.
The existing tasks for deployments, workers and legacy pipelines remain unchanged.

Paseo exposes both as named service scripts using its assigned host and port.
Worktree setup installs dependencies only. Infrastructure mode can change the
real installation: metadata migrations and UI edits use the shared database,
and requested operations execute the configured deployed flow image, not the
checkout's local Python code. Config/artifact mounts and token grants remain
operator prerequisites; launching a script does not provision them.

## 024 — A file still in a watched folder is not a new arrival

Status: accepted, 2026-11-11. Refines 021's folder retention.

A scan hashes each matching file in place. Content the connection already has is
reused as it stands: it is not copied again, its delivery time does not move, and
a stored copy that retention removed stays removed. A scan cannot tell bytes that
came back from bytes that never left, and treating presence as arrival made
`keep_last` keep files in name order and kept "last received" from ever going
stale. Resending the same bytes through upload or the receiving URL is a
deliberate arrival and still restores a removed copy, and so does a scan when
the file must be read again (a new Health cutoff or parser version) rather than
refusing it on every sync. Retention keeps the most recently arrived folder files.

Submitting an operation that the executor refuses closes it with a `blocked`
attempt (`SubmitFailed`), so nothing stays requested without a run to observe.

## 025 — Legacy per-source loads are retired; `run_dbt` stays for the mart schemas

Status: accepted, 2026-11-11. Completes the cutovers planned in 020 and 021. The
part that keeps `run_dbt` is superseded by 042 (2026-11-22).

GitHub, ListenBrainz, Bike Share and Health load through Miroir connections bound
to their original pipelines and raw schemas, so history and cursors carried over.
Their legacy deployments, the per-source flows, `src/sources/`, and their tasks
and configuration are removed. Browser history is retired without a replacement
for now; its raw data stays, and it returns as a Miroir connection with a watched
folder. The one-shot GitHub account-export seeding went with the legacy source;
its `export_*` tables stay and dbt still reads them, and a GitHub archive file
format would replace it (021).

The nightly `run_dbt` deployment on `local-pool` stays. Dashboards read the
original mart schemas (`music`, `github`, `health`, `biking`, `browser`,
`crossovers`), which only it builds; Miroir analyses build their own namespaced
copies. It can go once dashboards read analysis outputs. `local-pool` also runs
flows from other repositories.

## 026 — Native dlt resources are the connector authoring boundary

Status: accepted, 2026-11-12. Refines 020 and 021.

A selectable resource is a logical stream to mirror, not an HTTP endpoint or a
destination table. It may combine several requests, use GraphQL, or parse files.
Large connectors keep resource implementations in small modules; a thin dlt source
assembles them. Small connectors need not split files. Use dlt's REST API source,
resources, transformers, and incremental state directly rather than a Miroir
endpoint language or dependency engine.

dlt owns keys, write disposition, cursors, and extraction dependencies. Miroir's
resource catalog owns stable selection IDs, labels, descriptions, warnings, and
retention explanations; duplicated key/cursor declarations are removed from that
catalog. Metadata remains importable without dlt or network access. Source assembly
also performs no HTTP requests; account verification happens during extraction.

Connections select fetched resources during setup or in settings. Omission/null
continues to mean all resources; an explicit selection must be nonempty, distinct,
and declared. Saving checkboxes pins that explicit list. Deselecting does not drop
data or reset state; reselecting resumes the resource's existing state. Analysis
may still read retained data for a deselected resource. File resource selection is
not added by this change. Resource edits use configuration revision checks and
invalidate queued source operations through the existing execution revision.

GitHub is the first modular example. Its source/resource names, keys, table shapes,
and state keys remain unchanged. This is a bundled authoring contract, not an
arbitrary-code upload facility or a plugin installer.

## 027 — Shared connector diagnostics and revision-checked connection edits

Status: accepted, 2026-11-12. Refines 026's implementation.

Use a small `IntegrationError` with a safe message and typed kind for credential
rejection, rate limiting, and malformed upstream responses. GitHub's lightweight
check and extraction share account identity validation. HTTP 429 and rate-limited
403 responses are not rejected credentials; a successful response that consumes
the last quota slot is still successful. Other HTTP errors retain their original
exceptions, and the runner recognizes typed diagnostics through dlt's wrapping.
Safe diagnostics do not include upstream bodies or credential-bearing URLs.

Settings, resource, and folder edits use one private context-managed edit path:
read a snapshot, check its revision, resolve the connection from that snapshot,
mutate only the requested fields, validate, and atomically revision-check/save.
Exceptions abort the edit. HTTP input validates resource selection shape before
passing a typed list/null to the service; configuration remains authoritative for
declared IDs, uniqueness, and nonempty selection. Web edit failures share one
renderer that preserves the submitted values and stale revision for conflict
safety. No new persistence schema or source-state migration is involved.

## 028 — GitHub uses native REST resources, dependencies, and incremental lag

Status: accepted, 2026-11-12. Supersedes 027's no-state-migration scope for this slice.

Simple GitHub streams use dlt's declarative REST resources. An unselected
`owned_repositories` resource drives repository-scoped transformers, separate
from the broader selectable repositories stream. Parent discovery does not load
its own table. No global row ordering is asserted across repositories; pull-request
pagination stops within each repository only.

Issues, comments, and pull requests track native `updated_at` cursors with a
three-day lag; the calendar tracks `date` with a three-day lag. On the first
extraction native state is seeded from the corresponding legacy `last_run` or
`last_date`, preserving the overlap. Legacy keys remain historical migration inputs
and are not advanced afterward. Native state takes precedence on future runs;
failed extraction must roll back migration and cursor updates. Commit/search
resources retain their run-window state because record dates do not establish
when an old commit was pushed or indexed. Downgrading after native cursors advance
would resume older legacy watermarks, causing safe merge re-reads, not deletion.

The dlt HTTP client's retry predicate handles rate-limited 403 responses without
retrying permission failures, retaining standard 429/server retries and Retry-After.
Custom search pacing stays. No parallel extraction or credential-provider change.

## 029 — The Prefect adapter describes deployments in Prefect's own attributes

Status: accepted, 2026-11-16. Refines 013; supersedes 016's environment fallbacks.

An audit of the execution path asked whether the orchestrator seam forces
workarounds, and whether dlt offers a native Prefect runner to adopt instead.

dlt (1.31) ships no Prefect integration. It has an Airflow helper, it detects that
it runs inside a Prefect flow for its own telemetry, and `dlt.hub.run` declares
jobs and triggers for dltHub's own runtime. Running dlt under Prefect is a
`pipeline.run()` inside a `@flow`, which `sync_connection` already is. The runner
(`src/core/runner.py`) imports nothing from Prefect and stays as it is.

The adapter did carry a private vocabulary. One connection's deployment was
written three ways: a `DeploymentSpec` with its own field names, a hand-built
comparable form for it, and a second hand-built comparable form for what Prefect
returned (cron as a tuple, `image` lifted out of the job variables). The spec is
now the keyword arguments of `Flow.to_deployment`, by Prefect's names:
`parameters`, `schedules`, `work_pool_name`, `job_variables` (which holds
`image`, as Prefect stores it), `concurrency_limit`, `tags`. Apply is
`sync_connection.to_deployment(**spec).apply()`. Plan compares the same owned
attributes read from Prefect's response, serialized by Prefect's own models.

The configuration keeps its words: `deployment.work_pool`, `deployment.image`,
`deployment.job_variables`, and a connection's `schedule`. They already sit one
step from Prefect's, a first-class `image` matches Prefect's own `deploy(image=)`,
and one cron per connection is a product choice. Renaming them would cost a
migration of every stored configuration for no gain. `desired()` is the only
place that translates them.

**The deployment section is the only source of execution settings.** Decision 016
kept `MIROIR_PREFECT_POOL`, `MIROIR_IMAGE` and `MIROIR_PREFECT_JOB_VARIABLES` as
fallbacks for omitted fields and as a second way to enable Prefect. No
installation sets them any more (the cluster's server carries only
`MIROIR_PREFECT_CONFIG_PATH`), and a second source is exactly the
writer-dependent state 016 set out to remove. They are gone: a configuration with
a `deployment` section runs through Prefect, one without runs locally.
`MIROIR_PREFECT_CONFIG_PATH` stays, for the reason 016 gives.

**The adapter blocks and runs off the event loop.** It wrapped Prefect's dual
sync/async calls in a check that ran a returned coroutine to completion. Every
web route that reaches the adapter is synchronous or already hands the call to a
worker thread, so the check never fired; it is removed and the contract is stated
in the module instead. The two JSON routes that still ran a connection check (a
network call to the source) on the event loop now use a worker thread like their
form twins.

The seam is typed: `Orchestrator` in `src/core/service.py` is the three verbs
plus `submit`, and the adapter's methods keep their signatures through the error
wrapper. A second backend implements that protocol.

Consequences: plan output uses Prefect's names (`schedules`, `work_pool_name`;
an image change shows under `job_variables`). Nothing stored changes, and a
converged installation still plans nothing.

Kept on purpose: slugged schedules are removed before an update. Prefect refuses
to replace them with unslugged ones, and giving ours a slug would not help,
because Prefect then updates by slug and leaves a hand-added one in place.

## 030 — A local agent delivers device sources through the receiving URL

Status: accepted, 2026-11-20. Builds on 017 and 021; answers the "clients as
producers" item of the product plan.

Some sources exist only on a personal machine: a browser's history database, a
screen recorder's text and app-usage tables, a messages archive. Today one of them
(browser history) is collected by a scheduled task kept in the machine's own
configuration repo. The task copies the tables the connector reads into a file in
a synced folder, and the sync client carries it to a watched folder. That works
for one source and has three costs as sources are added:

- The contract is split. The task names the tables the connector reads, in another
  repo, so a change to either side needs a matching change to the other and
  nothing checks it.
- Nothing is acknowledged. A stopped task, a paused sync client and a quiet source
  look the same until someone notices stale data.
- Every run sends a full snapshot. With no record of what arrived, the task cannot
  send only what is new.

**Miroir ships a small agent for these sources.** It runs on the machine, reads
each enabled local source, and delivers what is new to that connection's receiving
URL (decision 017). It is one more sender to a transport that exists: the file it
sends is registered, validated, deduplicated and imported like an upload. No new
server endpoint is needed.

The product plan asks that a client's transport, identity, batching and delivery
acknowledgement be specified when a real one is built:

- **Transport.** HTTPS `POST` of one file to `/api/connections/{id}/receive`. The
  agent never writes to artifact storage or to a server folder.
- **Identity.** One connection per source instance, so a second machine is a second
  connection, as a second account already is. The agent holds each connection's
  receive-only token and nothing else: it cannot read data, history or
  configuration. It reads tokens from its environment and has no secret store.
- **Batching.** A collector returns the records newer than a cursor as one file, in
  a format its connector declares, together with the next cursor. Delivery runs
  about hourly: each receipt requests an import, and with Prefect configured each
  import is a flow run in its own job (decision 012). A collector may still send a
  full snapshot when that is simpler; the merge handles the overlap.
- **Acknowledgement.** A 2xx receipt is the acknowledgement. Only then does the
  agent advance that collector's cursor and delete the file. Anything else leaves
  the file in a local spool, retried on the next run, so a machine that cannot
  reach Miroir loses nothing. The connection's last-received time is the machine's
  delivery health.

**The agent keeps delivery state only.** Its cursor says what was acknowledged, not
what was loaded; load state stays with dlt. A lost or reset cursor sends records
again, which is safe for a connector that merges on source keys, as the file
connectors do. The agent starts from a configured cutoff and sends a long history
as several bounded batches, so the masking below applies to it too.

**Collectors ship raw records, minus what must not leave the machine.** They copy
the source's rows as they are, selecting columns and nothing more: no cleaning or
summarising. Interpretation belongs to the parser and the models, where it can be
run again over history when it changes. Collectors open sources read-only and
never write inside a source application's folder.

The one exception is safety. Text captured from a screen holds whatever was
displayed, passwords and keys included, and a received artifact is kept and
content-addressed, so a secret that reaches Miroir is hard to remove. A collector
for such a source masks or drops secrets before it writes the file: by fixed
rules, never by a model, and with a count of what it withheld in the file. The
source keeps the original, so a better rule is applied by collecting again.

**It lives in this repo but not in the server's package.** The agent and its
collectors are a separate tree, `src/agent/`, that imports only the Python
standard library, so a laptop does not install dlt, dbt or Prefect to send a file.
Collectors cannot sit inside the connector packages: importing a connector imports
its settings models. A test links the two halves instead: each collector's output
on a fixture must pass its connector's format validator and parser. A second test
fails if anything under `src/agent/` imports outside the standard library. How the
agent is installed on a machine is left to the first implementation; with nothing
to build, a pinned checkout is enough.

**The machine's configuration stays outside.** Which collectors are enabled, the
receiving URLs, the schedule and where tokens come from belong to whatever
configures the machine. The agent runs one pass and exits; the operating system's
scheduler starts it.

Not in scope, on purpose:

- Media that Miroir never parses (screenshots, attachments). Records carry a
  relative path; storing and moving the files is the installation's business.
- Live events. A feed that must arrive within seconds needs a path that does not
  start a flow run per delivery, and is a separate decision.
- Remote control. The server never sends the agent instructions.

Alternatives considered:

- *Keep per-source tasks and a synced folder.* Fine for one source; the costs above
  grow with each one. The watched folder stays supported, and a collector's file
  can still be dropped into one by hand.
- *The agent writes into the synced folder.* Offline queueing comes for free, but
  each batch is then stored three times and delivery is still unacknowledged.
- *The server pulls from the machine.* A laptop sleeps and roams, and the server
  would hold a credential into it.
- *Run the existing CLI on the machine.* Its `submit` registers into local artifact
  storage, not a remote Miroir, and it brings the whole dependency set.

Consequences:

- Received artifacts are kept (decision 021). Hourly incremental batches are small,
  but full snapshots would accumulate. Received files need the per-connection
  retention that folder files have (`keep_last`) before a full-snapshot source
  moves to the agent.
- Where Miroir is reachable only from the home network or a VPN, the spool is the
  normal case for a laptop, not an error path.
- Masking by rule is incomplete: a password typed in plain view matches no
  pattern. Screen text stays the most sensitive dataset in the warehouse and is
  handled that way downstream.
- A stolen machine's tokens can inject data into its own connections and do
  nothing else. Revoking them is per connection (decision 017).

First use: a ScreenMemory connector (OCR text and app-usage tables, both
append-only and keyed by timestamp) with its collector. Browser history moves to
the agent after it.

## 031 — The agent is its own package; part boundaries are tested

Status: accepted, 2026-11-20. Refines 030.

Decision 030 put the agent in `src/agent/`, inside the server's one package. That
left two things unclear. The agent could not be fetched or released on its own: it
was importable only as `src.agent`, from a checkout of everything. And "each part
has one job" was written in the repository map but checked nowhere, so one module
had already drifted: `src/integrations/sources.py` opened a *connection's* inputs
and imported core's configuration and operations, the only place where connectors
reached into core.

**The agent is a separate distribution in this repository.** It lives in `agent/`:
distribution `miroir-agent`, import name `miroir_agent`, command `miroir-agent`, no
dependencies, its own version and its own tests. A machine installs that folder
alone (`uvx --from "git+…#subdirectory=agent" miroir-agent`), or runs it from a
checkout with nothing installed.

The server does not depend on it. There is no workspace and no path dependency, so
the server's `pyproject.toml`, lock file and image are untouched, and neither side
can import the other. Two tests in the server's suite hold them together:

- `tests/test_agent_contract.py` collects a batch with the agent's collector and
  checks the connector's validator and reader accept it. It is the only server
  test that imports the agent.
- `tests/test_agent_package.py` runs the agent's own suite in a subprocess with
  site packages disabled, so the repository's one test command covers both and
  proves the agent needs nothing installed.

**Versions are independent; the file format carries compatibility.** Every batch
names its format and version, and the connector refuses what it does not know. An
agent and a server that disagree fail at delivery instead of loading wrong rows.

**Part boundaries are a test.** `tests/test_boundaries.py` reads every import under
`src/` and enforces:

| Part | May import, besides itself |
| --- | --- |
| `integrations` | `src.core.models` only |
| `analyses` | `src.core.models` only |
| `core` | `integrations`, `analyses` |
| `pipelines` | `core` |
| `adapters` | `core`, `pipelines` |
| `web` | `core`, `integrations` |
| `cli` | `core`, `integrations`, `adapters`, `web` |

Nothing under `src/` imports the agent. A connector's `formats.py` and
`contracts.py` never import dlt at module level. A folder with no rule fails the
test, so a new part has to state what it may use. `src/integrations/sources.py`
moved to `src/core/sources.py`, unchanged, which is what made the first rule true.
`docs/architecture.md` states each part's job beside these rules.

**The agent is a sender, not a transport.** Data reaches Miroir by a fetch or as a
delivered file, and a file arrives by upload, the receiving URL or a watched folder
(decision 021). Those three transports belong to core. The agent pushes to the
receiving URL, as Health Auto Export does; it adds a client, not a fourth way in.

Considered and left out:

- *A uv workspace, or the agent as a development dependency of the server.* Both
  put the agent's path in the server's lock file, which the image build reads, for
  the sake of one test's import. The contract test puts `agent/` on its own path
  instead.
- *A shared package for the file format, used by both sides.* It removes the
  duplicated knowledge of the format, but gives the agent a dependency and a
  second thing to release. The contract test and the versioned format do the same
  job for now.

Open:

- The server's import name is still `src`, which is not a releasable package name
  either. Renaming it touches every import, the image's commands and the Prefect
  deployments' entry points, so it needs its own decision and a coordinated
  release.
- CI lints and type-checks `src tests scripts` by explicit paths. Covering `agent/`
  needs a workflow change.

## 032 — The receiving URL takes a format; browser history gains incremental batches

Status: accepted, 2026-11-20. Refines 017, 021 and 030.

Decision 030 expected browser history to move to the agent as full snapshots, once
received files had the retention that folder files have. Building it showed a
better fit: the agent keeps a cursor, so it can send only what is new. Small
batches need no retention rule, and what leaves the machine can be masked row by
row, which a database snapshot does not allow without rewriting it.

**`browser_history` has a second format, `firefox_places_batch`.** Gzip JSON
Lines: a header, `row` records naming their Places table, and a trailer with the
row count per table. It loads through the same dlt source and resource names as
the snapshot format, so both feed the same `zen_*` tables and a connection can
take either. The snapshot format stays first and unchanged.

**The receiving URL takes `?format=<id>`.** It used the connector's first format
only, which was enough while each connector had one. The upload route already
took the parameter; receiving now does too. No parameter keeps the old behaviour,
and an unknown id is refused before anything is stored. The delivery token's
scope is unchanged.

**The agent names the format itself.** Each collector declares the format it
writes, and delivery sets the parameter on the configured URL. Someone
configuring a machine gives the connection's plain receiving URL and cannot get
this wrong.

**The cursor has to survive reused ids.** Places tables use integer keys without
`AUTOINCREMENT`, so clearing recent history frees the newest ids and the next
visits take them. A cursor on the id alone would skip those visits for good. The
collector therefore also takes any visit whose time is later than every visit
sent so far. The first design missed this; a review caught it.

**It reads a copy, and checks the copy is one moment in time.** The browser holds
an exclusive lock, so the collector copies the database and its WAL. Two files
copied in turn can straddle a checkpoint and still pass an integrity check, so it
compares both files' size and modification time before and after, and retries.

**Addresses are masked on the machine** by default: a password in the address,
the values of secret-named query and fragment parameters, and known token shapes.
Path segments are left alone, since a document's id is what makes a history row
useful. This is narrower than the screen-text rules of decision 030 on purpose.

Consequences:

- Received-file retention is no longer a precondition for browser history. It is
  still missing for any future source that can only send snapshots.
- A page that changes without a new visit (a title, a recalculated score) is not
  sent again until it is visited.
- An imported batch is read once per table. Batches are small; revisit if a
  source ever sends large ones.
- The snapshot path through a watched folder keeps working and masks nothing.

## 033 — Media: files that rows point at live in object storage

Status: accepted, 2026-11-20. Refines 017 and 030.

Some rows point at a file: a screen frame at its screenshot, later a message at
its attachment. Decision 030 left those files out ("storing and moving the files
is the installation's business"), which in practice meant a sync client. That
has four problems. Sync is a mirror, so deleting on the device deletes the copy.
It carries every file, including the screenshot of a frame whose text the
collector withheld. It only covers a folder on one machine, and attachments live
on others. And nothing but a path convention links a row to its file.

**Media is its own thing, separate from artifacts.** An artifact is an input
that gets parsed into rows. Media is never parsed, only referenced. A connector
declares a media kind: a name, the resource whose rows carry the key, and the
column that holds it. `screenmemory` declares `screenshots` on `frames.image_path`.

**It lives in an S3-compatible bucket, named by what the row carries.** The
object name is `prefix / connection / kind / key`, and the key is the value in
the row. The join between a row and its file is that column, with no lookup
table, and a folder copied into the bucket as it stands is already correctly
named.

**The server holds the only storage credentials.** A device asks for upload
links for a list of keys and sizes; the server answers per object `present`,
`upload` with a signed link, or `rejected`. The device sends each file with a
plain HTTP PUT and asks the server to confirm. Both calls are authenticated by
the connection's delivery token, whose scope grows from delivering files to
delivering files and media for that one connection. A device therefore needs no
storage library and no storage key, and can write only the objects the server
signed.

**Viewing goes through the server too.** `GET /media/{connection}/{kind}/{key}`,
for a logged-in administrator, redirects to a link valid for a few minutes. The
bucket stays private.

**The bucket is the record.** The server asks the bucket whether an object
exists and how large it is; there is no registry table. That avoids a metadata
schema change, which would force server, worker and flow image to be released
together, and it means objects placed by other means (a bulk copy of an existing
archive) are simply found.

**The agent uploads after the rows, never before.** Its media pass covers only
frames whose rows were already acknowledged, uses the same rule as the row
collector to skip withheld frames, and treats a signed link as a credential:
never printed, logged or stored.

**The storage library is behind a protocol.** Core defines `ObjectStore` (does
it exist, sign an upload, sign a download); the boto3 implementation is an
adapter, injected the way the orchestrator is.

Considered and left out:

- *A sync client, or a copy tool with its own key on each device.* No
  acknowledgement, the bucket layout agreed silently between two configurations,
  and a storage key on every machine. Garage keys grant read or write on a whole
  bucket, and write includes delete, so a laptop's key could erase the archive.
  A copy tool remains the right way to bring in an existing archive once.
- *Uploading through the server.* One network path instead of two, but every
  byte passes through the web process. Worth revisiting only if devices cannot
  reach the bucket.
- *Naming objects by their content hash.* It gives deduplication, but rows
  already carry a unique path and an existing archive would have to be renamed.
- *A registry table of uploads.* It would let Miroir show media delivery health
  and load a media table into the warehouse. Left until something needs it.

Consequences:

- Integrity is the size check plus TLS. The upload is not bound to a checksum.
- A device must reach `link_endpoint`, not only Miroir.
- Miroir never deletes media. Expiry, if wanted, is a lifecycle rule on the bucket.
- Whether media is arriving is visible in the agent's status, not in Miroir.
- A view link works for a browser already logged in to Miroir, including from a
  page on a sibling hostname, because the session cookie is same-site.

Open: an attachments collector, the timeline models and their dashboard, and the
deployment (bucket, key, configuration).

## 034 — A start date, and re-sending a range

Status: accepted, 2026-11-20. Refines 030.

Decision 030 gave the agent a cursor that only moves forward, and said a better
masking rule "is applied by collecting again". There was no practical way to
collect again: the only lever was deleting the cursor, which sends everything
from the start. The start itself was a raw timestamp on one collector and missing
on the other.

**Every collector takes `since`.** A date is that day in the machine's local time
zone; an instant must carry its UTC offset, so nothing depends on guessing a zone.
It is where a collector starts when it has no saved cursor. Changing it later does
not reach back past rows already sent.

**`resend` collects a date range again without touching saved state.** It reads
and writes neither the cursor, the spool nor the media cursor, holds the
collector's lock while it runs, and delivers each batch directly. Collectors take
a window for this; inside a window they send only what is dated in it. Because
imports merge on source keys, sending a range twice changes nothing, and sending
it after a rule change replaces the rows with their newly masked text.

This gives three uses one mechanism: applying a better rule to old data,
repairing a gap, and loading history out of order (recent first, older months
when convenient).

Limits:

- `resend --media` uploads files that are missing in the range. It cannot remove
  the file of a frame that a new rule now withholds; Miroir never deletes media.
- Tables without a date are outside a window: browser origins and bookmarks are
  not re-sent.
- `resend --out` is a dry run: it writes the batches and records nothing.

## 035 — The agent takes two libraries, and its parts get tested boundaries

Status: accepted, 2026-11-21. Refines 030 and 031.

Decisions 030 and 031 made the agent standard-library only, so that a laptop does
not install dlt, dbt or Prefect to send a file. That reason holds. The rule went
further than the reason, and five features later its cost showed in a read of the
whole package:

- The HTTP code sat on `urllib.request`. It opened a new connection for every
  request, so every screenshot paid a TLS handshake. It needed a handler to switch
  redirects off, and mapped three families of exceptions by hand, in two modules
  that had grown apart: a failed media upload named its step and reason, a failed
  row delivery said only `failed`.
- About 250 lines validated JSON by hand: the configuration, the saved state and
  the server's replies. A wrong value was reported without the key it belonged
  to.

The same read found the drift that 031 had found in the server. `cli.py` had
become the whole program. A collector's options were parsed in the configuration
module, its cursor checked and its source folders named in the command line
module, so a third collector meant edits in five places. Options and results
travelled as untyped dictionaries and tuples, and both collectors carried their
own copy of the batch-file writer.

**The agent depends on urllib3 and pydantic, and on nothing else.** urllib3 gives
one connection pool, no redirects by an argument, and one family of errors; it has
no dependencies of its own. pydantic turns the configuration, the state files and
the server's replies into typed values and names the key that is wrong; the
server already uses it for the same job. A test allows exactly these two imports
beside the standard library and checks the package declares exactly these two.

What this costs:

- The agent no longer runs from a bare checkout. A machine installs it with
  `uv tool install`, which was already the documented way. Its tests run in the
  server's environment, whose lock file pins both libraries; it has no lock file
  of its own.
- More code runs in a process that reads screen text and holds delivery tokens.
  Both libraries are among the most used Python packages; the list is closed by
  the test above, and adding to it needs a decision.
- Python 3.12 or later, the version the server's image runs.

**A collector is self-contained.** It declares its name, its batch format, a model
of its options (with the only copy of their defaults), a model of its cursor, the
folders it reads from, and optionally a media source. Everything else asks the
collector and names none: the registry is the one place that lists them. The rule
that the agent never writes inside a source folder has one implementation, on the
collector's base.

**One client and one failure type.** Every request goes through one client:
delivering a batch, asking for upload links, sending a file, confirming. Every
failure of a pass is a step and a reason from fixed lists, for rows as for media,
so what is printed is always safe to print and always says why. No retries inside
a pass: the next scheduled pass is the retry, as 030 decided.

**The agent's parts have import rules, and a test holds them**, as 031 did for the
server:

| Part | Job |
| --- | --- |
| `cli` | arguments, printing, exit code |
| `runner` | the passes for one collector: run, preview, resend, status |
| `config` | the configuration file as typed values |
| `state` | the state folder: cursor, spool, media position, status, lock |
| `transport` | every request the agent sends |
| `media` | upload a page of files in groups |
| `batch` | one batch file: header, records, counted trailer |
| `collectors` | one module per source, and the registry |
| `masking` | the fixed rules |

Lower parts do not import higher ones, only the registry imports a collector, and
no module imports another's private names. The agent's lint rules also cap a
function's branches, so a function that becomes a second program fails the check.

**Nothing changes outside the code.** The state folder, the batch formats, the
requests, the commands and the configuration file are as they were; an installed
agent carries on from its saved position. Three things do change for the user: a
failed row delivery says why, a configuration error names its key, and connections
are reused.

Considered and left out:

- *httpx.* A pleasanter interface, but five packages where urllib3 is one, and no
  release since 2024.
- *msgspec for validation.* Smaller than pydantic, but a second validation idiom in
  one repository.
- *A command-line library.* The trouble was dispatch written as one long function,
  not argument parsing.
- *Retries in the client.* One line with urllib3 if spurious failures show up; not
  needed while a failed pass costs an hour and loses nothing.
- *A shared package for the batch formats*, as in 031: still not worth a second
  thing to release.

## 036 — A secret scanner beside the agent's own masking rules

Status: accepted, 2026-11-21. Refines 030 and 035.

Decision 030 masks secrets "by fixed rules, never by a model". The rules were all
written here: private key blocks, a value after a name such as `password`, long
random strings, card numbers, and one expression for the token formats of about
a dozen providers. That last rule is the weak one. Providers number in the
hundreds, their formats change, and a format that is short or all hexadecimal
escapes the generic rules, which leave hex alone so that commit hashes and image
digests stay readable.

Maintained catalogues of such formats exist. Four tools were tried on the same
invented samples, with the agent's rules as the baseline:

- *detect-secrets* reports the line a secret is on, not where it is: for a GitHub
  token it records the prefix, for a private key the header line. It looks for
  random strings only inside quotes, and its last release was in May 2024.
- *gitleaks* is in maintenance, and a separate binary; its author's successor
  project is where most of the next tool's rules come from.
- *trufflehog* checks what it finds against the provider over the network unless
  told not to.
- *Kingfisher* has a Python package that runs in the agent's process, returns the
  exact position of each finding, loads the user's own rule files, and carries
  about 500 rules.

On the samples Kingfisher found every well-formed provider token, including one
with a single misread character, and left hashes, digests, identifiers and prose
alone. It found no `PASSWORD=...` assignment, no unknown random string and no
card number, at any confidence setting. The agent's rules found those. Neither
replaces the other.

**The agent masks the union of both.** Its own rules run on the original text
exactly as they did before the scanner existed. The scanner runs once on the
same original text. Whatever either marks is masked, so the scanner can only add:
nothing the agent's rules catch can be lost to it. Where the two overlap, the
region keeps the name of the agent's rule; a region only the scanner found
becomes `[masked:scanner]` and is counted under `scanner`, so the warehouse shows
what the catalogue adds. The batch formats do not change: the counts already
accept any rule name.

A first version ran the scanner as a stage before the token rule, and a review
showed why that was wrong. The scanner often reports part of a longer run, for
example the first 26 characters of a long GitLab token, and what was left no
longer matched any of the agent's rules and would have left in clear. Running
both on the original text and taking the union removes the interaction.

**Every finding is masked, including those the scanner hides in its own
reports.** It marks 47 rules as not visible because they are one half of a pair.
Some are identifiers, but some are the secret half, such as an AWS secret access
key. Masking them all costs a few account names and hostnames and is the only
choice that does not need a hand-kept list.

**The user can add rules.** The agent's configuration names rule files in the
scanner's format. They are fixed patterns like the rest, so "never by a model"
holds, and the same text gives the same output at a pinned version. A rule's id
must start with `custom.`, so a file cannot silently replace a built-in rule, and
rules load at the lowest confidence setting, so a rule marked low still fires.

**Always on, and closed on failure.** There is no switch and no fallback to the
agent's rules alone. If the scanner cannot start, the pass fails and says
`reason=masking`. If it cannot answer for one text, that text is replaced whole by
`[masked:unscanned]` and the pass goes on, so one frame can neither leak nor block
the cursor.

What this costs:

- A third dependency, and a native one of about 40 MB, in the process that reads
  screen text. Decision 035 closed the list at two; this decision opens it by
  one. It is pinned to an exact version, because its Python interface was first
  published in October 2026 and still moves.
- The package also contains code that checks or revokes a found credential
  through the provider's API. The agent imports the scanner and the rule loader
  only, and a test fails if `masking` names anything else from it.
- About four seconds to start the scanner in a pass that has text to mask.
- The catalogue changes with the pinned version, so an upgrade can mask old text
  differently. `resend` (decision 034) applies a new version to a past range.

Open:

- Scanning is documented as offline, and no socket was opened during a scan in
  a check on Linux. A traced run on macOS, with network access denied to the
  process, is still owed before this is relied on.
- The scanner can cache its compiled rules on disk and start faster. It is off:
  the agent writes nothing outside its state folder.

Considered and left out:

- *Replacing the agent's rules.* The measurements above: the generic rules carry
  what no catalogue knows.
- *Vendoring a rule catalogue as data and matching it with Python's `re`.* About
  one rule in twenty does not compile there, and most rely on filters that would
  have to be reimplemented, so the bare patterns would mask too much.
- *Masking only the findings the scanner shows.* That was the first choice and
  it drops real secrets; see above.
- *Checking each reported position against the secret it claims to hold.* The
  scanner is run with secrets redacted from its results, so the agent never holds
  a finding's value. Text that looks like UTF-16 would make the scanner report
  positions in its own decoded copy, so a text holding NUL characters counts as
  one it cannot answer for, and is masked whole.

## 037 — The agent is renamed the collector

Status: accepted, 2026-11-21. Renames what 030 to 036 call the agent.

Decision 030 called the device-side program "the agent", the usual word for a
small program installed on a machine that ships local data to a server. Two
things made it the wrong word here:

- The installations this is built for also run AI agents: an assistant, coding
  agents, and whatever comes next. "The agent" stopped saying which one, and the
  word should stay free for those.
- It suggests a process that stays running. This program runs one pass and exits;
  the operating system's scheduler starts it.

**The program is the collector**: distribution and command `miroir-collector`,
import name `miroir_collector`, folder `collector/`. The name says what it does:
it sits beside the sources, reads them, masks, and forwards to Miroir.

**What it reads from is a source.** The collector used to call one module per
local application "a collector". That is now a source, which is also what
`docs/sources/` already called the same thing on the server side.

| Before | After |
| --- | --- |
| the agent, `miroir-agent`, `agent/` | the collector, `miroir-collector`, `collector/` |
| a collector (ScreenMemory, browser history) | a source |
| `"collectors": [...]`, `"collector": "screenmemory"` | `"sources": [...]`, `"source": "screenmemory"` |
| `--collector ID` | `--source ID` |

Nothing else changes: behaviour, the state folder, the batch formats, the
requests, and the other configuration keys are as they were. The step name
`collect` and the failure reason `source` keep their meaning.

The old configuration keys are not accepted. There is one installation, and an
alias kept for it would outlive its reason. An old file fails with the normal
message, `top level: unknown key collectors`.

Decisions 030 to 036 and the implementation log up to this date keep their
wording; they are a record. Read "agent" there as the collector and "collector"
as a source.

The two programs stay two (decision 031). The question came up whether the
server's command line and this one should be one. They are both command lines,
but on opposite sides of a wall: one runs where the warehouse is, with its
credentials and its whole dependency set; the other runs on a personal machine
with a receive-only token and three libraries. One name for both would either put
the server's stack on a laptop or hold two installs apart by import discipline
alone, for the sake of a shared command name.

## 038 — Apple Messages through the collector, keyed so nothing arrives twice

Status: accepted, 2026-11-21. Builds on 030, 033, 036 and 037.

Messages (iMessage, SMS, RCS) live in a SQLite database on each Apple device,
`~/Library/Messages/chat.db` on a Mac. Three ways to read them were looked at on
the installation this is built for, by counting only:

- *The BlueBubbles server's API.* It is a web layer over the database of the one
  Mac it runs on. That Mac held three months of messages; the account's history
  goes back eight years on another machine. It cannot serve what its Mac does not
  hold, and its attachment download converts files unless told otherwise. It stays
  useful for live events, which are a separate decision (030).
- *imessage-exporter.* It writes text and HTML for reading, and names the
  attachments it copies by row numbers of the database it read, which do not
  survive that database being rebuilt.
- *Reading the database directly, as a source of the collector.* The same layout
  is found on every Mac and inside device backups, and the collector already
  masks, spools, delivers and uploads files.

**Messages are a source of the collector**, `apple_messages`, read from the
database in place, read-only, under one snapshot.

**Nothing may arrive twice.** The account's data exists on several devices, in
backups and in old exports, and the same messages will be sent again: by a second
machine, after a restore, or to add attachments that were missing. So:

- Every table is keyed by the identifiers Apple assigns, never by row numbers. Row
  numbers are local to one copy of the database and change when it is rebuilt,
  which iCloud does. Messages are keyed by `guid`, attachments by `guid`, handles
  by address and service, chats by `guid`.
- One connection stands for the account, not for a machine. This departs from 030,
  where a second machine is a second connection: there a machine had its own data,
  here every device holds a copy of the same data. Rows from any device merge on
  their key.
- The cursor is a row number, because that is what finds new rows cheaply, with
  a digest of the `guid` of up to eight recent rows beside it (a digest, because
  the cursor is printed by `status`). When none of the three most recent is where
  it was, the database was rebuilt, and the source starts again from the
  beginning. The merge makes that harmless. Deleting the latest message does not
  trigger it.
- Chat identifiers are not stable: macOS 26 renames their prefix, so two devices
  disagree. The chat's own identifier (a number, an address or a group id) is sent
  with it, and models match conversations on that.

A later source wins over an earlier one for the same row. Sending an old backup's
rows after the live data would put back the old text of an edited message, so
backups will contribute files, not rows, unless asked. The same holds between two
machines: one that still holds an older version of an edited message can put it
back when it next sends its edited messages, until it receives the newer state
itself. Sending from one always-on machine avoids it. A connection per machine,
with the models choosing the newest version of each message, would remove the case
entirely; it was left out because the files would then be stored once per
machine.

**Text is read on the machine, so it can be masked there.** Most rows hold their
text only in `attributedBody`, a binary archive: all but a handful per year in an
iCloud-synced database. A small reader takes the plain string out of it. On a
database of 10,299 messages it gave the same string as the `text` column for all
4,531 rows that have both, and a string for all 5,756 that have only the archive.
The text then goes through the same masking as screen text (036).

This is more interpretation than 030 allows a source, and it is taken for 030's
own exception: a secret that reaches Miroir is hard to remove, and the archive
cannot be masked without being read. For the same reason the first version does
not send the archive itself, nor `message_summary_info`, which holds every
earlier version of an edited message as further archives. Edit history and rich
formatting are therefore not in the warehouse yet. The database keeps them, and
`resend` (034) will bring them once they can be masked.

Messages are sent as soon as a pass sees them, so the warehouse can be about a
minute behind when the collector runs every minute. A message can still be edited
or unsent after that. The cursor keeps a digest of the edit and unsend times of
the messages already sent; when it no longer matches, every edited or unsent
message is sent again and the merge replaces the rows. That is more than is
needed, and the set is small, because edits are rare. A first version kept the
latest edit time instead and sent only what was newer. A review showed how that
loses an edit made on another device and synced afterwards with an older time,
and comparing state has no such case. Read and delivery times that change later
are not sent again; `resend` refreshes a range.

The source keeps a fingerprint of the database's layout, its table and column
names, with its cursor. When a system update changes the layout, the next pass
says so in its counts even if everything still works. The format of these files
is not documented and this is the early warning.

Not in this decision: the attachment files themselves, a record of which files
are stored, and reading backups and old exports. They need the media walk to
revisit old rows, because files appear on disk long after their rows, in no
order, when a conversation is downloaded or a backup restored. That is the next
step, and the keys above are chosen for it.

Contact names are a separate source: the database holds numbers and addresses,
and names live in the address book.

## 039 — The collector reports every run, and Miroir is where a source's health is seen

Status: accepted, 2026-11-21. Refines 030 and 035.

Decision 030 gave the collector delivery state only, and said the connection's
last-received time is the machine's delivery health. That turned out to be too
little. A failed run sits in a status file on the machine. Miroir sees what
arrives and nothing else, so it cannot tell a source with nothing new from one
that is broken or switched off, and the first real failure (a stalled screenshot
upload) was diagnosed by asking the machine's owner to paste output.

The question is sharper for sources that read another program's undocumented
files. An update of that program can change them, and the collector then either
fails or, worse, sends less than before while every run still succeeds.

**After every run the collector sends Miroir a small report.** It holds what the
status file holds: when, which version, the outcome of rows and of media, the
counts, and for a failure its step and reason. It is sent after runs that
delivered, runs with nothing new and runs that failed. All of it is fixed words
and counts, which decision 035 made safe to print and which is therefore safe to
send.

- The report goes to the connection's own address with its delivery token. No new
  credential and no new place to reach.
- It is one-way. Decision 030's rule stands: the server never sends the collector
  instructions, and the collector ignores what the reply says.
- It never changes a run. A report that cannot be sent leaves the outcome, the
  cursor and the spool alone; the collector counts it and says so in the next
  report, so failures that happened out of reach are still learnt of.
- A server that does not know reports is not an error.

This adds an endpoint, which 030 said was not needed. The receiving URL carries
data; this carries the account of how collecting went, and nothing else did.

**Miroir stores the reports and shows them** beside what it already knows: last
report, last delivery, last import. One line per source, from the device to the
warehouse.

**Miroir exposes them as metrics, and alerting is left to what the installation
already runs.** Three conditions cover the cases: no report for longer than a
source should be quiet, several failed runs in a row, and a jump in a count such
as unreadable or skipped rows. Silence can only be noticed from the server, which
is the reason reports go there. The metrics page has its own token, so a scraper
holds no admin credential, and it is off unless that token is set.

Considered and left out:

- *A notification from each machine on failure.* No history, a notification
  credential on every machine, and nothing notices a machine that is off.
- *A dead man's switch service.* It answers "did it run" and knows nothing of
  counts or reasons; one more service to run and to look at.
- *Pushing metrics from the machine into the monitoring system.* It exposes
  monitoring to laptops, needs another credential, and ties the collector to one
  installation's monitoring instead of to Miroir.
- *Deriving health from the batches that arrive.* A run with nothing new and a
  run that failed both send none.

Not in this decision: the alert rules themselves, which belong to the
installation; checks in the warehouse on what was loaded; and a fingerprint of a
source's layout in its counts, which the Apple Messages source adds (038).

### 038 follow-up — Apple Messages attachment uploads and contact names

Attachment files use the existing media API and are opt-in. The collector
limits uploads to the configured message scope and remembers fingerprints of
the attachment keys and links it has delivered. A changed key or a late link
sends its metadata again, without replaying message text. The file walk uses
both inode change time and metadata acknowledgement time so a file already on
disk is not skipped when its metadata becomes eligible later.

Contact names are separately opt-in. Where matching address book cards
disagree, the collector reports ambiguity and sends no name for that handle.

## 040 — A timeline inside Miroir, and its groundwork

Status: accepted, 2026-11-21. Refines 022 and 033. Phase 0 is built; the rest is
the agreed plan for the phases that follow.

Miroir loads screen frames, their screenshots and soon messages, but the only way
to look at them is a dashboard that lists rows. The timeline is a page of the
Miroir server for moving through them by time: a day of screenshots to scrub
through, a search over screen text that lands on a moment, and later a
conversation read in order with its attachments in place.

**What it is made of.**

- *Data:* one analysis package, `timeline` (022), with optional input roles
  (screen, messages, browser, music and the like). It builds incremental tables
  indexed by time, group and text: `screen_frames` (the only place screen text is
  indexed), `events` (the same columns for every source: one row per message,
  visit or listen, and one per app session for the screen, not one per frame),
  a `days` rollup, and `apps` (bundle identifier to a readable name). Each source
  reaches `events` through one short SQL model of its own. Text search uses
  Postgres's `simple` configuration. Times are shown in the package's timezone.
- *Reading:* the server reads the warehouse through a role that may only select
  from the timeline's tables, granted by the models themselves (dbt `grants`).
  The server never read the warehouse before this. A core module,
  `src/core/timeline.py`, holds that access; the routes are in `src/web/`.
- *Images:* thumbnails in two sizes (360 and 1280 pixels wide, WebP), made once by
  the background worker, stored in the bucket beside the originals, and streamed
  by the server behind the login with `Cache-Control: private,
  max-age=31536000, immutable`. Logging out sends `Clear-Site-Data: "cache"`.
  Video and other large files keep the redirect to a short signed link, so the
  storage serves range requests.
- *Stored media:* the worker that makes thumbnails also writes one warehouse row
  per stored file, so a model can tell an attachment that was uploaded from one
  that never was. This revises 033's "the bucket is the record" for the timeline:
  the bucket stays the record of what exists, and this table is the worker's copy
  of it for the models to join.
- *Pages:* server-rendered templates, htmx 2 for fragments, and three small
  JavaScript modules (scrubber, lanes, thread). No build step. Usable on a phone.
- *Conversations* are keyed by the chat's own identifier, never Apple's chat ID,
  which differs between macOS versions (038).

**Builds wait for a quiet moment.** Every analysis linking a connection builds
after each of its loads (022), and messages are meant to arrive about every
minute. A package may therefore state a minimum interval between builds. A load
that arrives within the interval after the latest build started does not build;
it makes sure one build is scheduled at the interval's end, and every load
arriving before then joins that build. The timeline will use a few minutes, so it
is a few minutes behind the warehouse. A package that states no interval builds
after every load, exactly as before. In detail:

- The interval runs from the start of the latest build of that analysis that was
  not blocked, inline or scheduled.
- The scheduled build is an ordinary `models` operation, a child of the load that
  scheduled it, with its earliest start in its options. It runs as a Prefect flow
  run of that load's deployment, scheduled for that time, with the operation ID as
  its idempotency key, and is observed like any submission.
- A load joins a build that has not started, was planned for the analysis's
  current receipt and its connection's current settings (otherwise it would be
  blocked), and was due no longer than one interval ago: a build whose run
  is still starting covers the load, because it will start after the load
  committed. One due longer ago may never run and is not joined.
- When the build cannot be deferred safely, the load builds now, as without an
  interval: when it does not run in a Prefect deployment (local runs, the
  subprocess executor), when the schedule is refused (the planned operation is
  then closed as blocked, `NotScheduled`), or when the metadata store fails. A
  redundant build costs a few minutes of dbt; a missing one leaves the analysis
  stale with nothing to rebuild it.
- Loads of different connections decide under an advisory lock per analysis, so
  two of them do not schedule two builds. A load that cannot take it within a
  minute builds now.
- A rebuild requested in the interface ignores the interval.
- A load that deferred every build records its transformation as `skipped`, with
  the detail `Analyses: <id> deferred`; the analysis shows as running until the
  scheduled build has run.

**Phase 0, the groundwork, contains:**

- Static files at `/static/`, served without the login (the login page needs the
  stylesheet; nothing there is private) and revalidated on each use
  (`Cache-Control: no-cache`, with an ETag), because their names carry no version.
  The stylesheet moved out of `base.html`; the two copy buttons and the error
  page's Back link moved their inline script to `/static/miroir.js`; two inline
  styles became classes. Every page looks and works as before.
- On every response: `Content-Security-Policy: default-src 'self'; img-src 'self'
  blob:; object-src 'none'; base-uri 'none'; form-action 'self';
  frame-ancestors 'none'`, `Referrer-Policy: same-origin`, and
  `X-Content-Type-Options: nosniff`. Outbound links also carry `rel=noreferrer`.
  The plan said `no-referrer`. Either keeps every Miroir address from other
  hosts, but `no-referrer` also makes browsers post Miroir's own forms with
  `Origin: null`, so every post, logging out included, would rest on
  `Sec-Fetch-Site` alone (the setup page already chose `same-origin` for this
  reason). `same-origin` keeps a real origin on them.
  The storage host is not in the policy yet: no page shows video before the
  conversations phase.
- htmx 2.0.11, vendored as `src/web/static/htmx.min.js`, with its version and
  SHA-256 in `VENDORED.md` beside it, checked by a test. `base.html` configures it
  with `allowEval: false`, `includeIndicatorStyles: false`, `allowScriptTags:
  false` and `selfRequestsOnly: true`. No page loads it yet.
- The read-only warehouse connection: `MIROIR_TIMELINE_DATABASE_URL`, or the parts
  `MIROIR_TIMELINE_DATABASE__{HOST,PORT,DATABASE,USERNAME,PASSWORD}` composed as
  for the metadata store. It must be PostgreSQL and is opened with psycopg2. Every
  session sets `statement_timeout` to 5 seconds and
  `default_transaction_read_only`, and these replace any `options` in the URL, so
  neither depends on the role. Without the setting, `/timeline` and
  `/api/timeline/health` say the timeline is not configured (503) and nothing else
  changes; the server still starts if the warehouse is down. The health check is
  one query that reads back the session's read-only flag and time limit.
- `ObjectStore.get` (bounded by a size the caller states) and `ObjectStore.put`,
  for the worker's thumbnails. The web process does not call them.
- The minimum build interval, above, in the shared build code. No package states
  one yet.
- Import rules: `src/core/timeline.py` imports nothing of the server's but
  `src.core.config` and `src.core.operations`, and only `src.web` and `src.cli`
  import it; nothing in `src/web/` or the timeline module imports an image library.

Choices made where the plan left them open, each the more conservative one: the
referrer policy above; the time limit is fixed in code rather than a setting; the policy has the extra
directives above; htmx gets two more restrictions than the plan named and is
loaded by no page; the timeline routes need the login like every other page and
are not linked from the header until there is something to see; a deferred
build that cannot be scheduled is built at once.

**Phase 1a, the package's screen and browser data (2026-11-21).** The `timeline`
package reads `screen` (ScreenMemory) and `browser` (browser history), both
optional, and states the five-minute interval. Where the plan left a choice open,
it was made this way, the more conservative way where it touched correctness,
privacy or cost:

- *At least one input.* A package whose inputs are all optional cannot be
  configured with none linked: it would have nothing to build from.
- *A frame's app* is the app usage row of the same second, the first by name if
  there are several, or none. Frames without an app neither start nor end a
  session; no app is carried over from an earlier frame. The collector's own
  exclusion of password managers matches frames to apps the same way.
- *An app session* is a run of frames of one app with no pause over five minutes,
  named after its first frame. Screen text stays out of `events`; a session's
  title is its app.
- *Late rows.* A late frame or app usage row recomputes only the sessions
  between the two nearest boundaries it cannot move. A session that no longer
  exists is kept by the screen's own model as a deleted row, which `events` and
  `days` remove. A frame, session or visit that moves to another time is also
  recounted where it was. A test checks that a series of incremental builds
  gives the same tables as one first build.
- *Which loads a build reads* (revised after review, below). Membership, not a
  range of load IDs. A build reads only rows of loads that `_dlt_loads` lists as
  complete, in first builds and in every lookup. The models that read raw tables
  remember the latest completion time they have read. Each build reads the loads
  completed after it, less ten minutes, with their IDs written into the query.
  So a load that completes out of order is read at the next build, and one that
  never completes is never read. What remains:
  - completion times come from the loading workers' clocks, so two clocks more
    than ten minutes apart can hide a load until a rebuild from scratch;
  - a load still rewriting a row while a build reads can hide that row's old
    version, and the next build corrects it;
  - more than 5,000 waiting loads are read as one rebuild.
  Nothing is locked against ingestion and no raw table is copied.
- *Indexes on raw tables.* dlt indexes only its row ID, so the models create the
  indexes they look rows up by (load ID, second, image path, page, dlt's list of
  loads) on the raw tables when missing. Each is built in a hook outside the
  model's transaction and committed at once: writes to that raw table wait only
  while that index is built. Concurrent creation was not used: dbt opens a
  transaction around every hook statement on Postgres, and `CREATE INDEX
  CONCURRENTLY` cannot run in one. A table the build's role does not own is not
  indexed and is read more slowly. No model reads all frames on an ordinary
  build. An index on the image path also spares dlt's merge a full read.
- *Inputs unlinked or replaced.* Every row records its input's connection and raw
  dataset. When those change, the next build removes that input's rows from every
  table and reads the new one from its start; it does not wait for anyone to
  press a button.
- *Rebuild from scratch.* A requested rebuild may ask for dbt's `--full-refresh`.
  This is an option recorded in the operation, offered on the analysis page, in
  the CLI and in the API.
- *Withheld text.* `screen_frames` keeps no text for a frame with a withholding
  reason. The server refuses a ScreenMemory batch whose frame has both; the
  collector never writes one.
- *Visits* are navigations only (no redirects, reloads, embeds or framed links);
  the site comes from the address, not the origins table; a page's new title
  reaches its visits.
- *The reader role* is the analysis parameter `reader_role`, a plain lower-case
  role name (`public` and `pg_*` refused). Empty grants nothing and revokes the
  `SELECT` an earlier build gave.
  - The four output tables grant it `SELECT` through dbt `grants`. The two source
    models state an empty grant list.
  - Every timeline table also ends its build, in the same transaction, with
    exactly the owner and, on the four outputs, the reader with `SELECT`. This
    holds whatever default privileges said.
  - One model grants the schema's `USAGE` in a post-hook. It is one model only,
    because two grants on one schema at once fail.
  - The role must be a dedicated one, with no other roles or default privileges
    in the warehouse.
- *Connection IDs* reach the models (`miroir_connections`) only for a package
  that asks, so the receipts of the other packages do not change.
- *No data tests on `screen_frames` or `events`*: each would read the whole table
  on every build. Unique indexes hold their keys.
- *Legacy builds* (no analysis) leave the timeline's models out.
- *Builds that waited* for the connection lock or the dbt slot read the
  configuration again once they hold each, and end as blocked if the
  analysis's receipt changed. The receipt holds everything a build applies
  (raw bindings, the settings a package reads, parameters), so a connection
  edit alone does not block. A load whose build was blocked this way records
  its transformation as skipped, not failed. Requested and scheduled builds
  keep the check every request has when it starts: a changed connection
  revision blocks them, as every operation runs only against the configuration
  it was requested with. After waiting, only the receipt matters for correctness.
- *Joining a deferred build.* A load joins only a scheduled build that has its
  own flow run bound. A load that builds at once registers that build under the
  same lock as the decision, so a second load deciding meanwhile defers instead.

*Review of phase 1a (2026-11-22).* An independent review of pull request #43
reported eleven findings; each was decided as follows:
1. Unlinked or replaced inputs: fixed, as above, with the rebuild from scratch.
2. Watermark by maximum load ID: fixed, membership as above.
3. Superseded settings after waiting for locks: fixed, checked again.
4. Joining a build with no run: fixed. Having `refresh` resubmit lost deferred
   builds was not built; the limitation below stands.
5. Old positions of moved rows: fixed for frames, sessions, visits and days.
6. Withheld text: fixed in the model and in the server's batch validation.
7. Default privileges on internal tables: fixed with the exact ACL and the
   dedicated-role requirement.
8. Two loads both building at once: fixed, registered under the decision lock.
9. Index creation: outside the model's transaction, not concurrent, and skipped
   for raw tables owned by another role.
10. Long uninterrupted sessions and the 50-range cap: left as they are, accepted
    costs.
11. A repeated local midnight: listed as a limit.

*Second review (2026-11-22)*, at `5fc94c4`. It confirmed the nine fixes and found
two regressions and one cost:
1. The re-check after waiting also compared the connection's revision, so any
   connection edit during a load blocked that load's own build, for every
   package. Fixed: only the receipt is compared after waiting, and a blocked
   child counts as skipped in its load's summary.
2. An old position was overwritten with null when the build that read a move
   failed part way and the next build read the same load again inside the
   margin. Fixed: old times are kept as a list, carried until every model that
   recounts them has read the row (its stamp at most theirs), then cleared.
3. `apps` and the join to old sessions read all sessions on every build. Fixed:
   a skip scan over an index on (deleted, group_key), and one index probe per
   session.

Considered and left out:

- *A single-page application (SvelteKit or similar).* A Node toolchain in the
  image and its dependency churn, for pages that templates and htmx serve.
- *A signed storage link per image.* No browser cache across links, and a second
  host in the policy.
- *Thumbnails made on request by the web server.* It would decode images and
  spend CPU in the process that serves the login. The worker makes them.
- *imgproxy*, whose result cache is a paid feature, and *thumbnails made by the
  Mac* (`sips`), which a copied archive would lack.
- *A declarative timeline source on the connector* (time column, title, text,
  media, group, renderer). About three sources could use it today; one SQL model
  per source does the same with no plumbing. Deferred until writing those models
  becomes the cost.
- *Building after every load*, as other analyses do: with minute-by-minute
  deliveries, builds would queue behind each other.
- *A periodic build instead of an interval*: it builds when nothing arrived and
  adds a schedule to keep in step with the configuration.

Deferred: contact sheets per hour, collapsing near-duplicate frames, trigram
search, video thumbnails, a timezone per event (until then, travel days are
shifted), location with self-hosted map tiles.

Consequences:

- Screenshots that are not masked stay in the browser's cache for a long time on
  each device used; logging out clears it. Whether Safari honours
  `Clear-Site-Data` is still to be checked; if not, only thumbnails are cached.
- A scheduled build runs through the connection whose load scheduled it. Like any
  request, it is blocked if that connection's settings change before it starts,
  and the analysis stays stale until its next load or a rebuild.
- A scheduled build that Prefect loses is closed by `refresh` like any lost run;
  the loads that joined it are built by the next load or a rebuild.
- The read-only role and its settings belong to the installation. The schema
  privilege (`USAGE`) on the timeline's schema is not something dbt `grants`
  gives; since phase 1a a post-hook of the package grants it.
- Incremental tables keep rows built by older model code: a change to the models
  reaches old rows at a rebuild from scratch.
- Where the clocks go back at midnight, a day of `days` starts at the later of
  the two midnights.

  gives and has to be granted with it.

### 040 amendment — Thumbnails and stored media (phase 1b)

Status: proposed with the phase 1b pull request, 2026-11-22, revised after its
review the same day. What building thumbnails and `media_objects` had to decide
that the plan above left open. Where a choice touched security, cost or the
shared scheduling, the more conservative one was taken; those are marked
*(conservative)*.

**How the worker knows what to derive: a queue in the metadata store.** The
server cannot write to the warehouse, so when it answers `stored` to a
confirmation it first records the file (connection, kind, key, size, time) in
`media_pending`, metadata schema 8. The derive run drains it. A failure to record
fails the confirmation. A file answered `present` to an upload request is queued
too: that is what a device is told on its next pass after a confirmation failed
or was lost, and it does not confirm such a file again. A file queued again with
the same size keeps its entry; with another size it replaces it, and a run that
handled the old size leaves the new one queued. Two other ways were weighed:

- *Listing the bucket on each run.* The listing is in name order, not time order,
  so finding new arrivals means walking everything each time, and nothing tells a
  run where the last stopped. It is kept for the backfill, below.
- *Reading the source's rows in the warehouse.* Rows arrive before their files
  (033), and some files never arrive: frames whose screenshot was withheld,
  attachments that were not on disk. Those would be looked for on every run, and a
  file arriving late would have to be noticed by polling.

**A derive is an operation of the connection, on its existing deployment.** Kind
`derive`, run by the same flow as a sync, so no new deployment and the same
submission, idempotency and reconciliation. Its outcome is recorded as the
attempt's transformation (ingestion `not_run`), with counts in the detail. It is
not a load: connection status, the last sync, "requested, not started" and the
analyses ignore it, and so does the connection page's history, which shows one
line instead (files waiting, the last derive run and its outcome). A settings edit
since the request does not block it, because its work is the queue and the media
settings, not the connection's settings.

**Debounced like a deferred build.** A request joins a derive run of the
connection that has not started and is bound to its own backend run. One due no
longer than a delay ago is joined as it is. One due longer ago is joined only if
the orchestrator says its flow run is still scheduled, pending or running: a long
load may hold the deployment, and then confirmations keep joining the one waiting
run instead of piling up empty ones; but a run that died before recording an
attempt is not joined. If the orchestrator cannot be asked, or there is none, an
overdue run is not joined. Otherwise the request records a run due
`media.derive_delay_seconds` from now (default 300) and submits it with that
start, under an advisory lock per connection; Prefect schedules it with the
operation ID as idempotency key. A run without a bound backend run (a local
executor, which starts it at once) is never joined. Requesting never fails a
confirmation: if Prefect refuses, or the deployment cannot be prepared, the
operation is closed as blocked and the files wait in the queue for the next
request. If Prefect loses a bound run, the next confirmation after it is overdue
asks Prefect, finds it ended, and requests a new one; `refresh` closes the lost
one. A run Prefect keeps scheduled but never starts is still joined, as Prefect
says it waits; a backfill requests a run regardless. Files are never lost by any
of this: they stay queued.

**Bounded runs** *(conservative)*. A run handles at most `media.derive_batch`
files per kind (default 500, two to three minutes) and, when more of a derived kind
wait and it made progress, schedules the next run due now, which nothing joins. A
run that changed nothing schedules nothing (an entry of a kind that is no longer
derived is not counted as waiting). The deployment's limit of one run at a time
means a backlog drains in steps that the connection's loads take turns with.
Without Prefect a run continues itself while it makes progress.

**`media_objects`.** One table per connection's dataset (its binding), so the
timeline's models find it beside the rows they join, as every input follows its
connection's binding (019). It is written by a dlt pipeline of its own,
`<pipeline_name>_media`, so the connection's load state is never touched, and the
write holds the connection's lock because both write the same dataset. Merged on
`(connection_id, kind, key)`. Columns: `connection_id`, `kind`, `key`, `size`,
`content_type` (from the extension, as at upload), `stored_at` (confirmation time;
for a backfilled file, the bucket's modification time), `thumbnail_w360`,
`thumbnail_w1280`, `thumbnail_status`, `derived_at`, `derive_version`. The status
is `made`, or why there is none: `unsupported`, `too_large`, `unreadable`,
`failed`. A queued file whose row has the same size and version is not read again,
whatever its status.

What a row means, for the slice that shows "not available" tiles:

- A row says the file was in the bucket when the run reached it. It is not
  removed when the object is later deleted (a lifecycle rule, a hand): a row can
  outlive its file.
- No row does not mean not stored. The file may still be queued; its kind may
  have no thumbnails, so it is not recorded at all (attachments today); it may
  have been confirmed by a server older than schema 8 and not yet backfilled; or
  it was gone from the bucket when the run reached it.
- A file replaced by another with the same key and the same size is not derived
  again; its thumbnails and row stay those of the first. Keys are unique per
  frame or attachment, so this is a known limit, not a case met in practice.

**The thumbnails.** WebP at quality 70 (360) and 80 (1280), libwebp method 2,
which measured 40% faster than its default for 1% more bytes. A decodable image
always gets both sizes, an image narrower than one keeping its width, so a reader
never has to fall back to the original for one size only. A very tall image is
fitted under WebP's height limit. EXIF orientation is applied, and a JPEG is
drafted for the width it is shown at, after its rotation. No metadata is written
into the copy *(conservative: a photo's place does not travel into the
thumbnail)*.

**Decoding untrusted bytes, in a child process** *(conservative throughout)*. The
worker never decodes in its own process. `src/core/thumbnails.py` runs as a child
(`python -m src.core.thumbnails`), started once per run and given one file at a
time over a pipe:

- What it can reach. Its environment holds `PATH` only, not the bucket key or the
  database passwords. On Linux the worker makes itself undumpable
  (`prctl(PR_SET_DUMPABLE, 0)`) before starting it, so the child cannot read the
  worker's environment or memory through `/proc`; without that it could, as it
  runs as the same user. It still has the network and every file the worker's
  user can read, the mounted volumes included (the configuration, the artifacts).
  It has no core file (`RLIMIT_CORE` 0).
- An address-space limit of 1 GiB, a CPU limit per file, and a 30-second
  deadline per file, for writing the file to it and for reading its answer,
  enforced by the worker, which then kills the child. On macOS (development) the
  address-space limit is not enforced: a file that would need more takes it from
  the machine instead of being refused; the shape bounds still apply.
- The original is read with the kind's size limit (20 MB for screenshots).
- The header is read and the shape checked before anything is decoded: at most
  40 million pixels, at most 16,384 on either side, and at most 16 times longer
  than wide either way, so an EXIF rotation changes nothing. Pillow keeps a
  pointer and an allocation per row, which made a 1 × 40,000,000 PNG of 350 KB
  cost 790 MB; the side and ratio bounds refuse it. Pillow's decompression-bomb
  warning is an error.
- Only the decoders of the kind's thumbnail extensions are tried (JPEG and PNG
  for screenshots), whatever the bytes claim; only the first frame is used.
- Any decoder error makes the file `unreadable`; it never fails the run.
- Every fresh child must first make thumbnails of a tiny built-in image for every
  decoder its kinds use (8 × 8, one per decoder in `IMAGE_DECODERS`, read by that
  decoder alone; a test makes sure each has one), and of the costliest image the
  bounds accept (40 million grey-and-alpha pixels, all zero, so 78 KB): about
  1.4 s and a peak of about 530 MB. A child that cannot means the decoder is
  unavailable (it did not start, an `OSError` starting it or making the worker
  undumpable, a limit was refused, a decoder is missing from its Pillow, the memory
  limit is too small for what the bounds accept, a module is missing): the run stops as
  failed with a fixed reason, the file in flight gets its try back, no row is
  written for it and nothing is scheduled. After a child is lost on a file the
  next one is checked at once, so a loss followed by a working child is the file's
  fault and costs it a try, and a loss followed by a broken one costs it nothing.
  Because the check covers the costliest accepted image, running out of memory on
  a real file afterwards is that file's own `too_large`.
- A file's count of decodes begun is raised only while it decodes, and reset when
  the decode reaches an outcome. A file whose decode did not finish three times
  while the decoder worked (the child crashed, was killed or took too long, or the
  run itself died while it decoded) is `failed`, and not read again by a plain
  run. A backfill asked to (`--retry`, `failed` by default, `unreadable` on
  request) queues the kind's files with those statuses again, marked to be derived
  whatever their rows say, to repair a past incident without a new
  `DERIVE_VERSION`. The mark is in the queue entry, so the retry is one write: a
  run that dies after it leaves the files queued and marked, and any later run
  derives them; their rows are replaced by the merge. Nothing that happens to the run after a file was decoded
  counts against that file.
- Rows are written, and their files leave the queue, every 100 files. A run that
  dies loses at most that much finished work, which the next run redoes.
- A storage error ends the run after recording what it finished; the rest stays
  queued.

Measured peaks inside the child, above its own 32 MB, for the worst images the
bounds accept (40 million pixels unless said): grey with alpha 480 MB (it becomes
RGBA), palette with transparency 365 MB, RGBA 328 MB, 16-bit grey 252 MB, grey or
1-bit 213 MB, RGB 176 MB, 16,384 × 2,441 RGBA 303 MB (304 MB rotated by EXIF),
16,384 × 1,024 RGBA 119 MB; a 40-megapixel JPEG 26 MB. The review measured the
child at 530 MB resident and the worker and child together at 711 MB at the worst
accepted shape. Every run reaches that once, in the check. A flow-run Job
therefore needs at least about 1 GiB for a derive run; 1.5 GiB leaves room. A Job
limit below that kills the child in the check, and the run stops as
"decoder unavailable" rather than charging files.

**HEIC.** The screenshots kind accepts `.heic`, but the collector's Screen Memory
source stores JPEG. `pillow-heif` is not added *(conservative: another native
decoder for untrusted input, for a type that does not occur)*; a HEIC screenshot
gets a row with `unsupported`. The attachments kind gets thumbnails in the
conversations phase, with HEIC then; until then it is not derived at all
(`MediaKind.thumbnails` is empty).

**Backfill walks the bucket.** The bucket is the record (033): a walk finds exactly
what is stored, including an archive copied in by other means, and nothing that
never was. Keys from the source's rows were rejected for the reasons above. The
walk lists `prefix / connection / kind /` from where the last walk of that
connection and kind stopped (`media_backfills`, the last name listed, valid key
or not, so a name too long to be a key can neither overflow the column nor stall
the walk), queues what it finds, and then derives like any run. Whether more
follow is the store's own answer (`IsTruncated`), not a short page. `--limit`
bounds the files walked per run. Repeating it is safe: files already derived are
recognised from `media_objects` without being read, and a finished walk starts
again from the beginning. It runs on the worker. Its `--duckdb` form, for
development, refuses unless `--metadata` names a local SQLite file, so it can
never drain the installation's queue or move its walk into a local file.

**The worker holds the bucket key.** 033 gave the storage credentials to the server
alone. Reading originals and writing thumbnails needs them on the worker's
flow-run Jobs too. Added to the shared flow-run Job template, as on this
installation, every connector's runs get them and any of them can read and write
the whole bucket. Without them derive runs are blocked and nothing else changes. The key needs read and
write on the bucket and nothing more.

**Boundaries.** Only `src/core/thumbnails.py` imports Pillow, and nothing in `src`
imports it: it runs as its own process. Only the runner imports the derive module,
inside a derive run. A test imports the server's modules and the derive module in
a fresh interpreter and checks no image library was loaded. `src/pipelines` may
now import `src.adapters.s3`, to build the worker's bucket client.

**Review.** An independent review of the first version found six defects and
seven lesser points. Fixed: a file whose decoding ended the process marked every
file before it in the batch as failed (attempts raised for the whole batch, rows
written at its end); the pixel cap did not bound memory for thin images; the
`--duckdb` backfill could drain the installation's queue; a run that changed
nothing scheduled the next one forever; the walk took a short page for the end and
could store a cursor longer than its column; a rotated JPEG was drafted to a
quarter of its resolution. Of the lesser points, a slow decode is now bounded by
the child's timeout, waiting runs absorb confirmations whatever their age, and
derive runs left the connection's history. Documented instead of built: what a
missing row means, that a row outlives its file, the same-key same-size limit,
that a run never started is joined until it is closed, and that every flow-run Job
holds the bucket key.

A second review confirmed those fixed and found five more, all fixed: a decoder
that failed on every file (one that could not start, or a memory limit too small)
turned the whole queue into permanent `failed` or `too_large` rows, which the
check of each fresh child now prevents; the child could read the worker's
environment through `/proc`, which `prctl` now refuses on Linux; an overdue bound
run that had died absorbed every confirmation until `refresh`, now asked of the
orchestrator; writing to the child had no deadline; and the child could leave a
core file. A backfill's retry was added to repair rows left by such an incident.

A third review confirmed those fixed and found one more and three small points,
all fixed: the check never decoded a JPEG, so a Pillow without libjpeg would have
made every screenshot `unreadable` (the check now covers every decoder the kinds
use); the retry could not repair `unreadable` rows (it takes the statuses now);
the retry queued files and deleted their rows in two steps, which a death between
would have left unrepaired (it is one marked queue write now); and an `OSError`
starting the child was not "decoder unavailable" (it is now). Left as a known
limit: a flow run Prefect still shows as running after its pod died before
recording an attempt is joined for as long as Prefect says so, and `refresh`
does not close it; cancelling it in Prefect, or a backfill, ends that.

Consequences:

- Metadata schema 8: server, worker and flow-run image are released together.
- Thumbnails lag uploads by the delay plus queueing behind the connection's loads.
- Measured on generated screens on a development machine (one core): about
  0.16 to 0.19 s per JPEG screenshot and 0.28 to 0.37 s per large PNG for a whole
  run, network to the bucket not included. Not yet measured on the cluster.
- A flow-run Job needs at least about 1 GiB of memory for a derive run (above).
- Each derive run spends about 1.4 s, and a peak of about 530 MB, on checking its
  decoder, and as much again after each lost file.
- A flow run that Prefect shows as running after its pod died before recording an
  attempt holds the queue until it is cancelled in Prefect or a backfill runs.

### 040 amendment — Queries, the API and images through the server (phase 1c, part 1)

Status: proposed with the phase 1c pull request, 2026-11-22. What the server side
of the timeline had to decide that the plan above left open. Choices that touched
security, privacy or cost took the more conservative option; those are marked
*(conservative)*. The routes and their answers are in the README (*The timeline
API*).

**Which tables.** The server reads the first analysis of the `timeline` package in
configuration order, in the schema its build writes, derived by the same code
(`analysis_plan`), so no setting names it. Further timeline analyses are allowed
and named in a `warning` on the health check and on every answer. Without one,
every timeline route, the status page and the health check included, says the
timeline is not configured (503). The timeline module may therefore import
`src.core.analysis`, which computes names and touches nothing.

**Stored files in the models.** `screen_frames` joins `media_objects` of the
screen connection's dataset (kind `screenshots`, that connection's rows only) and
gains `media_status`, `thumbnail_w360` and `thumbnail_w1280`; a session in
`events` takes its first frame's. The table is an optional input: without it the
columns are null. Null means no record, which is unknown, not absent (phase 1b).
The derive worker's loads are listed in the same dataset's `_dlt_loads`, so the
existing load cursor already sees them: a build reads again the frames whose
records a newly completed load wrote (an index on the raw `key` finds them), and a
frame arriving after its record finds it. That re-read moves the frame's stamp, so
the sessions around it are recomputed as for any changed frame; a derive run's
batch of at most 500 files costs one such recomputation. Tables built before these
columns gain them in a pre-hook (nullable, no rewrite) and read every frame and
session once. dbt's `on_schema_change` was not used: it would also have added the
`_deleted` column of the new rows to `events` and `days`. A test checks records
written before and after their frames, another connection's record in the same
dataset, a replaced record, the upgrade, and that the result equals a first build.

**Withheld wins** *(conservative)*. A frame with a withholding reason is shown as
withheld, with no thumbnail, whatever a record says: in a day's index, in a
frame, and in an event, whose `media_withheld` column (on every table with the
media columns) carries its first frame's reason from `screen_frames` through
`timeline_screen`. A frame withheld after it was built is a changed frame, so its
session is recomputed and the incremental tables still equal a first build.

*The media route asks the timeline too* *(conservative)*. A file can be in the
bucket for a frame that is withheld: stored before a newer rule withheld the frame
when it was sent again (034 never deletes). So `/media/{connection}/{kind}/{key}`,
at every size and for the plain redirect, first asks every timeline analysis
whose `screen` input is that connection, when the kind is `screenshots`:

- the key's frame withheld in any of them: 404, as if there were no file;
- a key none of them has a frame for (not loaded yet): not withheld, served as
  before;
- no such analysis: nothing to ask, the route behaves as before;
- an analysis exists but the timeline cannot answer: the route fails closed with
  the timeline routes' error, never the file: 503 when the reader is not
  configured (no `MIROIR_TIMELINE_DATABASE_URL`), the tables are not built yet or
  the warehouse is unreachable, 504 at the time limit. Without the reader setting
  this refuses that connection's screenshots outright, the stricter reading of
  "cannot answer".

This costs one lookup on `screen_frames`' unique `image_path` index per image.
It leaves one case open: the stored file of a frame that became withheld is
hidden, not deleted. Anyone with the bucket's key, or a link signed before, can
still read it.

**Queries.** Each runs in one read-only session with the 5-second limit and is
bounded: a days range of at most 400 days; at most 20,000 frames in a day's index
(`truncated` beyond); a frame's text cut at 100,000 characters; at most 500 events
a page, titles and bodies cut at 2,000 characters; at most 50 search results a
page; a search of at most 200 characters. Parameters are always bound. A query at
the time limit answers 504 with a fixed message; a table, schema or column that is
missing, or not readable by the role, answers 503 "not built yet".

*One rule for a time's day.* A local day runs from `(day::timestamp AT TIME ZONE
tz)` to the next day's. Where clocks go back at midnight (America/Havana on
2026-11-01) the local midnight comes twice, and Postgres reads it as the later
one, so the hour before belongs to the previous day. The index selects frames by
these bounds, which keeps the time index usable; selecting by `(captured_at AT
TIME ZONE tz)::date = day` would not use it. Everything that names a time's day
follows the same bounds: a frame's `day` and `t`, a moment's `day`, and the
models' `days` (`timeline_local_day` gives the day whose bounds hold a time, which
is the local date everywhere else). Before, `days` and a frame took the local date
while the index took the bounds, so a frame of that hour was counted on one day,
listed on another, and given a negative `t`. Toronto's 23- and 25-hour days are
tested too. `days` rows built before keep the old rule in such a timezone until a
rebuild from scratch.

*Searches the index cannot answer are refused* (400). A query whose every part is
negated (`-a`, `-"a b"`, `a or -b`) cannot use the text index and would read every
frame until the time limit, holding one of the server's four connections. The
server asks Postgres first: `querytree(websearch_to_tsquery('simple', q))` keeps
the part of a query an index can use, and is `T` when there is none and empty
when there is no word at all (`-`, `!!!`, `""`). Both are refused with a plain
message. `a -b`, phrases and `a or b` are searched. The `simple` configuration has
no stop words, so `the` is a word like any other.

- *Days* are read for the analysis's current timezone only; after a timezone
  change, nothing shows until the next build recounts.
- *Events* are those that started in the range, not those overlapping it: an
  overlap needs a scan from the beginning of time *(conservative: cost)*. A page
  that needs a session already in progress asks from earlier. Each source is one
  index range, merged; the keyset is `(started_at, event_id)`.
- *Moments* are fixed five-minute windows, aligned to multiples of five minutes,
  per app. Fixed windows page with a stable keyset; windows by pause would move as
  frames arrive. A moment shows its first match, its count and a snippet computed
  for the returned moments only. Newest first.
- *Snippets* are returned as alternating plain and matched strings, never as
  markup *(conservative)*: `ts_headline` marks matches with two private-use
  characters, removed from the text first, and the server splits on them.
- *Browser search* matches titles and addresses through `events.search`, newest
  first, one result per visit. The app filter applies to screen results only.
- Unknown parameters, and a parameter given twice, are refused *(conservative)*.
  An unencoded `+` in a time offset arrives as a space and is read back as `+`.

**Caching.** Every answer is `Cache-Control: private, no-store`, except a day's
screen index: `private, no-cache` with a weak ETag of its content, answered 304
when it matches. It holds keys, times and apps, no screen text; late frames can
still change a past day, so it is revalidated, never immutable. The ETag saves the
transfer, not the query. The timeline's JSON answers of 1 KB or more are gzipped
for a client that accepts it; nothing else is. They hold no secret and are
answered only to the logged-in administrator, so compressing them next to a query
the client chose discloses nothing it did not have.

**Images through the server.** `/media/{connection}/{kind}/{key}` without
parameters is unchanged: a redirect to a short signed link. With `size=360` or
`1280` it copies that thumbnail, with `size=original` an image original. This
revises phase 0's "the web process does not call" `ObjectStore.get`: it now reads,
and never writes.

- Checked as an upload is: a kind of that connection, a valid key, one of the
  kind's extensions. A thumbnail's kind (`screenshots.w360`) is not a kind of any
  integration, so it can be neither read nor uploaded by name; the server builds
  its object name from the base kind.
- The content type is fixed by the server (`image/webp`, or the original's by its
  extension), never the bucket's *(conservative)*, and `nosniff` applies.
- `Cache-Control: private, max-age=31536000, immutable`, the bucket's ETag as a
  strong tag, and 304 for a matching `If-None-Match`. The object is still fetched
  to learn its tag, so a 304 saves the browser's transfer, not the bucket's.
- 404 when the thumbnail does not exist or the kind has none; 400 for another size
  or any other parameter; 503 when the bucket cannot be asked.
- Originals are copied only for image types and up to 25 MB and the kind's own
  limit *(conservative: memory per request)*; anything else, video included, gets
  the signed-link redirect. A thumbnail is read with the worker's own bound,
  16 MB. Bytes are held in memory, not streamed in pieces: these bounds keep that
  small.
- Logging out sends `Clear-Site-Data: "cache"`.

Known limits:

- A record can outlive its file (phase 1b): the index then says thumbnails exist
  and the image answers 404; the page shows its placeholder.
- The stored file and thumbnails of a frame that became withheld stay in the
  bucket, hidden by the media route only.
- A thumbnail made again under a new `DERIVE_VERSION` keeps its old copy in a
  browser's cache until it is evicted or the user logs out.
- The day index's `connection` is its first frame's: a table holds one screen
  connection's rows (others are removed at the next build).

Measured on synthetic data, one development machine, Postgres 17: a day's index
of 3,001 frames takes 7 ms to query and 14 ms through the route, 109 KB of JSON,
15 KB gzipped. On 303,000 frames of a year with 80 skewed words each, the first
page of a screen search takes 108 ms (median; one run of 712 ms) for a word on 30%
of frames, 216 ms for two words that are on nearly every frame, 5 ms for a word on
1%, 2 ms for a rare one, 13 ms for the common word within a month, and 72 ms
within one app. `tests/test_timeline_benchmark.py` repeats this
(`MIROIR_TIMELINE_BENCHMARK=1`).

Left for later: deleting the stored file and its thumbnails when a frame becomes
withheld (the worker would have to learn of it from the rows, which the server
cannot write to).

*Second look (2026-11-22)*, fixed with the pages (part 2):
- *Only analyses that can answer are asked.* The media route asks the timeline
  analyses reading that connection that have a reader role, and skips one whose
  tables are not built or that cannot answer (time limit, warehouse error). One
  answer is enough; withheld in any that answers is withheld. Only when none can
  answer does it fail closed, with 504 if any reached the time limit and 503
  otherwise. A second, unbuilt or unread, analysis on the same connection no
  longer hides every screenshot.
- *Control characters.* A NUL reached psycopg2 as a `ValueError` (500). The
  media route checks the key with `valid_key` before the lookup (404); the
  timeline routes refuse control characters in `q`, `app` and a frame's key, and
  a cursor carrying a NUL (400).
- *A lookup connection of its own.* The media route's lookups use a pool of one
  connection, no overflow, a 2-second wait for it and a 1-second statement limit;
  the queries keep their four. Five of the reader role's six connections, so one
  is left for a health check or an operator. A lookup takes about 0.3 ms, so one
  connection serves a page of thumbnails; a test holds the queries' four
  connections with slow statements and an image still answers in under a second.
  A short pool wait on the shared pool was not chosen: a busy pool would then
  refuse images instead of serving them.

*Review (2026-11-22).* An independent review of pull request #49 found one high
and two low defects and two missing tests; all are fixed as above, each with a
test from its reproduction:
1. A withheld frame's screenshot was reported available in events and served at
   every size: withheld wins in events, and the media route asks the timeline.
2. Day bounds disagreed where clocks go back at midnight: one rule.
3. A search with only negated terms read every frame: refused.
4. A record that changes after the first build (`failed` to `made`) reaching
   `timeline_screen` and `events`: tested; it did.
5. The reader reading raw `media_objects`: tested; it cannot. The test of a
   table lacking only some media columns found that `events` did not read its
   rows again; it now does.

### 040 amendment — The Day view and search pages (phase 1c, part 2)

Status: proposed with the pages' pull request, 2026-11-22. What the first version
of the pages had to decide that the plan left open; *(conservative)* marks the
choices that touched security, privacy or cost. How to use them is in the README
(*The timeline's pages*).

**Pages and routes.** `/timeline` opens the latest day with data in the last 400
days (or today); `/timeline/{day}` is the Day view, `?frame=<key>` opens it at a
frame (a frame of another day redirects to its own); `/timeline/search` searches.
The plan's separate scrubber page (`/timeline/screen/{day}`) is the Day view
itself: the scrubber, the image and the frame's text sit above the lanes, so one
page holds what the first version needs. No JSON route was added: the pages use
the five of part 1. The month calendar, the day's counts, the frame named in the
address and the search results are rendered on the server, so they work without
JavaScript and are tested as templates; the lanes and the scrubber are drawn in
the browser from the day's index and its visits.

**Scripts.** Two plain ES modules under `/static/`: `timeline-core.js` (the logic,
no page: time to frame, key steps, lane geometry, which image size, the loader,
colours) and `timeline.js` (the page). No framework, no build step.
`timeline-core.js` is tested with Node's own test runner (`tests/js/`, run by
`tests/test_timeline_js.py`, skipped where no `node` is installed; GitHub's runners
have one). No package is installed for it.

**The scrubber.** A native range input over the day's seconds (23 to 25 hours),
so a finger, a mouse and assistive technology all work on it, and it is placed on
the same time axis as the lanes. A position shows the last frame at or before it.
Keys are taken over so that an arrow is a frame, Shift and an arrow a minute,
Page Up and Down an hour. While it moves it shows the 360-pixel thumbnail; a
quarter second without moving is a rest, which shows the 1280-pixel one, loads
the frame's details and preloads the two thumbnails on each side, once each.
The plan's original on pause was not taken: 1280 pixels fill the view, and the
original is one link away ("Open the original"), for a stored file with or
without thumbnails *(conservative: bytes)*.

**Requests while dragging.** One loader per size, each running one load at a
time, the latest frame asked for next: frames dragged past are never requested.
Small loads also start at least 100 ms apart, so a fast network does not fetch
every frame either; a large load or a details request for a frame left is
cancelled. Measured in Chromium on 1,500 synthetic frames: a drag of five seconds
over 193 frames made 39 thumbnail requests (four of them preloads), one large
image and one details request; the handler took at most 3.3 ms a move. Without
the spacing, a local server answered fast enough for 170 requests.

**Lanes.** Canvas, one per source: Screen coloured by app in runs (a run ends at
the next frame or after five minutes), Browser one mark per visit, up to 2,000
visits a day (four pages of the events route). The cursor is a positioned
element. The visits within fifteen minutes of the frame are listed as text; only
`http` and `https` addresses become links, with `rel="noreferrer noopener"`.

**Colours.** The API's app colours are used only if they are `#rrggbb`, and set
through the CSSOM (the policy refuses style attributes, not the CSSOM). Text on
them is black or white, whichever has the higher WCAG contrast.

**Search.** Rendered on the server; "Older results" swaps in the next page with
htmx (an ordinary link without it). Moments are listed in the route's order:
newest window first, then apps by name within a window.

**What the browser keeps** *(conservative)*. No service worker, no IndexedDB, no
local storage. htmx 2 writes the current address to session storage on every
page it is loaded on, which no setting stops; on the search page that address
holds the search's words. So:
- htmx's history is off (`historyEnabled: false`, `historyCacheSize: 0`): it
  keeps no snapshot of a page;
- logging out sends `Clear-Site-Data: "cache", "storage"` (was `"cache"`), and the
  logout form empties both storages before it is sent, for browsers that ignore
  the header;
- a page restored from the back-forward cache reloads, so after logging out Back
  shows the login, not the day. Checked in Chromium.

**Layout.** Up to 760 pixels wide, one column: image, the frame's text, lanes,
slider, calendar; lanes (2.4 rem) and slider (2.75 rem) are taller for a finger,
and a lane lets the page scroll vertically (`touch-action: pan-y`) while a
horizontal drag scrubs. Checked at 390 pixels with touch emulated: no horizontal
scrolling. A visible focus ring on everything focusable. `[hidden]` always hides,
whatever a class sets.

**The header** links the timeline once a reader connection and a timeline
analysis exist (phase 0 left it out until there was something to see).

Left for the conversations phase and after: the conversation view and message
search; zooming and panning the image (the plan's tap to zoom); a copy button for
a frame's text (it is selectable); merged events of other sources around the
cursor; checking that Safari honours `Clear-Site-Data`.

*Review of the pages (2026-11-22).* An independent review of pull request #50
found one blocking point, widened on purpose, and three more:

- *Search words are never in an address* *(conservative)*. This revises part 1's
  `GET /api/timeline/search`. What the owner searched for is text from their own
  screen; in an address it reached the server's access log, the log of the
  gateway in front of it (outside this repository, its pod logs readable by
  tooling that must never see private data) and the browser's history, which
  `Clear-Site-Data` does not clear. A log filter alone would have left the
  gateway and the history. So:
  - the page's search form is sent with POST, and "Older results" posts the
    words, filters and next-page token as hidden fields of a small form, with
    htmx (`hx-post` to the bare path, `hx-push-url="false"`) or without it;
    `GET /timeline/search` shows the empty form;
  - the JSON route is `POST /api/timeline/search` with a JSON object (at most
    8 KB): the API's answers are JSON and its clients send JSON; a form body
    would have been a second shape for one route. Every field is checked as
    before; `limit` must be a number, an unknown field is refused;
  - `q` in an address is refused with 400 "Search with POST" on both, a `GET` of
    the JSON route always, and a POST with parameters in its address too, so
    nothing quietly goes back to sending words where logs keep them. The words
    are not echoed in the refusal. The other JSON routes stay GET: their
    parameters are days, times, keys and cursors;
  - answers stay `private, no-store`. No anti-forgery token was added: the
    session cookie is `SameSite=Strict`, cross-origin posts are refused by the
    login check (`Origin`), and a search changes nothing;
  - htmx's history is off and no search uses `hx-push-url`; the address htmx
    notes in session storage is now the bare `/timeline/search`, and it is
    cleared at logout anyway;
  - the page's title no longer carries the words: browsers keep titles in their
    history beside the addresses (found while checking in the browser);
  - a results page replaces its history entry with an ordinary one for the same
    bare address (`history.replaceState`), so Back from a moment shows the empty
    form instead of the browser's "send the form again?" page; without
    JavaScript, Back offers to send it again, which the browser asks first.
  Checked in Chromium on the synthetic day (search, older results, a moment,
  Back, logout): the address bar, the navigation entries, the titles, session
  storage and the server's access log held no search word.
- *The access log keeps no query string*, on every path: a filter on uvicorn's
  access logger keeps the path and drops what follows `?`. Logger filters survive
  uvicorn's logging configuration, which only replaces handlers; a test starts a
  real uvicorn server after the app, as `serve` does, and reads its access lines.
- *An expired login during an htmx request* loaded the login page into the
  results. A request with `HX-Request` that needs the login now answers 401 with
  `HX-Redirect: /login`, which htmx follows as a full page load.
- *Connections per process*: up to five as the reader (four for queries, one for
  image lookups). During a rolling update two processes hold them, so the role's
  limit (six, defined in the home-ops repository) covers one process at full
  use, not two. Recorded in the README's notes; no limit is changed here.

### 040 amendment — The media upgrade on the warehouse (2026-11-23)

The first build with the media columns failed on the installation's warehouse
(PostgreSQL 18.1) in `screen_frames`: "no empty local buffer available". The
table was left with the four columns added and empty, and every later build took
it as upgraded, so every frame stayed "unknown". Found from counts and the schema
only; reproduced on synthetic rows. Three causes, each fixed:

- *Building without a transaction.* dbt sends a literal `commit` before a hook
  that runs outside the model's transaction; `screen_frames` and
  `timeline_browser` have one (the raw indexes), which also committed as SQL.
  psycopg2 opens transactions implicitly and does not see a commit sent as SQL,
  so it opened none again, and dbt sends no BEGIN of its own on PostgreSQL.
  Everything after the hook ran with each statement committed on its own: the
  removal of a replaced input's rows, the `alter table`, the staged rows, the
  upsert, the ACL post-hook. So a failed build kept the columns it had added,
  and the exact ACL was not, as stated above, applied in the model's
  transaction. Now the hook creates its indexes one by one, each committed, and
  then opens the model's transaction itself (`begin`); dbt's final COMMIT, or its
  ROLLBACK on failure, ends it. Checked in the server's statement log, and by a
  test in which a build fails at its end (a reader role that does not exist):
  the table keeps neither the columns nor any change. `timeline_screen`,
  `events`, `days` and `apps` have no such hook and were in a transaction.
- *The upgrade decided by columns.* It is now decided by rows
  (`timeline_media_pending`): a table reads everything again while a media
  column is missing or a row that the current models always give a
  `media_withheld` (true or false) has none: every frame, every session not
  deleted, every screen event (browser events carry no media; their nulls are
  by design). A partial index on those rows, created by a pre-hook, answers the
  check at once when none is left. A test puts a table in the installation's
  state (columns present, all null in `screen_frames`, absent downstream) and
  one build fills every frame and session. This does not rely on the build
  being atomic.
- *Staging in a temporary table.* An incremental dbt build writes its new rows
  to a temporary table, then analyses it, deletes the rows it replaces and
  inserts it. Temporary tables use local buffers (`temp_buffers`, 8 MB by
  default). On PostgreSQL 18 a large staged table fails with "no empty local
  buffer available" where 17 does not: reproduced on 18.1 with 8,000 frames of
  about 3 KB and the smallest `temp_buffers` (800 kB), with the same rows and
  setting passing on 17.11; the installation's 17,805 real frames did it with
  8 MB. Every build that reads everything on an existing table stages that much:
  the upgrade, a replaced or relinked input (`cursor.rebuild`), more loads than
  one statement lists; a large ordinary increment too. The timeline models now
  stage in an unlogged table of their own schema (`timeline_staging.sql`
  overrides dbt-postgres's temporary relation for models with the
  `timeline_upsert` strategy only), which uses shared buffers and spills like
  any table; it is created in the model's transaction and dropped by the
  upsert. Raising `temp_buffers` was not an option: it cannot change once a
  session has touched a temporary table, and threads share connections. Swapping
  in a table built from scratch, as a rebuild does, was considered for the
  full reads; it would need every model to render as a first build in those
  cases (`events` and `days` keep deleted rows and previous positions
  differently), while staging in a real table changes no model's result and
  covers every path at once. A full read is slower than a rebuild (about a
  minute for 18,000 frames of 3 KB here) and happens once.

*Rows with nothing behind them* (review of #51). A row whose raw frame or load
is gone is never rewritten, so its null `media_withheld` would make every later
build read everything again, silently. Each of the three models now ends its
build by giving such rows the value the models give: on `screen_frames`,
`media_withheld = (withheld is not null)` from the row's own `withheld`; on the
sessions not deleted and the screen events, `false`. The partial index makes
this free when nothing is pending. Rows that have data were all rewritten by
the full read, so only orphans are touched, and for every row with data the
incremental tables still equal a first build. An orphan keeps everything else
it had: its media status stays null ("not known yet"), and it stays until a
rebuild from scratch, which drops it, as before the media columns. That is
acceptable: its file cannot be shown from the timeline's record anyway, and a
withheld orphan still answers 404 on the media route, which reads `withheld`.
A session with nothing behind it is marked deleted by the full recompute
itself. A test makes one orphan in each table and builds twice: the first
settles them, the second rewrites no frame.

*What stays unexplained.* Neither this work nor the review reproduced the
failure with `temp_buffers` at 8 MB. On the installation: PostgreSQL 18.1,
`temp_buffers` 8 MB, `shared_buffers` 128 MB, `io_method` worker, `io_workers`
3, `io_combine_limit` 16, `effective_io_concurrency` 200,
`maintenance_io_concurrency` 16, and about 18,000 frames holding 38 MB of text
(median 2.2 KB, 99th percentile 5.3 KB, largest 7 KB). Here, 8,000 frames of
about 3 KB failed at 800 kB and passed at 8 MB, also with both I/O concurrency
settings at 200 and with frames of 20 KB; the review saw failures from 800 kB to
2 MB, growing with the row count, and none at 8 MB up to 708 MB of text. The
fix does not depend on the cause: no timeline model uses a temporary table any
more.

A rebuild from scratch (`--full-refresh`) never used the temporary table: dbt
builds a real table and swaps it in. With the code before this fix it fills
every frame on 18.1 with the smallest `temp_buffers` (checked on synthetic rows
in the installation's state), so it is the way out on an installation that has
not taken this fix.

The timeline's model tests run on PostgreSQL 17.11 and 18.1.

### 040 amendment — Conversations data (phase 2a)

Status: proposed with the phase 2a pull request, 2026-11-23. What giving the
timeline its Apple Messages data had to decide that the plan left open. It adds
tables only: no route reads them yet, and the pages come in a later slice.
*(conservative)* marks the choices that touched privacy, correctness or cost.
The tables are listed in the README (*The timeline package*).

**A third input, `messages`.** The `timeline` package reads an `apple_messages`
connection as the optional role `messages`. Any non-empty subset of `screen`,
`browser` and `messages` builds; every model builds, empty, for an input that is
not linked.

**One rule for both packages** *(conservative)*. Decision 041's rules move into
macros (`dbt/macros/messages.sql`) that the messages package's staging models and
the timeline's models both call: a person (`messages_address`), a chat's
identifier, whether it is a group, its conversation key, a contact's name, what a
row is (message, reaction, removal, event), a reaction's kind and target, a time
that is no time, edited and unsent, an attachment's type and link previews, and
which reactions stand. The messages package's tables are unchanged by the move,
and a test builds both packages on the same rows and compares conversations,
messages, senders, reactions, attachments, participants and names.

**The opaque conversation id** *(conservative)*. Every route will name a
conversation by `conversation_id`: 32 hex characters, the first 128 bits of
SHA-256 over a random key of the analysis and the conversation key. The key
(`timeline_secret`, one row, 244 random bits from two `gen_random_uuid()`) is made
at the first build and readable by the owner alone; a plain hash of the key was
rejected because direct conversation keys are phone numbers, few enough to try
every one, and the schema's name is no secret. What the id protects is what
leaves the database: an address, a log line or a browser's history holding it
cannot be turned back into a number or an address without the database, and the
key and the chats' lookup (`timeline_message_chats`) are the owner's alone, so
the id cannot be computed offline. It does not hide a handle from the reader
role, and is not meant to *(revised after review)*: the reader is the server,
behind the login, and it must show unnamed people by their number. Whoever holds
the reader's password reads everything the tables hold, and finds a number's
conversation in one query (`conversations.participants`, `people`,
`messages.sender_handle` and `sender_written`, the events' payloads, a direct
conversation's `display_name`, which falls back to the identifier). No table
the reader can read holds the conversation key column itself.
- Unchanged by ordinary incremental builds, by a rebuild from scratch
  (`timeline_secret` is `full_refresh=false`, so `--full-refresh` keeps it), by a
  replaced or unlinked input, a new timezone or a changed reader role.
- Changed by a new key: dropping `timeline_secret` or the analysis's schema, or a
  new analysis (another analysis id or Miroir id is another schema). Every id
  changes then, and links to conversations stop working.
- Changed for one conversation when its key changes: a change of decision 041's
  rule, or a chat that becomes a group (an unstyled chat that lists a second
  handle) or stops being one. Its messages follow it to the new id.
A random id stored per conversation was the alternative; it would have been lost
at every rebuild from scratch, or needed a table that a rebuild may not touch
holding every conversation key, which this keeps to a single random value.

**The tables.** Three the reader can read, `conversations`, `messages` and
`people`; two it cannot, `timeline_secret` and `timeline_message_chats` (one row
per Apple chat with its conversation key). A message's reactions and attachments
are ordered JSON arrays on its row rather than tables of their own: a page of a
conversation is then one indexed range, a reaction or a file record rewrites the
one message it belongs to, and the result has one key to merge on.
- *Text* is kept as the collector delivered it, masked on the device; nothing is
  masked or unmasked here, and the U+FFFC that stands for each attachment stays.
  `text_unreadable` marks an archive the collector could not read. The messages
  package still holds no text (041).
- *Attachments*, in order of creation time, then transfer name, then guid: the
  database's own order (its row numbers) is not sent, and the U+FFFC in the text
  cannot be matched to a file without the archive. Link previews are left out as
  in 041. Each carries what the worker recorded in `media_objects` (this
  connection's rows of kind `attachments`): `media_status`, `thumbnail_w360`,
  `thumbnail_w1280`, all null without a record or without the table, which means
  unknown, as for screenshots. No `media_objects` exists for Messages yet.
- *Reactions*, those that stand under 041's rule, oldest first, each with its
  kind, sender, part and time. A changed tapback, a removal and a removal that
  follows a change end in the same state as in the messages package.
- *Search*: a GIN index on `to_tsvector('simple', text)` of `messages`; a result
  has its `guid`, `conversation_id` and `sent_at`, enough to open the
  conversation at it.

**What `events` holds for a message.** One row per dated message: `title` is its
text, `body` null, `group_key` its `conversation_id`, the media columns its first
attachment's, and `payload` the guid, conversation, whether it is mine, the
sender, the service, the numbers of attachments and reactions, edited, unsent,
audio and the message it replies to. The title is the text because the title is
what a list of events shows as each line. Who sent it and in which conversation
are not written as names, because names change: the lane resolves them through
`people` and `conversations`. Its `search` is empty *(conservative: cost)*: message
text is searched in `messages`, and the browser's search through the shared index
would otherwise step over every message. An undated message is not on the time
axis and has no event. `days` counts messages per local day like any source.

**How names resolve.** `people` is the one small table of names: a person
(`messages_address`), the address book's name (decision 038's matching), the
handle as Messages writes it, and the name to show. A message's sender is shown
as that name, else as the message writes the handle (`sender_written`), as in
041. A conversation's `display_name` is its group's title, else its people's
names (four, then "+N"), else "Group"; for a direct conversation, its person's
name, else the handle as written. `people` and `conversations` are rebuilt every
build, so a renamed contact renames old messages and conversations at the next
build and no message row is rewritten. Two differences from 041, which picks the
title and the identifier from the chat with the latest message: the timeline takes
them from the chat loaded last, because it keeps no per-chat message times, and
writes a person one way across all conversations (041 per conversation).

**What makes a message be read again.** Rows of loads `_dlt_loads` lists as
complete, from the loads completed since the model's cursor, as for the screen:
- a message row (new, or sent again after an edit, an unsend or from another Mac):
  its row is replaced;
- a reaction or removal: its target;
- a link to a chat, an attachment's metadata, a link to an attachment, a media
  record: the messages they belong to;
- a chat whose conversation changed: all its messages. Chats and their handles are
  snapshots of a few hundred rows, so each build reads them whole and
  `timeline_message_chats` writes only the chats that changed, with a new stamp
  only when their conversation did. A chat sent again unchanged, or with a new
  title, reads no message. Contacts never make a message be read.
Each is found through an index on the raw tables (load ID, guid, chat, attachment,
media key, and the reaction's target expression), created by the build as for the
screen. A guid sent again as something other than a message is kept as a deleted
row until `events` and `days` have read it; `days`' last post-hook removes it. A
message that moves keeps its old times in `previous_sent_at` until `days` has read
it. After a new timezone every message is read again *(revised after review)*:
one code path instead of two, at the same cost. On the review's history, two
changes took 29.4 and 33.0 s by an update of `day` in place and 28.8 and 34.3 s
by a full read, and left the table at 359 and 385 MB: no update is HOT, and
either way every row and its text index entry are written again. The rows'
indexed `timezone` column tells the build. A load that writes no message still counts as read *(revised after
review)*: `messages` keeps the cursor it read in its table's comment
(`keep_cursor`), carried through the upsert by one more staged row that is never
inserted. Without it, contact-only loads, unchanged chats, empty batches and
media records for unknown keys were read again by every build until one wrote a
message, and at 5,000 the build read everything.
A test checks after every step of a sequence (an edit, an unsend, reactions
changed and removed, messages before their chat, a chat sent again, a chat that
becomes a group, attachment metadata and links late, media records written and
replaced, a contact renamed, a message moved and one that stops being a message,
a load completing late, a new timezone) that the tables equal a rebuild from
scratch, and that a rebuild from scratch keeps the ids.

**Staging and the transaction**, as the amendment *The media upgrade on the
warehouse* sets them for every timeline model: `messages`,
`timeline_message_chats`, `events` and `days` stage their new rows in an unlogged
table of the schema, not dbt's temporary table, and `messages` runs in the
transaction the raw-index hook opens again. Without the first, reading 8,000
messages of about 4 KB again into the existing tables failed on PostgreSQL 18.1
with the smallest `temp_buffers` ("no empty local buffer available"). Without the
second, a build that failed in a post-hook kept the messages it had written; a test
now checks that it keeps none. The same `begin` was what made `messages`' `set
local` below take effect.

**Reading everything** *(conservative: cost)*. When `messages` reads every
message (a first build, a replaced input, more loads than one statement lists),
it turns nested loops off for its statement (`set local enable_nestloop = off`).
The planner cannot tell how many raw rows are messages (a CASE over two columns
of a table that may not be analysed yet), took one, and repeated each message's
attachment aggregate once per message: 63 s for 20,000 messages, 3 s with hash
joins. Incremental builds keep their index probes.

**Size, and PostgreSQL 18.** Measured on synthetic histories in 320 conversations
(one message in ten with a reaction, one in twenty-five with an attachment, a
quarter of them about 4 KB of text) on one development machine:
- 180,000 messages (production's size), PostgreSQL 18.1, before #51's staging was
  merged in: first build 34 s
  (`messages` 20 s, `events` 6 s, `conversations` 1.9 s); a build after one new
  message 4.9 s, of which `messages` 1.4 s and `conversations` and `people`
  0.1 s each, the rest dbt's own start and the other models; a build with nothing
  new 4.3 s; reading all of them again into the existing tables (a replaced
  input) 124 s, once.
- 20,000 messages: first build 7 to 9 s, a build after one message 2.7 to 4.3 s,
  on PostgreSQL 17.11 and 18.1, the second with the smallest `temp_buffers`.
The models' tests ran on both versions.

Left for the routes and pages: which conversations to list and how (by
`last_message_at`, indexed); the month scrubber's counts per conversation (from
`messages` by `(conversation_id, sent_at)`, not a table); `subject`, group events
(renames, people added), stickers' own files, the emoji of an emoji reaction,
edit history and read times, none of which the tables hold; the source name
`messages` in the server's list of sources; and thumbnails for attachments, which
the worker does not make yet.

*Review of #52 (2026-11-23).* An independent review found, and these were fixed,
each with a test that fails without the fix:
1. *One long text stopped the timeline.* A message (or a frame) of about 1 MB of
   distinct words is more than a tsvector holds (1 MB of words); the GIN index
   failed every build, and every model after `messages` was skipped, screen and
   browser included. Both indexes now cover the first 100,000 characters
   (`timeline_search_vector`; `search_vector` and `SEARCH_CHARACTERS` in
   `src/core/timeline.py`, the same expression, which the server's search uses so
   the index answers). At most 400 KB of words. The text is stored whole; words
   after its first 100,000 characters are not found by search, in a message or a
   frame. An existing `screen_frames` gets the bounded index once.
2. *A reaction sent again elsewhere stayed on its old target.* Only its new
   target was recomputed. Each message now lists every reaction and removal row
   aimed at it (`_reactions_from`), and `messages`' post-hook keeps the same
   turned around in `timeline_message_reaction_targets` (owner only), indexed by
   the reaction's guid, in its own transaction. A row in a new load recomputes
   the messages the map gives for its guid, one btree probe per row, whatever the
   row is now, so a reaction sent again as a plain message still finds its old
   target. The sequence test runs with no margin and includes a reaction aimed
   elsewhere and one sent again as a message. *Second review:* the first version
   looked the guids up in a GIN index on `_reactions_from` with one array of
   every guid of the new loads; inside the margin of an import of 180,000
   messages that took 43 s and doubled the build (80.1 s). With the map that
   build takes 31.5 s (PostgreSQL 18.1, the review's history; 40.7 s before
   the fix of finding 2), and later builds about 3 s as before.
8. *An empty guid* (second review) passed batch validation and was stored, then
   deleted by the next build's staged cursor row, keyed `''`. The batch now
   refuses an empty value in every guid key (messages, chats, attachments and the
   three link tables), and the cursor row's guid is null, which matches no key.
3. *The cursor did not move* after loads that wrote no message (above).
4. *Backups were readable.* dbt renames a table it rebuilds whole to
   `…__dbt_backup` and drops it after the commit; it kept the reader's grant
   until then, and if the session died after the commit, until the next build
   dropped it. `conversations`, `people` and
   `apps` are rebuilt every build and now replace their rows in one transaction
   (strategy `timeline_replace`): nothing is renamed, and their index names stay
   the same for the routes. A rebuild from scratch still renames every table:
   `timeline_exact_acl` strips every grant from the backup in the transaction that
   makes it, so it is never readable. Staging tables (#51) are created and dropped
   inside the model's transaction and never visible. A test reads, as the reader,
   every relation of the schema while a build and a rebuild from scratch run.
5. *Pages changed.* `GET /timeline` opened a day with only messages, showing no
   frame and no visit. The days answer keeps to the server's sources (`SOURCES`)
   until the message pages add `messages`: home page, calendar and days API are as
   before.
6. *A new timezone* is read again in full (above).
7. *Atomic per model, not per build.* A failed build keeps what the models before
   the failure wrote: `messages`, `people` and `conversations` can be ahead of
   `events` and `days`, and deleted rows can stay, until the next build. No query
   of the server goes wrong on it; the pages must read `messages` with `not
   deleted` and must not assume a message has its event or its day count yet. One
   transaction per build was not attempted (dbt commits per model).

Recorded, not changed:
- The reader can read the bookkeeping columns of `messages` (`_input`, `_stamp`,
  `_cursor`, `_chats_seen`, `_reactions_from`, `previous_sent_at`). Column grants
  would need the dbt `grants` config, the exact ACL and every reader query to
  change together, not one line; they hold no message content.
- For the routes slice: `conversations` has no last-message preview or sender,
  lists conversations with no message, and has a null `last_message_at` when every
  message is undated; undated messages sort last in the `(conversation_id,
  sent_at, guid)` index, so keyset paging must handle null times; `media_withheld`
  knows only screenshots, so `/media/<connection>/attachments/<key>` is served
  with no timeline check, and `media_url` in the pages names `screenshots`; "me"
  has no row in `people`.
- For the thumbnails slice: the kind is `attachments` and the key is exactly the
  collector's `media_key` (the attachment's guid and its lower-case extension).
- `apps`, `conversations` and `people` are incremental now (`timeline_replace`)
  with dbt's default `on_schema_change` (ignore): a column added to one of them
  later is not added to its existing table, and its rows are written without it,
  until a rebuild from scratch. `timeline_add_media_columns` covers only the media
  columns of the screen models.
- The first build after this is deployed replaces `screen_frames`' search index
  (`timeline_search_index`) inside its transaction: searches of the screen wait
  for it, at most 4.1 s over 18,000 frames (60 MB) in the review, longer on a
  larger history.

Consequences:
- The reader role can read message text. It is the server's own role (040).
- Links to a conversation last as long as `timeline_secret`; dropping it, or the
  schema, breaks them.
- `conversations`, `people` and `apps` are rebuilt in full every build. They read the
  chats and contacts whole and scan `messages`' indexes, which grow with the
  history (measured above).

## 041 — A messages analysis: conversations by identifier, no text

Status: accepted, 2026-11-22. Builds on 022, 038 and 040. Revised the same day
after review (see the end).

Dashboards read an analysis package's tables, never a source's raw tables. The
`messages` package is what makes dashboards on Apple Messages possible: who is
talked to, when, how much and how fast replies come. It has one required input,
an `apple_messages` connection, and the package timezone. Creating an Apple
Messages connection now creates it, as for the other sources.

**A conversation is the chat's own identifier** (`chat_identifier`: a number, an
address or a group id), never Apple's chat guid. One person reached over iMessage,
SMS and RCS has a chat per service, and the same chat has a different guid on Macs
on different macOS versions (038); all of those are one conversation. A chat with
no identifier falls back to the last part of its guid, which holds the same value,
and failing that to the whole guid, so a key is never empty.

The same person's identifier can be written in several ways: an address in
another case, a number with or without its country code (`5555550113` for SMS,
`+15555550113` for iMessage). A direct conversation's key is therefore the
identifier under one rule, which mirrors the one the collector uses to match a
handle to a contact (`_phone` and `_email` in its Apple Messages source): an
address (it holds `@`) in lower case; a number as its digits, with a leading 1
dropped from eleven; anything shorter than seven digits, such as a short code, as
written. The rule is the dbt macro `messages_address`, and the same rule joins
senders, participants and contact names. Names fall back to the identifier as
written. A group keeps its identifier as it is: its id holds digits that are not
a number. One person reached at a number and at an address is still two
conversations: nothing in the data says they are the same person.

**What counts as a message.** A row is a message when its `item_type` is 0 and its
`associated_message_type` is 0, 2 or 3 (or absent). A reaction is 2000 to 2007
(love, like, dislike, laugh, emphasis, question, emoji, sticker), plus 1000, a
sticker placed on a message; 3000 to 3007 remove one. Every other `item_type` is a
group event, a call, a shared location or a kept audio message, and is left out,
as is any other associated type (4000 is a poll vote). These codes are not in
Apple's documentation. They were checked against the reader in imessage-exporter
and several other open-source readers of the same database. Where a code is not
known, the row counts as nothing rather than as a message.

**Reactions that were removed.** Messages deletes a tapback's row when it is
removed, but rows already loaded are never deleted (038), so the warehouse can hold
both. For each sender, target message, part and kind, the latest row decides: a
tapback stands if it is newer than any removal, and a removal at the same time
wins. A person holds one tapback per part of a message, so of a sender's standing
tapbacks on one part only the latest is kept: a change of kind with no removal row
counts once. If a changed tapback is later removed while the earlier kind has no
removal row of its own, the earlier one stands again; the data cannot tell this
case apart. The emoji of an emoji reaction is not sent by the collector; such
reactions have the kind `emoji` only.

Stickers (1000 and 2007) are not tapbacks: several can be placed on one message,
so each sticker row is one reaction. A sticker removal (3007) does not say which
sticker it removes and cancels none; a removed sticker that was already loaded
stays.

**Reply times** are measured only in direct conversations. A run is the messages
one side sends before the other answers. The first message of a run gets the
minutes since the first message of the run before it: how long the other side
waited, not how long since the last message of a burst. Every other message has
none. Unsent messages count as messages here too. Beyond 24 hours the value is
null: the conversation is taken to have started again, and a reply two days later
would otherwise dominate an average.

**Times.** Apple's clock starts at 2001-01-01, and a missing date reads as 0 or
close to it. A time before 2001-01-02 is therefore no time. An undated message
stays in `messages` and in its conversation's totals, and is absent from
`daily`, from first and last times and from reply times.

**No text.** The tables hold lengths and flags, never text. Message text, subjects
and group renames stay in the raw tables, which only the staging view reads. The
text of a group's title, a chat's display name and contact names are names, not
messages, and are kept. Lengths are counted after the collector's masking, without
the placeholder character Messages puts in the text for each attachment.

Choices where the data cannot tell:

- A message linked to chats of two conversations takes the first key in order. A
  message in no chat is kept, with no conversation: it counts in the daily totals
  and in no conversation.
- A sender is set only for messages from someone else. For my own messages the
  source's handle is the other person in a direct chat and empty in a group.
- A chat is a group when its style is 43, direct when 45, and otherwise a group
  when it has a room name or lists more than one handle. A conversation is a group
  when any of its chats is. An unnamed group with nobody listed is called "Group".
- Participants are the handles listed in a conversation's chats and those who sent
  in it, never me. A conversation with no listed handles and no received messages
  has none.
- An unsent message counts as sent and unsent, not edited, even if it carries an
  edit time.
- An attachment counts only when its metadata arrived. Link previews
  (`.pluginPayloadAttachment`, what Messages fetched for a link) are not
  attachments anyone sent and are left out, as are files on reactions (stickers).

Left out: the text and anything derived from it beyond its length, edit history,
read and delivery times, calls and shared locations, poll votes, and the
attachment files themselves. Merging a person's number and address into one
person waits for a reliable source of that link.

Every table is rebuilt in full. On 200,000 synthetic messages a build takes about
four seconds, of which dbt's own start is more than a third; incremental tables
would add complexity for little. The collector delivers about every minute, so the
package has a minimum build interval of 10 minutes (040): loads within it join one
build at its end, and the tables are at most about ten minutes behind the raw
data.

**Review.** An independent review of the first version found six things, all
taken: one person written two ways split into two conversations (the address
rule above); no build interval; a missing or epoch date started `daily` at
Apple's epoch and counted in some tables but not others; reply times measured
from the last message of a burst; stickers merged and cancelled like tapbacks,
and a changed tapback counted twice; and empty or bare-id names for an unnamed
group or a chat with no identifier.

## 042 — Dashboards read the analysis outputs; `run_dbt` is removed

Status: accepted, 2026-11-22. Supersedes the part of 025 that kept `run_dbt`.

On 2026-11-22 every Metabase dashboard was moved onto the Miroir analysis
outputs (the `miroir_<analysis>_…` schemas), and a `crossovers` analysis was
added in Miroir for the crossover dashboards. The schemas only the legacy
nightly `run_dbt` built were then dropped from the warehouse: the mart schemas
`music`, `health`, `github`, `biking`, `browser` and `crossovers`, the shared
`staging` and `intermediate` schemas, and the frozen `browser_history_data`
dataset. Nothing read them any more, and the next `run_dbt` would have
recreated them.

So `run_dbt` goes, with everything that existed only for it: the flow in
`transform.py`, `prefect.yaml` (it held no other deployment), the `deploy`,
`run:dbt` and `worker` mise tasks, and the `GITHUB__AUTHOR_EMAILS` secret
reference. The dbt macros lose the branches only a build without an analysis
reached: the author emails from that environment variable, and the raw
`select *` and "every input present" paths of `optional_input` and
`input_present`. `generate_schema_name` keeps its fallback to the bare group
name: dbt calls it for every model, even when it parses without an analysis.

Miroir deployments still come from the configuration (013). The `local-pool`
process worker runs flows from other repositories; this repository no longer
deploys to it.

**By hand.** Removing the deployment from the code does not remove it from the
Prefect server. An installation that registered `run_dbt` must delete it
(`mise run prefect -- deployment delete run-dbt/run_dbt`; check the name with
`deployment ls`) or pause its schedule, or the
next run recreates the dropped schemas.
