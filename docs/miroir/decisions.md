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

Status: accepted, 2026-11-11. Completes the cutovers planned in 020 and 021.

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
