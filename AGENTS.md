# AGENTS.md — working on this repo (Miroir)

Miroir is a self-hosted personal-data mirror: connections to personal sources
(ListenBrainz, Apple Health exports, GitHub, …) are created in a web interface,
executed as Prefect flow runs, loaded with dlt into a Postgres warehouse, and
modelled with dbt for Metabase. The product layer (config, credentials, status,
artifacts) is thin and home-grown; everything beneath it is commodity
infrastructure. Long-term direction: open source + a paid hosted tier; the
orchestrator adapter (below) is the seam that keeps the hosted option open.

**Read first:** `docs/miroir/decisions.md` (numbered, chronological — the
ground truth for *why*), `docs/miroir/README.md` (how everything is used),
`docs/miroir/implementation-log.md` (what was built, per session, with
validation notes). Older entries say "workspace" — the product was renamed to
Miroir (decision 014); history keeps its own words.

## Repo map

- `src/core/` — the product. `config.py` (validation, revisions,
  `metadata_url()`), `operations.py` (MetadataStore: operations, attempts,
  encrypted secrets, artifact registry, the queue of media to derive; Postgres
  schema `miroir_meta`),
  `runner.py` (register → lock → load → record → model; a package's minimum
  build interval defers builds, decision 040), `service.py` (everything both
  transports call), `secrets.py` (Fernet built-in store, env/fnox backends),
  `artifacts.py` (content-addressed receipt), `timeline.py` (the server's only,
  read-only, time-limited access to the warehouse), `derive.py` and
  `thumbnails.py` (the worker's thumbnails and `media_objects`; the only image
  decoding, run as a child process and imported by nothing).
- `src/web/` — FastAPI app + templates + `static/`. Token auth (`MIROIR_API_TOKEN`).
  A strict content security policy: no inline styles or scripts in templates.
  `connections.py` owns guided setup and connection settings; activity/ingestion
  routes stay in `app.py`. Integration delivery metadata drives the forms.
- `src/cli/` — `pipelines-mirroir`, one CLI over the same service.
- `src/adapters/prefect.py` — **the orchestrator seam** (decision 013): three
  verbs — `plan`/`apply` (deployments from config, drift reporting), `submit`
  (request → flow run, idempotent on the operation ID), `observe` (close
  unverified attempts from flow-run state). Any future hosted backend
  implements the same three verbs.
- `src/pipelines/miroir.py` — the Prefect flow `sync_connection` (one
  deployment per connection), and `transform.py` (dbt build).
- **Who may import whom** is in `docs/architecture.md` and enforced by
  `tests/test_boundaries.py` (decision 031). A new folder under `src/` fails
  that test until it is given a rule.
- `collector/` — `miroir-collector`, its own package (decisions 030, 031, 035): the
  local collector for device-only sources. Sources, masking, spool and delivery
  to a connection's receiving URL. **Standard library plus urllib3, pydantic
  and the Kingfisher scanner (pinned, imported by `scanner.py` only), nothing
  else** (decision 036); the server never imports it and it never imports
  `src/`. A source is one module under `collector/miroir_collector/sources/`
  that implements the contract in `sources/base.py`; no other module names
  a source. `collector/tests/test_boundaries.py` holds the collector's import rules
  and its dependency list, and its lint rules cap a function's branches. Its
  tests are in `collector/tests/`; `tests/test_collector_contract.py` ties a source
  to its connector, and `tests/test_collector_package.py` runs the collector's suite.
- Media (decision 033): files that rows point at. `src/core/media.py` signs
  upload and view links through the `ObjectStore` protocol; `src/adapters/s3.py`
  is the boto3 implementation; the collector's `media.py` uploads. No metadata
  table: the bucket is the record.
- `src/integrations/<name>/` — one package per connector: settings, credentials,
  check, resources, and inputs — a `fetch` and/or file `formats` with typed
  import options (decision 021) — plus its dlt source (`source.py`);
  `registry.py` only lists them. Upload, receiving URL, and watched folder are
  core transports that all register artifacts. The legacy per-source flows and
  `src/sources/` were retired after the 2026-11-11 cutovers (decision 025).
- `dbt/` — models (staging + reporting), profiles read
  `DESTINATION__POSTGRES__CREDENTIALS__*` env.
- `src/analyses/catalog.py` — analysis packages (typed input roles, dbt selector,
  outputs, parameters, version); top-level config analyses link connections to
  those roles. Integrations recommend a default package by ID (decision 022).
- `tests/` — unittest; no pytest. 163 tests, ~160s, include four against a real
  temporary Prefect API (`prefect_test_harness`), CLI subprocess tests, and seven
  disposable Postgres/dbt scenarios (set `MIROIR_TEST_POSTGRES_BIN` to the bin
  directory of an installed Postgres, or put `initdb`/`pg_ctl` on PATH; in this
  pod the mise shims have no default version, so use
  `~/.local/share/mise/installs/postgres/17.11/bin`). Without it the dbt class
  errors in `setUpClass`.

## Commands

```bash
uv run --locked python -m unittest discover -s tests          # the suite
uv run --locked ruff check --fix <paths> && uv run --locked ruff format <paths>
uv run --locked mypy --follow-imports=silent src
mise run miroir:deploy    # deployments apply (needs fnox secrets, see below)
mise run prefect -- <cmd> # prefect CLI with cluster credentials
```

Lint `src tests scripts`; mypy `src`. CI (`.github/workflows/test.yml`) runs the
same lint, format check, mypy and the full suite on pull requests and `main`;
`image.yml` only builds. Models reach the dashboards through Miroir analysis
builds, which run the pinned image, not `main`. Secrets for mise tasks come from ProtonPass via fnox
(`mise run pass:login`); grants are per task in `mise.toml`/`fnox.toml`.

## The deployed system (home-ops cluster, `automation` namespace)

- **Image**: `ghcr.io/h-wb/pipelines`, built by `.github/workflows/image.yml`
  on push to main. Tags: semver, `main`, `latest`, and `sha-<40hex>` — **always
  reference `sha-` tags**; the cluster's spegel mirror serves stale mutable
  tags (a `latest` once ran pre-rename code).
- **home-ops repo** (`~/Repositories/home-ops`, branch `main`, Flux GitOps,
  push with `--no-verify` from this pod — the pre-push hook fails on sandbox
  registry auth for unrelated apps):
  - `automation/miroir/` — the CNPG cluster `postgres-miroir` (cnpg/initdb
    component) and the server HelmRelease (`pipelines-mirroir serve`, route
    `miroir.<SECRET_DOMAIN>` behind envoy-internal).
  - `automation/prefect/app/` — the Prefect server + process worker
    (`local-pool`, flows from other repositories; this repo no longer deploys
    to it) and the miroir Kubernetes worker (`miroir-k8s`), the job
    template ConfigMap (NFS mount at `/miroir/data`, credentials via
    secretKeyRef), the `miroir` ExternalSecret (→ `miroir-secret`), RBAC.
- **State**: `miroir.json` + artifacts live on the NFS export
  (`/mnt/tank/hwe/Documents/data/miroir/`); flow-run Jobs mount it at
  `/miroir/data`. Metadata DB: schema `miroir_meta` in postgres-miroir
  (credentials from the CNPG `postgres-miroir-app` secret; the URL composes
  from `MIROIR_METADATA__*` — pin the psycopg2 dialect, see decisions).
- **Secrets**: `miroir-secret` (MIROIR_SECRET_KEY, MIROIR_API_TOKEN) from the
  Bitwarden item `miroir`; warehouse credentials from `postgres-metabase-app`.
  The workspace master key is the ONLY installation secret not derived in-cluster.
- **Operator API access**: the Bitwarden `miroir` item holds the API token.
  Resolve the CLI's `BWS_ACCESS_TOKEN` through its ProtonPass reference in
  home-ops `fnox.toml`, using an authorized active Pass session; keep credentials
  in process memory. This pod cannot read Kubernetes Secrets directly.

## Release and deploy cycle

1. Commit to `main` and push (pipelines repo). CI builds the image (≈2 min).
2. Set the new `sha-<full-commit>` in the live miroir.json `deployment.image`
   and Apply deployments; then pin it in home-ops (server + worker HelmRelease
   images) and push home-ops `main`. Execution settings now live in the config;
   the server no longer carries pool/image/job-variable fallback pins.
3. `flux -n flux-system reconcile source git flux-system`, then
   `flux -n automation reconcile kustomization <app> --with-source` (the
   kubeconfig: `kubernetes/clusters/main/kubeconfig` in home-ops; `kubectl`
   works read-only, `flux` reconcile and pod deletion are allowed).
4. Verify server/worker readiness, a flow run using the same image, and an empty
   deployments plan. Schema 6 was deployed with decision 022; schema 5 runtimes
   cannot run against it. Drain Miroir runs and preserve configuration plus a
   matching metadata recovery point before another incompatible migration.

## Working conventions

- **Decisions first**: design questions get an entry in `decisions.md` before
  or with the code; every session appends to `implementation-log.md` (what,
  why, validation). Commits follow the log.
- **Tests are the check**; the Prefect tests run against a real ephemeral API.
  New code runs lint + format + mypy + the suite before commit.
- **No secret values in this repo** — only ProtonPass/fnox references.
- **Readable errors**: product errors raise `OperationError` so both the web UI
  and CLI render them properly; never let raw HTTP/traceback errors escape.
- Container commands invoke the absolute interpreter
  (`/app/.venv/bin/python -m src.cli` / `-m prefect`) — a node once refused
  PATH lookups of console scripts despite a correct image.

## Known remaining work (2026-11-11)

- Legacy loads are retired (decision 025): GitHub, ListenBrainz, Bike Share and
  Health run on Miroir, bound to their original raw schemas. Operator leftovers:
  drop the unused `ws_8_personal_listenbrainz` and `ws_8_personal_health`
  schemas.
- Browser history: redo as a Miroir connection with a watched folder at
  `/miroir/data/browser` (the same Nextcloud-synced snapshot file the legacy
  flow loaded). `browser_history_data` was dropped on 2026-11-22, so the new
  connection starts from its own dataset.
- Dashboards read the Miroir analysis outputs and `run_dbt` is removed
  (decision 042). Operator leftover: delete or pause the `run_dbt` deployment
  in Prefect if it is still there.
- Cloud blockers (decision-adjacent, from the OSS+hosted analysis):
  per-tenant key separation, artifact storage abstraction (NFS paths are
  absolute on purpose today — they must resolve identically everywhere),
  multi-tenant metadata partitioning, accounts/OIDC.
- Cosmetic: generated dataset bindings still use the `ws_` prefix (pre-rename
  abbreviation). Every live connection now has an explicit binding, so renaming
  it only affects new connections.
- Audit leftovers (2026-11-11 log entry): GitHub date windows can miss late
  pushes/merges; browser rows keyed by profile-local SQLite IDs; Health units
  assumed, not read.
