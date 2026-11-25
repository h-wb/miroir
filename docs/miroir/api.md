---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How to start Miroir's server, the logins it accepts, every route of its HTTP API, and the headers its pages carry.
topics:
  - "[[Warehouse]]"
created: 2026-11-24T23:30
---

# Interface and API

One FastAPI app (`src/web/`) serves the pages and the JSON API over the same
services as the CLI (decision 010). Pages are rendered on the server with htmx
for small swaps.

```bash
export MIROIR_API_TOKEN="$(openssl rand -hex 24)"
export MIROIR_SECRET_KEY="$(uv run --locked pipelines-miroir generate-key)"
uv run --locked pipelines-miroir --config miroir.json serve --duckdb /tmp/miroir.duckdb
# http://127.0.0.1:8765; log in with the token. A missing miroir.json is created empty.
```

`--duckdb` is needed only without a `deployment` section, when requests run as
local subprocesses. Behind a TLS-terminating proxy, set `FORWARDED_ALLOW_IPS` to
the proxy's addresses so receiving URLs use HTTPS and cookies are Secure.

The overview lists connections and analyses apart: connections own source
settings, credentials, delivery, loads and schedules; analyses own
interpretation, builds, outputs and dashboard links (decision 022). The
configuration page edits the whole file with the same revision check.

## Logins

| Credential | Opens |
| --- | --- |
| `MIROIR_API_TOKEN`, as the login page's password (a session cookie) or `Authorization: Bearer` | Everything below except the delivery and metrics routes |
| A connection's delivery token, as a bearer | `receive`, `report`, `media/uploads` and `media/confirm` of that connection only |
| `MIROIR_METRICS_TOKEN`, as a bearer | `GET /metrics` only; the administrator's token and session are refused there |
| None | `/login`, `/healthz`, `/static/` |

An API call without a login gets 401 JSON; a page redirects to `/login`; an htmx
request gets `HX-Redirect: /login`.

## Routes

Edits of the configuration take `If-Match: <file_revision>` and answer 409 when
the file changed meanwhile.

| Method and path | Purpose |
| --- | --- |
| `GET /api/overview`, `GET /api/connections/{id}` | Status, history, files, delivery and collector state |
| `GET /api/integrations`, `GET /api/packages` | Connectors with settings schemas and credential fields; packages with inputs and parameters |
| `POST /api/connections` | Add a connection: `{integration, id, settings, credentials, resources?}`; returns the check result |
| `PUT /api/connections/{id}/settings` | Patch non-secret settings |
| `PUT /api/connections/{id}/resources` | `{"resources": [...]}`; null selects all; keeps data and cursors |
| `PUT /api/connections/{id}/credentials/{name}` | Replace a credential: `{"value": "..."}` (204) |
| `POST /api/connections/{id}/check`, `DELETE /api/connections/{id}` | Test or remove a connection |
| `POST /api/connections/{id}/prepare` | Prepare this connection's deployment and the Miroir's media deployment |
| `POST /api/connections/{id}/sync` | Request a sync (202, operation ID) |
| `POST /api/connections/{id}/artifacts?import=true&name=…&format=…&<option>=…` | Store the raw body as an artifact, optionally import it |
| `POST /api/connections/{id}/artifacts/{artifact}/import?replay=false` | Request an import (202) |
| `POST`, `DELETE /api/connections/{id}/delivery-token` | Create or replace (shown once), or revoke |
| `POST /api/connections/{id}/receive` | A sender's file ([the receiving URL](runs.md#the-receiving-url)) |
| `POST /api/connections/{id}/report` | A collector's report of a run (204; [observability](../observability.md)) |
| `POST /api/connections/{id}/media/uploads`, `…/media/confirm` | Upload links and confirmations ([Media](media.md#storage-and-links)) |
| `GET /api/analyses`, `GET /api/analyses/{id}` | Status, inputs, outputs, build history |
| `POST /api/analyses`, `PUT /api/analyses/{id}`, `DELETE /api/analyses/{id}` | Add `{id, package, inputs, timezone}`; change inputs, time zone, parameters or dashboard links; remove the configuration, keeping tables and history |
| `POST /api/analyses/{id}/models` | Rebuild without loading (202); needs a successful input load; `?full_refresh=true` from scratch |
| `GET /api/config`, `PUT /api/config` | Read or replace the configuration |
| `GET /api/deployments/plan`, `POST /api/deployments/apply`, `POST /api/refresh` | Reconcile with Prefect (needs the `deployment` section) |
| `GET /api/timeline/…`, `POST /api/timeline/search` | [Timeline](timeline.md#the-api) |
| `GET /media/{connection}/{kind}/{key}` | A file or its thumbnail ([images](timeline.md#images-through-the-server)) |
| `GET /metrics` | Prometheus metrics ([observability](../observability.md#metrics)) |
| `GET /healthz` | Liveness |

## Headers and pages

Every response carries
`Content-Security-Policy: default-src 'self'; img-src 'self' blob:; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'`,
`Referrer-Policy: same-origin` (no address of a Miroir page reaches another
host) and `X-Content-Type-Options: nosniff`. The policy refuses inline styles and
scripts, so templates load them from `src/web/static/` (served at `/static/`
without the login, revalidated on each load): `miroir.css`, `miroir.js`, the
timeline's scripts and the vendored `htmx.min.js`, whose version and SHA-256 are
in [VENDORED.md](../../src/web/static/VENDORED.md). A template with a `style`
attribute, a `<style>` element or an `on…=` handler fails
`tests/test_web_static.py`. The server's access log keeps each request's path and
drops its query string.
