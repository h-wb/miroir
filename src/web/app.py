"""HTTP API and web interface over the shared miroir services.

Single-user and self-hosted: one admin bearer token (`MIROIR_API_TOKEN`). Browsers
log in with it once and get a session cookie derived from it; the token itself
is never stored in the cookie. Automatic exports use connection-scoped,
receive-only bearer tokens rather than the administrator token.
"""

import hashlib
import hmac
import json
import logging
import os
import re
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool

from src.core import media
from src.core.config import ConfigurationConflict, ConfigurationError
from src.core.operations import OperationError, OrchestratorUnavailable
from src.core.reports import (
    INVALID_REPORT,
    MAX_REPORT_BYTES,
    InvalidReport,
    parse_report,
)
from src.core.secrets import SecretKeyError, SecretResolutionError
from src.core.service import MiroirService
from src.core.timeline import (
    TimelineDatabase,
    TimelineTimeout,
    TimelineUnavailable,
    media_withheld,
    timeline_source,
)
from src.integrations.validation import InvalidArtifact
from src.web.metrics import render_metrics

SESSION_COOKIE = "miroir_session"
DEFAULT_MAX_UPLOAD = 512 * 1024 * 1024
CHUNK = 1024 * 1024
MULTIPART_SLACK = 1024 * 1024  # form fields and part headers around the file
MAX_MEDIA_BODY = 1024 * 1024
MEDIA_POST_PATH = re.compile(
    r"/api/connections/[a-z][a-z0-9_]{0,23}/media/(uploads|confirm)"
)
DELIVERY_POST_PATH = re.compile(
    r"/api/connections/[a-z][a-z0-9_]{0,23}/(receive|report)"
)
METRICS_CONTENT_TYPE = "text/plain; version=0.0.4"
METRICS_TOKEN_REUSED = (
    "MIROIR_METRICS_TOKEN must differ from MIROIR_API_TOKEN: "
    "a scraper must not hold the admin token"
)
# On every response. Pages load styles and scripts from /static only; images may
# also be blobs the timeline makes from fetched frames (decision 040). No address
# reaches another host; "no-referrer" would also make browsers post our own forms
# with "Origin: null", which the login check cannot tell from another site.
SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; img-src 'self' blob:; object-src 'none'; "
        "base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
    ),
    "Referrer-Policy": "same-origin",
    "X-Content-Type-Options": "nosniff",
}
STATIC_DIR = Path(__file__).parent / "static"
# Thumbnails and images copied through the server (decision 040).
IMMUTABLE = "private, max-age=31536000, immutable"
# Query parameters of the upload API that are not import options.
UPLOAD_PARAMETERS = {"import", "name", "format"}

# Fixed messages for redirects: nothing user-supplied is reflected from the URL.
NOTICES = {
    "sync_requested": "Sync requested.",
    "models_requested": "Analysis rebuild requested.",
    "import_requested": "Import requested.",
    "submitted": "File stored.",
    "duplicate": "This exact file was already stored; nothing new was saved.",
    "already_imported": "Already imported with the current parser; use replay to load it again.",
    "saved": "Configuration saved. It applies to operations requested from now on.",
    "created": "Connection added.",
    "credential_saved": "Credential saved.",
    "checked": "Connection checked; see the credentials status below.",
    "removed": "Connection removed. Its secrets were deleted; loaded data and files were kept.",
    "applied": "Deployments applied: the configuration and Prefect now agree.",
    "delivery_revoked": "Automatic delivery token revoked. File uploads remain available.",
    "analysis_created": "Analysis added. It builds after its inputs next load; rebuild to build it now.",
    "analysis_removed": "Analysis removed. Its built tables were kept.",
}

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


class UploadTooLarge(ValueError):
    pass


class Unauthorized(Exception):
    pass


class NoQueryInAccessLog(logging.Filter):
    """uvicorn's access lines name the path with its query string (client,
    method, path, version, status). The query is dropped, on every path: an
    address may carry what was looked for (decision 040)."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str):
            record.args = (*args[:2], args[2].split("?", 1)[0], *args[3:])
        return True


def quiet_access_log() -> None:
    """Install the filter on uvicorn's access logger, once. Logger filters
    survive uvicorn's own logging configuration, which replaces handlers."""
    logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, NoQueryInAccessLog) for f in logger.filters):
        logger.addFilter(NoQueryInAccessLog())


def error_status(error: Exception) -> int:
    """HTTP status of a product error. 503 asks clients to retry later."""
    from src.core.runner import AlreadyImported

    if isinstance(error, (ConfigurationConflict, AlreadyImported)):
        return 409
    if isinstance(error, UploadTooLarge):
        return 413
    # Services may re-raise an outage with context (`raise ... from`).
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, OrchestratorUnavailable):
            return 503
        cause = cause.__cause__
    return 400


async def json_body(request: Request) -> Any:
    try:
        return await request.json()
    except ValueError:  # also UnicodeDecodeError
        raise OperationError("The request body must be valid JSON") from None


def resource_selection(value: Any) -> list[str] | None:
    """Shape validation at the transport boundary; config validates resource IDs."""
    if value is None:
        return None
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise OperationError("Resources must be a list of names or null")
    return value


def create_app(
    service: MiroirService,
    token: str,
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD,
    metrics_token: str | None = None,
    timeline: TimelineDatabase | None = None,
) -> FastAPI:
    """`metrics_token` enables `/metrics` for a scraper that holds only it.

    `timeline` is the read-only warehouse connection; without it the timeline
    routes say it is not configured.
    """
    if len(token) < 16:
        raise ValueError("MIROIR_API_TOKEN must be at least 16 characters")
    if metrics_token is not None and hmac.compare_digest(
        metrics_token.encode(), token.encode()
    ):
        raise ValueError(METRICS_TOKEN_REUSED)
    session = hmac.new(token.encode(), b"miroir-session", hashlib.sha256).hexdigest()
    quiet_access_log()
    app = FastAPI(title="Miroir", docs_url=None, redoc_url=None)

    def authorize(request: Request) -> None:
        header = request.headers.get("authorization", "")
        if header.lower().startswith("bearer "):
            if hmac.compare_digest(header[7:].strip().encode(), token.encode()):
                return
            raise Unauthorized
        cookie = request.cookies.get(SESSION_COOKIE, "")
        if not hmac.compare_digest(cookie.encode(), session.encode()):
            raise Unauthorized
        # Cookies are SameSite=Strict; also refuse cross-origin form posts outright.
        origin = request.headers.get("origin")
        if request.method == "GET" or not origin:
            return
        if origin == "null":
            # A browser hides the origin of a same-origin post when the page asked
            # for no referrer. Its own Sec-Fetch-Site header still tells them apart.
            if request.headers.get("sec-fetch-site") != "same-origin":
                raise Unauthorized
        elif urlsplit(origin).netloc != request.url.netloc:
            raise Unauthorized

    @app.middleware("http")
    async def limit_body(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.method == "POST" and MEDIA_POST_PATH.fullmatch(request.url.path):
            return await call_next(request)
        # A multipart upload is spooled to disk before its handler can check it.
        declared = request.headers.get("content-length", "")
        if declared.isdigit() and int(declared) > max_upload_bytes + MULTIPART_SLACK:
            message = f"Upload exceeds {max_upload_bytes} bytes"
            return JSONResponse({"error": message}, status_code=413)
        return await call_next(request)

    @app.middleware("http")
    async def require_login(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        # /metrics checks its own token and never accepts the admin's. The login
        # page needs the stylesheet; nothing under /static is private.
        if request.url.path in (
            "/login",
            "/healthz",
            "/metrics",
        ) or request.url.path.startswith("/static/"):
            return await call_next(request)
        # These routes authenticate their connection-scoped bearer before reading bytes.
        if request.method == "POST" and (
            DELIVERY_POST_PATH.fullmatch(request.url.path)
            or MEDIA_POST_PATH.fullmatch(request.url.path)
        ):
            return await call_next(request)
        try:
            authorize(request)
        except Unauthorized:
            if request.url.path.startswith("/api/"):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
            if request.headers.get("hx-request") == "true":
                # htmx would swap the login page into the page; it loads it instead.
                return Response(status_code=401, headers={"HX-Redirect": "/login"})
            return RedirectResponse("/login", status_code=303)
        return await call_next(request)

    @app.middleware("http")
    async def security_headers(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        response.headers.update(SECURITY_HEADERS)
        if request.url.path.startswith("/static/"):
            # Names carry no version: revalidate (ETag) so a deploy shows at once.
            response.headers["Cache-Control"] = "no-cache"
        return response

    async def known_error(request: Request, error: Exception) -> Response:
        status = error_status(error)
        if request.url.path.startswith("/api/"):
            return JSONResponse({"error": str(error)}, status_code=status)
        return templates.TemplateResponse(
            request, "error.html", {"message": str(error)}, status_code=status
        )

    # AlreadyImported and ConfigurationConflict are subclasses of these.
    for kind in (
        ConfigurationError,
        OperationError,
        InvalidArtifact,
        UploadTooLarge,
        SecretResolutionError,
        SecretKeyError,
    ):
        app.add_exception_handler(kind, known_error)

    async def save_stream(chunks: AsyncIterator[bytes], directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        handle, name = tempfile.mkstemp(dir=directory, prefix=".upload-")
        size = 0
        try:
            with os.fdopen(handle, "wb") as target:
                async for chunk in chunks:
                    size += len(chunk)
                    if size > max_upload_bytes:
                        raise UploadTooLarge(f"Upload exceeds {max_upload_bytes} bytes")
                    target.write(chunk)
        except BaseException:
            os.unlink(name)
            raise
        return Path(name)

    async def upload_chunks(upload: UploadFile) -> AsyncIterator[bytes]:
        while chunk := await upload.read(CHUNK):
            yield chunk

    def submit(
        connection_id: str,
        path: Path,
        options: dict[str, str],
        import_now: bool,
        name: str,
        input_format: str | None = None,
    ) -> dict:
        try:
            return service.submit_file(
                connection_id,
                path,
                options,
                import_now=import_now,
                original_name=name,
                input_format=input_format,
            )
        finally:
            path.unlink(missing_ok=True)

    staging = service.artifact_root / ".uploads"

    # ---- JSON API -------------------------------------------------------------

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    @app.get("/api/overview")
    def api_overview() -> dict:
        return service.overview()

    @app.get("/api/connections/{connection_id}")
    def api_connection(connection_id: str) -> dict:
        return service.connection(connection_id)

    @app.post("/api/connections/{connection_id}/sync", status_code=202)
    def api_sync(connection_id: str) -> dict:
        return {"operation_id": service.request_sync(connection_id)}

    @app.get("/api/packages")
    def api_packages() -> list:
        return service.packages()

    @app.get("/api/analyses")
    def api_analyses() -> list:
        return service.analyses()

    @app.get("/api/analyses/{analysis_id}")
    def api_analysis(analysis_id: str) -> dict:
        return service.analysis(analysis_id)

    @app.post("/api/analyses", status_code=201)
    async def api_create_analysis(request: Request) -> dict:
        body = await json_body(request)
        if not isinstance(body, dict) or not isinstance(body.get("inputs"), dict):
            raise OperationError("Expected {id, package, inputs, timezone}")
        analysis_id = service.create_analysis(
            str(body.get("id", "")),
            str(body.get("package", "")),
            body["inputs"],
            request.headers.get("if-match", "").strip('"'),
            timezone=str(body.get("timezone", "UTC")),
        )
        return {"id": analysis_id}

    @app.put("/api/analyses/{analysis_id}")
    async def api_update_analysis(request: Request, analysis_id: str) -> dict:
        values = await json_body(request)
        allowed = {"timezone", "parameters", "inputs", "dashboards"}
        if not isinstance(values, dict) or set(values) - allowed:
            raise OperationError(f"Expected an object with {sorted(allowed)}")
        service.update_analysis(
            analysis_id, values, request.headers.get("if-match", "").strip('"')
        )
        return {"saved": True}

    @app.post("/api/analyses/{analysis_id}/models", status_code=202)
    def api_models(analysis_id: str, full_refresh: bool = False) -> dict:
        return {
            "operation_id": service.request_models(
                analysis_id, full_refresh=full_refresh
            )
        }

    @app.delete("/api/analyses/{analysis_id}", status_code=204)
    def api_remove_analysis(request: Request, analysis_id: str) -> Response:
        service.remove_analysis(
            analysis_id, request.headers.get("if-match", "").strip('"')
        )
        return Response(status_code=204)

    @app.post("/api/connections/{connection_id}/artifacts")
    async def api_upload(
        request: Request,
        connection_id: str,
        name: str = "upload",
    ) -> JSONResponse:
        """Store the raw request body as one file (e.g. a Health Auto Export POST).

        `?import=true` also requests its import. `name` is only a label and
        `format` picks one of the integration's formats. Every other query
        parameter is an import option of that format (e.g. `?role=historical`).
        """
        import_now = request.query_params.get("import", "").lower() in (
            "1",
            "true",
            "yes",
        )
        options = {
            key: value
            for key, value in request.query_params.items()
            if key not in UPLOAD_PARAMETERS
        }
        path = await save_stream(request.stream(), staging)
        result = await run_in_threadpool(
            submit,
            connection_id,
            path,
            options,
            import_now,
            name,
            request.query_params.get("format"),
        )
        return JSONResponse(result, status_code=201 if result["new"] else 200)

    def bearer_token(request: Request) -> str:
        header = request.headers.get("authorization", "")
        return header[7:].strip() if header.lower().startswith("bearer ") else ""

    def delivery_authorized(request: Request, connection_id: str) -> bool:
        return service.verify_delivery_token(connection_id, bearer_token(request))

    @app.post("/api/connections/{connection_id}/receive")
    async def receive(request: Request, connection_id: str) -> JSONResponse:
        if not delivery_authorized(request, connection_id):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        path = await save_stream(request.stream(), staging)
        try:
            result = await run_in_threadpool(
                service.submit_file,
                connection_id,
                path,
                # Received files take the format's default import options.
                import_now=True,
                origin="webhook",
                input_format=request.query_params.get("format"),
            )
        finally:
            path.unlink(missing_ok=True)
        # Receive-only clients get a receipt, never internal storage paths/history.
        receipt = {"artifact_id": result["artifact"]["id"], "new": result["new"]}
        for field in ("operation_id", "already_imported"):
            if field in result:
                receipt[field] = result[field]
        return JSONResponse(receipt, status_code=201 if result["new"] else 200)

    async def deliver_media(
        request: Request,
        connection_id: str,
        operation: Callable[[str, str, object], list[dict[str, Any]]],
    ) -> Response:
        if not delivery_authorized(request, connection_id):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_MEDIA_BODY:
                raise UploadTooLarge(f"Media request exceeds {MAX_MEDIA_BODY} bytes")
            body.extend(chunk)
        try:
            payload = json.loads(body)
        except (ValueError, RecursionError):
            raise OperationError("The request body must be valid JSON") from None
        if (
            not isinstance(payload, dict)
            or payload.keys() != {"kind", "objects"}
            or not isinstance(payload["kind"], str)
        ):
            raise OperationError("The media request must contain a kind and objects")
        results = await run_in_threadpool(
            operation, connection_id, payload["kind"], payload["objects"]
        )
        return Response(json.dumps({"objects": results}), media_type="application/json")

    @app.post("/api/connections/{connection_id}/media/uploads")
    async def media_uploads(request: Request, connection_id: str) -> Response:
        return await deliver_media(
            request, connection_id, service.request_media_uploads
        )

    @app.post("/api/connections/{connection_id}/media/confirm")
    async def media_confirm(request: Request, connection_id: str) -> Response:
        return await deliver_media(request, connection_id, service.confirm_media)

    @app.post("/api/connections/{connection_id}/report", status_code=204)
    async def report(request: Request, connection_id: str) -> Response:
        """A collector's account of one run (decision 039): stored, never answered."""
        if not delivery_authorized(request, connection_id):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_REPORT_BYTES:
                raise UploadTooLarge(f"Report exceeds {MAX_REPORT_BYTES} bytes")
            body.extend(chunk)
        try:
            parsed = parse_report(bytes(body))
        except InvalidReport:
            return JSONResponse({"error": INVALID_REPORT}, status_code=422)
        stored = await run_in_threadpool(
            service.record_collector_report,
            connection_id,
            parsed,
            bearer_token(request),
        )
        if not stored:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return Response(status_code=204)

    @app.get("/metrics")
    def metrics(request: Request) -> Response:
        if not metrics_token:
            return Response(status_code=404)
        if not hmac.compare_digest(
            bearer_token(request).encode(), metrics_token.encode()
        ):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        # Set as is: Starlette would append a charset to a text media type.
        return Response(
            render_metrics(service.deliveries()),
            headers={"Content-Type": METRICS_CONTENT_TYPE},
        )

    @app.get("/media/{connection_id}/{kind}/{key:path}")
    def media_view(
        request: Request, connection_id: str, kind: str, key: str
    ) -> Response:
        """A redirect to a short signed link; with `size`, the bytes themselves.

        `size=360` or `1280` copies that thumbnail (404 if none was made);
        `size=original` copies an image original up to a size, and redirects for
        any other file. Copied bytes are cached as immutable: a key's file and its
        thumbnails do not change. The server never decodes them (decision 040).

        A file of a frame the timeline says is withheld is not found, at any size
        or as a link; a timeline that cannot say refuses rather than serve it.
        """
        if not media.valid_key(key):  # before the warehouse sees it (NUL)
            return Response(
                status_code=404, headers={"Cache-Control": "private, no-store"}
            )
        refused = withheld_refusal(connection_id, kind, key)
        if refused is not None:
            return refused
        size = request.query_params.get("size")
        if size is not None:
            return media_bytes(request, connection_id, kind, key, size)
        return media_link(connection_id, kind, key)

    def withheld_refusal(connection_id: str, kind: str, key: str) -> Response | None:
        no_store = {"Cache-Control": "private, no-store"}
        try:
            withheld = media_withheld(
                timeline, service.miroir(), connection_id, kind, key
            )
        except TimelineTimeout as error:
            return JSONResponse({"error": str(error)}, 504, headers=no_store)
        except TimelineUnavailable as error:
            return JSONResponse({"error": str(error)}, 503, headers=no_store)
        return Response(status_code=404, headers=no_store) if withheld else None

    def media_link(connection_id: str, kind: str, key: str) -> Response:
        try:
            url = service.media_view_url(connection_id, kind, key)
        except OperationError:
            return Response(status_code=404)
        return RedirectResponse(
            url, status_code=302, headers={"Cache-Control": "private, no-store"}
        )

    def media_bytes(
        request: Request, connection_id: str, kind: str, key: str, size: str
    ) -> Response:
        no_store = {"Cache-Control": "private, no-store"}
        if len(request.query_params.multi_items()) != 1:
            return Response(status_code=400, headers=no_store)
        try:
            if size == "original":
                found = service.media_image(connection_id, kind, key)
                content_type = media.image_type(key)
            elif size in ("360", "1280"):
                found = service.media_thumbnail(connection_id, kind, key, int(size))
                content_type = "image/webp"
            else:
                return Response(status_code=400, headers=no_store)
        except media.NotStreamed:
            return media_link(connection_id, kind, key)
        except media.StorageUnavailable:
            return Response(status_code=503, headers=no_store)
        except OperationError:
            return Response(status_code=404, headers=no_store)
        if found is None:
            return Response(status_code=404, headers=no_store)
        headers = {"Cache-Control": IMMUTABLE}
        if found.etag:
            # The store's tag for these bytes, as a strong tag.
            headers["ETag"] = '"' + found.etag.strip('"').replace('"', "") + '"'
            if headers["ETag"] in {
                tag.strip().removeprefix("W/")
                for tag in request.headers.get("if-none-match", "").split(",")
            }:
                return Response(status_code=304, headers=headers)
        return Response(found.body, media_type=content_type, headers=headers)

    @app.post("/api/connections/{connection_id}/delivery-token", status_code=201)
    def create_delivery_token(connection_id: str) -> JSONResponse:
        return JSONResponse(
            {"token": service.create_delivery_token(connection_id)},
            status_code=201,
            headers={"Cache-Control": "no-store"},
        )

    @app.delete("/api/connections/{connection_id}/delivery-token", status_code=204)
    def revoke_delivery_token(connection_id: str) -> Response:
        service.revoke_delivery_token(connection_id)
        return Response(status_code=204)

    @app.post(
        "/api/connections/{connection_id}/artifacts/{artifact_id}/import",
        status_code=202,
    )
    def api_import(connection_id: str, artifact_id: str, replay: bool = False) -> dict:
        return {
            "operation_id": service.request_import(connection_id, artifact_id, replay)
        }

    @app.get("/api/integrations")
    def api_integrations() -> list:
        return service.integrations()

    @app.post("/api/connections", status_code=201)
    async def api_create_connection(request: Request) -> dict:
        """Body: {"integration", "id", "settings": {...}, "credentials": {name: value}}."""
        body = await json_body(request)
        if not isinstance(body, dict):
            raise OperationError("Expected a JSON object")
        connection_id = service.create_connection(
            str(body.get("integration", "")),
            str(body.get("id", "")),
            body.get("settings") or {},
            body.get("credentials") or {},
            delivery_method=body.get("delivery_method"),
            resources=resource_selection(body.get("resources")),
        )
        check = await run_in_threadpool(service.check_connection, connection_id)
        return {"id": connection_id, "check": check}

    @app.put("/api/connections/{connection_id}/settings")
    async def api_save_connection_settings(
        request: Request, connection_id: str
    ) -> dict:
        settings = await json_body(request)
        if not isinstance(settings, dict):
            raise OperationError("Expected a settings object")
        service.update_connection_settings(
            connection_id, settings, request.headers.get("if-match", "").strip('"')
        )
        check = await run_in_threadpool(service.check_connection, connection_id)
        return {"saved": True, "check": check}

    @app.put("/api/connections/{connection_id}/resources")
    async def api_save_connection_resources(
        request: Request, connection_id: str
    ) -> dict:
        body = await json_body(request)
        if not isinstance(body, dict) or "resources" not in body:
            raise OperationError("Expected an object with resources")
        service.update_connection_resources(
            connection_id,
            resource_selection(body["resources"]),
            request.headers.get("if-match", "").strip('"'),
        )
        return {"saved": True}

    @app.post("/api/connections/{connection_id}/prepare")
    def api_prepare_connection(connection_id: str) -> dict:
        service.prepare_connection(connection_id)
        return {"ready": True}

    @app.put("/api/connections/{connection_id}/credentials/{name}", status_code=204)
    async def api_set_credential(
        request: Request, connection_id: str, name: str
    ) -> Response:
        """Body: {"value": "..."}. The value is never returned by any endpoint."""
        body = await json_body(request)
        value = body.get("value") if isinstance(body, dict) else None
        if not isinstance(value, str):
            raise OperationError('Expected {"value": "..."}')
        service.set_credential(connection_id, name, value)
        return Response(status_code=204)

    @app.post("/api/connections/{connection_id}/check")
    def api_check(connection_id: str) -> dict:
        return service.check_connection(connection_id)

    @app.delete("/api/connections/{connection_id}", status_code=204)
    def api_remove(connection_id: str) -> Response:
        service.remove_connection(connection_id)
        return Response(status_code=204)

    @app.post("/api/refresh")
    def api_refresh() -> list:
        return service.refresh()

    @app.get("/api/deployments/plan")
    def api_deployments_plan() -> list:
        return service.deployments_plan()

    @app.post("/api/deployments/apply")
    def api_deployments_apply() -> list:
        return service.deployments_apply()

    @app.post("/deployments/apply")
    def apply_deployments() -> Response:
        service.deployments_apply()
        return RedirectResponse("/?notice=applied", status_code=303)

    @app.get("/api/config")
    def api_config() -> dict:
        return service.configuration()

    @app.put("/api/config")
    async def api_save_config(request: Request) -> dict:
        expected = request.headers.get("if-match", "").strip('"')
        if not expected:
            raise OperationError(
                "If-Match with the file_revision you edited is required"
            )
        try:
            text = (await request.body()).decode("utf-8")
        except UnicodeDecodeError:
            raise OperationError("The configuration must be UTF-8 text") from None
        miroir = service.save_configuration(text, expected)
        return {"saved": True, "revision": miroir.revision}

    # ---- Web interface ---------------------------------------------------------

    def timeline_link() -> bool:
        """The header links the timeline only where there is one to see."""
        if timeline is None:
            return False
        try:
            return timeline_source(service.miroir()) is not None
        except (ConfigurationError, OperationError):
            return False

    def page(request: Request, name: str, **context: Any) -> HTMLResponse:
        context["notice"] = NOTICES.get(request.query_params.get("notice", ""))
        context.setdefault("timeline_link", timeline_link())
        return templates.TemplateResponse(request, name, context)

    @app.get("/login", response_class=HTMLResponse)
    def login_form(request: Request) -> HTMLResponse:
        return page(request, "login.html", failed=False)

    @app.post("/login")
    def login(request: Request, token_input: str = Form(alias="token")) -> Response:
        if not hmac.compare_digest(token_input.encode(), token.encode()):
            return templates.TemplateResponse(
                request, "login.html", {"failed": True, "notice": None}, status_code=401
            )
        response = RedirectResponse("/", status_code=303)
        response.set_cookie(
            SESSION_COOKIE,
            session,
            httponly=True,
            samesite="strict",
            secure=request.url.scheme == "https",
        )
        return response

    @app.post("/logout")
    def logout() -> Response:
        response = RedirectResponse("/login", status_code=303)
        response.delete_cookie(SESSION_COOKIE)
        # Screenshots stay in the browser's cache for a year otherwise, and htmx
        # keeps the last address (a search's words) in session storage.
        response.headers["Clear-Site-Data"] = '"cache", "storage"'
        return response

    from src.web.analyses import analysis_pages
    from src.web.connections import connection_pages
    from src.web.timeline import timeline_pages
    from src.web.timeline_pages import timeline_page_routes

    app.include_router(connection_pages(service, page))
    app.include_router(analysis_pages(service, page))
    app.include_router(timeline_pages(timeline, page, service.miroir))
    app.include_router(timeline_page_routes(timeline, page, service.miroir))
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", response_class=HTMLResponse)
    def overview(request: Request) -> HTMLResponse:
        data = service.overview()
        refresh = any(_in_flight(c["status"]) for c in data["connections"])
        return page(request, "overview.html", overview=data, refresh=refresh)

    @app.get("/connections/{connection_id}", response_class=HTMLResponse)
    def connection(request: Request, connection_id: str) -> HTMLResponse:
        data = service.connection(connection_id)
        return page(
            request,
            "connection.html",
            c=data,
            receive_path=f"/api/connections/{connection_id}/receive",
            refresh=_in_flight(data["status"]),
        )

    def back(connection_id: str, notice: str) -> RedirectResponse:
        return RedirectResponse(
            f"/connections/{connection_id}?notice={notice}", status_code=303
        )

    @app.post("/connections/{connection_id}/sync")
    def sync(connection_id: str) -> Response:
        service.request_sync(connection_id)
        return back(connection_id, "sync_requested")

    @app.post("/connections/{connection_id}/upload")
    async def upload(
        request: Request,
        connection_id: str,
        file: UploadFile,
        import_now: bool = Form(False),
    ) -> Response:
        form = await request.form()
        options = {
            key.removeprefix("option."): str(value)
            for key, value in form.items()
            if key.startswith("option.")
        }
        path = await save_stream(upload_chunks(file), staging)
        result = await run_in_threadpool(
            submit,
            connection_id,
            path,
            options,
            import_now,
            file.filename or "upload",
            str(form.get("format") or "") or None,
        )
        if result.get("already_imported"):
            return back(connection_id, "already_imported")
        if result.get("operation_id"):
            return back(connection_id, "import_requested")
        return back(connection_id, "submitted" if result["new"] else "duplicate")

    @app.post("/connections/{connection_id}/artifacts/{artifact_id}/import")
    def import_(
        connection_id: str, artifact_id: str, replay: bool = Form(False)
    ) -> Response:
        from src.core.runner import AlreadyImported

        try:
            service.request_import(connection_id, artifact_id, replay)
        except AlreadyImported:
            return back(connection_id, "already_imported")
        return back(connection_id, "import_requested")

    @app.get("/config", response_class=HTMLResponse)
    def config_form(request: Request) -> HTMLResponse:
        return page(request, "config.html", config=service.configuration(), error=None)

    @app.post("/config", response_model=None)
    def save_config(
        request: Request, text: str = Form(), file_revision: str = Form()
    ) -> Response:
        try:
            service.save_configuration(text, file_revision)
        except ConfigurationError as error:
            # Keep the edit on screen; a conflict also shows the file's current text.
            current = service.configuration()
            return templates.TemplateResponse(
                request,
                "config.html",
                {
                    "config": {**current, "text": text},
                    "current_text": current["text"]
                    if isinstance(error, ConfigurationConflict)
                    else None,
                    "error": str(error),
                    "notice": None,
                },
                status_code=409 if isinstance(error, ConfigurationConflict) else 400,
            )
        return RedirectResponse("/config?notice=saved", status_code=303)

    return app


def _in_flight(status: dict) -> bool:
    """Refresh the page while something is requested or running."""
    last = status["last_attempt"]
    return bool(
        status["requested_not_started"]
        or (
            last
            and (
                last["ingestion"] == "running"
                or last["transformation"] in ("pending", "running")
            )
        )
    )
