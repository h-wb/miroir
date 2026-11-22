"""The timeline (decision 040): its status page and its JSON API.

Without a read-only warehouse connection, or without a timeline analysis, every
route says the timeline is not configured; nothing else in the server depends on
it. The API only reads. Answers carry `Cache-Control: private, no-store`, except a
day's screen index, which a browser may keep and revalidate by its ETag: it holds
no screen text, and late frames can still change a past day.
"""

import gzip
import hashlib
import json
import re
from collections.abc import Callable, Coroutine
from datetime import date, datetime
from typing import Any
from unicodedata import category

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.routing import APIRoute

from src.core import timeline
from src.core.config import ResolvedMiroir
from src.core.operations import OperationError
from src.core.timeline import (
    MAX_EVENTS,
    MAX_SEARCH,
    NO_ANALYSIS,
    NOT_CONFIGURED,
    SOURCES,
    TimelineDatabase,
    TimelineReader,
    TimelineTimeout,
    TimelineUnavailable,
    read_cursor,
    timeline_source,
)

Page = Callable[..., HTMLResponse]
NO_STORE = "private, no-store"
REVALIDATE = "private, no-cache"
# Smaller answers are sent as they are.
COMPRESS_FROM = 1024
_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")
_INTEGER = re.compile(r"[0-9]{1,6}")


class _NotConfigured(TimelineUnavailable):
    pass


class _TimelineRoute(APIRoute):
    """Errors of the API as JSON that no cache keeps: 400 for a parameter, 503 when
    the timeline cannot answer, 504 when a query reached the time limit."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def guarded(request: Request) -> Response:
            try:
                return await handler(request)
            except TimelineTimeout as error:
                return _error(str(error), 504)
            except TimelineUnavailable as error:
                return _error(str(error), 503)
            except OperationError as error:
                return _error(str(error), 400)

        return guarded


def _error(message: str, status: int) -> JSONResponse:
    return JSONResponse(
        {"error": message}, status_code=status, headers={"Cache-Control": NO_STORE}
    )


def _parameters(request: Request, allowed: set[str]) -> dict[str, str]:
    """The query's parameters, each at most once and each one this route reads."""
    items = request.query_params.multi_items()
    names = [name for name, _ in items]
    unknown = sorted(set(names) - allowed)
    if unknown:
        raise OperationError(f"Unknown parameter: {unknown[0]}")
    if len(names) != len(set(names)):
        raise OperationError("A parameter is given more than once")
    return dict(items)


def _day(value: str | None, name: str) -> date:
    if value is None:
        raise OperationError(f"Missing parameter: {name}")
    try:
        if not _DAY.fullmatch(value):
            raise ValueError
        return date.fromisoformat(value)
    except ValueError:
        raise OperationError(f"{name} must be a date, YYYY-MM-DD") from None


def _optional_day(value: str | None, name: str) -> date | None:
    return None if value is None else _day(value, name)


def _time(value: str | None, name: str) -> datetime | date:
    """A local day, or a time with its offset. A `+` sent unencoded arrives as a
    space; no ISO time has a space, so it is read back as `+`."""
    if value is None:
        raise OperationError(f"Missing parameter: {name}")
    if _DAY.fullmatch(value):
        return _day(value, name)
    try:
        if len(value) > 40:
            raise ValueError
        moment = datetime.fromisoformat(value.replace(" ", "+"))
    except ValueError:
        moment = None
    if moment is None or moment.tzinfo is None or "T" not in value:
        raise OperationError(
            f"{name} must be a date (YYYY-MM-DD) or a time with an offset"
            " (2026-11-20T09:30:00+01:00)"
        )
    return moment


def _plain(value: str | None, name: str) -> str | None:
    """Text with no control characters: a NUL cannot reach the database."""
    if value is not None and any(category(char) == "Cc" for char in value):
        raise OperationError(f"{name} must not contain control characters")
    return value


def _limit(value: str | None, default: int, most: int) -> int:
    if value is None:
        return default
    if not _INTEGER.fullmatch(value) or not 1 <= int(value) <= most:
        raise OperationError(f"limit must be a whole number from 1 to {most}")
    return int(value)


def _encoded(body: Any) -> bytes:
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode()


def _answer(
    request: Request, body: Any, cache: str = NO_STORE, etag: bool = False
) -> Response:
    """JSON, compressed for a client that accepts it. With `etag`, a weak ETag of
    the content (the same for either encoding), and 304 when it matches."""
    content = _encoded(body)
    headers = {"Cache-Control": cache, "Vary": "Accept-Encoding"}
    if etag:
        tag = 'W/"' + hashlib.sha256(content).hexdigest()[:32] + '"'
        headers["ETag"] = tag
        if tag in _tags(request.headers.get("if-none-match", "")):
            return Response(status_code=304, headers=headers)
    accepts = request.headers.get("accept-encoding", "")
    if len(content) >= COMPRESS_FROM and "gzip" in accepts.lower():
        content = gzip.compress(content, compresslevel=6)
        headers["Content-Encoding"] = "gzip"
    return Response(content, media_type="application/json", headers=headers)


def _tags(header: str) -> set[str]:
    """If-None-Match's entity tags, weak or not: a weak comparison."""
    tags = {tag.strip() for tag in header.split(",") if tag.strip()}
    return tags | {f"W/{tag}" for tag in tags if not tag.startswith("W/")}


def timeline_pages(
    database: TimelineDatabase | None,
    page: Page,
    miroir: Callable[[], ResolvedMiroir],
) -> APIRouter:
    router = APIRouter()
    api = APIRouter(route_class=_TimelineRoute)

    def reader() -> TimelineReader:
        if database is None:
            raise _NotConfigured(NOT_CONFIGURED)
        source = timeline_source(miroir())
        if source is None:
            raise _NotConfigured(NO_ANALYSIS)
        return TimelineReader(database, source)

    @router.get("/api/timeline/health")
    def timeline_health() -> JSONResponse:
        state = timeline.status(database, miroir() if database else None)
        return JSONResponse(
            state,
            status_code=200 if state["ready"] else 503,
            headers={"Cache-Control": NO_STORE},
        )

    @api.get("/api/timeline/days")
    def days(request: Request) -> Response:
        query = _parameters(request, {"from", "to"})
        start, end = _day(query.get("from"), "from"), _day(query.get("to"), "to")
        return _answer(request, reader().days(start, end))

    @api.get("/api/timeline/screen/{day}")
    def screen_day(request: Request, day: str) -> Response:
        _parameters(request, set())
        index = reader().screen_day(_day(day, "day"))
        return _answer(request, index, cache=REVALIDATE, etag=True)

    @api.get("/api/timeline/frames/{key:path}")
    def frame(request: Request, key: str) -> Response:
        _parameters(request, set())
        if not _plain(key, "key") or len(key) > 512:
            raise OperationError("Invalid frame key")
        found = reader().frame(key)
        if found is None:
            return _error("Unknown frame", 404)
        return _answer(request, found)

    @api.get("/api/timeline/events")
    def events(request: Request) -> Response:
        query = _parameters(request, {"from", "to", "source", "limit", "after"})
        start, end = _time(query.get("from"), "from"), _time(query.get("to"), "to")
        sources = tuple(query.get("source", ",".join(SOURCES)).split(","))
        if (
            not sources
            or len(set(sources)) != len(sources)
            or set(sources) - set(SOURCES)
        ):
            raise OperationError(f"source must be one or more of {', '.join(SOURCES)}")
        after = None
        if "after" in query:
            at, event_id = read_cursor(query["after"], (str, str))
            after = (_cursor_time(at), event_id)
        return _answer(
            request,
            reader().events(
                start, end, sources, _limit(query.get("limit"), 200, MAX_EVENTS), after
            ),
        )

    @api.get("/api/timeline/search")
    def search(request: Request) -> Response:
        query = _parameters(
            request, {"q", "kind", "app", "from", "to", "limit", "before"}
        )
        words = _plain(query.get("q"), "q")
        if words is None:
            raise OperationError("Missing parameter: q")
        kind = query.get("kind", "screen")
        start = _optional_day(query.get("from"), "from")
        end = _optional_day(query.get("to"), "to")
        limit = _limit(query.get("limit"), 20, MAX_SEARCH)
        if kind == "screen":
            app = _plain(query.get("app"), "app")
            if app is not None and not 1 <= len(app) <= 255:
                raise OperationError("app must be a bundle identifier")
            before_moment = None
            if "before" in query:
                bucket, bundle = read_cursor(query["before"], (int, str))
                before_moment = (bucket, bundle)
            found = reader().search_screen(words, app, start, end, limit, before_moment)
        elif kind == "browser":
            if "app" in query:
                raise OperationError("app filters screen results only")
            before_visit = None
            if "before" in query:
                at, event_id = read_cursor(query["before"], (str, str))
                before_visit = (_cursor_time(at), event_id)
            found = reader().search_browser(words, start, end, limit, before_visit)
        else:
            raise OperationError("kind must be screen or browser")
        return _answer(request, found)

    router.include_router(api)
    return router


def _cursor_time(value: str) -> datetime:
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        moment = None
    if moment is None or moment.tzinfo is None:
        raise OperationError("Invalid paging cursor")
    return moment
