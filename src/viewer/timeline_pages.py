"""The timeline's pages (decision 040, phase 1c): the Day view and search. The
conversation pages are in `conversations.py`; the Day view's Messages lane and
the search's Messages kind are here, where those pages live (phase 2c).

Rendered on the server from the same reader the JSON API uses; the Day view's
scrubber and lanes are filled in the browser by `/static/timeline.js` from that
API. Every page is `Cache-Control: private, no-store`. Data reaches templates
as values, so Jinja escapes it; nothing from the warehouse is markup.
"""

import calendar
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException
from starlette.formparsers import MultiPartException

from src.kernel.errors import OperationError
from src.viewer import reader as timeline
from src.viewer.conversations import thread_url
from src.viewer.reader import (
    FRAMES_KIND,
    MAX_QUERY,
    MEDIA_NO_THUMBNAILS,
    MEDIA_THUMBNAILS,
    MEDIA_UNKNOWN,
    MEDIA_WITHHELD,
    MESSAGES,
    NO_ANALYSIS,
    NOT_CONFIGURED,
    TimelineReader,
    TimelineTimeout,
    TimelineUnavailable,
    timeline_source,
    valid_conversation_id,
)
from src.viewer.settings import ViewerSettings
from src.viewer.timeline import (
    NO_STORE,
    PAGE_KINDS,
    SEARCH_KINDS,
    USE_POST,
    _day,
    _plain,
    search_fields,
)

Page = Callable[..., HTMLResponse]
# What a tile says where there is no image to show (the index's media codes).
MEDIA_LABELS = {
    # No record of the file in the timeline yet: it may be stored all the same.
    MEDIA_UNKNOWN: "Image state not known yet",
    MEDIA_THUMBNAILS: "Screenshot",
    MEDIA_NO_THUMBNAILS: "No thumbnail could be made",
    MEDIA_WITHHELD: "Withheld",
}
# How far back `/timeline` looks for the latest day with data.
LATEST_WITHIN_DAYS = 399
SEARCH_PAGE = 20
# What the search form sends; `limit` is the page's own.
SEARCH_FORM_FIELDS = {"q", "kind", "app", "from", "to", "before", "conversation"}
FORM_TOO_LARGE = "The search form was too large; search again."
# A value longer than this is not shown back in the form.
ECHO_LIMIT = 255


def media_url(connection: str | None, key: str, size: int | str) -> str:
    return (
        f"/media/{quote(connection or '', safe='')}/{FRAMES_KIND}/"
        f"{quote(key, safe='/')}?size={size}"
    )


def day_url(day: str, frame: str | None = None) -> str:
    return f"/timeline/{day}" + (f"?{urlencode({'frame': frame})}" if frame else "")


def _clock(iso: str) -> str:
    return datetime.fromisoformat(iso).strftime("%H:%M:%S")


def _level(count: int) -> int:
    """A calendar cell's shade: none, then about tens, hundreds, thousands."""
    return min(4, len(str(count))) if count > 0 else 0


def _activity(row: dict[str, Any]) -> int:
    """Frames of the screen, visits of the browser, messages where linked."""
    screen, browser = row.get("screen") or {}, row.get("browser") or {}
    messages = row.get(MESSAGES) or {}
    return (
        int(screen.get("frames", 0))
        + int(browser.get("events", 0))
        + int(messages.get("events", 0))
    )


def _month(reader: TimelineReader, day: date) -> list[list[dict[str, Any]]]:
    """The weeks of a day's month, Monday first, with each day's counts."""
    first = day.replace(day=1)
    last = day.replace(day=calendar.monthrange(day.year, day.month)[1])
    found = reader.days(first, last, reader.source.sources)["days"]
    counts = {row["day"]: _activity(row) for row in found}
    weeks = []
    for week in calendar.Calendar().monthdatescalendar(day.year, day.month):
        weeks.append(
            [
                {
                    "day": cell.isoformat(),
                    "number": cell.day,
                    "in_month": cell.month == day.month,
                    "count": counts.get(cell.isoformat(), 0),
                    "level": _level(counts.get(cell.isoformat(), 0)),
                    "selected": cell == day,
                }
                for cell in week
            ]
        )
    return weeks


def timeline_page_routes(settings: ViewerSettings, page: Page) -> APIRouter:
    database = settings.database
    router = APIRouter()

    def unavailable(request: Request, message: str, status: int) -> Response:
        state = {"configured": True, "ready": False, "message": message}
        if message in (NOT_CONFIGURED, NO_ANALYSIS):
            state["configured"] = False
        return _no_store(page(request, "timeline.html", timeline=state), status)

    def reader_or_none() -> TimelineReader | None:
        if database is None:
            return None
        source = timeline_source(settings.configuration())
        return TimelineReader(database, source) if source else None

    def guarded(
        request: Request, render: Callable[[TimelineReader], Response]
    ) -> Response:
        if database is None:
            return unavailable(request, NOT_CONFIGURED, 503)
        found = reader_or_none()
        if found is None:
            return unavailable(request, NO_ANALYSIS, 503)
        try:
            return render(found)
        except TimelineTimeout as error:
            return unavailable(request, str(error), 504)
        except TimelineUnavailable as error:
            return unavailable(request, str(error), 503)

    @router.get("/timeline", response_class=HTMLResponse)
    def timeline_home(request: Request) -> Response:
        """The latest day with data, or the status when there is no timeline."""
        state = timeline.status(
            database, settings.configuration() if database else None
        )
        if not state["ready"]:
            return _no_store(page(request, "timeline.html", timeline=state), 503)

        def latest(reader: TimelineReader) -> Response:
            today = datetime.now(ZoneInfo(reader.source.timezone)).date()
            start = today - timedelta(days=LATEST_WITHIN_DAYS)
            days = reader.days(start, today)["days"]
            chosen = days[-1]["day"] if days else today.isoformat()
            return RedirectResponse(
                day_url(chosen), status_code=303, headers={"Cache-Control": NO_STORE}
            )

        return guarded(request, latest)

    @router.get("/timeline/search", response_class=HTMLResponse)
    def search_form(request: Request) -> Response:
        """The empty form. Words never come in an address (decision 040)."""
        if "q" in request.query_params:
            return guarded(
                request,
                lambda reader: _search(request, {}, reader, page, refused=USE_POST),
            )
        return guarded(request, lambda reader: _search(request, {}, reader, page))

    @router.post("/timeline/search", response_class=HTMLResponse)
    async def search_page(request: Request) -> Response:
        """Results of the form, or the next page of them for htmx."""
        if request.query_params:
            refusal = USE_POST if "q" in request.query_params else None
            return await run_in_threadpool(
                guarded,
                request,
                lambda reader: _search(
                    request,
                    {},
                    reader,
                    page,
                    refused=refusal or "The search takes no parameters in its address",
                ),
            )
        try:
            # Six fields at most are expected; none needs more than a few KB.
            form = await request.form(max_files=0, max_fields=10, max_part_size=8192)
        except (HTTPException, MultiPartException):
            return await run_in_threadpool(
                guarded,
                request,
                lambda reader: _search(
                    request, {}, reader, page, refused=FORM_TOO_LARGE
                ),
            )
        fields = {
            name: value
            for name, value in form.items()
            if name in SEARCH_FORM_FIELDS and isinstance(value, str)
        }
        return await run_in_threadpool(
            guarded, request, lambda reader: _search(request, fields, reader, page)
        )

    @router.get("/timeline/{day}", response_class=HTMLResponse)
    def day_page(request: Request, day: str) -> Response:
        chosen = _day(day, "day")
        key = _plain(request.query_params.get("frame"), "frame")

        def render(reader: TimelineReader) -> Response:
            frame = reader.frame(key) if key else None
            if frame is not None and frame["day"] != chosen.isoformat():
                # A frame opens on its own day.
                return RedirectResponse(
                    day_url(frame["day"], key),
                    status_code=303,
                    headers={"Cache-Control": NO_STORE},
                )
            # Messages count only where linked: the sources are the analysis's.
            counts = reader.days(chosen, chosen, reader.source.sources)["days"]
            totals = counts[0] if counts else {}
            linked = bool(reader.source.messages)
            context = {
                "source": reader.source.describe(),
                "day": chosen.isoformat(),
                "title": chosen.strftime("%A %d %B %Y"),
                "previous": (chosen - timedelta(days=1)).isoformat(),
                "next": (chosen + timedelta(days=1)).isoformat(),
                "weeks": _month(reader, chosen),
                "month": chosen.strftime("%B %Y"),
                "previous_month": (chosen.replace(day=1) - timedelta(days=1))
                .replace(day=1)
                .isoformat(),
                "next_month": (chosen.replace(day=28) + timedelta(days=4))
                .replace(day=1)
                .isoformat(),
                "frames": (totals.get("screen") or {}).get("frames", 0),
                "visits": (totals.get("browser") or {}).get("events", 0),
                "frame": _frame_view(frame) if frame else None,
                "missing_frame": bool(key) and frame is None,
                "labels": MEDIA_LABELS,
            }
            if linked:
                context["messages_linked"] = True
                context["messages"] = (totals.get(MESSAGES) or {}).get("events", 0)
            return _no_store(page(request, "timeline_day.html", **context), 200)

        return guarded(request, render)

    return router


def _frame_view(frame: dict[str, Any]) -> dict[str, Any]:
    media = frame["media"]
    code = {
        "unknown": MEDIA_UNKNOWN,
        "thumbnails": MEDIA_THUMBNAILS,
        "stored": MEDIA_NO_THUMBNAILS,
        "withheld": MEDIA_WITHHELD,
    }[media["state"]]
    return {
        **frame,
        "clock": _clock(frame["captured_at"]),
        "code": code,
        "image": media_url(media["connection"], frame["key"], 1280)
        if code == MEDIA_THUMBNAILS
        else None,
        # A stored file, with or without thumbnails, may be opened as it is.
        "original": media_url(media["connection"], frame["key"], "original")
        if code in (MEDIA_THUMBNAILS, MEDIA_NO_THUMBNAILS)
        else None,
    }


def _search(
    request: Request,
    fields: dict[str, str],
    reader: TimelineReader,
    page: Page,
    refused: str | None = None,
) -> Response:
    words = fields.get("q", "")
    kind = fields.get("kind", "screen")
    linked = bool(reader.source.messages)
    kinds = SEARCH_KINDS if linked else PAGE_KINDS
    if kind not in kinds:
        kind = "screen"
    # The conversation filter is for messages; another kind leaves it.
    conversation_id = fields.get("conversation", "")
    if kind != MESSAGES or not linked:
        fields = {k: v for k, v in fields.items() if k != "conversation"}
        conversation_id = ""
    context: dict[str, Any] = {
        "source": reader.source.describe(),
        # Shown back in the form, never in an address, and only at a length the
        # search could have taken: an oversized value is refused, not echoed.
        "q": words if len(words) <= MAX_QUERY else "",
        "kind": kind,
        "app": _echo(fields.get("app", "")),
        "from_day": _echo(fields.get("from", "")),
        "to_day": _echo(fields.get("to", "")),
        "labels": MEDIA_LABELS,
        "results": None,
        "refused": refused,
        "more": None,
    }
    if linked:
        context["messages_linked"] = True
        context["conversation"] = None
        if valid_conversation_id(conversation_id):
            found_conversation = reader.conversation(conversation_id)
            context["conversation"] = {
                "id": conversation_id,
                "name": found_conversation["display_name"]
                if found_conversation
                else "this conversation",
            }
    fragment = request.headers.get("hx-request") == "true"
    status = 400 if refused else 200
    if words.strip() and not refused:
        try:
            found = search_fields(
                lambda: reader, {**fields, "limit": str(SEARCH_PAGE)}, kinds
            )
        except (TimelineUnavailable, TimelineTimeout):
            raise  # the page's own error, from `guarded`
        except OperationError as error:
            context["refused"] = str(error)
            status = 400
        else:
            if kind == "screen":
                context["results"] = [
                    _moment(m, found["apps"]) for m in found["moments"]
                ]
            elif kind == MESSAGES:
                context["results"] = [_message(m) for m in found["messages"]]
            else:
                context["results"] = [_visit(v) for v in found["visits"]]
            if found["next"]:
                # The next page is asked for with these fields in a POST body.
                context["more"] = {
                    **{k: v for k, v in fields.items() if k != "before" and v},
                    "before": found["next"],
                }
    name = "_timeline_results.html" if fragment else "timeline_search.html"
    return _no_store(page(request, name, **context), status)


def _echo(value: str) -> str:
    return value if len(value) <= ECHO_LIMIT else ""


def _moment(moment: dict[str, Any], apps: list[dict[str, str]]) -> dict[str, Any]:
    app = apps[moment["app"]] if moment["app"] is not None else None
    code = moment["media"]
    return {
        **moment,
        "clock": _clock(moment["at"]),
        "until_clock": _clock(moment["until"]),
        "app": app,
        "thumbnail": media_url(moment["connection"], moment["key"], 360)
        if code == MEDIA_THUMBNAILS
        else None,
        "code": code,
        "open": day_url(moment["day"], moment["key"]),
    }


def _message(message: dict[str, Any]) -> dict[str, Any]:
    return {
        **message,
        "clock": _clock(message["sent_at"]),
        "day": message["day"] or message["sent_at"][:10],
        # In a direct conversation the sender is its name: not said twice.
        "who": "You"
        if message["from_me"]
        else ""
        if message["sender"] == message["display_name"]
        else (message["sender"] or ""),
        "open": thread_url(message["conversation_id"], message["guid"]),
    }


def _visit(visit: dict[str, Any]) -> dict[str, Any]:
    return {
        **visit,
        "clock": _clock(visit["at"]),
        "day": visit["at"][:10],
        "open": day_url(visit["at"][:10]),
        # Only web addresses are links; anything else stays text.
        "link": visit["url"]
        if isinstance(visit["url"], str)
        and visit["url"].startswith(("https://", "http://"))
        else None,
    }


def _no_store(response: Response, status: int) -> Response:
    response.status_code = status
    response.headers["Cache-Control"] = NO_STORE
    return response
