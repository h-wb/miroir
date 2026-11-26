"""The timeline's pages (decision 040, phase 1c): the Day view and search. The
conversation pages are in `conversations.py`.

The lanes are the analysis's (`src.analyses.views`, package split step 11):
the Day view draws one per lane shown, with its day totals and the events
around a moment, and search has one kind per lane. A lane's plugin, named by
it, adds what is its own: the scrubber's frames, the thread's conversations.

Rendered on the server from the same reader the JSON API uses; the Day view's
scrubber and lanes are filled in the browser by `/static/viewer/timeline.js`
from that API. Every page is `Cache-Control: private, no-store`. Data reaches templates
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

from src.analyses.views import SCRUBBER, THREAD, Lane
from src.kernel.errors import OperationError
from src.viewer import reader as timeline
from src.viewer.conversations import thread_url
from src.viewer.reader import (
    MAX_QUERY,
    MEDIA_NO_THUMBNAILS,
    MEDIA_THUMBNAILS,
    MEDIA_UNKNOWN,
    MEDIA_WITHHELD,
    NO_ANALYSIS,
    NOT_CONFIGURED,
    TimelineReader,
    TimelineSource,
    TimelineTimeout,
    TimelineUnavailable,
    timeline_source,
    valid_conversation_id,
)
from src.viewer.settings import ViewerSettings
from src.viewer.timeline import (
    NO_STORE,
    USE_POST,
    _day,
    _plain,
    search_fields,
    search_kinds,
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


def media_url(connection: str | None, kind: str, key: str, size: int | str) -> str:
    """A file's address in the `/media` view route, from its row's connection
    and kind."""
    return (
        f"/media/{quote(connection or '', safe='')}/{quote(kind, safe='')}/"
        f"{quote(key, safe='/')}?size={size}"
    )


def day_url(day: str, frame: str | None = None) -> str:
    return f"/timeline/{day}" + (f"?{urlencode({'frame': frame})}" if frame else "")


def _clock(iso: str) -> str:
    return datetime.fromisoformat(iso).strftime("%H:%M:%S")


def _level(count: int) -> int:
    """A calendar cell's shade: none, then about tens, hundreds, thousands."""
    return min(4, len(str(count))) if count > 0 else 0


def _count(counts: dict[str, int] | None) -> int:
    """A source's count on a day: its frames where it has them (the scrubber's),
    else its events."""
    counts = counts or {}
    return int(counts.get("frames", counts.get("events", 0)))


def _activity(row: dict[str, Any]) -> int:
    """Every lane's count on a day, summed."""
    return sum(_count(counts) for name, counts in row.items() if name != "day")


def _capital(text: str) -> str:
    return text[:1].upper() + text[1:]


def _lanes(source: TimelineSource, totals: dict[str, Any]) -> list[dict[str, Any]]:
    """The Day view's lanes, as data the page renders and its script reads."""
    lanes = []
    for lane in source.shown:
        count = _count(totals.get(lane.source))
        lanes.append(
            {
                "source": lane.source,
                "label": lane.label,
                "plugin": lane.plugin or "",
                "colour": lane.colour,
                "count": count,
                "unit": lane.units[0] if count == 1 else lane.units[1],
                "near": f"{_capital(lane.units[1])} around this time",
            }
        )
    return lanes


def _kinds(source: TimelineSource) -> list[dict[str, str]]:
    """The search form's kinds, one per lane shown."""
    return [
        {"value": lane.source, "label": _kind_label(lane), "search_in": lane.search_in}
        for lane in (source.lane(kind) for kind in search_kinds(source))
        if lane is not None
    ]


def _kind_label(lane: Lane) -> str:
    # The scrubber searches its frames' text; another lane its events.
    if lane.plugin == SCRUBBER:
        return f"{lane.label} text"
    return _capital(lane.units[1])


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
            # A lane counts where it is shown: the sources are the analysis's.
            counts = reader.days(chosen, chosen, reader.source.sources)["days"]
            totals = counts[0] if counts else {}
            lanes = _lanes(reader.source, totals)
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
                "lanes": lanes,
                "recorded": any(lane["count"] for lane in lanes),
                # Each lane but the scrubber lists its events around the frame.
                "near": [lane for lane in lanes if lane["plugin"] != SCRUBBER],
                "kinds": _kinds(reader.source),
                "frame": _frame_view(frame) if frame else None,
                "missing_frame": bool(key) and frame is None,
                "labels": MEDIA_LABELS,
            }
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
        "image": media_url(media["connection"], media["kind"], frame["key"], 1280)
        if code == MEDIA_THUMBNAILS
        else None,
        # A stored file, with or without thumbnails, may be opened as it is.
        "original": media_url(
            media["connection"], media["kind"], frame["key"], "original"
        )
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
    kinds = _kinds(reader.source)
    values = [option["value"] for option in kinds]
    kind = fields.get("kind", values[0] if values else "")
    if kind not in values:
        kind = values[0] if values else ""
    lane = reader.source.lane(kind)
    plugin = lane.plugin if lane else None
    # The conversation filter is for the thread's kind; another kind leaves it.
    conversation_id = fields.get("conversation", "")
    if plugin != THREAD:
        fields = {k: v for k, v in fields.items() if k != "conversation"}
        conversation_id = ""
    context: dict[str, Any] = {
        "source": reader.source.describe(),
        # Shown back in the form, never in an address, and only at a length the
        # search could have taken: an oversized value is refused, not echoed.
        "q": words if len(words) <= MAX_QUERY else "",
        "kind": kind,
        "kinds": kinds,
        "plugin": plugin or "",
        "searched": lane.search_in if lane else "",
        "app": _echo(fields.get("app", "")),
        "from_day": _echo(fields.get("from", "")),
        "to_day": _echo(fields.get("to", "")),
        "labels": MEDIA_LABELS,
        "results": None,
        "refused": refused,
        "more": None,
    }
    context["conversation"] = None
    if reader.source.thread is not None and valid_conversation_id(conversation_id):
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
            found = search_fields(lambda: reader, {**fields, "limit": str(SEARCH_PAGE)})
        except (TimelineUnavailable, TimelineTimeout):
            raise  # the page's own error, from `guarded`
        except OperationError as error:
            context["refused"] = str(error)
            status = 400
        else:
            if plugin == SCRUBBER:
                context["results"] = [
                    _moment(m, found["apps"], found["media_kind"])
                    for m in found["moments"]
                ]
            elif plugin == THREAD:
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


def _moment(
    moment: dict[str, Any], apps: list[dict[str, str]], kind: str
) -> dict[str, Any]:
    app = apps[moment["app"]] if moment["app"] is not None else None
    code = moment["media"]
    return {
        **moment,
        "clock": _clock(moment["at"]),
        "until_clock": _clock(moment["until"]),
        "app": app,
        "thumbnail": media_url(moment["connection"], kind, moment["key"], 360)
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
