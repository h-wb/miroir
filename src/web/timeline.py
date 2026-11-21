"""The timeline (decision 040). Phase 0: whether it can read the warehouse.

Without a read-only warehouse connection the routes say so; nothing else in the
server depends on it.
"""

from collections.abc import Callable

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.core import timeline
from src.core.timeline import TimelineDatabase

Page = Callable[..., HTMLResponse]


def timeline_pages(database: TimelineDatabase | None, page: Page) -> APIRouter:
    router = APIRouter()

    @router.get("/timeline", response_class=HTMLResponse)
    def timeline_home(request: Request) -> HTMLResponse:
        state = timeline.status(database)
        response = page(request, "timeline.html", timeline=state)
        response.status_code = 200 if state["ready"] else 503
        return response

    @router.get("/api/timeline/health")
    def timeline_health() -> JSONResponse:
        state = timeline.status(database)
        return JSONResponse(state, status_code=200 if state["ready"] else 503)

    return router
