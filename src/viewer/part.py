"""What the viewer brings to the web shell, as plain values (package split,
step 8): its routers, the paths they answer, how it widens the content security
policy there, and its header links. The viewer imports nothing of the shell;
the composition root (`src.cli.compose`) mounts these into it, with the
viewer's template and static folders (`TEMPLATES_DIR`, `STATIC_DIR`).
"""

from collections.abc import Callable

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from src.viewer.conversations import conversation_pages
from src.viewer.media import media_routes
from src.viewer.reader import timeline_source
from src.viewer.settings import ViewerSettings
from src.viewer.timeline import timeline_pages
from src.viewer.timeline_pages import timeline_page_routes

Page = Callable[..., HTMLResponse]
# Each is itself and what is below it: `/timeline*`, `/api/timeline/*` and the
# `/media/{connection}/{kind}/{key}` view route, as the gateway will route them.
PATHS = ("/timeline", "/api/timeline", "/media")
# Images may also be blobs the timeline makes from fetched frames (decision
# 040): on the viewer's paths only.
POLICY = {"img-src": ("blob:",)}


def routers(settings: ViewerSettings, page: Page) -> list[APIRouter]:
    """The viewer's routers, in the order they are included."""
    return [
        timeline_pages(settings, page),
        # Before the Day view's `/timeline/{day}`, which would take the path.
        conversation_pages(settings, page),
        timeline_page_routes(settings, page),
        # The `/media` view route, after the viewer's withheld check (step 7).
        media_routes(settings),
    ]


def links(settings: ViewerSettings) -> list[tuple[str, str]]:
    """The header's links, as (address, label): the timeline only where there
    is one to see, and its conversations where it reads a `messages` input.
    Whatever reading the configuration raises reaches the caller."""
    if settings.database is None:
        return []
    source = timeline_source(settings.configuration())
    if source is None:
        return []
    found = [("/timeline", "Timeline")]
    if source.messages:
        found.append(("/timeline/conversations", "Conversations"))
    return found
