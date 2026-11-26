"""The control plane's composition roots (package split, steps 8 and 10): what
`serve` serves, with the viewer or without it.

`serve` builds its application here (`web_app`), and so do the tests that need
both parts. The admin part and the viewer import neither each other nor this
module; each brings its own folders, links and paths, and the shell checks the
login for both. The viewer's settings and its mount are in `src.cli.view`, which
`view` uses alone. `serve --without-viewer` (`admin_app`) mounts the admin part
only and opens no reader database; its header still links the viewer's pages,
named from the configuration alone (`src.viewer.navigation`), since the gateway
sends them to `view`.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from fastapi import FastAPI

from src.analyses.views import LANES
from src.control.config import ConfigurationError, ResolvedMiroir
from src.control.service import MiroirService
from src.kernel.errors import OperationError
from src.viewer import navigation
from src.web.app import DEFAULT_MAX_UPLOAD, mount_admin
from src.web.shell import Link, Part, Shell

if TYPE_CHECKING:
    from src.viewer.database import TimelineDatabase
    from src.viewer.settings import ViewerSettings


def web_app(
    service: MiroirService,
    token: str,
    *,
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD,
    metrics_token: str | None = None,
    viewer: "ViewerSettings | None" = None,
    timeline: "TimelineDatabase | None" = None,
) -> FastAPI:
    """What `serve` serves: the shell with the admin part, then the viewer.

    `viewer` is what the viewer reads of the installation. Without it, it is
    built from `timeline`, the read-only warehouse connection, the service's
    configuration and its media object store; without either the timeline
    routes say it is not configured.
    """
    # Imported here: `serve --without-viewer` loads nothing of the viewer but
    # its navigation.
    from src.cli.view import mount_viewer, viewer_settings

    shell = Shell(token)
    mount_admin(shell, service, max_upload_bytes, metrics_token)
    if viewer is None:
        viewer = viewer_settings(timeline, service.miroir, lambda: service.object_store)
    mount_viewer(shell, viewer)
    return shell.finish()


def admin_app(
    service: MiroirService,
    token: str,
    *,
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD,
    metrics_token: str | None = None,
) -> FastAPI:
    """What `serve --without-viewer` serves: the shell with the admin part
    alone, its header linking the viewer's pages that `view` serves."""
    shell = Shell(token)
    mount_admin(shell, service, max_upload_bytes, metrics_token)
    shell.mount(Part(links=viewer_links(service.miroir)))
    return shell.finish()


def viewer_links(miroir: Callable[[], ResolvedMiroir]) -> Callable[[], list[Link]]:
    """The viewer's header links from the configuration alone, as the viewer
    declares them: those `view`'s pages carry, without its reader."""

    def links() -> list[Link]:
        try:
            analyses = miroir().analyses
        except (ConfigurationError, OperationError):
            # A configuration that does not load links nothing; its pages say why.
            return []
        first = next((a for a in analyses if LANES.get(a.package.id)), None)
        if first is None:
            return []
        found = navigation.links(first.inputs, LANES[first.package.id])
        return [Link(href, label) for href, label in found]

    return links
