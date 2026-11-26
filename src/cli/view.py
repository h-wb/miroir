"""The viewer's composition root (package split, steps 6, 8 and 10): its
settings, its mount into a shell, and what `view` serves, the viewer alone.

The viewer never reads the configuration: it is handed a `ViewerSettings`,
built here. It names each analysis's schema as the build does (`analysis_plan`),
so no setting names it. This module and what it imports load neither the
control plane's service, runner or operations nor dlt, prefect or an image
library, so the `view` process holds none of them; reading the configuration
loads the connector registry, never a connector's source.
"""

from collections.abc import Callable

from fastapi import APIRouter, FastAPI, Request

from src.analyses.views import LANES, WITHHOLDS
from src.control.analysis import analysis_plan
from src.control.config import ConfigurationError, ResolvedAnalysis, ResolvedMiroir
from src.kernel.errors import OperationError
from src.media.store import ObjectStore
from src.viewer import STATIC_DIR as VIEWER_STATIC
from src.viewer import TEMPLATES_DIR as VIEWER_TEMPLATES
from src.viewer import part as viewer_part
from src.viewer.database import TimelineDatabase
from src.viewer.settings import (
    TimelineAnalysis,
    ViewerConfiguration,
    ViewerSettings,
    WithholdingAnalysis,
)
from src.web.navigation import LINKS as ADMIN_LINKS
from src.web.shell import Link, Part, Shell


def viewer_app(token: str, settings: ViewerSettings) -> FastAPI:
    """What `view` serves: the shell with the viewer alone, and `/healthz`.

    Its header carries the admin pages' links first, as `serve`'s does; the
    gateway sends those pages to `serve`. A login here lands on the timeline.
    """
    shell = Shell(token, home="/timeline")
    health = APIRouter()

    @health.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    shell.mount(Part(links=lambda: ADMIN_LINKS, public=_health), [health])
    mount_viewer(shell, settings)
    return shell.finish()


def _health(request: Request) -> bool:
    return request.url.path == "/healthz"


def mount_viewer(shell: Shell, settings: ViewerSettings) -> None:
    """Mount the viewer's routes, folders, links and policy into the shell."""

    def links() -> list[Link]:
        try:
            return [Link(href, label) for href, label in viewer_part.links(settings)]
        except (ConfigurationError, OperationError):
            # A configuration that does not load links nothing; its pages say why.
            return []

    shell.mount(
        Part(
            templates=VIEWER_TEMPLATES,
            static=VIEWER_STATIC,
            static_url=viewer_part.STATIC_URL,
            paths=viewer_part.PATHS,
            policy=viewer_part.POLICY,
            links=links,
        ),
        viewer_part.routers(settings, shell.page),
    )


def viewer_configuration(miroir: ResolvedMiroir) -> ViewerConfiguration:
    """What the viewer reads of a configuration."""
    return ViewerConfiguration(
        # The analyses of a package that declares lanes (the timeline's).
        analyses=tuple(
            _analysis(a) for a in miroir.analyses if LANES.get(a.package.id)
        ),
        media_kinds={c.config.id: c.integration.media for c in miroir.connections},
        media=miroir.media,
        withholding=tuple(
            _withholding(a) for a in miroir.analyses if WITHHOLDS.get(a.package.id)
        ),
    )


def viewer_settings(
    database: TimelineDatabase | None,
    miroir: Callable[[], ResolvedMiroir],
    object_store: Callable[[], ObjectStore | None] = lambda: None,
) -> ViewerSettings:
    """The viewer's settings over the configuration as it is when it answers."""
    return ViewerSettings(
        database, lambda: viewer_configuration(miroir()), object_store
    )


def _built(analysis: ResolvedAnalysis) -> tuple[str, bool]:
    """Its built tables' schema, as its build names it, and whether its build
    grants the viewer's reader role those tables: whether the analysis sets its
    own `reader_role` parameter, for any package that declares views and takes
    one. A package without that parameter grants the reader nothing."""
    plan = analysis_plan(analysis)
    schema = plan.relations[0].split(".")[0]
    parameters = analysis.package.parameters.model_validate(analysis.config.parameters)
    return schema, bool(getattr(parameters, "reader_role", None))


def _analysis(analysis: ResolvedAnalysis) -> TimelineAnalysis:
    schema, readable = _built(analysis)
    return TimelineAnalysis(
        id=analysis.id,
        schema=schema,
        timezone=analysis.config.timezone,
        inputs={role: c.config.id for role, c in analysis.inputs.items()},
        readable=readable,
        lanes=LANES[analysis.package.id],
    )


def _withholding(analysis: ResolvedAnalysis) -> WithholdingAnalysis:
    schema, readable = _built(analysis)
    return WithholdingAnalysis(
        id=analysis.id,
        schema=schema,
        connections=tuple(c.config.id for c in analysis.inputs.values()),
        readable=readable,
        withholds=WITHHOLDS[analysis.package.id],
    )
