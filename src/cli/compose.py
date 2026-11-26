"""The web server's composition root (package split, step 8): the shell, the
control plane's admin part and the viewer, and the viewer's settings.

`serve` builds its application here (`web_app`), and so do the tests that need
both parts. The admin part and the viewer import neither each other nor this
module; each brings its own folders, links and paths, and the shell checks the
login for both. The viewer never reads the configuration: it is handed a
`ViewerSettings`, built here. It names each analysis's schema as the build does
(`analysis_plan`), so no setting names it.
"""

from collections.abc import Callable

from fastapi import FastAPI

from src.analyses.views import WITHHOLDS
from src.core.analysis import analysis_plan
from src.core.config import ConfigurationError, ResolvedAnalysis, ResolvedMiroir
from src.core.service import MiroirService
from src.kernel.errors import OperationError
from src.media.store import ObjectStore
from src.viewer import STATIC_DIR as VIEWER_STATIC
from src.viewer import TEMPLATES_DIR as VIEWER_TEMPLATES
from src.viewer import part as viewer_part
from src.viewer.database import TimelineDatabase
from src.viewer.reader import PACKAGE
from src.viewer.settings import (
    TimelineAnalysis,
    ViewerConfiguration,
    ViewerSettings,
    WithholdingAnalysis,
)
from src.web.app import DEFAULT_MAX_UPLOAD, mount_admin
from src.web.shell import Link, Part, Shell


def web_app(
    service: MiroirService,
    token: str,
    *,
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD,
    metrics_token: str | None = None,
    viewer: ViewerSettings | None = None,
    timeline: TimelineDatabase | None = None,
) -> FastAPI:
    """What `serve` serves: the shell with the admin part, then the viewer.

    `viewer` is what the viewer reads of the installation. Without it, it is
    built from `timeline`, the read-only warehouse connection, the service's
    configuration and its media object store; without either the timeline
    routes say it is not configured.
    """
    shell = Shell(token)
    mount_admin(shell, service, max_upload_bytes, metrics_token)
    if viewer is None:
        viewer = viewer_settings(timeline, service.miroir, lambda: service.object_store)
    mount_viewer(shell, viewer)
    return shell.finish()


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
            paths=viewer_part.PATHS,
            policy=viewer_part.POLICY,
            links=links,
        ),
        viewer_part.routers(settings, shell.page),
    )


def viewer_configuration(miroir: ResolvedMiroir) -> ViewerConfiguration:
    """What the viewer reads of a configuration."""
    return ViewerConfiguration(
        analyses=tuple(
            _analysis(a) for a in miroir.analyses if a.package.id == PACKAGE
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
    """Its built tables' schema, and whether its build grants the viewer's
    reader role those tables."""
    plan = analysis_plan(analysis)
    schema = plan.relations[0].split(".")[0]
    return schema, bool(plan.variables.get("timeline_reader_role"))


def _analysis(analysis: ResolvedAnalysis) -> TimelineAnalysis:
    schema, readable = _built(analysis)
    return TimelineAnalysis(
        id=analysis.id,
        schema=schema,
        timezone=analysis.config.timezone,
        inputs={role: c.config.id for role, c in analysis.inputs.items()},
        readable=readable,
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
