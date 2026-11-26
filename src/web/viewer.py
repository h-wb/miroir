"""The viewer's settings, read from the configuration (package split, step 6).

The viewer never reads the configuration: the composition root hands it a
`ViewerSettings`. `serve` builds it here (`src.cli.main`), and so does
`create_app` when a caller passes only the reader database. It names each
analysis's schema as the build does (`analysis_plan`), so no setting names it.
It moves to the composition root when `create_app` stops mounting the viewer
(package split, step 8).
"""

from collections.abc import Callable

from src.analyses.views import WITHHOLDS
from src.core.analysis import analysis_plan
from src.core.config import ResolvedAnalysis, ResolvedMiroir
from src.media.store import ObjectStore
from src.viewer.database import TimelineDatabase
from src.viewer.reader import PACKAGE
from src.viewer.settings import (
    TimelineAnalysis,
    ViewerConfiguration,
    ViewerSettings,
    WithholdingAnalysis,
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
