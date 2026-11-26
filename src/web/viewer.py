"""The viewer's settings, read from the configuration (package split, step 6).

The viewer never reads the configuration: the composition root hands it a
`ViewerSettings`. `serve` builds it here (`src.cli.main`), and so does
`create_app` when a caller passes only the reader database. It names each
timeline analysis's schema as the build does (`analysis_plan`), so no setting
names it. It moves to the composition root when `create_app` stops mounting the
viewer (package split, step 8).
"""

from collections.abc import Callable

from src.core.analysis import analysis_plan
from src.core.config import ResolvedAnalysis, ResolvedMiroir
from src.viewer.database import TimelineDatabase
from src.viewer.reader import PACKAGE
from src.viewer.settings import TimelineAnalysis, ViewerConfiguration, ViewerSettings


def viewer_configuration(miroir: ResolvedMiroir) -> ViewerConfiguration:
    """What the viewer reads of a configuration."""
    return ViewerConfiguration(
        analyses=tuple(
            _analysis(a) for a in miroir.analyses if a.package.id == PACKAGE
        ),
        media_kinds={
            c.config.id: tuple(kind.name for kind in c.integration.media)
            for c in miroir.connections
        },
    )


def viewer_settings(
    database: TimelineDatabase | None, miroir: Callable[[], ResolvedMiroir]
) -> ViewerSettings:
    """The viewer's settings over the configuration as it is when it answers."""
    return ViewerSettings(database, lambda: viewer_configuration(miroir()))


def _analysis(analysis: ResolvedAnalysis) -> TimelineAnalysis:
    plan = analysis_plan(analysis)
    return TimelineAnalysis(
        id=analysis.id,
        schema=plan.relations[0].split(".")[0],
        timezone=analysis.config.timezone,
        inputs={role: c.config.id for role, c in analysis.inputs.items()},
        readable=bool(plan.variables.get("timeline_reader_role")),
    )
