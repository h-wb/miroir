"""The viewer's functions over a configuration, as the composition root calls
them: the viewer itself reads only its settings (package split, step 6)."""

from src.cli.compose import viewer_configuration
from src.core.config import ResolvedMiroir
from src.viewer import reader
from src.viewer.database import TimelineDatabase


def timeline_source(miroir: ResolvedMiroir) -> reader.TimelineSource | None:
    return reader.timeline_source(viewer_configuration(miroir))


def status(
    database: TimelineDatabase | None, miroir: ResolvedMiroir | None = None
) -> dict:
    configuration = None if miroir is None else viewer_configuration(miroir)
    return reader.status(database, configuration)
