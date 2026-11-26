"""What the viewer reads of the installation, handed to it as one value.

The composition root builds it (package split, step 6): the viewer never reads
the configuration file, never derives a schema name and never imports the
control plane. The configuration can change while the server runs, so the part
read from it is a function the viewer calls when it answers; the reader database
is opened once, from the environment.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from src.viewer.database import TimelineDatabase


@dataclass(frozen=True)
class TimelineAnalysis:
    """An analysis of the timeline package, as the viewer reads it."""

    id: str
    # Its built tables' schema, named as the build names it.
    schema: str
    timezone: str
    # Linked input role -> connection id; unlinked optional roles are absent.
    inputs: Mapping[str, str] = field(default_factory=dict)
    # Whether its build grants the reader role, so its tables can be read.
    readable: bool = False


@dataclass(frozen=True)
class ViewerConfiguration:
    """The configuration as the viewer reads it."""

    # The timeline package's analyses, in configuration order.
    analyses: tuple[TimelineAnalysis, ...] = ()
    # Connection id -> the media kinds its connector declares.
    media_kinds: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class ViewerSettings:
    """The viewer's settings: its reader database, None when not configured, and
    the configuration as it is now."""

    database: TimelineDatabase | None
    configuration: Callable[[], ViewerConfiguration]
