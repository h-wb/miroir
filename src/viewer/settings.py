"""What the viewer reads of the installation, handed to it as one value.

The composition root builds it (package split, step 6): the viewer never reads
the configuration file, never derives a schema name and never imports the
control plane. The configuration can change while the server runs, so the part
read from it is a function the viewer calls when it answers; the reader database
is opened once, from the environment, and the media object store is the one the
composition root built.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from src.analyses.views import Lane, Withhold
from src.media.config import MediaConfig
from src.media.kinds import MediaKind
from src.media.store import ObjectStore
from src.viewer.database import TimelineDatabase


@dataclass(frozen=True)
class TimelineAnalysis:
    """An analysis of a package that declares lanes (`src.analyses.views`), as
    the viewer reads it."""

    id: str
    # Its built tables' schema, named as the build names it.
    schema: str
    timezone: str
    # Linked input role -> connection id; unlinked optional roles are absent.
    inputs: Mapping[str, str] = field(default_factory=dict)
    # Whether its build grants the reader role, so its tables can be read.
    readable: bool = False
    # Its package's lanes, linked or not, in their order.
    lanes: tuple[Lane, ...] = ()


@dataclass(frozen=True)
class WithholdingAnalysis:
    """An analysis whose views hide some rows' files (`src.analyses.views`)."""

    id: str
    # Its built tables' schema, named as the build names it.
    schema: str
    # The connections its inputs link, in any role.
    connections: tuple[str, ...]
    # Whether its build grants the reader role, so its tables can be read.
    readable: bool
    withholds: tuple[Withhold, ...]


@dataclass(frozen=True)
class ViewerConfiguration:
    """The configuration as the viewer reads it."""

    # The analyses of a package that declares lanes, in configuration order.
    analyses: tuple[TimelineAnalysis, ...] = ()
    # Connection id -> the media kinds its connector declares.
    media_kinds: Mapping[str, tuple[MediaKind, ...]] = field(default_factory=dict)
    # The media section; None when media is not configured.
    media: MediaConfig | None = None
    # Every analysis that declares a withhold, of any package, in configuration
    # order: the `/media` view route asks them before it serves a file.
    withholding: tuple[WithholdingAnalysis, ...] = ()


def _no_store() -> ObjectStore | None:
    return None


@dataclass(frozen=True)
class ViewerSettings:
    """The viewer's settings: its reader database, None when not configured, and
    the configuration as it is now."""

    database: TimelineDatabase | None
    configuration: Callable[[], ViewerConfiguration]
    # The media object store the composition root built, None when media is not
    # configured; asked when a file is viewed.
    object_store: Callable[[], ObjectStore | None] = _no_store
