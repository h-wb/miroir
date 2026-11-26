"""The viewer's header links, as a declaration that needs no reader (package
split, step 10).

`view` serves the viewer's pages and `serve --without-viewer` the admin pages,
in two processes; both headers carry the same links. The viewer's part asks this
over its settings; the admin process asks it over the configuration it reads
(`src.cli.compose.viewer_links`) and loads nothing else of the viewer. It reads
only the lane declarations (`src.analyses.views`), data.
"""

from collections.abc import Collection

from src.analyses.views import THREAD, Lane


def links(
    roles: Collection[str] | None, lanes: tuple[Lane, ...] = ()
) -> list[tuple[str, str]]:
    """The header's links, as (address, label), given the linked input roles of
    the first analysis of a package that declares lanes and those lanes, or
    None when there is none: the timeline, and its conversations where the
    thread's lane is linked."""
    if roles is None:
        return []
    found = [("/timeline", "Timeline")]
    if any(lane.plugin == THREAD and lane.role in roles for lane in lanes):
        found.append(("/timeline/conversations", "Conversations"))
    return found
