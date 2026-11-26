"""The viewer's header links, as a declaration that needs no reader (package
split, step 10).

`view` serves the viewer's pages and `serve --without-viewer` the admin pages,
in two processes; both headers carry the same links. The viewer's part asks this
over its settings; the admin process asks it over the configuration it reads
(`src.cli.compose.viewer_links`) and loads nothing else of the viewer. Standard library
only.
"""

from collections.abc import Collection

# The analysis package the viewer shows, and the input role of its conversations.
PACKAGE = "timeline"
MESSAGES_ROLE = "messages"


def links(roles: Collection[str] | None) -> list[tuple[str, str]]:
    """The header's links, as (address, label), given the linked input roles of
    the first analysis of the timeline package, or None when there is none: the
    timeline, and its conversations where it reads a `messages` input."""
    if roles is None:
        return []
    found = [("/timeline", "Timeline")]
    if MESSAGES_ROLE in roles:
        found.append(("/timeline/conversations", "Conversations"))
    return found
