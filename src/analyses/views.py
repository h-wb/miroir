"""What the viewer shows of an analysis package, declared as data.

A package's built tables are read by the viewer; what they mean for it is said
here, beside the catalog, so the viewer names no package, role, source or media
kind (package split, steps 7 and 11). Data only: this module imports nothing of
the viewer, media or the control plane.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Withhold:
    """Rows of a built model that hide the files they point at.

    `kind` is a media kind of a connection the package reads as an input; a row
    of `model` whose `key_column` is a file's key and whose `flag_column` is not
    null hides that file, at any size and as a link (decision 040).
    """

    kind: str
    model: str
    key_column: str
    flag_column: str


# The plugins the viewer implements, by name: a lane names one only for a screen
# of its own (the scrubber's frames, the thread's conversations). An unknown
# name fails the catalog test.
SCRUBBER = "scrubber"
THREAD = "thread"
PLUGINS = (SCRUBBER, THREAD)
# How a lane colours its marks: by each event's group (a palette colour from a
# stable hash of `group_key`; the scrubber draws its apps' own colours), or all
# in the lane's one colour.
COLOURS = ("group", "lane")


@dataclass(frozen=True)
class Lane:
    """One source of the timeline's `events` on the Day view (package split,
    step 11).

    `role` is an input of the package; `source` the `events.source` its rows
    carry, written by the model `timeline_<source>`. The viewer draws the lane,
    its marks, the events around a moment, its day totals, its search and its
    media from `events` alone; a `plugin` adds a screen of its own.

    The rest says how the pages word it: `units` count a day's events (one,
    several), `search_in` where its search looks ("in page titles"). A lane is
    shown where its role is linked, or `always`: then it is shown, counted on
    the latest day `/timeline` opens and read by the events API when no source
    is asked for, linked or not. `media_kind` is the kind of the files its rows
    and its plugin's rows point at (a kind its role's connector declares).
    """

    role: str
    source: str
    label: str
    colour: str = "group"
    plugin: str | None = None
    units: tuple[str, str] = ("event", "events")
    search_in: str = "in its events"
    always: bool = False
    media_kind: str | None = None

    def built(self) -> dict[str, Any]:
        """What the models read of it, as the build hands it to them and
        `contract` keeps it: its role, its source and the kind of its files.
        Nothing of its wording, so a change of label, colour, units or search
        words changes no build (the receipt holds the build's variables)."""
        return {"role": self.role, "source": self.source, "media_kind": self.media_kind}


# A frame the collector withheld keeps its row, with the reason in `withheld`,
# and hides its screenshot.
SCREEN_FRAMES = Withhold("screenshots", "screen_frames", "image_path", "withheld")

# Package id -> where its views hide files.
WITHHOLDS: Mapping[str, tuple[Withhold, ...]] = {
    "timeline": (SCREEN_FRAMES,),
}

# Package id -> its lanes, in the order the Day view shows them. The order is
# also the order `events` unites its sources in: keep it.
LANES: Mapping[str, tuple[Lane, ...]] = {
    "timeline": (
        Lane(
            "screen",
            "screen",
            "Screen",
            "group",
            SCRUBBER,
            units=("screen frame", "screen frames"),
            search_in="on screen",
            always=True,
            media_kind="screenshots",
        ),
        Lane(
            "browser",
            "browser",
            "Browser",
            "lane",
            units=("browser visit", "browser visits"),
            search_in="in page titles",
            always=True,
        ),
        Lane(
            "messages",
            "messages",
            "Messages",
            "lane",
            THREAD,
            units=("message", "messages"),
            search_in="in messages",
            media_kind="attachments",
        ),
    ),
}

# The version of the timeline's tables as the viewer reads them, which the
# package writes in its `contract` relation: major.minor. A column or relation
# added is a minor change, one removed or changed a major one; the viewer
# answers 503 for a major it does not know.
CONTRACT_VERSION = "1.0"


def declares_views(package_id: str) -> bool:
    """Whether a package's built tables are read by the viewer."""
    return bool(WITHHOLDS.get(package_id) or LANES.get(package_id))
