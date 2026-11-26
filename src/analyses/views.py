"""What the viewer shows of an analysis package, declared as data.

A package's built tables are read by the viewer; what they mean for it is said
here, beside the catalog, so the viewer names no package, role or media kind
(package split, step 7). Data only: this module imports nothing of the viewer,
media or the control plane. Lanes join it in step 11.
"""

from collections.abc import Mapping
from dataclasses import dataclass


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


# A frame the collector withheld keeps its row, with the reason in `withheld`,
# and hides its screenshot.
SCREEN_FRAMES = Withhold("screenshots", "screen_frames", "image_path", "withheld")

# Package id -> where its views hide files.
WITHHOLDS: Mapping[str, tuple[Withhold, ...]] = {
    "timeline": (SCREEN_FRAMES,),
}
