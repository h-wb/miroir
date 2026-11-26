"""Media kinds: what a connector declares about the files its rows point at.

Connectors import this module and nothing else of media. A kind is stored by
media, never parsed (decision 033).
"""

from collections.abc import Iterable
from dataclasses import dataclass

from src.kernel.errors import OperationError
from src.media.profiles import PROFILES, Profile


@dataclass(frozen=True)
class MediaKind:
    """Files that a resource's rows point at."""

    name: str
    resource: str
    column: str
    extensions: tuple[str, ...]
    max_bytes: int
    # How the worker makes thumbnails of it, by name in `src.media.profiles`
    # (decision 040). A kind with none is not derived: no thumbnails and no
    # `media_objects` rows.
    profile: str | None = None
    # The largest file the worker reads to make thumbnails of; a larger one is
    # recorded `too_large` without being read. None means `max_bytes`.
    derive_max_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.profile is not None and self.profile not in PROFILES:
            raise ValueError(f"unknown media profile {self.profile!r}")

    @property
    def derive_profile(self) -> Profile | None:
        return None if self.profile is None else PROFILES[self.profile]

    @property
    def derive_bound(self) -> int:
        if self.derive_max_bytes is None:
            return self.max_bytes
        return min(self.derive_max_bytes, self.max_bytes)


def kind_named(kinds: Iterable[MediaKind], name: str) -> MediaKind:
    for kind in kinds:
        if kind.name == name:
            return kind
    raise OperationError("Unknown media kind")


def derived_kind(kinds: Iterable[MediaKind], name: str) -> MediaKind | None:
    """The kind, if the worker makes thumbnails of it."""
    kind = kind_named(kinds, name)
    return kind if kind.profile is not None else None
