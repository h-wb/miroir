"""The options of a backfill run: which kind to walk, how far, and which
recorded statuses to derive again (after a decoder incident; decision 040)."""

from collections.abc import Iterable
from typing import Any

from src.kernel.errors import OperationError
from src.media.kinds import MediaKind, derived_kind

# Files one backfill run may walk.
MAX_BACKFILL = 1_000_000
# Statuses a backfill may derive again: those a decoder fault can leave behind.
RETRY_STATUSES = ("failed", "unreadable")


def backfill_options(
    kinds: Iterable[MediaKind],
    name: str,
    limit: int,
    retry: Iterable[str] = (),
) -> dict[str, Any]:
    if derived_kind(kinds, name) is None:
        raise OperationError("The worker makes no thumbnails of this media kind")
    if type(limit) is not int or not 1 <= limit <= MAX_BACKFILL:
        raise OperationError(f"The limit must be between 1 and {MAX_BACKFILL}")
    statuses = sorted(set(retry))
    if not set(statuses) <= set(RETRY_STATUSES):
        raise OperationError(
            f"Only these statuses can be retried: {', '.join(RETRY_STATUSES)}"
        )
    options: dict[str, Any] = {"kind": name, "limit": limit}
    if statuses:
        options["retry"] = statuses
    return options
