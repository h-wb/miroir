"""The failures a pass can report, in fixed words that are safe to print."""

from typing import Literal

Step = Literal["collect", "deliver", "links", "put", "confirm"]
Reason = Literal[
    "source",
    "masking",
    "state",
    "token",
    "http",
    "timeout",
    "network",
    "reply",
    "not_stored",
    "link",
    "file",
    "internal",
]


class Failure(Exception):
    """Why a pass stopped: the step it was in and a reason, never source content."""

    def __init__(
        self, step: Step, reason: Reason, http_status: int | None = None
    ) -> None:
        self.step = step
        self.reason = reason
        self.http_status = http_status if reason == "http" else None
        super().__init__(f"{step}: {reason}")

    def fields(self) -> dict[str, str | int | None]:
        return dict(step=self.step, reason=self.reason, http_status=self.http_status)


class ConfigError(Exception):
    """A configuration problem, without input values."""
