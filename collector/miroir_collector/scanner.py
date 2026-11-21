"""Lazy, fail-closed adapter for the Kingfisher scanner."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from .errors import Failure

if TYPE_CHECKING:
    from kingfisher_sdk import Scanner

Span = tuple[int, int]
_SCAN_TIMEOUT_SECONDS = 5


def _character_spans(text: str, spans: Iterable[Span]) -> list[Span]:
    data = text.encode("utf-8")
    merged: list[Span] = []
    for start, end in sorted(spans):
        if not 0 <= start < end <= len(data) or any(
            pos < len(data) and data[pos] & 0xC0 == 0x80 for pos in (start, end)
        ):
            raise ValueError("Invalid scanner offsets")
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))

    result = []
    byte_offset = char_offset = 0
    for start, end in merged:
        # Decode successive slices to translate UTF-8 offsets in one pass.
        char_start = char_offset + len(data[byte_offset:start].decode("utf-8"))
        char_offset = char_start + len(data[start:end].decode("utf-8"))
        result.append((char_start, char_offset))
        byte_offset = end
    return result


class Unscanned(Exception):
    """No part of the input may leave when a scan fails."""


class ScannerLayer:
    def __init__(self, rule_files: Sequence[Path]) -> None:
        self._rule_files = tuple(rule_files)
        self._scanner: Scanner | None = None
        self._failed = False

    def _start(self) -> Scanner:
        if self._failed:
            raise Failure("collect", "masking")
        if self._scanner is None:
            try:
                from kingfisher_sdk import Rules, Scanner

                if self._rule_files:
                    custom = Rules(
                        paths=self._rule_files,
                        builtins=False,
                        confidence="low",
                        cache=False,
                    )
                    if any(
                        not rule["id"].startswith("custom.")
                        for rule in custom.metadata()
                    ):
                        raise ValueError("Invalid custom rule ID")
                rules = Rules(paths=self._rule_files, confidence="low", cache=False)
                self._scanner = Scanner(rules, redact=True)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:  # noqa: BLE001 - includes native panics
                self._failed = True
                raise Failure("collect", "masking") from None
        return self._scanner

    def spans(self, text: str) -> list[Span]:
        if not text.strip() and not self._failed:
            return []
        scanner = self._start()
        if "\x00" in text:
            # The scanner decodes such text as UTF-16 and reports positions in
            # its own copy, so its answer cannot be placed in ours.
            raise Unscanned
        try:
            findings = scanner.scan(text, timeout=_SCAN_TIMEOUT_SECONDS)
            return _character_spans(
                text,
                (
                    (location["start_offset"], location["end_offset"])
                    for finding in findings
                    for location in (finding.location,)
                ),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:  # noqa: BLE001 - includes native panics
            raise Unscanned from None
