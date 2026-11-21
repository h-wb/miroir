"""Masking for screen text and browser history."""

import math
import re
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Sequence
from pathlib import Path

from .scanner import ScannerLayer, Span, Unscanned

_BEGIN = re.compile(r"\bBEGIN\s+(?:[\w-]+\s+)*PRIVATE KEY\b")
_END = re.compile(r"\bEND\s+(?:[\w-]+\s+)*PRIVATE KEY\b")
_BASE64 = re.compile(r"[A-Za-z0-9+/=]{16,}")
_TOKEN = re.compile(
    r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}"
    r"|(?:glpat-|sk-)[A-Za-z0-9_-]{20,}|(?:AKIA|ASIA)[0-9A-Z]{16}(?![0-9A-Z])"
    r"|xox[abprs]-[A-Za-z0-9-]{10,}|AIza[0-9A-Za-z_-]{35}(?![0-9A-Za-z_-])"
    r"|npm_[A-Za-z0-9]{36}(?![A-Za-z0-9])|(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}"
    r"|tskey-[A-Za-z0-9-]{20,}|AGE-SECRET-KEY-1[A-Z0-9]{50,}"
    r"|eyJ[A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9_-]{8,}){2})(?!\w)"
    r"|\b0\.[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"
    r"\.[A-Za-z0-9+/=]{20,}:[A-Za-z0-9+/=]{20,}(?![\w+/=])"
    r"|\b(?i:Bearer)\s+([A-Za-z0-9._~+/=-]{20,})"
)
_ASSIGNMENT = re.compile(
    r"(?<![^\W_])(?:password|passwd|passphrase|pwd|secret|token|api[ _-]?key"
    r"|access[ _-]?key|private[ _-]?key|client[ _-]?secret|seed)"
    r"[ \t]*[:=][ \t]*['\"]?([^\s'\"]+)",
    re.IGNORECASE,
)
_RANDOM = re.compile(r"[A-Za-z0-9_-]{32,}")
_HEX = re.compile(r"[A-Fa-f0-9]+")
_UUID = re.compile(r"[A-Fa-f0-9]{8}(?:-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}")
_LETTER = re.compile(r"[A-Za-z]")
_LOWER = re.compile(r"[a-z]")
_UPPER = re.compile(r"[A-Z]")
_DIGIT = re.compile(r"[0-9]")
_CARD = re.compile(r"[0-9]+(?:[ -][0-9]+)*")
_AUTHORITY = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://([^/?#]*)")
_SECRET_NAMES = {
    "key",
    "code",
    "auth",
    "sig",
    "sid",
    "otp",
    "jwt",
    "pwd",
    "passwd",
    "assertion",
    "samlresponse",
    "credential",
}
_SECRET_SUFFIXES = (
    "token",
    "secret",
    "password",
    "signature",
    "apikey",
    "accesskey",
    "sessionid",
)


def _private_keys(text: str) -> Iterator[tuple[int, int]]:
    lines = text.splitlines(keepends=True)
    i = offset = 0
    while i < len(lines):
        if (begin := _BEGIN.search(lines[i])) is None:
            offset += len(lines[i])
            i += 1
            continue
        end = (
            i + 1
            if _END.search(lines[i], begin.end())
            else next(
                (
                    j + 1
                    for j in range(i + 1, min(i + 201, len(lines)))
                    if _END.search(lines[j])
                ),
                None,
            )
        )
        if end is None:
            end = i + 1
            while end < len(lines) and _BASE64.fullmatch(lines[end].rstrip("\r\n")):
                end += 1
        block = "".join(lines[i:end])
        stop = offset + len(block)
        yield offset, offset + len(block.rstrip("\r\n"))
        i, offset = end, stop


def _assignment(value: str) -> bool:
    return len(value) >= 20 or (
        len(value) >= 8 and bool(_LETTER.search(value) and _DIGIT.search(value))
    )


def _random(value: str) -> bool:
    if (
        _HEX.fullmatch(value)
        or _UUID.fullmatch(value)
        or not all(pattern.search(value) for pattern in (_LOWER, _UPPER, _DIGIT))
    ):
        return False
    frequencies = (n / len(value) for n in Counter(value).values())
    return -sum(p * math.log2(p) for p in frequencies) >= 3.5


def _luhn(value: str) -> bool:
    digits = value.replace(" ", "").replace("-", "")
    if len(digits) not in (15, 16):
        return False
    total = 0
    for i, digit in enumerate(reversed(digits)):
        n = int(digit) * (2 if i % 2 else 1)
        total += n - 9 if n > 9 else n
    return total % 10 == 0


def _matches(
    text: str, pattern: re.Pattern[str], accept: Callable[[str], bool] = bool
) -> Iterator[tuple[int, int]]:
    for match in pattern.finditer(text):
        group = match.lastindex or 0
        if accept(match[group]):
            yield match.span(group)


MaskSpan = tuple[int, int, str]
_Rule = tuple[str, Callable[[str], Iterable[Span]]]
_TOKEN_RULE: _Rule = ("token", lambda s: _matches(s, _TOKEN))
_TEXT_RULES: tuple[_Rule, ...] = (
    ("private_key", _private_keys),
    _TOKEN_RULE,
    ("assignment", lambda s: _matches(s, _ASSIGNMENT, _assignment)),
    ("random", lambda s: _matches(s, _RANDOM, _random)),
    ("card", lambda s: _matches(s, _CARD, _luhn)),
)


def _own_spans(text: str, rules: Sequence[_Rule]) -> list[MaskSpan]:
    remaining = [(0, len(text))]
    spans: list[MaskSpan] = []
    for rule, find in rules:
        updated = []
        for lower, upper in remaining:
            offset = lower
            for start, end in find(text[lower:upper]):
                updated.append((offset, lower + start))
                spans.append((lower + start, lower + end, rule))
                offset = lower + end
            updated.append((offset, upper))
        remaining = updated
    return spans


def _union(spans: Iterable[MaskSpan]) -> list[MaskSpan]:
    merged: list[MaskSpan] = []
    for start, end, rule in sorted(spans):
        if merged and start <= merged[-1][1]:
            lower, upper, label = merged[-1]
            merged[-1] = (lower, max(upper, end), rule if label == "scanner" else label)
        else:
            merged.append((start, end, rule))
    return merged


def _render(text: str, spans: Iterable[MaskSpan]) -> tuple[str, dict[str, int]]:
    parts: list[str] = []
    counts: Counter[str] = Counter()
    offset = 0
    for start, end, rule in spans:
        parts.extend((text[offset:start], f"[masked:{rule}]"))
        counts[rule] += 1
        offset = end
    parts.append(text[offset:])
    return "".join(parts), dict(counts)


def _url_userinfo(text: str) -> Iterator[Span]:
    if match := _AUTHORITY.match(text):
        userinfo, at, _ = match[1].rpartition("@")
        _, colon, password = userinfo.partition(":")
        if at and colon and password:
            end = match.start(1) + len(userinfo)
            yield end - len(password), end


def _url_params(text: str) -> Iterator[Span]:
    before_fragment, hash_, fragment = text.partition("#")
    base, question, query = before_fragment.partition("?")
    route, separator, fragment_query = fragment.partition("?")
    offset = len(before_fragment) + len(hash_)
    if separator and "=" not in route:
        offset += len(route) + len(separator)
        fragment = fragment_query
    for params, start in ((query, len(base) + len(question)), (fragment, offset)):
        for param in re.split(r"[&;]", params):
            name, equals, value = param.partition("=")
            normal = name.lower().replace("-", "").replace("_", "")
            end = start + len(param)
            if (
                equals
                and value
                and (normal in _SECRET_NAMES or normal.endswith(_SECRET_SUFFIXES))
            ):
                yield end - len(value), end
            start = end + 1


_URL_RULES: tuple[_Rule, ...] = (
    ("url_userinfo", _url_userinfo),
    ("url_param", _url_params),
    _TOKEN_RULE,
)


class Masker:
    """The fixed rules, plus the scanner's catalogue and the user's rule files."""

    def __init__(self, rule_files: Sequence[Path] = ()) -> None:
        self._scanner = ScannerLayer(rule_files)

    def _spans(self, text: str, rules: Sequence[_Rule]) -> list[MaskSpan]:
        own = _own_spans(text, rules)
        try:
            scanner = self._scanner.spans(text)
        except Unscanned:
            return [(0, len(text), "unscanned")]
        return _union([*own, *((start, end, "scanner") for start, end in scanner)])

    def spans(self, text: str) -> list[MaskSpan]:
        """Masking regions in original-text character offsets, with their labels."""
        return self._spans(text, _TEXT_RULES)

    def text(self, text: str) -> tuple[str, dict[str, int]]:
        return _render(text, self.spans(text))

    def tokens(self, text: str) -> tuple[str, dict[str, int]]:
        return _render(text, self._spans(text, (_TOKEN_RULE,)))

    def url(self, text: str) -> tuple[str, dict[str, int]]:
        return _render(text, self._spans(text, _URL_RULES))
