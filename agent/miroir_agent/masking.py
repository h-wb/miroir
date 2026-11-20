"""Deterministic masking for screen text."""

import math
import re
from collections import Counter
from collections.abc import Callable, Iterator

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


def mask(text: str) -> tuple[str, dict[str, int]]:
    # Replacements are separate from unmasked fragments, so no rule touches them.
    parts = [(text, False)]
    counts: Counter[str] = Counter()
    rules: tuple[tuple[str, Callable[[str], Iterator[tuple[int, int]]]], ...] = (
        ("private_key", _private_keys),
        ("token", lambda s: _matches(s, _TOKEN)),
        ("assignment", lambda s: _matches(s, _ASSIGNMENT, _assignment)),
        ("random", lambda s: _matches(s, _RANDOM, _random)),
        ("card", lambda s: _matches(s, _CARD, _luhn)),
    )
    for rule, find in rules:
        updated = []
        for part, masked in parts:
            if masked:
                updated.append((part, True))
                continue
            offset = 0
            for start, end in find(part):
                updated.extend(
                    ((part[offset:start], False), (f"[masked:{rule}]", True))
                )
                counts[rule] += 1
                offset = end
            updated.append((part[offset:], False))
        parts = updated
    return "".join(part for part, _ in parts), dict(counts)


def mask_tokens(text: str) -> tuple[str, dict[str, int]]:
    parts: list[str] = []
    offset = count = 0
    for start, end in _matches(text, _TOKEN):
        parts.extend((text[offset:start], "[masked:token]"))
        offset = end
        count += 1
    parts.append(text[offset:])
    return "".join(parts), {"token": count} if count else {}


def mask_url(text: str) -> tuple[str, dict[str, int]]:
    counts: Counter[str] = Counter()
    if match := _AUTHORITY.match(text):
        userinfo, at, _ = match[1].rpartition("@")
        _, colon, password = userinfo.partition(":")
        if at and colon and password:
            end = match.start(1) + len(userinfo)
            text = text[: end - len(password)] + "[masked:url_userinfo]" + text[end:]
            counts["url_userinfo"] += 1

    def redact(params: str) -> str:
        parts = re.split(r"([&;])", params)
        for i in range(0, len(parts), 2):
            name, equals, value = parts[i].partition("=")
            normal = name.lower().replace("-", "").replace("_", "")
            if (
                equals
                and value
                and (normal in _SECRET_NAMES or normal.endswith(_SECRET_SUFFIXES))
            ):
                parts[i] = name + "=[masked:url_param]"
                counts["url_param"] += 1
        return "".join(parts)

    before_fragment, hash_, fragment = text.partition("#")
    base, question, query = before_fragment.partition("?")
    route, separator, fragment_query = fragment.partition("?")
    if separator and "=" not in route:
        fragment = route + separator + redact(fragment_query)
    else:
        fragment = redact(fragment)
    text = base + question + redact(query) + hash_ + fragment
    text, tokens = mask_tokens(text)
    counts.update(tokens)
    return text, dict(counts)
