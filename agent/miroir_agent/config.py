"""Strict JSON configuration, independent of server settings."""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .collectors import COLLECTORS


@dataclass(frozen=True)
class CollectorConfig:
    id: str
    collector: str
    receive_url: str
    token_env: str
    timeout_seconds: int
    options: dict[str, Any]


@dataclass(frozen=True)
class Config:
    state_dir: Path
    collectors: list[CollectorConfig]


def _object(value: Any, allowed: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or value.keys() - allowed:
        raise ValueError("Invalid agent configuration object")
    return value


def _string(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("Expected a nonempty configuration string")
    return value


def _integer(obj: dict[str, Any], key: str, default: int, minimum: int) -> int:
    value = obj.get(key, default)
    if type(value) is not int or value < minimum:
        raise ValueError("Invalid configuration integer")
    return value


def _strings(obj: dict[str, Any], key: str) -> list[str]:
    value = obj.get(key, [])
    if not isinstance(value, list) or not all(
        isinstance(s, str) and s.strip() for s in value
    ):
        raise ValueError("Expected a list of nonblank configuration strings")
    return value


def _url(url: str) -> str:
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        raise ValueError("Invalid receiving URL") from None
    if (
        any(ord(c) <= 32 or ord(c) == 127 for c in url)
        or not parts.hostname
        or parts.username is not None
        or port == 0
        or parts.fragment
        or not (
            parts.scheme == "https"
            or (
                parts.scheme == "http"
                and parts.hostname in {"localhost", "127.0.0.1", "::1"}
            )
        )
    ):
        raise ValueError("Invalid receiving URL")
    return url


def _options(name: str, raw: Any) -> dict[str, Any]:
    if name != "screenmemory":
        return _object(raw, set())
    options = _object(
        raw,
        {
            "data_dir",
            "cutoff_ts",
            "settle_seconds",
            "max_rows",
            "drop_bundle_ids",
            "drop_markers",
        },
    )
    max_rows = _integer(options, "max_rows", 20000, 1)
    if max_rows > 200000:
        raise ValueError("Invalid max_rows")
    return {
        "data_dir": str(Path(_string(options, "data_dir")).expanduser()),
        "cutoff_ts": _integer(options, "cutoff_ts", 0, -(2**63)),
        "settle_seconds": _integer(options, "settle_seconds", 300, 0),
        "max_rows": max_rows,
        "drop_bundle_ids": _strings(options, "drop_bundle_ids"),
        "drop_markers": _strings(options, "drop_markers"),
    }


def load_config(path: Path) -> Config:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError):
        raise ValueError("Cannot read agent configuration") from None
    obj = _object(raw, {"state_dir", "collectors"})
    state_dir = Path(_string(obj, "state_dir")).expanduser()
    items = obj.get("collectors")
    if type(items) is not list:
        raise ValueError("Expected a list of collectors")
    collectors: list[CollectorConfig] = []
    ids: set[str] = set()
    for raw_item in items:
        item = _object(
            raw_item,
            {
                "id",
                "collector",
                "receive_url",
                "token_env",
                "timeout_seconds",
                "options",
            },
        )
        id_ = _string(item, "id")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,23}", id_) or id_ in ids:
            raise ValueError("Invalid or duplicate collector id")
        ids.add(id_)
        name = _string(item, "collector")
        if name not in COLLECTORS:
            raise ValueError("Unknown collector")
        token_env = _string(item, "token_env")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token_env):
            raise ValueError("Invalid token environment variable name")
        collectors.append(
            CollectorConfig(
                id_,
                name,
                _url(_string(item, "receive_url")),
                token_env,
                _integer(item, "timeout_seconds", 120, 1),
                _options(name, item.get("options", {})),
            )
        )
    return Config(state_dir, collectors)
