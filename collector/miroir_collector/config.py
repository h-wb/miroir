"""Strict JSON configuration, independent of server settings."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit

from pydantic import AfterValidator, Field, ValidationError

from .errors import ConfigError
from .models import Model
from .sources import SOURCES
from .sources.base import Nonempty, Options, Source, UserPath
from .transport import check_url


@dataclass(frozen=True)
class SourceConfig:
    id: str
    source: Source[Any, Any]
    receive_url: str
    token_env: str
    timeout_seconds: int
    options: Options


@dataclass(frozen=True)
class Config:
    state_dir: Path
    sources: list[SourceConfig]
    masking_rules: tuple[Path, ...]


class _Entry(Model):
    id: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,23}$")]
    source: Nonempty
    receive_url: Annotated[Nonempty, AfterValidator(check_url)]
    token_env: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]
    timeout_seconds: int = Field(default=120, ge=1)
    options: Any = Field(default_factory=dict)


def _rule_file(path: Path) -> Path:
    try:
        if path.is_file():
            return path
    except OSError:
        pass
    raise ValueError("file not found") from None


class _Masking(Model):
    rules: list[Annotated[UserPath, AfterValidator(_rule_file)]] = Field(
        default_factory=list
    )


class _Document(Model):
    state_dir: UserPath
    sources: list[_Entry]
    masking: _Masking = Field(default_factory=_Masking)


_MESSAGES = {
    "extra_forbidden": "unknown key",
    "missing": "required key",
    "bool_type": "expected a boolean",
    "int_type": "expected an integer",
    "list_type": "expected a list",
    "string_type": "expected a string",
    "greater_than_equal": "number out of range",
    "less_than_equal": "number out of range",
    "string_pattern_mismatch": "invalid format",
    "model_type": "expected an object",
}
_FIXED_ERRORS = {
    "file not found",
    "Invalid receiving URL",
    "Invalid date or offset-qualified instant",
    "Expected a nonempty string",
    "Expected a valid path",
    "Expected a nonblank string",
}


def _validate[M: Model](model: type[M], raw: object, prefix: str = "") -> M:
    try:
        return model.model_validate(raw)
    except ValidationError as error:
        item = error.errors(include_input=False, include_url=False)[0]
        loc = ".".join(
            str(key)[:40].encode("unicode_escape").decode("ascii")
            for key in item["loc"]
        )
        path = ".".join(part for part in (prefix, loc) if part) or "top level"
        message = _MESSAGES.get(item["type"], "invalid value")
        validator_error = str(item.get("ctx", {}).get("error", ""))
        if validator_error in _FIXED_ERRORS:
            message = validator_error
        raise ConfigError(f"{path}: {message}") from None


def _source(entry: _Entry, index: int, ids: set[str]) -> SourceConfig:
    prefix = f"sources.{index}"
    if entry.id in ids:
        raise ConfigError(f"{prefix}.id: duplicate source id")
    ids.add(entry.id)
    source = SOURCES.get(entry.source)
    if source is None:
        raise ConfigError(f"{prefix}.source: unknown source")
    options = _validate(source.options_model, entry.options, f"{prefix}.options")
    if source.media(options) is not None and not urlsplit(
        entry.receive_url
    ).path.endswith("/receive"):
        raise ConfigError(
            f"{prefix}.receive_url: Media requires a URL ending in /receive"
        )
    return SourceConfig(
        entry.id,
        source,
        entry.receive_url,
        entry.token_env,
        entry.timeout_seconds,
        options,
    )


def load_config(path: Path) -> Config:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError):
        raise ConfigError(
            "top level: cannot read the file, or it is not valid JSON"
        ) from None
    if isinstance(raw, dict) and "collectors" in raw:
        raise ConfigError("top level: unknown key collectors")
    document = _validate(_Document, raw)
    ids: set[str] = set()
    return Config(
        document.state_dir,
        [_source(entry, index, ids) for index, entry in enumerate(document.sources)],
        tuple(document.masking.rules),
    )
