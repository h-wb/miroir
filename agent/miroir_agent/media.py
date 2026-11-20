"""Plan, stream and confirm media uploads without exposing signed links."""

import http.client
import json
import os
import re
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import BinaryIO, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, build_opener

from .collectors import MediaItem
from .config import CollectorConfig, _url
from .deliver import _NoRedirect

_MAX_REPLY = 2 * 1024**2
_GROUP_SIZE = 25
_PUT_WORKERS = 4

Step = Literal["collect", "links", "put", "confirm"]
Reason = Literal[
    "http",
    "timeout",
    "network",
    "reply",
    "not_stored",
    "link",
    "token",
    "file",
    "state",
    "source",
]


class MediaFailure(ValueError):
    def __init__(
        self, step: Step, reason: Reason, http_status: int | None = None
    ) -> None:
        super().__init__("Media delivery failed")
        self.step = step
        self.reason = reason
        self.http_status = http_status if reason == "http" else None


def token_for(config: CollectorConfig) -> str:
    token = os.environ.get(config.token_env, "")
    if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
        raise MediaFailure("links", "token")
    return token


@contextmanager
def _delivery_errors(step: Step) -> Iterator[None]:
    try:
        yield
    except MediaFailure:
        raise
    except HTTPError as error:
        error.close()
        if step == "put" and 300 <= error.code < 400:
            raise MediaFailure(step, "link") from None
        raise MediaFailure(step, "http", error.code) from None
    except TimeoutError:
        raise MediaFailure(step, "timeout") from None
    except URLError as error:
        reason: Reason = (
            "timeout" if isinstance(error.reason, TimeoutError) else "network"
        )
        raise MediaFailure(step, reason) from None
    except (OSError, http.client.HTTPException, OverflowError):
        raise MediaFailure(step, "network") from None
    except (ValueError, RecursionError):
        raise MediaFailure(step, "reply") from None


def _post(
    config: CollectorConfig,
    token: str,
    kind: str,
    action: Literal["uploads", "confirm"],
    items: list[MediaItem],
) -> list[dict[str, str]]:
    step: Step = "links" if action == "uploads" else "confirm"
    base = urlsplit(config.receive_url)
    url = base._replace(
        path=base.path.removesuffix("/receive") + f"/media/{action}", query=""
    ).geturl()
    request = Request(
        url,
        data=json.dumps(
            {
                "kind": kind,
                "objects": [{"key": item.key, "size": item.size} for item in items],
            }
        ).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with _delivery_errors(step):
        with build_opener(_NoRedirect()).open(
            request, timeout=config.timeout_seconds
        ) as response:
            raw = response.read(_MAX_REPLY + 1)
        return _results(raw, step, items)


def _results(raw: bytes, step: Step, items: list[MediaItem]) -> list[dict[str, str]]:
    if len(raw) > _MAX_REPLY:
        raise MediaFailure(step, "reply")
    document = json.loads(raw)
    if not isinstance(document, dict) or document.keys() != {"objects"}:
        raise MediaFailure(step, "reply")
    results = document["objects"]
    if not isinstance(results, list) or len(results) != len(items):
        raise MediaFailure(step, "reply")
    for item, result in zip(items, results):
        if not isinstance(result, dict) or result.get("key") != item.key:
            raise MediaFailure(step, "reply")
        if not all(isinstance(value, str) for value in result.values()):
            raise MediaFailure(step, "reply")
        status = result.get("status")
        fields = {"key", "status"}
        if step == "links":
            if status == "upload":
                content_type = result.get("content_type")
                if not isinstance(content_type, str) or not (
                    re.fullmatch(r"[ -~]+", content_type) and content_type.strip()
                ):
                    raise MediaFailure(step, "reply")
                fields.update(("url", "content_type"))
            elif status == "rejected":
                fields.add("reason")
            elif status != "present":
                raise MediaFailure(step, "reply")
        if result.keys() != fields:
            raise MediaFailure(step, "reply")
    return results


@contextmanager
def _file_errors() -> Iterator[None]:
    try:
        yield
    except OSError:
        raise MediaFailure("put", "file") from None


class _UploadBody:
    def __init__(self, file: BinaryIO) -> None:
        self._file = file

    def read(self, size: int = -1, /) -> bytes:
        with _file_errors():
            return self._file.read(size)


def _put(item: MediaItem, url: str, content_type: str, timeout: int) -> None:
    try:
        _url(url)
    except ValueError:
        raise MediaFailure("put", "link") from None
    with _file_errors(), item.path.open("rb") as file:
        if os.fstat(file.fileno()).st_size != item.size:
            raise MediaFailure("put", "file")
        request = Request(
            url,
            data=_UploadBody(file),
            headers={"Content-Type": content_type, "Content-Length": str(item.size)},
            method="PUT",
        )
        with (
            _delivery_errors("put"),
            build_opener(_NoRedirect()).open(request, timeout=timeout) as response,
        ):
            if response.status not in (200, 201, 204):
                raise MediaFailure("put", "http", response.status)


def deliver_media(
    config: CollectorConfig,
    token: str,
    kind: str,
    items: list[MediaItem],
    counts: dict[str, int],
) -> None:
    for offset in range(0, len(items), _GROUP_SIZE):
        group = items[offset : offset + _GROUP_SIZE]
        results = _post(config, token, kind, "uploads", group)
        pending: list[tuple[MediaItem, str, str]] = []
        for item, result in zip(group, results):
            status = result["status"]
            if status == "upload":
                pending.append((item, result["url"], result["content_type"]))
            else:
                counts[status] += 1
        if not pending:
            continue
        with ThreadPoolExecutor(max_workers=_PUT_WORKERS) as pool:
            puts = [
                pool.submit(_put, item, url, content_type, config.timeout_seconds)
                for item, url, content_type in pending
            ]
            failure = None
            for put in puts:
                try:
                    put.result()
                except MediaFailure as error:
                    if failure is None:
                        failure = error
            if failure is not None:
                raise failure
        confirmed = _post(
            config, token, kind, "confirm", [item for item, _, _ in pending]
        )
        stored = sum(result["status"] == "stored" for result in confirmed)
        counts["uploaded"] += stored
        if stored != len(confirmed):
            raise MediaFailure("confirm", "not_stored")
