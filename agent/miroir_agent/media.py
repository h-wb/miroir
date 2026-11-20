"""Plan, stream and confirm media uploads without exposing signed links."""

import http.client
import json
import os
import re
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, build_opener

from .collectors import MediaItem
from .config import CollectorConfig, _url
from .deliver import _NoRedirect

_MAX_REPLY = 2 * 1024**2
_FAILED = "Media delivery failed"


def token_for(config: CollectorConfig) -> str:
    token = os.environ.get(config.token_env, "")
    if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
        raise ValueError(_FAILED)
    return token


def _post(
    config: CollectorConfig, token: str, kind: str, action: str, items: list[MediaItem]
) -> list[dict[str, str]]:
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
    with build_opener(_NoRedirect()).open(
        request, timeout=config.timeout_seconds
    ) as response:
        raw = response.read(_MAX_REPLY + 1)
    if len(raw) > _MAX_REPLY:
        raise ValueError(_FAILED)
    document = json.loads(raw)
    if not isinstance(document, dict) or document.keys() != {"objects"}:
        raise ValueError(_FAILED)
    results = document["objects"]
    if not isinstance(results, list) or len(results) != len(items):
        raise ValueError(_FAILED)
    for item, result in zip(items, results):
        if not isinstance(result, dict) or result.get("key") != item.key:
            raise ValueError(_FAILED)
        if not all(isinstance(value, str) for value in result.values()):
            raise ValueError(_FAILED)
        status = result.get("status")
        fields = {"key", "status"}
        if action == "confirm":
            if status != "stored":
                raise ValueError(_FAILED)
        elif status == "upload":
            fields.update(("url", "content_type"))
        elif status == "rejected":
            fields.add("reason")
        elif status != "present":
            raise ValueError(_FAILED)
        if result.keys() != fields:
            raise ValueError(_FAILED)
    return results


def _put(item: MediaItem, url: str, content_type: str, timeout: int) -> None:
    _url(url)
    if not re.fullmatch(r"[ -~]+", content_type) or not content_type.strip():
        raise ValueError(_FAILED)
    with item.path.open("rb") as body:
        if os.fstat(body.fileno()).st_size != item.size:
            raise ValueError(_FAILED)
        request = Request(
            url,
            data=body,
            headers={"Content-Type": content_type, "Content-Length": str(item.size)},
            method="PUT",
        )
        with build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
            if response.status not in (200, 201, 204):
                raise ValueError(_FAILED)


def deliver_media(
    config: CollectorConfig,
    token: str,
    kind: str,
    items: list[MediaItem],
    counts: dict[str, int],
) -> None:
    try:
        confirmed = 0
        # A single timestamp can contain more than one request's worth of frames.
        for offset in range(0, len(items), 500):
            batch = items[offset : offset + 500]
            results = _post(config, token, kind, "uploads", batch)
            uploaded: list[MediaItem] = []
            for item, result in zip(batch, results):
                status = result["status"]
                if status == "upload":
                    _put(
                        item,
                        result["url"],
                        result["content_type"],
                        config.timeout_seconds,
                    )
                    uploaded.append(item)
                else:
                    counts[status] += 1
            if uploaded:
                _post(config, token, kind, "confirm", uploaded)
                confirmed += len(uploaded)
        counts["uploaded"] += confirmed
    except HTTPError as error:
        error.close()
        raise ValueError(_FAILED) from None
    except (
        OSError,
        http.client.HTTPException,
        ValueError,
        OverflowError,
        RecursionError,
    ):
        raise ValueError(_FAILED) from None
