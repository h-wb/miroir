"""Stream a pending batch to its configured endpoint without redirects."""

import http.client
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO
from urllib.error import HTTPError
from urllib.parse import unquote_plus, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .collectors import COLLECTORS
from .config import CollectorConfig


class _NoRedirect(HTTPRedirectHandler):
    def http_error_302(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: http.client.HTTPMessage,
    ) -> None:
        # Fall through to urllib's default HTTP error handler; never parse Location.
        return None

    http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302


@dataclass(frozen=True)
class Delivery:
    status: int | None = None
    receipt: dict[str, str | bool] = field(default_factory=dict)

    @property
    def acknowledged(self) -> bool:
        return self.status in (200, 201)


def _receipt(response: http.client.HTTPResponse, token: str) -> dict[str, str | bool]:
    try:
        data = json.loads(response.read(65536))
    except (ValueError, RecursionError):
        return {}
    if not isinstance(data, dict):
        return {}
    receipt: dict[str, str | bool] = {}
    artifact_id = data.get("artifact_id")
    if isinstance(artifact_id, str) and token not in artifact_id:
        receipt["artifact_id"] = artifact_id
    if isinstance(new := data.get("new"), bool):
        receipt["new"] = new
    return receipt


def _receive_url(config: CollectorConfig) -> str:
    url = urlsplit(config.receive_url)
    params = [
        part
        for part in url.query.split("&")
        if part and unquote_plus(part.partition("=")[0]) != "format"
    ]
    params.append(f"format={COLLECTORS[config.collector].format_id}")
    return url._replace(query="&".join(params)).geturl()


def deliver(config: CollectorConfig, path: Path) -> Delivery:
    token = os.environ.get(config.token_env, "")
    if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
        return Delivery()
    try:
        with path.open("rb") as body:
            request = Request(
                _receive_url(config),
                data=body,
                method="POST",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/gzip",
                    "Content-Length": str(os.fstat(body.fileno()).st_size),
                },
            )
            with build_opener(_NoRedirect()).open(
                request, timeout=config.timeout_seconds
            ) as response:
                status = response.status
                return Delivery(
                    status, _receipt(response, token) if status in (200, 201) else {}
                )
    except HTTPError as error:
        error.close()
        return Delivery(error.code)
    except (OSError, http.client.HTTPException, ValueError, OverflowError):
        return Delivery()
