"""All collector HTTP requests, without redirects or retries."""

import json
import os
import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Annotated, Any, BinaryIO, Literal, Self
from urllib.parse import unquote_plus, urlsplit

from pydantic import Field, ValidationError, field_validator
from urllib3 import PoolManager, Timeout
from urllib3.exceptions import HTTPError, NewConnectionError
from urllib3.exceptions import TimeoutError as HTTPTimeout
from urllib3.response import BaseHTTPResponse

from . import __version__
from .errors import Failure, Reason, Step
from .models import Model

_MAX_REPLY = 2 * 1024**2
_RECEIPT_LIMIT = 65536
PUT_WORKERS = 4


def check_url(url: str) -> str:
    """Allow https, or http to this machine; no credentials, no fragment."""
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


@dataclass(frozen=True)
class Receipt:
    status: int
    artifact_id: str | None = None
    new: bool | None = None

    def fields(self) -> dict[str, str | bool]:
        return {
            key: value
            for key, value in (("artifact_id", self.artifact_id), ("new", self.new))
            if value is not None
        }


class Upload(Model):
    key: str
    status: Literal["upload"]
    url: str
    content_type: str

    @field_validator("content_type")
    @classmethod
    def printable(cls, value: str) -> str:
        if not (re.fullmatch(r"[ -~]+", value) and value.strip()):
            raise ValueError("Invalid content type")
        return value


class Present(Model):
    key: str
    status: Literal["present"]


class Rejected(Model):
    key: str
    status: Literal["rejected"]
    reason: str


Link = Annotated[Upload | Present | Rejected, Field(discriminator="status")]


class Links(Model):
    objects: list[Link]


class Confirmed(Model):
    key: str
    status: str


class Confirmation(Model):
    objects: list[Confirmed]


@contextmanager
def _file_errors(step: Step) -> Iterator[None]:
    try:
        yield
    except OSError:
        raise Failure(step, "file" if step == "put" else "state") from None


class _Body:
    def __init__(self, file: BinaryIO, step: Step) -> None:
        self.file = file
        self.step = step

    def read(self, size: int = -1, /) -> bytes:
        with _file_errors(self.step):
            return self.file.read(size)


class Client:
    """Every request the collector sends. Each method returns or raises `Failure`."""

    def __init__(self, receive_url: str, token_env: str, timeout: int) -> None:
        self.receive_url = receive_url
        self.token_env = token_env
        self.pool = PoolManager(
            maxsize=PUT_WORKERS,
            block=True,
            retries=False,
            timeout=Timeout(connect=timeout, read=timeout),
        )

    def close(self) -> None:
        self.pool.clear()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _token(self, step: Step) -> str:
        token = os.environ.get(self.token_env, "")
        if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
            raise Failure(step, "token")
        return token

    def require_token(self, step: Step) -> None:
        self._token(step)

    def _request(
        self, step: Step, url: str, body: Any, headers: dict[str, str]
    ) -> tuple[int, bytes]:
        method = "PUT" if step == "put" else "POST"
        try:
            response = self.pool.request(
                method,
                url,
                body=body,
                headers={"User-Agent": f"miroir-collector/{__version__}", **headers},
                redirect=False,
                preload_content=False,
            )
            try:
                status = response.status
                _check_status(step, status)
                return status, _response_body(step, response)
            finally:
                if not response.closed:
                    response.close()
                response.release_conn()
        except (HTTPError, OSError, OverflowError) as error:
            raise Failure(step, _transport_reason(error)) from None

    def post_batch(self, path: Path, format_id: str) -> Receipt:
        """Deliver one batch file; returns only when Miroir acknowledged it."""
        token = self._token("deliver")
        url = urlsplit(self.receive_url)
        params = [
            part
            for part in url.query.split("&")
            if part and unquote_plus(part.partition("=")[0]) != "format"
        ]
        params.append(f"format={format_id}")
        with _file_errors("deliver"), path.open("rb") as file:
            status, raw = self._request(
                "deliver",
                url._replace(query="&".join(params)).geturl(),
                _Body(file, "deliver"),
                {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/gzip",
                    "Content-Length": str(os.fstat(file.fileno()).st_size),
                },
            )
        return _receipt(status, raw, token)

    def _post(
        self,
        step: Literal["links", "confirm"],
        kind: str,
        objects: Sequence[tuple[str, int]],
    ) -> Any:
        token = self._token(step)
        base = urlsplit(self.receive_url)
        action = "uploads" if step == "links" else "confirm"
        url = base._replace(
            path=base.path.removesuffix("/receive") + f"/media/{action}", query=""
        ).geturl()
        _, raw = self._request(
            step,
            url,
            json.dumps(
                {
                    "kind": kind,
                    "objects": [{"key": key, "size": size} for key, size in objects],
                }
            ).encode(),
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        if len(raw) > _MAX_REPLY:
            raise Failure(step, "reply")
        try:
            return json.loads(raw)
        except (ValueError, RecursionError):
            raise Failure(step, "reply") from None

    def upload_links(self, kind: str, objects: Sequence[tuple[str, int]]) -> list[Link]:
        """Ask where to send each (key, size); one answer per object, in order."""
        raw = self._post("links", kind, objects)
        try:
            results = Links.model_validate(raw).objects
        except ValidationError:
            raise Failure("links", "reply") from None
        _check_order("links", objects, results)
        return results

    def confirm(self, kind: str, objects: Sequence[tuple[str, int]]) -> list[str]:
        """Ask Miroir whether each object arrived; one status per object, in order."""
        raw = self._post("confirm", kind, objects)
        try:
            results = Confirmation.model_validate(raw).objects
        except ValidationError:
            raise Failure("confirm", "reply") from None
        _check_order("confirm", objects, results)
        return [item.status for item in results]

    def put(self, path: Path, size: int, url: str, content_type: str) -> None:
        """Send one file to its upload link."""
        try:
            check_url(url)
        except ValueError:
            raise Failure("put", "link") from None
        with _file_errors("put"), path.open("rb") as file:
            if os.fstat(file.fileno()).st_size != size:
                raise Failure("put", "file")
            self._request(
                "put",
                url,
                _Body(file, "put"),
                {"Content-Type": content_type, "Content-Length": str(size)},
            )


def _transport_reason(error: BaseException) -> Reason:
    # urllib3 wraps a timeout while sending the body in ProtocolError, and a
    # refused connection used to be a kind of connect timeout.
    seen: BaseException | None = error
    for _ in range(5):
        if seen is None or isinstance(seen, NewConnectionError):
            break
        if isinstance(seen, (HTTPTimeout, TimeoutError)):
            return "timeout"
        seen = seen.__cause__ or seen.__context__
    return "network"


def _check_status(step: Step, status: int) -> None:
    if step == "put" and 300 <= status < 400:
        raise Failure(step, "link")
    accepted = {"put": (200, 201, 204), "deliver": (200, 201)}.get(
        step, range(200, 300)
    )
    if status not in accepted:
        raise Failure(step, "http", status)


def _response_body(step: Step, response: BaseHTTPResponse) -> bytes:
    if step == "deliver":
        return response.read(_RECEIPT_LIMIT, decode_content=False)
    if step == "put":
        length = response.length_remaining
        if length is not None and 0 <= length <= _RECEIPT_LIMIT:
            with suppress(HTTPError, OSError):
                response.read(decode_content=False)
        return b""
    return response.read(_MAX_REPLY + 1, decode_content=False)


def _check_order(
    step: Step, objects: Sequence[tuple[str, int]], results: Sequence[Link | Confirmed]
) -> None:
    if [key for key, _ in objects] != [item.key for item in results]:
        raise Failure(step, "reply")


def _receipt(status: int, raw: bytes, token: str) -> Receipt:
    try:
        data = json.loads(raw)
    except (ValueError, RecursionError):
        return Receipt(status)
    if not isinstance(data, dict):
        return Receipt(status)
    artifact_id = data.get("artifact_id")
    new = data.get("new")
    return Receipt(
        status,
        artifact_id
        if isinstance(artifact_id, str) and token not in artifact_id
        else None,
        new if isinstance(new, bool) else None,
    )
