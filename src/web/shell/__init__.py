"""The web shell (package split, step 8): one HTTP surface that the control
plane's admin pages and the viewer both mount, so neither imports the other.

Single-user and self-hosted: one admin bearer token (`MIROIR_API_TOKEN`).
Browsers log in with it once and get a session cookie derived from it; the token
itself is never stored in the cookie. The shell owns that check, `/login` and
`/logout`, the security headers with a content security policy per mounted
part, `/static/` over its own folder (and each part's that has no prefix of
its own; a part's prefix lets a gateway route its files by path), the template
loader over its own folder (`base.html`, `login.html`, `error.html`) and each
part's, and the header's navigation, to which each part adds its links. A part
adds its routes to `Shell.app` or hands them to `Shell.mount`; the composition
root (`src.cli`) decides which parts a process mounts. It imports only the kernel.
"""

import hashlib
import hmac
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import Environment, FileSystemLoader, select_autoescape

SESSION_COOKIE = "miroir_session"
STATIC_URL = "/static"
TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
# The policy of every response a part does not widen. Pages load styles,
# scripts and images from /static only; no address reaches another host;
# "no-referrer" would also make browsers post our own forms with
# "Origin: null", which the login check cannot tell from another site.
POLICY: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("default-src", ("'self'",)),
    ("img-src", ("'self'",)),
    ("object-src", ("'none'",)),
    ("base-uri", ("'none'",)),
    ("form-action", ("'self'",)),
    ("frame-ancestors", ("'none'",)),
)


def content_security_policy(extra: Mapping[str, Sequence[str]] | None = None) -> str:
    """The shell's policy, with a part's sources added to its directives."""
    extra = extra or {}
    unknown = set(extra) - {name for name, _ in POLICY}
    if unknown:
        raise ValueError(f"A part may only widen the shell's directives: {unknown}")
    return "; ".join(
        " ".join((name, *sources, *extra.get(name, ()))) for name, sources in POLICY
    )


# On every response; the policy is the shell's unless a mounted part answers
# the path and widens it (`Part.policy`).
SECURITY_HEADERS = {
    "Content-Security-Policy": content_security_policy(),
    "Referrer-Policy": "same-origin",
    "X-Content-Type-Options": "nosniff",
}


@dataclass(frozen=True)
class Link:
    """A link in the header of every page but the login."""

    href: str
    label: str


def _no_links() -> Sequence[Link]:
    return ()


def _never(request: Request) -> bool:
    return False


@dataclass(frozen=True)
class Part:
    """What a part brings to the shell besides its routes."""

    # Its templates, found after the shell's and earlier parts'; they extend
    # `base.html` by name.
    templates: Path | None = None
    # Its static files, served under `static_url`: `/static/` beside the shell's,
    # or a prefix below it of its own, such as `/static/viewer`.
    static: Path | None = None
    static_url: str = STATIC_URL
    # The paths it answers, for its policy: "/timeline" is it and what is below.
    paths: tuple[str, ...] = ()
    # Sources it adds to the shell's directives, on its own paths only.
    policy: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    # Its header links, asked each time a page with a header is drawn.
    links: Callable[[], Sequence[Link]] = _no_links
    # Requests that pass without the login, because they check a token of their own.
    public: Callable[[Request], bool] = _never
    # Fixed messages a redirect names by `?notice=`: nothing is reflected.
    notices: Mapping[str, str] = field(default_factory=dict)


class Unauthorized(Exception):
    pass


class NoQueryInAccessLog(logging.Filter):
    """uvicorn's access lines name the path with its query string (client,
    method, path, version, status). The query is dropped, on every path: an
    address may carry what was looked for (decision 040)."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str):
            record.args = (*args[:2], args[2].split("?", 1)[0], *args[3:])
        return True


def quiet_access_log() -> None:
    """Install the filter on uvicorn's access logger, once. Logger filters
    survive uvicorn's own logging configuration, which replaces handlers."""
    logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, NoQueryInAccessLog) for f in logger.filters):
        logger.addFilter(NoQueryInAccessLog())


def _answers(prefix: str, path: str) -> bool:
    return path == prefix or path.startswith(prefix.rstrip("/") + "/")


class Shell:
    """The HTTP surface parts mount into: build it, mount each part, then
    `finish` it into the application to serve."""

    def __init__(self, token: str, home: str = "/") -> None:
        """`home` is where a login lands: the page of this process to start on."""
        if len(token) < 16:
            raise ValueError("MIROIR_API_TOKEN must be at least 16 characters")
        self._token = token
        self._home = home
        self._session = hmac.new(
            token.encode(), b"miroir-session", hashlib.sha256
        ).hexdigest()
        self._parts: list[Part] = []
        # Each part that widens the policy: its paths and its policy.
        self._policies: list[tuple[tuple[str, ...], str]] = []
        self._notices: dict[str, str] = {}
        self._static: list[Path] = [STATIC_DIR]
        # Parts' folders served under a prefix of their own: (prefix, folder).
        self._prefixed: list[tuple[str, Path]] = []
        self._loader = FileSystemLoader([str(TEMPLATES_DIR)])
        self.templates = Jinja2Templates(
            env=Environment(loader=self._loader, autoescape=select_autoescape())
        )
        # Drawn lazily by base.html, so a page without a header asks no part.
        self.templates.env.globals["navigation"] = self.navigation
        self._finished = False
        quiet_access_log()
        self.app = FastAPI(title="Miroir", docs_url=None, redoc_url=None)
        self._routes()

    def accepts(self, token: str) -> bool:
        """Whether this is the admin token."""
        return hmac.compare_digest(token.encode(), self._token.encode())

    def authorize(self, request: Request) -> None:
        """The admin's bearer token, or the session cookie on a same-origin request."""
        header = request.headers.get("authorization", "")
        if header.lower().startswith("bearer "):
            if self.accepts(header[7:].strip()):
                return
            raise Unauthorized
        cookie = request.cookies.get(SESSION_COOKIE, "")
        if not hmac.compare_digest(cookie.encode(), self._session.encode()):
            raise Unauthorized
        # Cookies are SameSite=Strict; also refuse cross-origin form posts outright.
        origin = request.headers.get("origin")
        if request.method == "GET" or not origin:
            return
        if origin == "null":
            # A browser hides the origin of a same-origin post when the page asked
            # for no referrer. Its own Sec-Fetch-Site header still tells them apart.
            if request.headers.get("sec-fetch-site") != "same-origin":
                raise Unauthorized
        elif urlsplit(origin).netloc != request.url.netloc:
            raise Unauthorized

    def mount(self, part: Part, routers: Sequence[APIRouter] = ()) -> None:
        """Add a part: its folders, links, policy and public paths, and the
        routers it hands over, in their order."""
        if self._finished:
            raise RuntimeError("The shell is finished: mount every part before")
        if part.policy and not part.paths:
            raise ValueError("A part that widens the policy names the paths it answers")
        if any(prefix.rstrip("/") == "" for prefix in part.paths):
            raise ValueError("A part cannot widen the policy of every page")
        url = part.static_url.rstrip("/")
        if url != STATIC_URL and not url.startswith(STATIC_URL + "/"):
            raise ValueError(f"A part's static files are served below {STATIC_URL}/")
        if url in (prefix for prefix, _ in self._prefixed):
            raise ValueError(f"Two parts serve their static files at {url}")
        self._parts.append(part)
        if part.policy:
            self._policies.append((part.paths, content_security_policy(part.policy)))
        self._notices.update(part.notices)
        if part.templates is not None:
            self._loader.searchpath.append(str(part.templates))
        if part.static is not None and url == STATIC_URL:
            self._static.append(part.static)
        elif part.static is not None:
            self._prefixed.append((url, part.static))
        for router in routers:
            self.app.include_router(router)

    def navigation(self) -> list[Link]:
        """The header's links: each part's, in the order they were mounted."""
        return [link for part in self._parts for link in part.links()]

    def page(self, request: Request, name: str, **context: Any) -> HTMLResponse:
        context["notice"] = self._notices.get(request.query_params.get("notice", ""))
        return self.templates.TemplateResponse(request, name, context)

    def policy(self, path: str) -> str:
        """The content security policy of a path: that of the part that answers
        it, or the shell's."""
        for paths, policy in self._policies:
            if any(_answers(prefix, path) for prefix in paths):
                return policy
        return SECURITY_HEADERS["Content-Security-Policy"]

    def finish(self) -> FastAPI:
        """The application: the login check and the headers wrap every part's
        own middleware, and `/static/` serves every part's folder, each under
        its prefix."""
        if self._finished:
            raise RuntimeError("The shell is already finished")
        self._finished = True
        app = self.app

        @app.middleware("http")
        async def require_login(
            request: Request, call_next: Callable[[Request], Awaitable[Response]]
        ) -> Response:
            # The login page needs the stylesheet; nothing under /static is private.
            path = request.url.path
            if path == "/login" or path.startswith("/static/"):
                return await call_next(request)
            if any(part.public(request) for part in self._parts):
                return await call_next(request)
            try:
                self.authorize(request)
            except Unauthorized:
                if path.startswith("/api/"):
                    return JSONResponse({"error": "unauthorized"}, status_code=401)
                if request.headers.get("hx-request") == "true":
                    # htmx would swap the login page into the page; it loads it instead.
                    return Response(status_code=401, headers={"HX-Redirect": "/login"})
                return RedirectResponse("/login", status_code=303)
            return await call_next(request)

        @app.middleware("http")
        async def security_headers(
            request: Request, call_next: Callable[[Request], Awaitable[Response]]
        ) -> Response:
            response = await call_next(request)
            response.headers.update(SECURITY_HEADERS)
            response.headers["Content-Security-Policy"] = self.policy(request.url.path)
            if request.url.path.startswith("/static/"):
                # Names carry no version: revalidate (ETag) so a deploy shows at once.
                response.headers["Cache-Control"] = "no-cache"
            return response

        # A prefix of a part's own first: the first mount that matches answers.
        for url, folder in self._prefixed:
            app.mount(url, StaticFiles(directory=folder))
        static = StaticFiles(directory=STATIC_DIR)
        # A name is looked for in the shell's folder, then in each part's.
        static.all_directories = list(self._static)
        app.mount(STATIC_URL, static, name="static")
        return app

    def _routes(self) -> None:
        app = self.app

        @app.get("/login", response_class=HTMLResponse)
        def login_form(request: Request) -> HTMLResponse:
            return self.page(request, "login.html", failed=False)

        @app.post("/login")
        def login(request: Request, token_input: str = Form(alias="token")) -> Response:
            if not self.accepts(token_input):
                return self.templates.TemplateResponse(
                    request,
                    "login.html",
                    {"failed": True, "notice": None},
                    status_code=401,
                )
            response = RedirectResponse(self._home, status_code=303)
            response.set_cookie(
                SESSION_COOKIE,
                self._session,
                httponly=True,
                samesite="strict",
                secure=request.url.scheme == "https",
            )
            return response

        @app.post("/logout")
        def logout() -> Response:
            response = RedirectResponse("/login", status_code=303)
            response.delete_cookie(SESSION_COOKIE)
            # Screenshots stay in the browser's cache for a year otherwise, and htmx
            # keeps the last address (a search's words) in session storage.
            response.headers["Clear-Site-Data"] = '"cache", "storage"'
            return response
