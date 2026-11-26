"""The `/media/{connection}/{kind}/{key}` view route and its withheld check
(decision 040; package split, step 7).

A browser views a file through it: a redirect to a short signed link, or, with
`size`, the bytes of a thumbnail or a small image copied through the server. The
viewer calls media's view side (`src.media.links`) after its withheld check,
which asks every analysis whose views declare a `Withhold` for the file's kind
(`src.analyses.views`); no kind, role or package is named here. The collector's
upload routes stay in the control plane.
"""

import sqlalchemy as sa
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from src.analyses.views import Withhold
from src.kernel.errors import OperationError
from src.media.links import NotStreamed, read_image, read_thumbnail, view_url
from src.media.names import image_type, valid_key
from src.media.store import StorageUnavailable
from src.media.target import MediaConnection
from src.viewer.database import NOT_CONFIGURED, TimelineDatabase
from src.viewer.reader import (
    NOT_BUILT,
    SCHEMA_NAME,
    TimelineTimeout,
    TimelineUnavailable,
    session,
)
from src.viewer.settings import (
    ViewerConfiguration,
    ViewerSettings,
    WithholdingAnalysis,
)

# Thumbnails and images copied through the server (decision 040).
IMMUTABLE = "private, max-age=31536000, immutable"
NO_STORE = {"Cache-Control": "private, no-store"}


def media_withheld(
    database: TimelineDatabase | None,
    configuration: ViewerConfiguration,
    connection_id: str,
    kind: str,
    key: str,
) -> bool | None:
    """Whether a file is hidden by a row of a view that declares a withhold.

    None when no analysis reading this connection declares a withhold for this
    kind, which the connection's connector declares: there is nothing to ask.
    Otherwise the analyses that can answer are asked: those with a reader role
    whose tables are built. One answer is enough; withheld in any that answers
    is withheld; a key none knows (no row yet) is not. Fails closed: when none
    can answer, it raises as the timeline routes do, so the file is never served
    unchecked.
    """
    declared = configuration.media_kinds.get(connection_id, ())
    if not any(k.name == kind for k in declared):
        return None
    asked = [
        (analysis, withhold)
        for analysis in configuration.withholding
        if connection_id in analysis.connections
        for withhold in analysis.withholds
        if withhold.kind == kind
    ]
    if not asked:
        return None
    if database is None:
        raise TimelineUnavailable(NOT_CONFIGURED)
    readable = [(a, w) for a, w in asked if a.readable]
    for analysis, _ in readable:
        if not SCHEMA_NAME.fullmatch(analysis.schema):  # validated IDs; kept honest
            raise TimelineUnavailable("The timeline analysis has an unexpected schema")
    answered = False
    refusal: TimelineUnavailable | TimelineTimeout = TimelineUnavailable(NOT_BUILT)
    for analysis, withhold in readable:
        try:
            if _hides(database, analysis, withhold, key):
                return True
        except TimelineTimeout as error:
            refusal = error
        except TimelineUnavailable as error:
            if not isinstance(refusal, TimelineTimeout):
                refusal = error
        else:
            answered = True
    if not answered:
        raise refusal
    return False


def _hides(
    database: TimelineDatabase,
    analysis: WithholdingAnalysis,
    withhold: Withhold,
    key: str,
) -> bool:
    """Whether the row with this key hides its file; False if there is none.
    Through the lookup connection, so it never waits behind searches."""
    with session(database, lookup=True) as db:
        found = db.execute(
            sa.text(
                f'SELECT "{withhold.flag_column}" IS NOT NULL'
                f' FROM "{analysis.schema}"."{withhold.model}"'
                f' WHERE "{withhold.key_column}" = :key'
            ),
            {"key": key},
        ).scalar_one_or_none()
    return bool(found)


def _connection(
    configuration: ViewerConfiguration, connection_id: str
) -> MediaConnection:
    kinds = configuration.media_kinds.get(connection_id)
    if kinds is None:
        raise OperationError("Unknown connection")
    return MediaConnection(connection_id, kinds)


def media_routes(settings: ViewerSettings) -> APIRouter:
    router = APIRouter()

    @router.get("/media/{connection_id}/{kind}/{key:path}")
    def media_view(
        request: Request, connection_id: str, kind: str, key: str
    ) -> Response:
        """A redirect to a short signed link; with `size`, the bytes themselves.

        `size=360` or `1280` copies that thumbnail (404 if none was made);
        `size=original` copies an image original up to a size, and redirects for
        any other file. Copied bytes are cached as immutable: a key's file and its
        thumbnails do not change. The server never decodes them (decision 040).

        A file a view says is withheld is not found, at any size or as a link;
        views that cannot say refuse rather than serve it.
        """
        if not valid_key(key):  # before the warehouse sees it (NUL)
            return Response(status_code=404, headers=NO_STORE)
        configuration = settings.configuration()
        refused = withheld_refusal(configuration, connection_id, kind, key)
        if refused is not None:
            return refused
        size = request.query_params.get("size")
        if size is not None:
            return media_bytes(request, configuration, connection_id, kind, key, size)
        return media_link(configuration, connection_id, kind, key)

    def withheld_refusal(
        configuration: ViewerConfiguration, connection_id: str, kind: str, key: str
    ) -> Response | None:
        try:
            withheld = media_withheld(
                settings.database, configuration, connection_id, kind, key
            )
        except TimelineTimeout as error:
            return JSONResponse({"error": str(error)}, 504, headers=NO_STORE)
        except TimelineUnavailable as error:
            return JSONResponse({"error": str(error)}, 503, headers=NO_STORE)
        return Response(status_code=404, headers=NO_STORE) if withheld else None

    def media_link(
        configuration: ViewerConfiguration, connection_id: str, kind: str, key: str
    ) -> Response:
        try:
            url = view_url(
                configuration.media,
                settings.object_store(),
                _connection(configuration, connection_id),
                kind,
                key,
            )
        except OperationError:
            return Response(status_code=404)
        return RedirectResponse(url, status_code=302, headers=NO_STORE)

    def media_bytes(
        request: Request,
        configuration: ViewerConfiguration,
        connection_id: str,
        kind: str,
        key: str,
        size: str,
    ) -> Response:
        if len(request.query_params.multi_items()) != 1:
            return Response(status_code=400, headers=NO_STORE)
        media, store = configuration.media, settings.object_store()
        try:
            if size == "original":
                connection = _connection(configuration, connection_id)
                found = read_image(media, store, connection, kind, key)
                content_type = image_type(key)
            elif size in ("360", "1280"):
                connection = _connection(configuration, connection_id)
                found = read_thumbnail(media, store, connection, kind, key, int(size))
                content_type = "image/webp"
            else:
                return Response(status_code=400, headers=NO_STORE)
        except NotStreamed:
            return media_link(configuration, connection_id, kind, key)
        except StorageUnavailable:
            return Response(status_code=503, headers=NO_STORE)
        except OperationError:
            return Response(status_code=404, headers=NO_STORE)
        if found is None:
            return Response(status_code=404, headers=NO_STORE)
        headers = {"Cache-Control": IMMUTABLE}
        if found.etag:
            # The store's tag for these bytes, as a strong tag.
            headers["ETag"] = '"' + found.etag.strip('"').replace('"', "") + '"'
            if headers["ETag"] in {
                tag.strip().removeprefix("W/")
                for tag in request.headers.get("if-none-match", "").split(",")
            }:
                return Response(status_code=304, headers=headers)
        return Response(found.body, media_type=content_type, headers=headers)

    return router
