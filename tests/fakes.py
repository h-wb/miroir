from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from urllib.parse import quote, unquote, urlsplit

from src.core.media import ListedObject, Listing, ObjectTooLarge, StoredObject


class FakeObjectStore:
    def __init__(self):
        self.objects: dict[str, int] = {}
        self.bodies: dict[str, tuple[bytes, str]] = {}
        self.modified: dict[str, datetime] = {}
        self.gets: list[str] = []
        # Some S3-compatible stores return short pages while more remain.
        self.page_size = 1000

    def add(
        self,
        name: str,
        body: bytes,
        content_type: str = "application/octet-stream",
        modified: datetime | None = None,
    ) -> None:
        """An object stored by a device or a copy, not by the code under test."""
        self.bodies[name] = (body, content_type)
        self.objects[name] = len(body)
        self.modified[name] = modified or datetime.now(timezone.utc)

    def list_objects(self, prefix: str, after: str, limit: int) -> Listing:
        names = sorted(n for n in self.objects if n.startswith(prefix) and n > after)
        page = min(limit, 1000, self.page_size)
        return Listing(
            [
                ListedObject(
                    n,
                    self.objects[n],
                    self.modified.get(n, datetime(2026, 1, 1, tzinfo=timezone.utc)),
                )
                for n in names[:page]
            ],
            len(names) > page,
        )

    def head(self, name: str) -> int | None:
        return self.objects.get(name)

    def get(self, name: str, max_bytes: int) -> StoredObject | None:
        self.gets.append(name)
        if name not in self.bodies:
            return None
        body, content_type = self.bodies[name]
        if len(body) > max_bytes:
            raise ObjectTooLarge(f"Stored object exceeds {max_bytes} bytes")
        return StoredObject(body, content_type, f'"{len(body)}"')

    def put(self, name: str, body: bytes, content_type: str) -> None:
        self.bodies[name] = (body, content_type)
        self.objects[name] = len(body)

    def presign_put(self, name: str, content_type: str, expires: int) -> str:
        return (
            f"https://objects.test/{name}?put=1&type={content_type}&expires={expires}"
        )

    def presign_get(self, name: str, expires: int) -> str:
        return f"https://objects.test/{name}?get=1&expires={expires}"


class ServedObjectStore(FakeObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.content_types: dict[str, str] = {}
        self.put_count = 0

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_PUT(self) -> None:
                name = unquote(urlsplit(self.path).path.removeprefix("/"))
                store.objects[name] = len(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                store.content_types[name] = self.headers["Content-Type"]
                store.put_count += 1
                self.send_response(200)
                self.end_headers()

        store = self
        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def presign_put(self, name: str, content_type: str, expires: int) -> str:
        return f"http://127.0.0.1:{self._server.server_port}/{quote(name)}?put=1"

    def close(self) -> None:
        self._server.shutdown()
        self._thread.join()
        self._server.server_close()
