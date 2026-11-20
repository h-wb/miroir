from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from urllib.parse import quote, unquote, urlsplit


class FakeObjectStore:
    def __init__(self):
        self.objects: dict[str, int] = {}

    def head(self, name: str) -> int | None:
        return self.objects.get(name)

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
