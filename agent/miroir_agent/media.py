"""Upload media in bounded groups."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from .collectors.base import MediaItem
from .errors import Failure, Step
from .transport import PUT_WORKERS, Client, Upload

_GROUP_SIZE = 25


def _put_group(client: Client, pending: list[tuple[MediaItem, Upload]]) -> None:
    with ThreadPoolExecutor(max_workers=PUT_WORKERS) as pool:
        puts = [
            pool.submit(client.put, item.path, item.size, link.url, link.content_type)
            for item, link in pending
        ]
        for put in puts:
            put.result()


def upload(
    client: Client,
    kind: str,
    items: list[MediaItem],
    counts: dict[str, int],
    *,
    on_step: Callable[[Step], None] | None = None,
) -> None:
    for offset in range(0, len(items), _GROUP_SIZE):
        group = items[offset : offset + _GROUP_SIZE]
        if on_step is not None:
            on_step("links")
        results = client.upload_links(kind, [(item.key, item.size) for item in group])
        pending: list[tuple[MediaItem, Upload]] = []
        for item, link in zip(group, results, strict=True):
            if isinstance(link, Upload):
                pending.append((item, link))
            else:
                counts[link.status] += 1
        if not pending:
            continue
        if on_step is not None:
            on_step("put")
        _put_group(client, pending)
        if on_step is not None:
            on_step("confirm")
        confirmed = client.confirm(kind, [(item.key, item.size) for item, _ in pending])
        stored = confirmed.count("stored")
        counts["uploaded"] += stored
        if stored != len(confirmed):
            raise Failure("confirm", "not_stored")
