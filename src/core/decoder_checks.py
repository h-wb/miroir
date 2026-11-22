"""One tiny image per decoder the worker may use (decision 040).

A fresh decoder child must make thumbnails of the image of every decoder its
kinds use before it gets a file, so a Pillow that lost a decoder (a build
without libjpeg, a broken library) stops the run instead of marking every file
of that type unreadable. Made with Pillow once, kept as data: nothing here
decodes. `tests/test_thumbnails.py` checks there is one for every decoder in
`src.core.media.IMAGE_DECODERS`."""

import base64

_IMAGES = {
    "BMP": (
        "Qk32AAAAAAAAADYAAAAoAAAACAAAAAgAAAABABgAAAAAAMAAAADEDgAAxA4AAAAAAAAAAAAA"
        "7+/v7+/v7+/v7+/v7+/v7+/v7+/v7+/v0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQsLCwsLCw"
        "sLCwsLCwsLCwsLCwsLCwsLCwkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQcHBwcHBwcHBwcHBw"
        "cHBwcHBwcHBwcHBwUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQLy8vLy8vLy8vLy8vLy8vLy8v"
        "Ly8vLy8vEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ"
    ),
    "GIF": (
        "R0lGODdhCAAIAIIAAO/v79DQ0LCwsJCQkHBwcFBQUC8vLxAQECwAAAAACAAIAAAIJwAPCBwo"
        "0IDBgwYLKFyokIDDhw4HSJwoUYDFixYDaNyoEYDHjx4DAgA7"
    ),
    "JPEG": (
        "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDABALDA4MChAODQ4SERATGCgaGBYWGDEjJR0oOjM9"
        "PDkzODdASFxOQERXRTc4UG1RV19iZ2hnPk1xeXBkeFxlZ2P/2wBDARESEhgVGC8aGi9jQjhC"
        "Y2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2NjY2P/wAAR"
        "CAAIAAgDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAA"
        "AgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkK"
        "FhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWG"
        "h4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl"
        "5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREA"
        "AgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYk"
        "NOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOE"
        "hYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk"
        "5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwA/4Q7/AKZ/pRRRQB//2Q=="
    ),
    "PNG": (
        "iVBORw0KGgoAAAANSUhEUgAAAAgAAAAICAIAAABLbSncAAAAJklEQVR4nGMUEBBgwAZY5OXl"
        "sUsoKipil1BQUKC1BOP79++xSgAAaiQFWLgo/mwAAAAASUVORK5CYII="
    ),
    "TIFF": (
        "SUkqAAgAAAAKAAABBAABAAAACAAAAAEBBAABAAAACAAAAAIBAwADAAAAhgAAAAMBAwABAAAA"
        "AQAAAAYBAwABAAAAAgAAABEBBAABAAAAjAAAABUBAwABAAAAAwAAABYBBAABAAAACAAAABcB"
        "BAABAAAAwAAAABwBAwABAAAAAQAAAAAAAAAIAAgACAAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ"
        "EBAvLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy9QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBwcHBw"
        "cHBwcHBwcHBwcHBwcHBwcHBwcHCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCwsLCwsLCwsLCw"
        "sLCwsLCwsLCwsLCwsLDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDQ0NDv7+/v7+/v7+/v7+/v7+/v"
        "7+/v7+/v7+8="
    ),
    "WEBP": (
        "UklGRjIAAABXRUJQVlA4ICYAAACQAQCdASoIAAgAAUAmJaQAAuaanMAA/vp4P1aqx+Bb+X+c"
        "4AAAAA=="
    ),
}


def check_image(decoder: str) -> bytes | None:
    """An 8 x 8 image only `decoder` reads, or None if there is none."""
    encoded = _IMAGES.get(decoder)
    return None if encoded is None else base64.b64decode(encoded)
