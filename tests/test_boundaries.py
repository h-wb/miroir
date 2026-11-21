import ast
import unittest
from collections.abc import Iterator
from importlib.util import resolve_name
from pathlib import Path

_ALLOWED = {
    "integrations": ("src.core.models",),
    "analyses": ("src.core.models",),
    "core": ("src.integrations", "src.analyses"),
    "pipelines": ("src.core",),
    "adapters": ("src.core", "src.pipelines"),
    "web": ("src.core", "src.integrations"),
    "cli": ("src.core", "src.integrations", "src.adapters", "src.web"),
}
# The warehouse reader (decision 040): it needs nothing of the write side, and
# only the server's transports open it; flow runs hold their own credentials.
_TIMELINE = "src.core.timeline"
_TIMELINE_IMPORTS = ("src.core.config", "src.core.operations")
_TIMELINE_IMPORTERS = ("src.web", "src.cli")
# Media is never decoded in the web process: thumbnails are the worker's.
_IMAGE_LIBRARIES = ("PIL", "pillow_heif")


def _within(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(prefix + ".")


def _import_names(node: ast.AST, package: str) -> Iterator[str]:
    if isinstance(node, ast.Import):
        yield from (alias.name for alias in node.names)
    elif isinstance(node, ast.ImportFrom):
        module = resolve_name("." * node.level + (node.module or ""), package)
        # A from-import can name a submodule, e.g. `from src.core import models`.
        yield from (
            module if alias.name == "*" else f"{module}.{alias.name}"
            for alias in node.names
        )


def _outside_functions(node: ast.AST) -> Iterator[ast.AST]:
    yield node
    for child in ast.iter_child_nodes(node):
        if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield from _outside_functions(child)


class BoundaryTests(unittest.TestCase):
    def test_server_import_boundaries(self) -> None:
        src = Path(__file__).resolve().parents[1] / "src"
        errors: list[str] = []
        for folder in sorted(src.iterdir()):
            if folder.is_dir() and folder.name not in (*_ALLOWED, "__pycache__"):
                errors.append(f"src/{folder.name}:1: part has no import rule")

        for path in sorted(src.rglob("*.py")):
            relative = path.relative_to(src)
            part = relative.parts[0] if len(relative.parts) > 1 else ""
            package = ".".join(("src", *relative.parts[:-1]))
            module = ".".join(("src", *relative.with_suffix("").parts))
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Import, ast.ImportFrom)):
                    continue
                for name in _import_names(node, package):
                    location = f"src/{relative}:{node.lineno}"
                    if any(
                        _within(name, prefix)
                        for prefix in ("miroir_collector", "miroir_agent", "src.agent")
                    ):
                        errors.append(f"{location}: server must not import {name}")
                    elif part in _ALLOWED and name.startswith("src."):
                        allowed = (f"src.{part}", *_ALLOWED[part])
                        if not any(_within(name, prefix) for prefix in allowed):
                            errors.append(f"{location}: {part} may not import {name}")
                    if (
                        module == _TIMELINE
                        and name.startswith("src.")
                        and not any(_within(name, p) for p in _TIMELINE_IMPORTS)
                    ):
                        errors.append(f"{location}: timeline may not import {name}")
                    if _within(name, _TIMELINE) and not any(
                        _within(module, p) for p in (_TIMELINE, *_TIMELINE_IMPORTERS)
                    ):
                        errors.append(f"{location}: only web and cli import {name}")
                    if (part == "web" or module == _TIMELINE) and any(
                        _within(name, library) for library in _IMAGE_LIBRARIES
                    ):
                        errors.append(f"{location}: {module} may not decode images")

            if part == "integrations" and path.name in {"formats.py", "contracts.py"}:
                for node in _outside_functions(tree):
                    if isinstance(node, (ast.Import, ast.ImportFrom)) and any(
                        _within(name, "dlt") for name in _import_names(node, package)
                    ):
                        errors.append(
                            f"src/{relative}:{node.lineno}: "
                            "formats.py and contracts.py may not import dlt at module level"
                        )

        self.assertFalse(errors, "\n".join(errors))
