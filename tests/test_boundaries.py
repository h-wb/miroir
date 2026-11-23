import ast
import subprocess
import sys
import unittest
from collections.abc import Iterator
from importlib.util import resolve_name
from pathlib import Path

_ALLOWED = {
    "integrations": ("src.core.models",),
    "analyses": ("src.core.models",),
    "core": ("src.integrations", "src.analyses"),
    # The flow builds the worker's object store from its environment.
    "pipelines": ("src.core", "src.adapters.s3"),
    "adapters": ("src.core", "src.pipelines"),
    "web": ("src.core", "src.integrations"),
    "cli": ("src.core", "src.integrations", "src.adapters", "src.web"),
}
# The warehouse reader (decision 040): it needs nothing of the write side, and
# only the server's transports open it; flow runs hold their own credentials.
# It names the timeline's schema as the build does (src.core.analysis).
_TIMELINE = "src.core.timeline"
_TIMELINE_IMPORTS = ("src.core.config", "src.core.operations", "src.core.analysis")
_TIMELINE_IMPORTERS = ("src.web", "src.cli")
# Media is never decoded in the web process, nor in the worker's own: one module
# decodes, and it runs as a child process that nothing imports. Only the runner
# imports the derive run that starts it, inside the run.
_IMAGE_LIBRARIES = ("PIL", "pillow_heif")
_DECODER = "src.core.thumbnails"
_DERIVE = "src.core.derive"
_DERIVE_IMPORTER = "src.core.runner"


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
                    elif module != _DECODER and any(
                        _within(name, library) for library in _IMAGE_LIBRARIES
                    ):
                        errors.append(f"{location}: only {_DECODER} imports {name}")
                    if _within(name, _DECODER) and module != _DECODER:
                        errors.append(f"{location}: {name} runs as its own process")
                    if _within(name, _DERIVE) and module not in (
                        _DERIVE,
                        _DERIVE_IMPORTER,
                    ):
                        errors.append(f"{location}: only the runner imports {name}")
            if module == _DERIVE_IMPORTER:
                for node in _outside_functions(tree):
                    if isinstance(node, (ast.Import, ast.ImportFrom)) and any(
                        _within(name, _DERIVE) for name in _import_names(node, package)
                    ):
                        errors.append(
                            f"src/{relative}:{node.lineno}: "
                            f"the runner imports {_DERIVE} inside the run only"
                        )

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

    def test_the_server_and_the_worker_load_no_image_library(self) -> None:
        # What `serve` imports, what its routes import while answering, and the
        # worker's derive run, whose decoding happens in a child process.
        modules = (
            "src.web.app",
            "src.web.connections",
            "src.web.timeline",
            "src.web.timeline_pages",
            "src.core.service",
            "src.core.runner",
            "src.core.media",
            "src.core.timeline",
            "src.adapters.prefect",
            "src.adapters.s3",
        )
        probe = (
            "import importlib, sys\n"
            f"for name in {modules!r}: importlib.import_module(name)\n"
            "server = sorted(m for m in sys.modules if m.split('.')[0] in"
            f" {_IMAGE_LIBRARIES!r} or m in ({_DECODER!r}, {_DERIVE!r}))\n"
            f"importlib.import_module({_DERIVE!r})\n"
            "worker = sorted(m for m in sys.modules if m.split('.')[0] in"
            f" {_IMAGE_LIBRARIES!r} or m == {_DECODER!r})\n"
            "print(server + worker)"
        )
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "[]")
