import ast
import subprocess
import sys
import unittest
from collections.abc import Iterator
from importlib.util import resolve_name
from pathlib import Path

# The kernel is the shared vocabulary (package split, step 3): the standard
# library and pydantic, and no other part. Connectors and analyses import it in
# place of the control plane, which they never import.
_KERNEL = "src.kernel"
_KERNEL_LIBRARIES = ("pydantic", "pydantic_core", "annotated_types")
_MEDIA = "src.media"
_VIEWER = "src.viewer"
_ALLOWED = {
    "kernel": (),
    # A connector declares its media kinds, and knows nothing else of media.
    "integrations": (_KERNEL, f"{_MEDIA}.kinds"),
    "analyses": (_KERNEL,),
    # Files that rows point at (package split, steps 4 and 5): the kernel only,
    # never the control plane, web, cli, the viewer or the connector registry.
    # The control plane hands it what it reads (src.media.target).
    "media": (_KERNEL,),
    "control": (_KERNEL, "src.integrations", "src.analyses", _MEDIA),
    "flows": (_KERNEL, "src.control", _MEDIA),
    "adapters": (_KERNEL, "src.control", "src.flows"),
    # The control plane's admin part, mounted into the shell; never the viewer.
    "web": (_KERNEL, "src.control", "src.integrations", "src.analyses", _MEDIA),
    # One HTTP surface (package split, step 8): login, headers, static files,
    # the base page and the navigation; both web and the viewer mount it.
    "web.shell": (_KERNEL,),
    # The composition roots: `serve` composes the shell, the admin part and the
    # viewer, and reads the analyses' view declarations for the viewer's settings.
    "cli": (
        _KERNEL,
        "src.control",
        "src.integrations",
        "src.analyses",
        "src.adapters",
        "src.web",
        _MEDIA,
        _VIEWER,
    ),
    # Reading the warehouse and showing it (package split, steps 6 and 7): the
    # kernel, media and the analyses' view declarations, never the control
    # plane, web, the connectors or the loading stack. Its settings arrive as one
    # value (src.viewer.settings).
    "viewer": (_KERNEL, _MEDIA, "src.analyses.views"),
}
# Libraries a part never imports, and those only one module may import.
_NEVER = {
    "media": ("fastapi", "starlette", "prefect"),
    "viewer": ("dlt", "prefect"),
}
_ONLY = {
    # Media is never decoded in the web process, nor in the worker's own: one
    # module decodes, and it runs as a child process that nothing imports.
    "PIL": f"{_MEDIA}.decoder",
    "pillow_heif": f"{_MEDIA}.decoder",
    "boto3": f"{_MEDIA}.s3",
    "botocore": f"{_MEDIA}.s3",
}
_IMAGE_LIBRARIES = ("PIL", "pillow_heif")
_DECODER = f"{_MEDIA}.decoder"
# Only the runner imports the derive run that starts the decoder, inside the run.
_DERIVE = f"{_MEDIA}.derive"
_DERIVE_IMPORTER = "src.control.runner"


def _within(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(prefix + ".")


def _import_names(node: ast.AST, package: str) -> Iterator[str]:
    if isinstance(node, ast.Import):
        yield from (alias.name for alias in node.names)
    elif isinstance(node, ast.ImportFrom):
        module = resolve_name("." * node.level + (node.module or ""), package)
        # A from-import can name a submodule, e.g. `from src.kernel import models`.
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
            if relative.parts[:2] == ("web", "shell"):
                part = "web.shell"
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
                    elif (
                        part == "kernel"
                        and not name.startswith("src.")
                        and name.split(".")[0] not in sys.stdlib_module_names
                        and not any(_within(name, lib) for lib in _KERNEL_LIBRARIES)
                    ):
                        errors.append(f"{location}: kernel may not import {name}")
                    elif part in _ALLOWED and name.startswith("src."):
                        allowed = (f"src.{part}", *_ALLOWED[part])
                        if not any(_within(name, prefix) for prefix in allowed):
                            errors.append(f"{location}: {part} may not import {name}")
                    if any(_within(name, lib) for lib in _NEVER.get(part, ())):
                        errors.append(f"{location}: {part} may not import {name}")
                    for library, owner in _ONLY.items():
                        if _within(name, library) and module != owner:
                            errors.append(f"{location}: only {owner} imports {name}")
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
            "src.cli.compose",
            "src.web.shell",
            "src.web.app",
            "src.web.connections",
            "src.viewer.timeline",
            "src.viewer.timeline_pages",
            "src.viewer.conversations",
            "src.viewer.media",
            "src.control.service",
            "src.control.runner",
            "src.viewer.reader",
            "src.adapters.prefect",
            "src.media.backfill",
            "src.media.config",
            "src.media.kinds",
            "src.media.links",
            "src.media.names",
            "src.media.profiles",
            "src.media.queue",
            "src.media.s3",
            "src.media.store",
            "src.media.target",
            "src.media.uploads",
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

    def test_the_viewer_loads_no_loading_stack_scheduler_or_media_library(self) -> None:
        # Importing every viewer module, in a fresh interpreter, loads none of
        # what the control plane and media's write side need.
        src = Path(__file__).resolve().parents[1] / "src"
        modules = sorted(
            ".".join(("src", *p.relative_to(src).with_suffix("").parts)).removesuffix(
                ".__init__"
            )
            for p in (src / "viewer").rglob("*.py")
        )
        self.assertIn(f"{_VIEWER}.reader", modules)
        libraries = ("dlt", "prefect", "PIL", "pillow_heif", "boto3", "botocore")
        probe = (
            "import importlib, sys\n"
            f"for name in {modules!r}: importlib.import_module(name)\n"
            "print(sorted({m.split('.')[0] for m in sys.modules} &"
            f" set({libraries!r})))"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=src.parent,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "[]")
