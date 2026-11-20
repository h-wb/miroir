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
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Import, ast.ImportFrom)):
                    continue
                for name in _import_names(node, package):
                    location = f"src/{relative}:{node.lineno}"
                    if _within(name, "miroir_agent") or name.split(".", 2)[:2] == [
                        "src",
                        "agent",
                    ]:
                        errors.append(f"{location}: server must not import {name}")
                    elif part in _ALLOWED and name.startswith("src."):
                        allowed = (f"src.{part}", *_ALLOWED[part])
                        if not any(_within(name, prefix) for prefix in allowed):
                            errors.append(f"{location}: {part} may not import {name}")

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
