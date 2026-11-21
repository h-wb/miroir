import ast
import sys
import tomllib
import unittest
from collections.abc import Iterator
from importlib.util import resolve_name
from pathlib import Path

_COLLECTOR = Path(__file__).resolve().parents[1]
_SOURCES = {"sources.screenmemory", "sources.zen_history"}
# CLI constructs State for runner calls and handles a status read failure.
_ALLOWED = {
    "": set(),
    "__main__": {"cli"},
    **dict.fromkeys(("errors", "models", "dates", "files"), set()),
    "masking": {"scanner"},
    "scanner": {"errors"},
    "batch": {"errors", "files"},
    "transport": {"errors", "models", ""},
    "sources.base": {"errors", "models", "dates", "masking"},
    **dict.fromkeys(_SOURCES, {"sources.base", "batch", "masking", "errors"}),
    "sources": {"sources.base", *_SOURCES},
    "state": {"errors", "models", "files"},
    "media": {"transport", "sources.base", "errors"},
    "config": {"sources", "sources.base", "transport", "errors", "models"},
    "cli": {
        "runner",
        "config",
        "errors",
        "sources.base",
        "dates",
        "",
        "state",
        "masking",
    },
}
_ALLOWED["runner"] = set(_ALLOWED) - {"cli", "__main__", *_SOURCES}
_EXTERNAL = {
    "pydantic": {
        "models",
        "sources",
        "sources.base",
        *_SOURCES,
        "config",
        "state",
        "transport",
    },
    "urllib3": {"transport"},
    "kingfisher_sdk": {"scanner"},
}


def _modules(package: Path) -> dict[str, Path]:
    result = {}
    for path in package.rglob("*.py"):
        parts = path.relative_to(package).with_suffix("").parts
        result[".".join(parts[:-1] if parts[-1] == "__init__" else parts)] = path
    return result


def _imports(
    node: ast.AST, package: str, modules: set[str]
) -> Iterator[tuple[str, str]]:
    if isinstance(node, ast.Import):
        yield from ((alias.name, "") for alias in node.names)
    elif isinstance(node, ast.ImportFrom):
        name = resolve_name("." * node.level + (node.module or ""), package)
        for alias in node.names:
            candidate = f"{name}.{alias.name}"
            if candidate in modules:
                yield candidate, ""
            else:
                yield name, alias.name


def _violation(part: str, source: str, symbol: str) -> str | None:
    root, _, suffix = source.partition(".")
    if symbol.startswith("_") and not symbol.startswith("__"):
        return f"private import {source}.{symbol}"
    if root == "miroir_collector":
        if suffix not in _ALLOWED[part]:
            return f"{part} may not import {source}"
    elif root in _EXTERNAL:
        if part not in _EXTERNAL[root]:
            return f"{part} may not import {source}"
    elif root not in sys.stdlib_module_names:
        return f"unexpected dependency {source}"
    return None


class BoundaryTests(unittest.TestCase):
    def test_import_boundaries(self) -> None:
        modules = _modules(_COLLECTOR / "miroir_collector")
        self.assertEqual(modules.keys(), _ALLOWED.keys())
        full_names = {f"miroir_collector.{part}" for part in modules if part}
        errors = []
        for part, path in modules.items():
            parent = part if path.stem == "__init__" else part.rpartition(".")[0]
            package = ".".join(filter(None, ("miroir_collector", parent)))
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                for source, symbol in _imports(node, package, full_names):
                    if error := _violation(part, source, symbol):
                        errors.append(
                            f"{path.relative_to(_COLLECTOR)}:{node.lineno}: {error}"
                        )
        self.assertFalse(errors, "\n".join(errors))

    def test_declared_dependencies(self) -> None:
        project = tomllib.loads(
            (_COLLECTOR / "pyproject.toml").read_text(encoding="utf-8")
        )["project"]
        self.assertCountEqual(
            project["dependencies"],
            [
                "urllib3>=2.2,<3",
                "pydantic>=2.10,<3",
                "kingfisher-secret-scanner==1.6.4",
            ],
        )

    def test_only_scanner_and_rules_are_imported_from_kingfisher(self) -> None:
        path = _COLLECTOR / "miroir_collector" / "scanner.py"
        imports = {
            (source, symbol)
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
            for source, symbol in _imports(node, "miroir_collector", set())
            if source.split(".")[0] == "kingfisher_sdk"
        }
        self.assertEqual(
            imports, {("kingfisher_sdk", "Rules"), ("kingfisher_sdk", "Scanner")}
        )
