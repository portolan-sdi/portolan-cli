"""Every rashid name this package uses must come from rashid's public API."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
LIBRARY_ROOTS = ("portolan_cli", "packages/portolan-core/src")
SEARCH_ROOTS = (*LIBRARY_ROOTS, "tests")

# Library code imports rashid only from rashid_core.api. Ruff banned-api cannot
# ban every submodule and still allow rashid_core.api, so this test is the gate.
LIBRARY_RASHID_MODULES = frozenset({"rashid_core.api"})


def _rashid_modules(tree: ast.AST) -> list[str]:
    """Every rashid module path named by an import in ``tree``."""
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(
                alias.name for alias in node.names if alias.name.split(".")[0] == "rashid_core"
            )
        elif (
            isinstance(node, ast.ImportFrom)
            and node.level == 0
            and node.module
            and node.module.split(".")[0] == "rashid_core"
        ):
            modules.append(node.module)
    return modules


def _python_files(roots: tuple[str, ...] = SEARCH_ROOTS) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        files.extend(sorted(p for p in (REPO_ROOT / root).rglob("*.py")))
    return files


def _outside_public_api(modules: list[str]) -> list[str]:
    """The rashid modules a library file must not import from."""
    return [module for module in modules if module not in LIBRARY_RASHID_MODULES]


class TestRashidImportsArePublic:
    def test_no_import_reaches_a_private_rashid_module(self) -> None:
        private: list[str] = []
        for path in _python_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            private.extend(
                f"{path.relative_to(REPO_ROOT)}: {module}"
                for module in _rashid_modules(tree)
                if any(segment.startswith("_") for segment in module.split("."))
            )
        assert private == [], (
            "These imports reach into rashid's private modules, which a rashid "
            "patch release may rename or delete. Import from rashid_core.api instead:\n"
            + "\n".join(private)
        )

    def test_library_imports_rashid_only_from_its_public_api(self) -> None:
        """A public submodule such as rashid_core.model can still move in a patch."""
        outside = [
            f"{path.relative_to(REPO_ROOT)}: {module}"
            for path in _python_files(LIBRARY_ROOTS)
            for module in _outside_public_api(_rashid_modules(_parsed(path)))
        ]
        assert outside == [], (
            "Library code imports rashid only from rashid_core.api:\n" + "\n".join(outside)
        )

    def test_a_public_submodule_import_is_detected(self) -> None:
        tree = ast.parse(
            "from rashid_core.model import Severity\nimport rashid_core.catalog\n"
            "from rashid_core.api import Report\nfrom rashid_core import validate\n"
        )
        assert _outside_public_api(_rashid_modules(tree)) == [
            "rashid_core.model",
            "rashid_core.catalog",
            "rashid_core",
        ]

    def test_the_sweep_sees_the_imports_it_is_meant_to_grade(self) -> None:
        """A sweep that silently matched nothing would pass forever."""
        found = [module for path in _python_files() for module in _rashid_modules(_parsed(path))]
        assert "rashid_core.api" in found

    def test_a_private_import_is_detected(self) -> None:
        tree = ast.parse(
            "from rashid_core.rules._common import links_of\nimport rashid_core._multihash\n"
        )
        assert _rashid_modules(tree) == ["rashid_core.rules._common", "rashid_core._multihash"]


def _parsed(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
