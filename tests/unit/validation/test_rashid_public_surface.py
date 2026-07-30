"""Every rashid name this package uses must come from rashid's public API.

rashid's version range (``>=0.1.2,<0.2.0``) lets it refactor private modules in
any patch release. An import of ``rashid.rules._common`` or ``rashid._multihash``
therefore turns a routine rashid patch into an ``ImportError`` at ``portolan
check`` startup. rashid#57 promoted the names we need to ``rashid.api``; this
test keeps the reach from growing back.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
SEARCH_ROOTS = ("portolan_cli", "tests")


def _rashid_modules(tree: ast.AST) -> list[str]:
    """Every rashid module path named by an import in ``tree``."""
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(
                alias.name for alias in node.names if alias.name.split(".")[0] == "rashid"
            )
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            if node.module.split(".")[0] == "rashid":
                modules.append(node.module)
    return modules


def _python_files() -> list[Path]:
    files: list[Path] = []
    for root in SEARCH_ROOTS:
        files.extend(sorted(p for p in (REPO_ROOT / root).rglob("*.py")))
    return files


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
            "patch release may rename or delete. Import from rashid.api instead:\n"
            + "\n".join(private)
        )

    def test_the_sweep_sees_the_imports_it_is_meant_to_grade(self) -> None:
        """A sweep that silently matched nothing would pass forever."""
        found = [module for path in _python_files() for module in _rashid_modules(_parsed(path))]
        assert "rashid.api" in found

    def test_a_private_import_is_detected(self) -> None:
        tree = ast.parse("from rashid.rules._common import links_of\nimport rashid._multihash\n")
        assert _rashid_modules(tree) == ["rashid.rules._common", "rashid._multihash"]


def _parsed(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
