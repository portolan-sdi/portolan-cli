"""Plugins reach Portolan through the public API only (#947).

If a built-in plugin needs a private module, an outside plugin cannot do the
same work. The fix is to widen ``portolan_cli.api``, not to import past it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[3]

PUBLIC_MODULES = frozenset({"portolan_cli.api", "portolan_cli.hookspecs", "portolan_cli.testing"})

# Each plugin package: its source directory and its own import name.
PLUGIN_PACKAGES = {
    "example": (_REPO / "tests/plugins/portolan_example/src/portolan_example", "portolan_example"),
    "iceberg": (_REPO / "portolan_cli/backends/iceberg", "portolan_cli.backends.iceberg"),
}


def _private_imports(source: Path, own_package: str) -> list[str]:
    found: list[str] = []
    for path in sorted(source.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules = [node.module]
                if node.module == "portolan_cli":
                    modules = [f"portolan_cli.{alias.name}" for alias in node.names]
            else:
                continue
            for module in modules:
                if module.startswith(own_package):
                    continue
                if module.split(".")[0] == "portolan_cli" and module not in PUBLIC_MODULES:
                    found.append(f"{path.relative_to(source)}:{node.lineno} imports {module}")
    return found


@pytest.mark.parametrize("name", sorted(PLUGIN_PACKAGES))
def test_plugin_imports_only_the_public_api(name: str) -> None:
    source, own_package = PLUGIN_PACKAGES[name]

    assert source.is_dir()
    assert _private_imports(source, own_package) == []


def test_the_check_finds_a_private_import(tmp_path: Path) -> None:
    """The scan reports each private form: `import x.y`, `from x.y import`, `from x import y`."""
    (tmp_path / "bad.py").write_text(
        "import portolan_cli.versions\n"
        "from portolan_cli.sync.upload import upload_file\n"
        "from portolan_cli import output, api\n"
        "from portolan_cli.api import hookimpl\n",
        encoding="utf-8",
    )

    found = [line.split(" imports ")[1] for line in _private_imports(tmp_path, "bad")]

    assert found == ["portolan_cli.versions", "portolan_cli.sync.upload", "portolan_cli.output"]
