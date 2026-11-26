"""Command parity for the Iceberg plugin commands `version rollback` and `version prune`.

The catalog uses the Iceberg backend with its default SQLite catalog, so no
Docker service is needed. The tests need the `iceberg` extra.
"""

from __future__ import annotations

import shutil
import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.backends.iceberg.commands import PruneResult
    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="PyIceberg SQL catalog warehouse paths break on Windows"
        " (https://github.com/apache/iceberg-python/issues/1005)",
    ),
]


@pytest.fixture
def iceberg_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Path], None]:
    """Build an Iceberg-backed catalog with one collection, `points`, at version 1.0.0."""
    pytest.importorskip("pyiceberg")
    # Hide the developer's ~/.pyiceberg.yaml, as tests/iceberg/conftest.py does.
    home = tmp_path / "pyiceberg-home"
    home.mkdir()
    (home / ".pyiceberg.yaml").write_text("portolan-test-isolation: true\n", encoding="utf-8")
    monkeypatch.setenv("PYICEBERG_HOME", str(home))

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        run_cli("config", "set", "backend", "iceberg", cwd=root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


def test_version_rollback(parity: Parity, iceberg_catalog: Callable[[Path], None]) -> None:
    """`version rollback` reports the restored version. Each build has its own timestamp."""
    from portolan_cli.backends.iceberg.commands import rollback_collection

    parity(
        "version rollback",
        ["points", "1.0.0"],
        catalog=iceberg_catalog,
        call=lambda root: rollback_collection("points", "1.0.0", catalog_root=root),
        ignore=["created"],
    )


def test_version_rollback_unknown_version(
    parity: Parity, iceberg_catalog: Callable[[Path], None]
) -> None:
    """A version the collection does not hold fails both sides with the same error."""
    from portolan_cli.backends.iceberg.commands import rollback_collection

    parity.error(
        "version rollback",
        ["points", "9.9.9"],
        catalog=iceberg_catalog,
        call=lambda root: rollback_collection("points", "9.9.9", catalog_root=root),
        raises=ValueError,
    )


def test_version_prune(
    parity: Parity,
    iceberg_catalog: Callable[[Path], None],
    run_cli: RunCli,
    valid_polygons_geojson: Path,
) -> None:
    """`version prune --dry-run` reports the old version it would remove."""
    from portolan_cli.backends.iceberg.commands import prune_collection

    def two_versions(root: Path) -> None:
        iceberg_catalog(root)
        shutil.copy(valid_polygons_geojson, root / "points" / "polygons.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "polygons.geojson"))

    def pruned_one(root: Path) -> PruneResult:
        result = prune_collection("points", keep=1, dry_run=True, catalog_root=root)
        assert [v.version for v in result.pruned] == ["1.0.0"], result
        return result

    parity(
        "version prune",
        ["points", "--keep", "1", "--dry-run"],
        catalog=two_versions,
        call=pruned_one,
        ignore=["pruned.*.created"],
    )
