"""Command parity for the Iceberg commands `version rollback` and `version prune`.
The default SQLite catalog needs no Docker; the tests need the `iceberg` extra.
"""

from __future__ import annotations

import shutil
import sys
from typing import TYPE_CHECKING

import pytest

from tests.parity.snapshot import tree

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.backends.iceberg.commands import PruneResult, RollbackResult
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


@pytest.fixture
def two_versions(
    iceberg_catalog: Callable[[Path], None], run_cli: RunCli, valid_polygons_geojson: Path
) -> Callable[[Path], None]:
    """Build the Iceberg catalog, then add a second file, so `points` is at 1.1.0."""

    def build(root: Path) -> None:
        iceberg_catalog(root)
        shutil.copy(valid_polygons_geojson, root / "points" / "polygons.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "polygons.geojson"))
        assert_versions(root, current="1.1.0", listed=["1.0.0", "1.1.0"])

    return build


def assert_versions(root: Path, *, current: str, listed: list[str]) -> None:
    """Fail unless the backend reports ``current`` and exactly the versions ``listed``."""
    from portolan_cli.version_ops import get_current_version, list_versions

    assert get_current_version("points", catalog_root=root).version == current
    assert [v.version for v in list_versions("points", catalog_root=root)] == listed


def test_version_rollback(
    parity: Parity, two_versions: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`version rollback` makes 1.0.0 current and keeps 1.1.0 in the history."""
    from portolan_cli.backends.iceberg.commands import rollback_collection

    def rollback(root: Path) -> RollbackResult:
        result = rollback_collection("points", "1.0.0", catalog_root=root)
        assert_versions(root, current="1.0.0", listed=["1.0.0", "1.1.0"])
        return result

    parity(
        "version rollback",
        ["points", "1.0.0"],
        catalog=two_versions,
        call=rollback,
        ignore=["created"],
    )

    # The harness removes the catalog that the command wrote, so check a second run.
    command_root = tmp_path / "command-side"
    two_versions(command_root)
    run_cli("version", "rollback", "points", "1.0.0", cwd=command_root)
    assert_versions(command_root, current="1.0.0", listed=["1.0.0", "1.1.0"])


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
    parity: Parity, two_versions: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`version prune --keep 1` removes 1.0.0 and leaves 1.1.0."""
    from portolan_cli.backends.iceberg.commands import prune_collection

    def prune(root: Path) -> PruneResult:
        result = prune_collection("points", keep=1, catalog_root=root)
        assert [v.version for v in result.pruned] == ["1.0.0"]
        assert_versions(root, current="1.1.0", listed=["1.1.0"])
        return result

    parity(
        "version prune",
        ["points", "--keep", "1"],
        catalog=two_versions,
        call=prune,
        ignore=["pruned.*.created"],
    )

    command_root = tmp_path / "command-side"
    two_versions(command_root)
    run_cli("version", "prune", "points", "--keep", "1", cwd=command_root)
    assert_versions(command_root, current="1.1.0", listed=["1.1.0"])


def test_version_prune_dry_run(
    parity: Parity, two_versions: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`version prune --dry-run` reports 1.0.0 and writes nothing."""
    from portolan_cli.backends.iceberg.commands import prune_collection

    def dry_run(root: Path) -> PruneResult:
        before = tree(root)
        result = prune_collection("points", keep=1, dry_run=True, catalog_root=root)
        assert [v.version for v in result.pruned] == ["1.0.0"]
        assert tree(root) == before
        return result

    parity(
        "version prune",
        ["points", "--keep", "1", "--dry-run"],
        catalog=two_versions,
        call=dry_run,
        ignore=["pruned.*.created"],
    )

    command_root = tmp_path / "command-side"
    two_versions(command_root)
    before = tree(command_root)
    run_cli("version", "prune", "points", "--keep", "1", "--dry-run", cwd=command_root)
    assert tree(command_root) == before
