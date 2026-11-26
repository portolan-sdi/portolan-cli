"""Command parity for `scan`: the command against `run_scan`."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.scan.workflow import ScanOutcome, run_scan
from portolan_core.errors import PathNotFoundError

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def raw_catalog(
    build_conformant_catalog: Callable[..., Path], valid_points_geojson: Path
) -> Callable[[Path], None]:
    """Build a catalog with untracked GeoJSON files.

    `points/my points.geojson` has a space in its name, so `--fix` renames it.
    `flood/flood_rp10.geojson` and `flood/flood_rp50.geojson` share a
    return-period marker, so `--suggest-collections` groups them.
    """

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "my points.geojson")
        (root / "flood").mkdir()
        for name in ("flood_rp10.geojson", "flood_rp50.geojson"):
            shutil.copy(valid_points_geojson, root / "flood" / name)

    return build


def _names(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir())


def _scan_command(
    run_cli: RunCli, build: Callable[[Path], None], root: Path, *args: str
) -> dict[str, Any]:
    """Run `portolan scan <args> --json` on a fresh catalog at `root` and return `data`."""
    build(root)
    result = run_cli("scan", *args, "--json", cwd=root)
    data: dict[str, Any] = json.loads(result.stdout)["data"]
    return data


def test_scan(parity: Parity, raw_catalog: Callable[[Path], None]) -> None:
    """`scan` reports the files, the issues, and the fix commands."""
    parity("scan", [], catalog=raw_catalog, call=lambda root: run_scan(Path()))


def test_scan_fix_with_suggestions(
    parity: Parity, raw_catalog: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`scan --fix --suggest-collections` renames the file and suggests a collection."""
    outcomes: list[ScanOutcome] = []

    def call(root: Path) -> ScanOutcome:
        outcomes.append(run_scan(Path(), fix=True, suggest_collections=True))
        return outcomes[-1]

    parity("scan", [".", "--fix", "--suggest-collections"], catalog=raw_catalog, call=call)

    (outcome,) = outcomes
    suggestions = [s.to_dict() for s in outcome.result.collection_suggestions]
    # The suggestion holds str(Path), as the command output did before #1133.
    flood = [str(Path("flood") / name) for name in ("flood_rp10.geojson", "flood_rp50.geojson")]
    # The return-period marker and the shared prefix each suggest the group.
    assert [(s["suggested_name"], sorted(s["files"])) for s in suggestions] == [
        ("flood", flood),
        ("flood_rp", flood),
    ]
    assert [f.preview for f in outcome.result.applied_fixes] == [
        "Rename: my points.geojson → my-points.geojson"
    ]
    assert _names(parity.root / "points") == ["my-points.geojson"]

    command_root = tmp_path / "command-side"
    data = _scan_command(run_cli, raw_catalog, command_root, "--fix", "--suggest-collections")
    assert data["applied_fixes"] == [f.to_dict() for f in outcome.result.applied_fixes]
    assert _names(command_root / "points") == ["my-points.geojson"]


def test_scan_fix_dry_run(
    parity: Parity, raw_catalog: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`scan --fix --dry-run` proposes the rename and applies nothing."""
    parity(
        "scan",
        ["--fix", "--dry-run"],
        catalog=raw_catalog,
        call=lambda root: run_scan(Path(), fix=True, dry_run=True),
    )
    assert _names(parity.root / "points") == ["my points.geojson"]

    command_root = tmp_path / "command-side"
    data = _scan_command(run_cli, raw_catalog, command_root, "--fix", "--dry-run")
    assert [f["preview"] for f in data["proposed_fixes"]] == [
        "Rename: my points.geojson → my-points.geojson"
    ]
    assert _names(command_root / "points") == ["my points.geojson"]


def test_scan_of_a_file_fails(parity: Parity, raw_catalog: Callable[[Path], None]) -> None:
    """`scan <file>` fails with the error that `run_scan` raises."""
    target = Path("points") / "my points.geojson"
    parity.error(
        "scan",
        [str(target)],
        catalog=raw_catalog,
        call=lambda root: run_scan(target),
        raises=NotADirectoryError,
    )


def test_scan_of_a_missing_path_fails(parity: Parity, raw_catalog: Callable[[Path], None]) -> None:
    """`scan <missing>` fails with the error that `run_scan` raises."""
    target = Path("no-such-dir")
    parity.error(
        "scan",
        [str(target)],
        catalog=raw_catalog,
        call=lambda root: run_scan(target),
        raises=PathNotFoundError,
    )
