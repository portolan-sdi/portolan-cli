"""Command parity for `version current`, `version list`, and `version bump`."""

from __future__ import annotations

import hashlib
import json
import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.version_ops import (
    VersionBumpResult,
    bump_collection_version,
    describe_current_version,
    describe_version_history,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, at version 1.0.0."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


def test_version_current(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`version current` reports the latest version. Each build has its own timestamp."""
    parity(
        "version current",
        ["points"],
        catalog=points_catalog,
        call=lambda root: describe_current_version("points", catalog_root=root),
        ignore=["created"],
    )


def test_version_current_missing_collection(
    parity: Parity, points_catalog: Callable[[Path], None]
) -> None:
    """A collection with no versions.json fails both sides with the same error."""
    parity.error(
        "version current",
        ["missing"],
        catalog=points_catalog,
        call=lambda root: describe_current_version("missing", catalog_root=root),
        raises=FileNotFoundError,
    )


def test_version_list(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`version list` reports each version of the collection."""
    parity(
        "version list",
        ["points"],
        catalog=points_catalog,
        call=lambda root: describe_version_history("points", catalog_root=root),
        ignore=["versions.*.created"],
    )


def tree(root: Path) -> dict[str, bytes]:
    """Return the bytes of each file below ``root``, by relative path."""
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def assert_bumped(root: Path) -> None:
    """Fail unless versions.json makes 1.1.0 current, with the edited asset and its checksum."""
    versions = json.loads((root / "points" / "versions.json").read_text())
    source = (root / "points" / "points.geojson").read_bytes()
    latest = versions["versions"][-1]
    assert versions["current_version"] == "1.1.0"
    assert [v["version"] for v in versions["versions"]] == ["1.0.0", "1.1.0"]
    assert (latest["message"], latest["breaking"]) == ("Edit the source", False)
    assert latest["changes"] == ["points.geojson"]
    assert latest["assets"]["points.geojson"]["sha256"] == hashlib.sha256(source).hexdigest()


@pytest.fixture
def modified_catalog(points_catalog: Callable[[Path], None]) -> Callable[[Path], None]:
    """Build the `points` catalog, then edit the source asset and its STAC checksum."""

    def build(root: Path) -> None:
        points_catalog(root)
        source = root / "points" / "points.geojson"
        source.write_bytes(source.read_bytes() + b"\n")
        collection_path = root / "points" / "collection.json"
        collection = json.loads(collection_path.read_text())
        for asset in collection["assets"].values():
            if asset["href"].endswith("points.geojson"):
                asset["file:size"] = source.stat().st_size
                asset["file:checksum"] = "1220" + hashlib.sha256(source.read_bytes()).hexdigest()
        collection_path.write_text(json.dumps(collection, indent=2))

    return build


def test_version_bump(
    parity: Parity, modified_catalog: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`version bump` publishes a version for an asset that changed on disk."""

    def library_bump(root: Path) -> VersionBumpResult:
        result = bump_collection_version(
            "points", "1.1.0", notes="Edit the source", catalog_root=root
        )
        assert result.modified == ["points.geojson"]
        assert_bumped(root)
        return result

    parity(
        "version bump",
        ["points", "1.1.0", "--yes", "-m", "Edit the source"],
        catalog=modified_catalog,
        call=library_bump,
        ignore=["created"],
    )

    # The harness removes the catalog that the command wrote, so check a second run.
    command_root = tmp_path / "command-side"
    modified_catalog(command_root)
    run_cli(
        "version", "bump", "points", "1.1.0", "--yes", "-m", "Edit the source", cwd=command_root
    )
    assert_bumped(command_root)


def test_version_bump_no_changes(
    parity: Parity, points_catalog: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`version bump` on an unchanged collection creates no version and writes nothing."""

    def library_bump(root: Path) -> VersionBumpResult:
        before = tree(root)
        result = bump_collection_version("points", "1.1.0", catalog_root=root)
        assert tree(root) == before
        return result

    parity(
        "version bump",
        ["points", "1.1.0", "--yes"],
        catalog=points_catalog,
        call=library_bump,
    )

    command_root = tmp_path / "command-side"
    points_catalog(command_root)
    before = tree(command_root)
    run_cli("version", "bump", "points", "1.1.0", "--yes", cwd=command_root)
    assert tree(command_root) == before
