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


def _published(result: VersionBumpResult) -> VersionBumpResult:
    """Fail unless the bump published a version, so the parity covers that path."""
    assert result.published is not None, result
    assert result.modified == ["points.geojson"]
    return result


def test_version_bump(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`version bump` publishes a version for an asset that changed on disk."""

    def modified(root: Path) -> None:
        # Edit the source asset and its STAC checksum, as a publisher does by hand.
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

    parity(
        "version bump",
        ["points", "1.1.0", "--yes", "-m", "Edit the source"],
        catalog=modified,
        call=lambda root: _published(
            bump_collection_version("points", "1.1.0", notes="Edit the source", catalog_root=root)
        ),
        ignore=["created"],
    )


def test_version_bump_no_changes(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`version bump` on an unchanged collection creates no version."""
    parity(
        "version bump",
        ["points", "1.1.0", "--yes"],
        catalog=points_catalog,
        call=lambda root: bump_collection_version("points", "1.1.0", catalog_root=root),
    )
