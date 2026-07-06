"""Unit tests for ``_remove_from_versions`` asset-key matching.

Regression coverage for finding A / issue #589: item-level assets are keyed
``{item_id}/{filename}`` in versions.json, but ``_remove_from_versions`` used to
match only the bare filename (and its ``.parquet`` variant), leaving phantom
entries behind after ``portolan rm``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from portolan_cli.remove import _remove_from_versions
from portolan_cli.versions import (
    SPEC_VERSION,
    Asset,
    Version,
    VersionsFile,
    read_versions,
    write_versions,
)


def _catalog_with_asset(tmp_path: Path, asset_key: str) -> tuple[Path, Path]:
    """Build a managed catalog whose one collection tracks a single asset.

    Returns the collection directory and its versions.json path.
    """
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("{}\n", encoding="utf-8")

    coll_dir = tmp_path / "mycoll"
    coll_dir.mkdir()

    asset = Asset(sha256="abc", size_bytes=1, href=f"mycoll/{asset_key}")
    version = Version(
        version="1.0.0",
        created=datetime.now(timezone.utc),
        breaking=False,
        assets={asset_key: asset},
        changes=[asset_key],
    )
    versions_file = VersionsFile(
        spec_version=SPEC_VERSION,
        current_version="1.0.0",
        versions=[version],
    )
    versions_path = coll_dir / "versions.json"
    write_versions(versions_path, versions_file)
    return coll_dir, versions_path


@pytest.mark.unit
def test_remove_from_versions_drops_item_scoped_key(tmp_path: Path) -> None:
    """rm of an item-level asset removes its ``{item_id}/{filename}`` key.

    Pre-fix, the matcher only checked ``data.parquet`` (bare) so the tracked
    ``data/data.parquet`` key survived and the assertion below would fail.
    """
    coll_dir, versions_path = _catalog_with_asset(tmp_path, "data/data.parquet")

    # Physical file whose stem "data" maps to item_id "data" -> "data/data.parquet".
    file_path = coll_dir / "data.parquet"

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert "data/data.parquet" not in latest.assets


@pytest.mark.unit
def test_remove_from_versions_does_not_over_match_other_item(tmp_path: Path) -> None:
    """A different item's asset with the same filename is left untouched.

    Guards against matching by bare filename component: removing ``data.parquet``
    must not touch ``other/data.parquet`` belonging to a different item.
    """
    _coll_dir, versions_path = _catalog_with_asset(tmp_path, "other/data.parquet")

    file_path = _coll_dir / "data.parquet"  # item_id "data", not "other"

    _remove_from_versions(file_path, versions_path)

    latest = read_versions(versions_path).versions[-1]
    assert "other/data.parquet" in latest.assets
