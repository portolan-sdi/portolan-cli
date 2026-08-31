"""Freshness lookups against the layout `portolan add` writes (issue #709).

`add` puts an item in its own directory and records its assets in versions.json
under the key ``{item_id}/{filename}``. The existing detection tests build the
flat layout, where an item JSON sits beside its data file in the collection
directory and the versions.json key is the bare file name.
``metadata/scan.py`` documents the flat layout as unsupported, so nothing
covered the key `add` really writes. The bare-name lookup therefore never
matched, and every item-level asset read as STALE.

These tests pin the hierarchical layout on both readers.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from portolan_cli.metadata.detection import get_stored_metadata
from portolan_cli.metadata.models import MetadataStatus
from portolan_cli.metadata.update import update_versions_tracking

pytestmark = pytest.mark.unit

ITEM_ID = "scene-001"
DATA_NAME = "scene-001.tif"
STORED_MTIME = 1705312200.0
STORED_SHA256 = "abc123def456789012345678901234567890123456789012345678901234abcd"


def _item_json(item_id: str, href: str) -> dict[str, Any]:
    return {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": item_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
        },
        "bbox": [0.0, 0.0, 1.0, 1.0],
        "properties": {"datetime": None},
        "links": [],
        "assets": {
            "data": {
                "href": href,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
            }
        },
        "collection": "rasters",
    }


def _versions_json(asset_key: str, **overrides: Any) -> dict[str, Any]:
    """versions.json exactly as `add` writes it for an item-level asset.

    `add` records ``mtime`` and never ``source_mtime`` for an item-level asset,
    so the reader has to accept ``mtime`` as the baseline.
    """
    asset: dict[str, Any] = {
        "sha256": STORED_SHA256,
        "size_bytes": 1024,
        "href": f"rasters/{asset_key}",
        "mtime": STORED_MTIME,
    }
    asset.update(overrides)
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2026-01-15T10:30:00Z",
                "breaking": False,
                "assets": {asset_key: asset},
                "changes": [asset_key],
            }
        ],
    }


@pytest.fixture
def hierarchical_collection(tmp_path: Path) -> tuple[Path, Path]:
    """A collection holding one item directory. Returns (collection_dir, data_file)."""
    collection_dir = tmp_path / "rasters"
    item_dir = collection_dir / ITEM_ID
    item_dir.mkdir(parents=True)

    data_file = item_dir / DATA_NAME
    data_file.write_bytes(b"II*\x00" + b"\x00" * 64)
    (item_dir / f"{ITEM_ID}.json").write_text(json.dumps(_item_json(ITEM_ID, DATA_NAME)))
    (collection_dir / "versions.json").write_text(
        json.dumps(_versions_json(f"{ITEM_ID}/{DATA_NAME}"))
    )
    return collection_dir, data_file


class TestStoredMetadataLookup:
    """`get_stored_metadata` resolves the key `add` writes."""

    def test_reads_the_item_prefixed_key(self, hierarchical_collection: tuple[Path, Path]) -> None:
        """The entry lives under ``{item_id}/{filename}``, not ``{filename}``.

        Before the fix the lookup used the bare file name and returned
        ``source_mtime=None``, which made `is_stale` answer ``new_file``.
        """
        collection_dir, data_file = hierarchical_collection

        stored = get_stored_metadata(data_file, collection_dir)

        assert stored is not None
        assert stored.source_mtime == STORED_MTIME
        assert stored.sha256 == STORED_SHA256

    def test_prefers_source_mtime_over_mtime(self, tmp_path: Path) -> None:
        """An entry `--fix` refreshed carries both fields. ``source_mtime`` wins."""
        collection_dir = tmp_path / "rasters"
        item_dir = collection_dir / ITEM_ID
        item_dir.mkdir(parents=True)
        data_file = item_dir / DATA_NAME
        data_file.write_bytes(b"II*\x00")
        (item_dir / f"{ITEM_ID}.json").write_text(json.dumps(_item_json(ITEM_ID, DATA_NAME)))
        (collection_dir / "versions.json").write_text(
            json.dumps(_versions_json(f"{ITEM_ID}/{DATA_NAME}", source_mtime=999.0))
        )

        stored = get_stored_metadata(data_file, collection_dir)

        assert stored is not None
        assert stored.source_mtime == 999.0

    def test_still_reads_a_bare_key(self, tmp_path: Path) -> None:
        """A hand-written or older versions.json may use the bare file name."""
        collection_dir = tmp_path / "rasters"
        item_dir = collection_dir / ITEM_ID
        item_dir.mkdir(parents=True)
        data_file = item_dir / DATA_NAME
        data_file.write_bytes(b"II*\x00")
        (item_dir / f"{ITEM_ID}.json").write_text(json.dumps(_item_json(ITEM_ID, DATA_NAME)))
        (collection_dir / "versions.json").write_text(json.dumps(_versions_json(DATA_NAME)))

        stored = get_stored_metadata(data_file, collection_dir)

        assert stored is not None
        assert stored.source_mtime == STORED_MTIME


class TestVersionsTrackingUpdate:
    """`update_versions_tracking` writes back to the key it read."""

    def test_refreshes_the_item_prefixed_entry(
        self, hierarchical_collection: tuple[Path, Path]
    ) -> None:
        """Before the fix the bare-name lookup raised KeyError.

        `_fix_single_file` discarded that error, so the baseline never landed and
        the next run read STALE again.
        """
        collection_dir, data_file = hierarchical_collection
        versions_path = collection_dir / "versions.json"

        update_versions_tracking(data_file, versions_path)

        assets = json.loads(versions_path.read_text())["versions"][-1]["assets"]
        entry = assets[f"{ITEM_ID}/{DATA_NAME}"]
        assert entry["source_mtime"] == pytest.approx(data_file.stat().st_mtime)
        assert entry["sha256"] == STORED_SHA256

    def test_creates_no_duplicate_bare_key(
        self, hierarchical_collection: tuple[Path, Path]
    ) -> None:
        """The refresh must not add a second entry under the bare file name."""
        collection_dir, data_file = hierarchical_collection
        versions_path = collection_dir / "versions.json"

        update_versions_tracking(data_file, versions_path)

        assets = json.loads(versions_path.read_text())["versions"][-1]["assets"]
        assert set(assets) == {f"{ITEM_ID}/{DATA_NAME}"}


class TestTouchedButIdentical:
    """A touched file with no stored heuristics must not read BREAKING."""

    def test_content_hash_clears_a_moved_mtime(
        self, tmp_path: Path, valid_points_parquet: Path
    ) -> None:
        """`git clone` resets mtimes. The stored sha256 settles the question.

        `add` stores no ``schema_fingerprint`` for an item-level asset, so
        `is_stale` cannot prove a schema break. The collection-level path already
        falls back to the content hash (#512). The item path must do the same
        instead of reporting BREAKING.
        """
        import shutil

        from portolan_cli.metadata.detection import check_file_metadata
        from portolan_cli.sync.checksums import compute_checksum

        collection_dir = tmp_path / "rasters"
        item_dir = collection_dir / ITEM_ID
        item_dir.mkdir(parents=True)
        data_file = item_dir / "scene-001.parquet"
        shutil.copy(valid_points_parquet, data_file)
        (item_dir / f"{ITEM_ID}.json").write_text(json.dumps(_item_json(ITEM_ID, data_file.name)))
        (collection_dir / "versions.json").write_text(
            json.dumps(
                _versions_json(
                    f"{ITEM_ID}/{data_file.name}",
                    sha256=compute_checksum(data_file),
                    mtime=STORED_MTIME,
                )
            )
        )

        result = check_file_metadata(data_file, collection_dir)

        assert result.status == MetadataStatus.FRESH
