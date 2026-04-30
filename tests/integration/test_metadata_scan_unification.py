"""Integration tests for unified metadata scanner (#345 + #384).

These tests pin the manifest-driven scanning contract introduced in
ADR-0041:

- Issue #345: collection-level assets registered in collection.json (e.g.,
  items.parquet from `add --stac-geoparquet`) must NOT be reported as MISSING
  by metadata_fresh.

- Issue #384: every status that `check` reports must be actionable by
  `check --fix` — either fixed or explained as cannot-fix. No silent skips.

- Orphans: parquet/tif files under a collection that are not registered in
  any STAC manifest are reported as ORPHANED with a register-or-delete hint.

- Genuine MISSING: an item directory containing a data file but lacking
  item.json is detected by `check` and `--fix` creates the item.json at the
  hierarchical location matching what `add` produces.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.metadata.models import MetadataStatus
from portolan_cli.metadata.scan import scan_catalog_metadata


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _write_catalog_json(catalog_dir: Path) -> None:
    """Write a minimal valid catalog.json."""
    (catalog_dir / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "test-catalog",
                "stac_version": "1.1.0",
                "description": "Test catalog",
                "links": [{"rel": "self", "href": "./catalog.json"}],
            },
            indent=2,
        )
    )


def _write_collection_json(
    collection_dir: Path,
    *,
    collection_id: str,
    extra_assets: dict | None = None,
) -> None:
    """Write a minimal valid collection.json with optional extra assets."""
    data = {
        "type": "Collection",
        "id": collection_id,
        "stac_version": "1.1.0",
        "description": f"Test collection {collection_id}",
        "license": "CC0-1.0",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [{"rel": "self", "href": "./collection.json"}],
        "assets": extra_assets or {},
    }
    (collection_dir / "collection.json").write_text(json.dumps(data, indent=2))


def _write_item_json(
    item_dir: Path,
    *,
    item_id: str,
    asset_href: str,
    media_type: str,
) -> None:
    """Write a minimal valid item.json at hierarchical location.

    Convention matches `_create_and_save_item` in dataset.py:
    {item_dir}/{item_id}.json with assets resolved relative to item_dir.
    """
    data = {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": item_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
        "bbox": [0.0, 0.0, 1.0, 1.0],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "links": [],
        "assets": {
            "data": {
                "href": asset_href,
                "type": media_type,
                "roles": ["data"],
            }
        },
    }
    (item_dir / f"{item_id}.json").write_text(json.dumps(data, indent=2))


def _make_raster_collection_with_items_parquet(
    catalog_dir: Path,
    valid_singleband_cog: Path,
) -> Path:
    """Build a raster collection with N items + items.parquet rollup at root.

    Mirrors `add --stac-geoparquet` output: per-tile item subdirs each with
    item.json + .tif, plus collection-level items.parquet asset registered
    in collection.json under key 'geoparquet-items'.

    Returns the collection directory path.
    """
    _write_catalog_json(catalog_dir)
    collection_dir = catalog_dir / "rasters"
    collection_dir.mkdir()

    # Two raster items in subdirs
    for item_id in ("scene-001", "scene-002"):
        item_dir = collection_dir / item_id
        item_dir.mkdir()
        shutil.copy(valid_singleband_cog, item_dir / f"{item_id}.tif")
        _write_item_json(
            item_dir,
            item_id=item_id,
            asset_href=f"{item_id}.tif",
            media_type="image/tiff; application=geotiff; profile=cloud-optimized",
        )

    # Collection-level items.parquet (STAC-GeoParquet rollup) registered
    # exactly as stac_parquet.add_parquet_link_to_collection writes it.
    items_parquet = collection_dir / "items.parquet"
    items_parquet.write_bytes(b"PAR1")  # placeholder bytes — scanner only checks existence
    _write_collection_json(
        collection_dir,
        collection_id="rasters",
        extra_assets={
            "geoparquet-items": {
                "href": "./items.parquet",
                "type": "application/vnd.apache.parquet",
                "title": "STAC items as GeoParquet",
                "roles": ["stac-items"],
            }
        },
    )
    return collection_dir


# =============================================================================
# Issue #345: collection-level items.parquet must not trip metadata_fresh
# =============================================================================


@pytest.mark.integration
class TestIssue345CollectionLevelAssetsNotMissing:
    """Bug #345: items.parquet at collection root flagged as MISSING."""

    def test_scanner_does_not_flag_registered_items_parquet_as_missing(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """Manifest-driven scanner: items.parquet is a registered collection
        asset, not an item-needing-JSON, so MISSING count must be 0."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _make_raster_collection_with_items_parquet(catalog_dir, valid_singleband_cog)

        report = scan_catalog_metadata(catalog_dir)

        assert report.missing_count == 0, (
            f"items.parquet wrongly flagged MISSING — paths: "
            f"{[str(r.file_path) for r in report.filter_by_status(MetadataStatus.MISSING)]}"
        )

    def test_check_passes_for_collection_with_items_parquet(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """End-to-end: portolan check exits 0 for valid catalog with rollup."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        # .portolan sentinel makes catalog discoverable
        (catalog_dir / ".portolan").mkdir()
        (catalog_dir / ".portolan" / "config.yaml").write_text("version: 1\n")
        _make_raster_collection_with_items_parquet(catalog_dir, valid_singleband_cog)

        result = runner.invoke(cli, ["check", str(catalog_dir), "--metadata"])

        assert result.exit_code == 0, (
            f"check failed for valid catalog with items.parquet rollup.\noutput:\n{result.output}"
        )


# =============================================================================
# Issue #384: check ↔ --fix symmetry
# =============================================================================


@pytest.mark.integration
class TestIssue384CheckFixSymmetry:
    """Bug #384: every status check reports, --fix must address."""

    def test_missing_reported_by_check_is_actionable_by_fix(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """Item dir on disk with data but no item.json → MISSING from check
        AND --fix successfully creates the item.json."""
        from portolan_cli.metadata.fix import FixAction, fix_metadata

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        # Item dir with data but no item.json — genuine MISSING.
        item_dir = collection_dir / "scene-001"
        item_dir.mkdir()
        shutil.copy(valid_singleband_cog, item_dir / "scene-001.tif")

        report = scan_catalog_metadata(catalog_dir)
        missing = report.filter_by_status(MetadataStatus.MISSING)
        assert len(missing) >= 1, "scanner failed to detect MISSING item"

        fix_report = fix_metadata(collection_dir, report, dry_run=False)
        created = [r for r in fix_report.results if r.action == FixAction.CREATED]
        assert created, f"--fix did not create any items (got: {fix_report.to_dict()})"

        # Item.json must land at hierarchical path matching what `add` writes.
        expected_item_json = item_dir / "scene-001.json"
        assert expected_item_json.exists(), (
            f"item.json written to wrong location. Expected {expected_item_json}, "
            f"got: {list(item_dir.iterdir())}"
        )

    def test_check_and_fix_use_same_scanner(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """No status appears in check that --fix doesn't see. The shared
        scanner guarantees this by construction."""
        from portolan_cli.metadata.fix import fix_metadata

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        # Mix: one valid item + one MISSING item dir + one ORPHANED file
        valid_item_dir = collection_dir / "scene-001"
        valid_item_dir.mkdir()
        shutil.copy(valid_singleband_cog, valid_item_dir / "scene-001.tif")
        _write_item_json(
            valid_item_dir,
            item_id="scene-001",
            asset_href="scene-001.tif",
            media_type="image/tiff; application=geotiff",
        )

        missing_item_dir = collection_dir / "scene-002"
        missing_item_dir.mkdir()
        shutil.copy(valid_singleband_cog, missing_item_dir / "scene-002.tif")

        # Orphan: file at collection root not in any manifest
        (collection_dir / "stray.parquet").write_bytes(b"PAR1")

        scan_report = scan_catalog_metadata(catalog_dir)

        # Every non-FRESH result must produce a fix entry — no silent drops.
        non_fresh = [r for r in scan_report.results if r.status != MetadataStatus.FRESH]
        fix_report = fix_metadata(collection_dir, scan_report, dry_run=True)
        assert len(fix_report.results) == len(non_fresh), (
            f"Fix dropped non-fresh results. "
            f"scan non-fresh={len(non_fresh)}, fix entries={len(fix_report.results)}"
        )


# =============================================================================
# Orphan detection (#384 expected behavior #3)
# =============================================================================


@pytest.mark.integration
class TestOrphanFiles:
    """Unregistered files under a collection are reported as ORPHANED."""

    def test_orphan_parquet_at_collection_root_reported(
        self,
        tmp_path: Path,
    ) -> None:
        """Parquet at collection root not in collection.json.assets and not
        in any item.json → ORPHANED, not MISSING."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "vectors"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="vectors")

        # Stray parquet — not registered anywhere
        (collection_dir / "leftover.parquet").write_bytes(b"PAR1")

        report = scan_catalog_metadata(catalog_dir)
        orphans = report.filter_by_status(MetadataStatus.ORPHANED)
        assert len(orphans) == 1, (
            f"expected 1 ORPHANED, got {len(orphans)}. results={report.to_dict()}"
        )
        assert orphans[0].file_path.name == "leftover.parquet"
        assert orphans[0].fix_hint, "orphan must include fix_hint"

    def test_orphan_is_not_auto_fixed(
        self,
        tmp_path: Path,
    ) -> None:
        """--fix reports cannot-fix for orphans (no action)."""
        from portolan_cli.metadata.fix import FixAction, fix_metadata

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "vectors"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="vectors")
        (collection_dir / "stray.parquet").write_bytes(b"PAR1")

        report = scan_catalog_metadata(catalog_dir)
        fix_report = fix_metadata(collection_dir, report, dry_run=False)

        # Orphan must produce a fix entry (no silent drop) but no creation.
        actions = [r.action for r in fix_report.results]
        assert FixAction.CREATED not in actions, "--fix incorrectly created an item for an orphan"
        assert any(r.action == FixAction.SKIPPED for r in fix_report.results), (
            f"orphan must produce SKIPPED entry with cannot-fix message. "
            f"got: {fix_report.to_dict()}"
        )
        # File must remain untouched
        assert (collection_dir / "stray.parquet").exists()


# =============================================================================
# Vector single-file collection-level (ADR-0031): no false MISSING
# =============================================================================


@pytest.mark.integration
class TestVectorCollectionLevelAsset:
    """Vector single-file pattern from ADR-0031: no item.json expected."""

    def test_collection_level_vector_asset_not_missing(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """data.parquet registered as collection asset → not MISSING."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "boundaries"
        collection_dir.mkdir()
        shutil.copy(valid_points_parquet, collection_dir / "data.parquet")
        _write_collection_json(
            collection_dir,
            collection_id="boundaries",
            extra_assets={
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                }
            },
        )

        report = scan_catalog_metadata(catalog_dir)
        assert report.missing_count == 0, (
            f"collection-level vector asset wrongly flagged MISSING: {report.to_dict()}"
        )
        assert report.passed, f"scan should pass cleanly, got: {report.to_dict()}"
