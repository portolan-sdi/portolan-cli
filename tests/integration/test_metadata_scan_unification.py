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


# Helpers shared by F1/F3/F4 tests below.


def _write_versions_json_for(
    collection_dir: Path,
    *,
    asset_filename: str,
    file_path: Path,
) -> None:
    """Write a versions.json recording `file_path` as a tracked asset.

    Captures source_mtime, sha256, feature_count, and schema_fingerprint
    from the file's *current* state so a subsequent scanner call returns
    FRESH. Tests then mutate the file (and bump mtime) to force STALE,
    avoiding the BREAKING-on-None-fingerprint trap that bit me earlier.
    """
    import hashlib

    from portolan_cli.metadata.detection import (
        compute_schema_fingerprint,
        get_current_metadata,
    )

    sha = hashlib.sha256(file_path.read_bytes()).hexdigest()
    current = get_current_metadata(file_path)
    versions = {
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "assets": {
                    asset_filename: {
                        "source_mtime": file_path.stat().st_mtime,
                        "sha256": sha,
                        "feature_count": current.current_feature_count,
                        "schema_fingerprint": compute_schema_fingerprint(file_path),
                    }
                },
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions, indent=2))


def _bump_mtime(path: Path, delta: float = 60.0) -> None:
    """Force mtime forward to defeat any equality fast-path."""
    import os

    new_mtime = path.stat().st_mtime + delta
    os.utime(path, (new_mtime, new_mtime))


# =============================================================================
# F1: collection-level registered assets get FRESH/STALE/BREAKING checks
# =============================================================================


@pytest.mark.integration
class TestCollectionLevelFreshness:
    """ADR-0041 / #350: collection-level data assets must be freshness-checked.

    Before this fix, the scanner registered collection-level assets but never
    called `check_file_metadata` on them, so STALE/BREAKING were silently
    undetectable for the exact layout #350 mandates.
    """

    def test_collection_level_vector_asset_reports_fresh(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """data.parquet registered + tracked in versions.json → FRESH."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "boundaries"
        collection_dir.mkdir()
        data_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_path)
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
        _write_versions_json_for(
            collection_dir,
            asset_filename="data.parquet",
            file_path=data_path,
        )

        report = scan_catalog_metadata(catalog_dir)

        fresh = report.filter_by_status(MetadataStatus.FRESH)
        assert len(fresh) == 1, (
            f"collection-level data.parquet should produce one FRESH result, "
            f"got: {report.to_dict()}"
        )
        assert fresh[0].file_path.name == "data.parquet"

    def test_collection_level_vector_asset_reports_stale_on_change(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Replacing data.parquet with a row-different file after the
        versions.json snapshot must surface as STALE.

        Regression test for the F1 blind spot (prior code never called any
        freshness check on collection-level assets) AND for the bbox-None
        heuristic guard: a touch-only mtime bump should NOT be STALE; only
        a real content/schema change should. We exercise the latter here
        by writing a parquet file with a different feature count.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "boundaries"
        collection_dir.mkdir()
        data_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_path)
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
        _write_versions_json_for(
            collection_dir,
            asset_filename="data.parquet",
            file_path=data_path,
        )

        # Replace the file with a different-shaped parquet to force a real
        # content delta (different feature count) past the mtime fast-path.
        new_table = pa.table({"id": list(range(50)), "value": list(range(50))})
        pq.write_table(new_table, data_path)
        _bump_mtime(data_path)

        report = scan_catalog_metadata(catalog_dir)

        stale = report.filter_by_status(MetadataStatus.STALE)
        breaking = report.filter_by_status(MetadataStatus.BREAKING)
        non_fresh = stale + breaking
        assert any(r.file_path.name == "data.parquet" for r in non_fresh), (
            f"collection-level asset mutation must surface as STALE or "
            f"BREAKING, got: {report.to_dict()}"
        )

    def test_collection_level_asset_touch_is_not_stale(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Bumping mtime alone (no content change) must NOT mark the
        collection asset stale. Tests the heuristics_changed guard for
        the both-bboxes-None case."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "boundaries"
        collection_dir.mkdir()
        data_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, data_path)
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
        _write_versions_json_for(
            collection_dir,
            asset_filename="data.parquet",
            file_path=data_path,
        )

        _bump_mtime(data_path)  # touch only — feature count + schema unchanged

        report = scan_catalog_metadata(catalog_dir)

        non_fresh_for_asset = [
            r
            for r in report.results
            if r.file_path.name == "data.parquet" and r.status != MetadataStatus.FRESH
        ]
        assert not non_fresh_for_asset, (
            f"touch-only mtime bump must not produce STALE/BREAKING. report={report.to_dict()}"
        )


# =============================================================================
# F3: legacy flat layout is ORPHANED, not silently freshness-checked
# =============================================================================


@pytest.mark.integration
class TestLegacyFlatLayoutIsOrphaned:
    """ADR-0041: a single layout (hierarchical). Flat sibling JSON is no
    longer treated as a valid item by the scanner — the data file is
    reported as ORPHANED so users migrate via `portolan add`.
    """

    def test_flat_sibling_json_data_file_reported_orphaned(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "vectors"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="vectors")

        # Legacy flat: data + sibling item.json at collection root, with NO
        # entry in collection.json.assets.
        data_path = collection_dir / "things.parquet"
        shutil.copy(valid_points_parquet, data_path)
        (collection_dir / "things.json").write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "id": "things",
                    "geometry": None,
                    "bbox": [0.0, 0.0, 1.0, 1.0],
                    "properties": {"datetime": "2024-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {
                        "data": {
                            "href": "./things.parquet",
                            "type": "application/vnd.apache.parquet",
                            "roles": ["data"],
                        }
                    },
                }
            )
        )

        report = scan_catalog_metadata(catalog_dir)

        orphans = report.filter_by_status(MetadataStatus.ORPHANED)
        assert any(o.file_path.name == "things.parquet" for o in orphans), (
            f"flat-layout data file must be ORPHANED, not silently passed via "
            f"sibling JSON fallback. report={report.to_dict()}"
        )
        # And no FRESH/STALE for the flat data file.
        for r in report.results:
            if r.file_path.name == "things.parquet":
                assert r.status == MetadataStatus.ORPHANED


# =============================================================================
# F4: nested catalogs (ADR-0032 Pattern 1 + Pattern 2)
# =============================================================================


@pytest.mark.integration
class TestNestedCatalogPatterns:
    """ADR-0032 nested catalog shapes must be walked correctly.

    Pattern 1: catalog → sub-catalog → collection → item.
    Pattern 2: collection → year-subcatalog → item (collection still owns
               versions.json + data context).
    """

    def test_pattern1_subcatalog_with_collection(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """Sub-catalog containing a collection: items inside are scanned."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)

        sub_cat = catalog_dir / "geo"
        sub_cat.mkdir()
        (sub_cat / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "geo",
                    "stac_version": "1.1.0",
                    "description": "geo sub-catalog",
                    "links": [{"rel": "self", "href": "./catalog.json"}],
                }
            )
        )
        collection_dir = sub_cat / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        item_dir = collection_dir / "scene-001"
        item_dir.mkdir()
        shutil.copy(valid_singleband_cog, item_dir / "scene-001.tif")
        _write_item_json(
            item_dir,
            item_id="scene-001",
            asset_href="scene-001.tif",
            media_type="image/tiff; application=geotiff",
        )

        report = scan_catalog_metadata(catalog_dir)
        # The item is NOT tracked in versions.json so it's MISSING-stored ⇒
        # scanner reports STALE (no stored mtime). The point is: it WAS
        # found at all, proving pattern-1 traversal works.
        scanned = [r.file_path.name for r in report.results]
        assert "scene-001.tif" in scanned, (
            f"pattern-1 sub-catalog item not reached by scanner. results={scanned}"
        )

    def test_pattern2_subcatalog_inside_collection_resolves_versions_at_collection(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """Pattern 2: items live under a sub-catalog *within* a collection.
        versions.json sits at the collection root; the scanner must resolve
        item assets against the collection, not the sub-catalog.
        """
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        # Year sub-catalog inside the collection.
        year_sub = collection_dir / "2024"
        year_sub.mkdir()
        (year_sub / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "2024",
                    "stac_version": "1.1.0",
                    "description": "year subcatalog",
                    "links": [{"rel": "self", "href": "./catalog.json"}],
                }
            )
        )
        item_dir = year_sub / "scene-001"
        item_dir.mkdir()
        item_data = item_dir / "scene-001.tif"
        shutil.copy(valid_singleband_cog, item_data)
        _write_item_json(
            item_dir,
            item_id="scene-001",
            asset_href="scene-001.tif",
            media_type="image/tiff; application=geotiff",
        )

        # versions.json at the COLLECTION root, keyed by basename.
        _write_versions_json_for(
            collection_dir,
            asset_filename="scene-001.tif",
            file_path=item_data,
        )

        report = scan_catalog_metadata(catalog_dir)

        # Asset found and resolved against collection_dir → FRESH.
        fresh = report.filter_by_status(MetadataStatus.FRESH)
        assert any(r.file_path.name == "scene-001.tif" for r in fresh), (
            f"Pattern 2 item not resolved against collection-level "
            f"versions.json. report={report.to_dict()}"
        )

    def test_pattern2_stale_is_fixable(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """STALE on a Pattern-2 nested item must be fixable: --fix walks
        ancestors to find the collection and updates the right versions.json.
        """
        from portolan_cli.metadata.fix import FixAction, fix_metadata

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        year_sub = collection_dir / "2024"
        year_sub.mkdir()
        (year_sub / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "2024",
                    "stac_version": "1.1.0",
                    "description": "year subcatalog",
                    "links": [{"rel": "self", "href": "./catalog.json"}],
                }
            )
        )
        item_dir = year_sub / "scene-001"
        item_dir.mkdir()
        item_data = item_dir / "scene-001.tif"
        shutil.copy(valid_singleband_cog, item_data)
        _write_item_json(
            item_dir,
            item_id="scene-001",
            asset_href="scene-001.tif",
            media_type="image/tiff; application=geotiff",
        )
        _write_versions_json_for(
            collection_dir,
            asset_filename="scene-001.tif",
            file_path=item_data,
        )
        # Force STALE.
        _bump_mtime(item_data)

        report = scan_catalog_metadata(catalog_dir)
        stale = report.filter_by_status(MetadataStatus.STALE)
        assert any(r.file_path.name == "scene-001.tif" for r in stale)

        # Fix called with catalog root as `directory` — must walk ancestors
        # to find collection_dir.
        fix_report = fix_metadata(catalog_dir, report, dry_run=False)
        updated = [r for r in fix_report.results if r.action == FixAction.UPDATED]
        assert any(r.file_path.name == "scene-001.tif" for r in updated), (
            f"Pattern-2 STALE not updated by --fix. fix_report={fix_report.to_dict()}"
        )


# =============================================================================
# F5: stray subdir without {dir_name}.{ext} → ORPHANED, not MISSING
# =============================================================================


@pytest.mark.integration
class TestStraySubdirIsOrphaned:
    """A non-item subdir (no `{name}.{ext}` data file) must not be coerced
    into being treated as an item-needing-JSON. Its files are ORPHANED.
    """

    def test_random_subdir_files_are_orphaned_not_missing(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "vectors"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="vectors")

        # User stashed exports under scratch/ — there is NO scratch.parquet.
        scratch = collection_dir / "scratch"
        scratch.mkdir()
        shutil.copy(valid_points_parquet, scratch / "export-a.parquet")
        shutil.copy(valid_points_parquet, scratch / "export-b.parquet")

        report = scan_catalog_metadata(catalog_dir)

        # Must NOT report MISSING (would imply --fix should create scratch.json).
        assert report.missing_count == 0, (
            f"stray subdir wrongly emitted MISSING — would create "
            f"scratch.json incorrectly. report={report.to_dict()}"
        )
        orphan_names = {o.file_path.name for o in report.filter_by_status(MetadataStatus.ORPHANED)}
        assert {"export-a.parquet", "export-b.parquet"} <= orphan_names, (
            f"stray files must be ORPHANED, got: {orphan_names}. full report={report.to_dict()}"
        )

    def test_real_item_subdir_with_matching_data_still_missing(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """Sanity check: legitimate item dir (`scene-001/scene-001.tif`)
        without item.json still emits MISSING. The F5 heuristic must not
        regress the genuine MISSING shape #384 covers.
        """
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        item_dir = collection_dir / "scene-001"
        item_dir.mkdir()
        shutil.copy(valid_singleband_cog, item_dir / "scene-001.tif")

        report = scan_catalog_metadata(catalog_dir)
        assert report.missing_count == 1, (
            f"genuine item-shaped dir without item.json must emit MISSING. "
            f"report={report.to_dict()}"
        )


# =============================================================================
# CLI: catalog-root resolution for `check --metadata --fix`
# =============================================================================


@pytest.mark.integration
class TestCheckResolvesCatalogRoot:
    """Reviewer concern: `check --metadata --fix` passed `path` straight to
    the scanner, so running from a subdir without catalog.json silently
    produced an empty report. The fix workflow must resolve the catalog
    root via the existing find_catalog_root sentinel walk and fail
    explicitly when no catalog can be found.
    """

    def _make_catalog(self, root: Path, valid_singleband_cog: Path) -> Path:
        root.mkdir()
        (root / ".portolan").mkdir()
        (root / ".portolan" / "config.yaml").write_text("version: 1\n")
        _write_catalog_json(root)
        collection_dir = root / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")
        item_dir = collection_dir / "scene-001"
        item_dir.mkdir()
        shutil.copy(valid_singleband_cog, item_dir / "scene-001.tif")
        return collection_dir

    def test_fix_resolves_root_from_collection_subdir(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        """Running `check --metadata --fix` from a collection subdir must
        find the catalog root and act on it (creating the missing item.json),
        not silently skip with an empty report.
        """
        catalog_dir = tmp_path / "catalog"
        collection_dir = self._make_catalog(catalog_dir, valid_singleband_cog)

        result = runner.invoke(
            cli,
            ["check", str(collection_dir), "--metadata", "--fix", "--json"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        metadata_fix = payload.get("data", {}).get("metadata_fix")
        assert metadata_fix is not None and metadata_fix["total_count"] >= 1, (
            f"running --fix from a subdir must reach the catalog root and "
            f"act on its items. got: {payload}"
        )
        # The fix should land at the hierarchical item.json location.
        assert (collection_dir / "scene-001" / "scene-001.json").exists()

    def test_fix_fails_explicitly_outside_any_catalog(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """If no catalog root is found, `--fix` must fail loudly rather
        than reporting an empty success — matches `find_catalog_root`'s
        git-style contract used elsewhere in the CLI.
        """
        non_catalog = tmp_path / "scratch"
        non_catalog.mkdir()

        result = runner.invoke(
            cli,
            ["check", str(non_catalog), "--metadata", "--fix"],
        )
        assert result.exit_code != 0, (
            f"--fix outside a catalog must fail; got exit=0 output={result.output}"
        )


# =============================================================================
# scan_catalog_metadata library contract: missing catalog.json is an error,
# not a vacuous success. Vacuous-pass would let library callers treat any
# random directory as a fresh catalog.
# =============================================================================


@pytest.mark.integration
class TestScannerRejectsMissingCatalog:
    def test_scan_catalog_metadata_raises_when_catalog_json_missing(
        self,
        tmp_path: Path,
    ) -> None:
        non_catalog = tmp_path / "no-catalog"
        non_catalog.mkdir()

        with pytest.raises(FileNotFoundError):
            scan_catalog_metadata(non_catalog)


# =============================================================================
# Vector-format orphan detection (ADR-0014 accepts non-cloud-native formats).
# Stray .gpkg/.shp/.geojson at collection root must surface as ORPHANED so
# users see them, mirroring the .pmtiles contract (orphan-checked but not
# freshness-checked since no extractor exists for those formats).
# =============================================================================


@pytest.mark.integration
class TestVectorFormatOrphans:
    @pytest.mark.parametrize("ext", [".gpkg", ".shp", ".geojson", ".fgb"])
    def test_unregistered_vector_format_at_collection_root_is_orphan(
        self,
        tmp_path: Path,
        ext: str,
    ) -> None:
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "vectors"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="vectors")

        stray = collection_dir / f"leftover{ext}"
        stray.write_bytes(b"\0\0\0\0")

        report = scan_catalog_metadata(catalog_dir)
        orphan_names = {o.file_path.name for o in report.filter_by_status(MetadataStatus.ORPHANED)}
        assert stray.name in orphan_names, (
            f"{ext} stray must be ORPHANED, got: {orphan_names}. full={report.to_dict()}"
        )
