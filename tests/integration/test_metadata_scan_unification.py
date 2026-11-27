"""Integration tests for the manifest-driven metadata scanner and its ``check --fix`` contract.

Registered assets are not MISSING, unregistered files are ORPHANED, and --fix acts on each status.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.metadata.models import MetadataStatus
from portolan_cli.metadata.scan import scan_catalog_metadata
from portolan_core.checksums import file_fields

if TYPE_CHECKING:
    from pathlib import Path


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
                "title": "Test Catalog",
                "description": "Test catalog",
                "links": [
                    {"rel": "self", "href": "./catalog.json"},
                    {"rel": "agents", "href": "./AGENTS.md", "type": "text/markdown"},
                ],
            },
            indent=2,
        )
    )
    # AGENTS.md required at catalog root (RULE-0080)
    (catalog_dir / "AGENTS.md").write_text("# AGENTS.md — Test Catalog\n")


def _write_collection_json(
    collection_dir: Path,
    *,
    collection_id: str,
    extra_assets: dict[str, Any] | None = None,
) -> None:
    """Write a minimal valid collection.json with optional extra assets."""
    data = {
        "type": "Collection",
        "id": collection_id,
        "stac_version": "1.1.0",
        "title": f"Test collection {collection_id}",
        "description": f"Test collection {collection_id}",
        "license": "CC0-1.0",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [
            {"rel": "self", "href": "./collection.json"},
            {"rel": "agents", "href": "./AGENTS.md", "type": "text/markdown"},
        ],
        "assets": extra_assets or {},
    }
    (collection_dir / "collection.json").write_text(json.dumps(data, indent=2))
    # AGENTS.md required at collection level (RULE-0081)
    (collection_dir / "AGENTS.md").write_text(f"# AGENTS.md — {collection_id}\n")


def _write_item_json(
    item_dir: Path,
    *,
    item_id: str,
    asset_href: str,
    media_type: str,
) -> None:
    """Write ``{item_dir}/{item_id}.json`` as `add` does, with assets relative to item_dir."""
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


def _write_catalog_with_data_asset(
    catalog_dir: Path,
    source: Path,
    *,
    collection_id: str,
    filename: str,
) -> tuple[Path, Path]:
    """Write a catalog with one collection that registers a copy of SOURCE.

    Return the collection directory and the path of the data file.
    """
    catalog_dir.mkdir()
    _write_catalog_json(catalog_dir)
    collection_dir = catalog_dir / collection_id
    collection_dir.mkdir()
    data_path = collection_dir / filename
    shutil.copy(source, data_path)
    _write_collection_json(
        collection_dir,
        collection_id=collection_id,
        extra_assets={
            "data": {
                "href": f"./{filename}",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
            }
        },
    )
    return collection_dir, data_path


def _write_catalog_with_iceberg_asset(catalog_dir: Path, *, href: str) -> Path:
    """Write a catalog with an agriculture collection that has one Iceberg asset at HREF."""
    catalog_dir.mkdir()
    _write_catalog_json(catalog_dir)
    collection_dir = catalog_dir / "agriculture"
    collection_dir.mkdir()
    _write_collection_json(
        collection_dir,
        collection_id="agriculture",
        extra_assets={
            "data": {
                "href": href,
                "type": "application/x-iceberg",
                "roles": ["data"],
            }
        },
    )
    return collection_dir


def _write_year_subcatalog_item(collection_dir: Path, source_cog: Path) -> Path:
    """Write a 2024 sub-catalog with item scene-001 in COLLECTION_DIR. Return the tif path."""
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
    shutil.copy(source_cog, item_data)
    _write_item_json(
        item_dir,
        item_id="scene-001",
        asset_href="scene-001.tif",
        media_type="image/tiff; application=geotiff",
    )
    return item_data


def _make_raster_collection_with_items_parquet(
    catalog_dir: Path,
    valid_singleband_cog: Path,
) -> Path:
    """Build a raster collection with two items and a registered items.parquet rollup.

    Return the collection directory.
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
    # exactly as stac_parquet.register_mirror_asset writes it.
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
                **file_fields(items_parquet),
            }
        },
    )
    return collection_dir


# A collection-level items.parquet must not trip metadata_fresh


@pytest.mark.integration
class TestIssue345CollectionLevelAssetsNotMissing:
    """A registered items.parquet at the collection root is not MISSING."""

    def test_scanner_does_not_flag_registered_items_parquet_as_missing(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
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
        """The hand-built catalog trips unrelated rules, so only items.parquet findings count."""
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        # .portolan sentinel makes catalog discoverable
        (catalog_dir / ".portolan").mkdir()
        (catalog_dir / ".portolan" / "config.yaml").write_text("version: 1\n")
        _make_raster_collection_with_items_parquet(catalog_dir, valid_singleband_cog)

        result = runner.invoke(cli, ["check", str(catalog_dir), "--metadata", "--json"])

        findings = json.loads(result.output)["data"]["findings"]
        offending = [f for f in findings if "items.parquet" in f["message"]]
        assert offending == [], f"items.parquet flagged as a defect: {offending}"


# check and --fix symmetry


@pytest.mark.integration
class TestIssue384CheckFixSymmetry:
    """--fix addresses every status that check reports."""

    def test_missing_reported_by_check_is_actionable_by_fix(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
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


# Orphan detection


@pytest.mark.integration
class TestOrphanFiles:
    """Unregistered files under a collection are reported as ORPHANED."""

    def test_orphan_parquet_at_collection_root_reported(
        self,
        tmp_path: Path,
    ) -> None:
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


# Vector single-file collection-level: no false MISSING


@pytest.mark.integration
class TestVectorCollectionLevelAsset:
    """A single-file vector collection expects no item.json."""

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

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "href",
        [
            "file:///nonexistent/warehouse/portolake/agriculture",
            "gs://bucket/warehouse/portolake/agriculture",
            "s3://bucket/warehouse/portolake/agriculture",
            "https://example.com/data/airports.parquet",
        ],
    )
    def test_collection_asset_with_scheme_qualified_href_not_missing(
        self,
        tmp_path: Path,
        href: str,
    ) -> None:
        """The scanner owns only the local filesystem, so a non-local href is out of scope."""
        catalog_dir = tmp_path / "catalog"
        _write_catalog_with_iceberg_asset(catalog_dir, href=href)

        report = scan_catalog_metadata(catalog_dir)
        assert report.missing_count == 0, (
            f"scheme-qualified href {href!r} wrongly flagged MISSING: {report.to_dict()}"
        )

    @pytest.mark.parametrize(
        "href",
        [
            # Relative warehouse paths: only the application/x-iceberg media
            # type marks them as non-local.
            "data/v3/statfi_paavo",
            "./warehouse/portolake/agriculture",
            "../shared/warehouse/agriculture",
        ],
    )
    def test_collection_iceberg_asset_with_relative_href_not_missing(
        self,
        tmp_path: Path,
        href: str,
    ) -> None:
        """Otherwise ``check --fix`` opens the joined local path and fails "Data file not found"."""
        catalog_dir = tmp_path / "catalog"
        _write_catalog_with_iceberg_asset(catalog_dir, href=href)

        report = scan_catalog_metadata(catalog_dir)
        assert report.missing_count == 0, (
            f"relative iceberg href {href!r} wrongly flagged MISSING: {report.to_dict()}"
        )

    def test_iceberg_warehouse_files_inside_collection_not_orphaned(
        self,
        tmp_path: Path,
    ) -> None:
        """The warehouse's own ``.parquet`` files belong to the Iceberg backend, not STAC."""
        catalog_dir = tmp_path / "catalog"
        collection_dir = _write_catalog_with_iceberg_asset(catalog_dir, href="data/v3/statfi_paavo")
        # Materialize the warehouse with real Iceberg-style data + metadata.
        warehouse = collection_dir / "data" / "v3" / "statfi_paavo"
        (warehouse / "data").mkdir(parents=True)
        (warehouse / "metadata").mkdir(parents=True)
        (warehouse / "data" / "00000-0-abc.parquet").write_bytes(b"PAR1")
        (warehouse / "metadata" / "v1.metadata.json").write_text("{}")

        report = scan_catalog_metadata(catalog_dir)
        assert report.missing_count == 0, report.to_dict()
        assert report.orphaned_count == 0, (
            f"Iceberg warehouse files wrongly flagged ORPHANED: {report.to_dict()}"
        )


# Helpers shared by the freshness and layout tests below.


def _write_versions_json_for(
    collection_dir: Path,
    *,
    asset_filename: str,
    file_path: Path,
) -> None:
    """Write a versions.json that tracks `file_path` in its current state, so a scan is FRESH.

    It records the schema fingerprint too, because a None fingerprint reads as BREAKING.
    """
    import hashlib

    from portolan_cli.metadata.detection import (
        compute_schema_fingerprint,
        get_current_metadata,
    )

    sha = hashlib.sha256(file_path.read_bytes()).hexdigest()
    current = get_current_metadata(file_path)
    # The full schema that `add` writes. The scan reads versions.json through
    # read_versions, which refuses a file without the required fields.
    versions = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "changes": [asset_filename],
                "assets": {
                    asset_filename: {
                        "source_mtime": file_path.stat().st_mtime,
                        "sha256": sha,
                        "size_bytes": file_path.stat().st_size,
                        "href": f"{collection_dir.name}/{asset_filename}",
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


# Collection-level registered assets get FRESH/STALE/BREAKING checks


@pytest.mark.integration
class TestCollectionLevelFreshness:
    """Collection-level data assets are freshness-checked."""

    def test_collection_level_vector_asset_reports_fresh(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        catalog_dir = tmp_path / "catalog"
        collection_dir, data_path = _write_catalog_with_data_asset(
            catalog_dir, valid_points_parquet, collection_id="boundaries", filename="data.parquet"
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
        import pyarrow as pa
        import pyarrow.parquet as pq

        catalog_dir = tmp_path / "catalog"
        collection_dir, data_path = _write_catalog_with_data_asset(
            catalog_dir, valid_points_parquet, collection_id="boundaries", filename="data.parquet"
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
        """Covers the heuristics_changed guard when both bboxes are None."""
        catalog_dir = tmp_path / "catalog"
        collection_dir, data_path = _write_catalog_with_data_asset(
            catalog_dir, valid_points_parquet, collection_id="boundaries", filename="data.parquet"
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


# A flat layout is ORPHANED, not freshness-checked


@pytest.mark.integration
class TestLegacyFlatLayoutIsOrphaned:
    """A data file with a flat sibling JSON is ORPHANED, so users move it with `portolan add`."""

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


# Nested catalogs (Pattern 1 + Pattern 2)


@pytest.mark.integration
class TestNestedCatalogPatterns:
    """Pattern 1: catalog → sub-catalog → collection → item.

    Pattern 2: collection → year sub-catalog → item; the collection owns versions.json.
    """

    def test_pattern1_subcatalog_with_collection(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
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
        # The untracked item reads STALE; the test only proves the scan finds it.
        scanned = [r.file_path.name for r in report.results]
        assert "scene-001.tif" in scanned, (
            f"pattern-1 sub-catalog item not reached by scanner. results={scanned}"
        )

    def test_pattern2_subcatalog_inside_collection_resolves_versions_at_collection(
        self,
        tmp_path: Path,
        valid_singleband_cog: Path,
    ) -> None:
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        # Year sub-catalog inside the collection.
        item_data = _write_year_subcatalog_item(collection_dir, valid_singleband_cog)

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
        """--fix walks up to the collection and updates its versions.json."""
        from portolan_cli.metadata.fix import FixAction, fix_metadata

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        _write_catalog_json(catalog_dir)
        collection_dir = catalog_dir / "rasters"
        collection_dir.mkdir()
        _write_collection_json(collection_dir, collection_id="rasters")

        item_data = _write_year_subcatalog_item(collection_dir, valid_singleband_cog)
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


# A stray subdir without {dir_name}.{ext} → ORPHANED, not MISSING


@pytest.mark.integration
class TestStraySubdirIsOrphaned:
    """The files of a subdir with no `{name}.{ext}` data file are ORPHANED, not an item."""

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


# CLI: catalog-root resolution for `check --metadata --fix`


@pytest.mark.integration
class TestCheckResolvesCatalogRoot:
    """`check --metadata --fix` finds the catalog root from a subdir, or fails without one."""

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
        catalog_dir = tmp_path / "catalog"
        collection_dir = self._make_catalog(catalog_dir, valid_singleband_cog)

        result = runner.invoke(
            cli,
            ["check", str(collection_dir), "--metadata", "--fix", "--json"],
        )
        # The hand-built catalog exits 1 on re-check; only the `fix` section
        # matters. GDAL warns on stderr, so read stdout.
        payload = json.loads(result.stdout)
        metadata_fix = payload.get("data", {}).get("fix", {}).get("metadata_fix")
        assert metadata_fix is not None
        assert metadata_fix["total_count"] >= 1, (
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
        non_catalog = tmp_path / "scratch"
        non_catalog.mkdir()

        result = runner.invoke(
            cli,
            ["check", str(non_catalog), "--metadata", "--fix"],
        )
        assert result.exit_code != 0, (
            f"--fix outside a catalog must fail; got exit=0 output={result.output}"
        )


# scan_catalog_metadata raises on a missing catalog.json, so a random
# directory never passes as a fresh catalog.


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


# A stray non-cloud-native vector file is ORPHANED but not freshness-checked,
# like .pmtiles, because no extractor reads it.


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


# Collection-level vector assets must not be perpetually STALE


def _write_versions_json_mtime_only(
    collection_dir: Path,
    *,
    asset_filename: str,
    file_path: Path,
) -> None:
    """Write a versions.json in the shape ``add`` writes: ``mtime`` only, no heuristics."""
    import hashlib

    sha = hashlib.sha256(file_path.read_bytes()).hexdigest()
    versions = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "changes": [asset_filename],
                "assets": {
                    asset_filename: {
                        "sha256": sha,
                        "size_bytes": file_path.stat().st_size,
                        "href": f"{collection_dir.name}/{asset_filename}",
                        "mtime": file_path.stat().st_mtime,
                    }
                },
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions, indent=2))


@pytest.mark.integration
class TestIssue512CollectionFreshnessAfterAdd:
    """A collection-level vector asset that `add` records with only `mtime` reads FRESH."""

    def test_add_then_scan_reports_fresh(
        self,
        fresh_catalog_no_versions: Path,
        fixtures_dir: Path,
    ) -> None:
        from portolan_cli.add import add

        collection_dir = fresh_catalog_no_versions / "demographics"
        collection_dir.mkdir()
        target_file = collection_dir / "census.parquet"
        target_file.write_bytes((fixtures_dir / "simple.parquet").read_bytes())

        add(
            catalog_root=fresh_catalog_no_versions,
            path=target_file,
            collection_id="demographics",
            item_id=None,
            title="Census Data 2020",
            description="Population demographics from 2020 census",
        )

        report = scan_catalog_metadata(fresh_catalog_no_versions)

        asset_results = [r for r in report.results if r.file_path.name == "census.parquet"]
        assert len(asset_results) == 1, f"expected one result, got: {report.to_dict()}"
        assert asset_results[0].status == MetadataStatus.FRESH, (
            f"freshly added collection-level asset must be FRESH, "
            f"got {asset_results[0].status}: {report.to_dict()}"
        )

    def test_mtime_only_versions_entry_reads_fresh(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        catalog_dir = tmp_path / "catalog"
        collection_dir, data_path = _write_catalog_with_data_asset(
            catalog_dir, valid_points_parquet, collection_id="boundaries", filename="data.parquet"
        )
        _write_versions_json_mtime_only(
            collection_dir, asset_filename="data.parquet", file_path=data_path
        )

        report = scan_catalog_metadata(catalog_dir)

        results = [r for r in report.results if r.file_path.name == "data.parquet"]
        assert len(results) == 1, f"expected one result, got: {report.to_dict()}"
        assert results[0].status == MetadataStatus.FRESH, (
            f"mtime-only entry with unchanged file must be FRESH, "
            f"got {results[0].status}: {report.to_dict()}"
        )

    def test_mtime_only_entry_never_escalates_to_breaking(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """Without a stored `schema_fingerprint`, no schema break is provable."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        catalog_dir = tmp_path / "catalog"
        collection_dir, data_path = _write_catalog_with_data_asset(
            catalog_dir, valid_points_parquet, collection_id="boundaries", filename="data.parquet"
        )
        _write_versions_json_mtime_only(
            collection_dir, asset_filename="data.parquet", file_path=data_path
        )

        # Change the schema (new columns) and bump mtime past the fast path.
        pq.write_table(pa.table({"a": [1, 2], "b": [3, 4]}), data_path)
        _bump_mtime(data_path, delta=120.0)

        report = scan_catalog_metadata(catalog_dir)

        results = [r for r in report.results if r.file_path.name == "data.parquet"]
        assert len(results) == 1, f"expected one result, got: {report.to_dict()}"
        assert results[0].status != MetadataStatus.BREAKING, (
            f"mtime-only entry must not escalate to BREAKING, "
            f"got {results[0].status}: {report.to_dict()}"
        )

    def test_mtime_only_entry_touched_but_identical_reads_fresh(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """A clone resets mtimes; the sha256 fallback keeps a byte-identical asset FRESH."""
        catalog_dir = tmp_path / "catalog"
        collection_dir, data_path = _write_catalog_with_data_asset(
            catalog_dir, valid_points_parquet, collection_id="boundaries", filename="data.parquet"
        )
        _write_versions_json_mtime_only(
            collection_dir, asset_filename="data.parquet", file_path=data_path
        )

        # Simulate a clone/checkout: mtime moves well past the fast-path
        # tolerance, but the bytes are untouched.
        _bump_mtime(data_path, delta=120.0)

        report = scan_catalog_metadata(catalog_dir)

        results = [r for r in report.results if r.file_path.name == "data.parquet"]
        assert len(results) == 1, f"expected one result, got: {report.to_dict()}"
        assert results[0].status == MetadataStatus.FRESH, (
            f"touched-but-identical mtime-only entry must be FRESH via the "
            f"content-hash fallback, got {results[0].status}: {report.to_dict()}"
        )

    def test_mtime_only_entry_with_changed_content_is_not_masked(
        self,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """The content-hash fallback must not mask a genuine change."""

        import pyarrow as pa
        import pyarrow.parquet as pq

        catalog_dir = tmp_path / "catalog"
        collection_dir, data_path = _write_catalog_with_data_asset(
            catalog_dir, valid_points_parquet, collection_id="boundaries", filename="data.parquet"
        )
        _write_versions_json_mtime_only(
            collection_dir, asset_filename="data.parquet", file_path=data_path
        )

        # Genuinely change the bytes (and bump mtime past the fast path).
        pq.write_table(pa.table({"a": [1, 2, 3], "b": [4, 5, 6]}), data_path)
        _bump_mtime(data_path, delta=120.0)

        report = scan_catalog_metadata(catalog_dir)

        results = [r for r in report.results if r.file_path.name == "data.parquet"]
        assert len(results) == 1, f"expected one result, got: {report.to_dict()}"
        assert results[0].status != MetadataStatus.FRESH, (
            f"a genuinely changed asset must not be masked as FRESH, "
            f"got {results[0].status}: {report.to_dict()}"
        )
