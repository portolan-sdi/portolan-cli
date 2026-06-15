"""Integration tests for PMTiles generation (Issue #115).

Tests the complete PMTiles generation workflow with real tippecanoe execution.
These tests require tippecanoe to be installed; they skip gracefully if not.

Tests use collection-level asset structure per ADR-0031.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any

import pytest

# Skip entire module if tippecanoe not available
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        shutil.which("tippecanoe") is None,
        reason="tippecanoe not installed (required for PMTiles generation)",
    ),
]


@pytest.fixture
def collection_with_geoparquet(tmp_path: Path, fixtures_dir: Path) -> Path:
    """Create a minimal collection with a GeoParquet asset.

    Structure per ADR-0031:
        collection/
            collection.json  (with assets section)
            roads.parquet
            versions.json
    """
    collection_dir = tmp_path / "roads"
    collection_dir.mkdir()

    # Copy real GeoParquet fixture
    src = fixtures_dir / "realdata" / "road-detections.parquet"
    dst = collection_dir / "roads.parquet"
    shutil.copy(src, dst)

    # Create collection.json with collection-level asset
    collection_json = {
        "type": "Collection",
        "id": "roads",
        "stac_version": "1.1.0",
        "description": "Road detections",
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        ],
        "extent": {
            "spatial": {"bbox": [[-61.0, 13.7, -60.9, 13.9]]},
            "temporal": {"interval": [[None, None]]},
        },
        "license": "proprietary",
        "assets": {
            "roads-data": {
                "href": "./roads.parquet",
                "type": "application/vnd.apache.parquet",
                "title": "Road Detections",
                "roles": ["data"],
            }
        },
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Create minimal versions.json (all required fields per versions.py)
    versions_json = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2026-01-01T00:00:00Z",
                "breaking": False,
                "assets": {},
                "changes": [],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

    # Create catalog root marker
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("catalog_id: test-catalog\n")

    return collection_dir


@pytest.fixture
def collection_with_two_geoparquet(tmp_path: Path, fixtures_dir: Path) -> Path:
    """Create a collection with two GeoParquet assets (a.parquet, b.parquet)."""
    collection_dir = tmp_path / "roads"
    collection_dir.mkdir()

    src = fixtures_dir / "realdata" / "road-detections.parquet"
    shutil.copy(src, collection_dir / "a.parquet")
    shutil.copy(src, collection_dir / "b.parquet")

    collection_json = {
        "type": "Collection",
        "id": "roads",
        "stac_version": "1.1.0",
        "description": "Road detections",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-61.0, 13.7, -60.9, 13.9]]},
            "temporal": {"interval": [[None, None]]},
        },
        "assets": {
            "a": {
                "href": "./a.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
            },
            "b": {
                "href": "./b.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
            },
        },
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    versions_json = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2026-01-01T00:00:00Z",
                "breaking": False,
                "assets": {},
                "changes": [],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("catalog_id: test-catalog\n")

    return collection_dir


class TestPMTilesGeneration:
    """Test PMTiles generation from GeoParquet."""

    def test_generate_creates_valid_pmtiles(self, collection_with_geoparquet: Path) -> None:
        """PMTiles file is created with valid header (magic bytes)."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        result = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
        )

        # Should have generated one PMTiles file
        assert len(result.generated) == 1
        assert len(result.skipped) == 0
        assert len(result.failed) == 0
        assert result.success is True

        # File exists
        pmtiles_path = collection_with_geoparquet / "roads.pmtiles"
        assert pmtiles_path.exists()

        # Valid PMTiles magic bytes (0x504d = "PM")
        magic = pmtiles_path.read_bytes()[:2]
        assert magic == b"PM", f"Invalid PMTiles magic: {magic!r}"

        # File has reasonable size (> 1KB for road-detections)
        assert pmtiles_path.stat().st_size > 1000

    def test_skip_when_pmtiles_newer(self, collection_with_geoparquet: Path) -> None:
        """PMTiles regeneration skipped when output is newer than source."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        # First generation
        result1 = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
        )
        assert len(result1.generated) == 1

        # Small delay to ensure mtime differs
        time.sleep(0.1)

        # Second generation (should skip)
        result2 = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
        )
        assert len(result2.generated) == 0
        assert len(result2.skipped) == 1

    def test_force_regenerates(self, collection_with_geoparquet: Path) -> None:
        """Force flag regenerates PMTiles even when up-to-date."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        # First generation
        result1 = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
        )
        assert len(result1.generated) == 1

        pmtiles_path = collection_with_geoparquet / "roads.pmtiles"
        first_mtime = pmtiles_path.stat().st_mtime

        time.sleep(0.1)

        # Force regeneration
        result2 = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
            force=True,
        )
        assert len(result2.generated) == 1
        assert len(result2.skipped) == 0

        # File was actually regenerated (mtime changed)
        assert pmtiles_path.stat().st_mtime > first_mtime

    def test_asset_registered_in_collection_json(self, collection_with_geoparquet: Path) -> None:
        """PMTiles asset is registered in collection.json with correct metadata."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
        )

        # Read updated collection.json
        collection_data = json.loads((collection_with_geoparquet / "collection.json").read_text())
        assets = collection_data["assets"]

        # PMTiles asset should exist
        assert "roads-data-tiles" in assets, f"Expected PMTiles asset, got: {list(assets.keys())}"

        pmtiles_asset = assets["roads-data-tiles"]
        assert pmtiles_asset["href"] == "./roads.pmtiles"
        assert pmtiles_asset["type"] == "application/vnd.pmtiles"
        assert pmtiles_asset["roles"] == ["visual"]
        assert "tiles" in pmtiles_asset["title"].lower()

    def test_version_tracked(self, collection_with_geoparquet: Path) -> None:
        """PMTiles and its thumbnail share a single versions.json snapshot.

        The side-step generates the PMTiles and (when rendering succeeds) a
        thumbnail. Both belong to ONE version bump, not two (Issue #519), and
        the snapshot carries both with checksum and size.
        """
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        # Read initial version
        versions_data_before = json.loads(
            (collection_with_geoparquet / "versions.json").read_text()
        )
        initial_version = versions_data_before["current_version"]

        generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
        )

        # Read updated versions.json
        versions_data_after = json.loads((collection_with_geoparquet / "versions.json").read_text())

        # Exactly one new version snapshot for the whole side-step (Issue #519).
        assert versions_data_after["current_version"] != initial_version
        assert len(versions_data_after["versions"]) == len(versions_data_before["versions"]) + 1

        # Latest snapshot tracks the PMTiles file.
        latest_version = versions_data_after["versions"][-1]
        assert "roads.pmtiles" in latest_version["assets"]

        pmtiles_asset = latest_version["assets"]["roads.pmtiles"]
        assert "sha256" in pmtiles_asset
        assert pmtiles_asset["size_bytes"] > 0

        # If a thumbnail was actually rendered (matplotlib/contextily available;
        # basemap fetch is networked, so it may be absent offline), it must live
        # in the SAME snapshot as the PMTiles with its own checksum (Issue #519).
        if (collection_with_geoparquet / "roads.thumb.jpg").exists():
            assert "roads.thumb.jpg" in latest_version["assets"]
            thumb_asset = latest_version["assets"]["roads.thumb.jpg"]
            assert "sha256" in thumb_asset
            assert thumb_asset["size_bytes"] > 0

    @staticmethod
    def _reset_versions_to_baseline(collection: Path) -> None:
        """Drop generated version snapshots, leaving only the empty 1.0.0 baseline.

        Reproduces the original #519 state: artifacts exist on disk and in
        collection.json, but versions.json does not track them.
        """
        data = json.loads((collection / "versions.json").read_text())
        data["versions"] = data["versions"][:1]
        data["current_version"] = data["versions"][0]["version"]
        (collection / "versions.json").write_text(json.dumps(data, indent=2))

    def test_skip_path_backfills_untracked_artifacts(
        self, collection_with_geoparquet: Path
    ) -> None:
        """Skip path heals artifacts left untracked by the old code (Issue #519).

        Reproduce the pre-fix state (PMTiles + thumbnail on disk and in
        collection.json, absent from versions.json), then run generation again.
        The PMTiles is up-to-date so generation is skipped, but the untracked
        artifacts must still be backfilled into versions.json — without forcing
        a regenerate.
        """
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        # First run actually generates the artifacts.
        generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet, catalog_root=catalog_root
        )
        pmtiles = collection_with_geoparquet / "roads.pmtiles"
        thumb = collection_with_geoparquet / "roads.thumb.jpg"
        assert pmtiles.exists(), "PMTiles should have been generated"
        thumb_rendered = thumb.exists()

        # Simulate the #519 bug state: forget the artifacts in versions.json. Also
        # drop the thumbnail STAC asset from collection.json so the skip path must
        # re-register it (no orphaned versions.json entry).
        self._reset_versions_to_baseline(collection_with_geoparquet)
        coll_path = collection_with_geoparquet / "collection.json"
        coll = json.loads(coll_path.read_text())
        coll["assets"] = {
            k: v for k, v in coll["assets"].items() if "thumbnail" not in v.get("roles", [])
        }
        coll_path.write_text(json.dumps(coll, indent=2))
        before = json.loads((collection_with_geoparquet / "versions.json").read_text())

        # Second run: PMTiles is up-to-date -> skip path -> backfill.
        result = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet, catalog_root=catalog_root
        )
        assert pmtiles in result.skipped, "Up-to-date PMTiles must be skipped, not regenerated"

        after = json.loads((collection_with_geoparquet / "versions.json").read_text())
        # Exactly one backfill version added.
        assert len(after["versions"]) == len(before["versions"]) + 1
        latest = after["versions"][-1]["assets"]
        assert "roads.pmtiles" in latest, "Untracked PMTiles must be backfilled on skip"
        if thumb_rendered:
            assert "roads.thumb.jpg" in latest, "Untracked thumbnail must be backfilled on skip"
            # And it must be re-registered as a STAC asset, not left orphaned.
            coll_after = json.loads(coll_path.read_text())
            thumb_stac = [
                k for k, v in coll_after["assets"].items() if "thumbnail" in v.get("roles", [])
            ]
            assert thumb_stac, "Backfilled thumbnail must be re-registered as a STAC asset"

    def test_backfill_is_idempotent_when_already_tracked(
        self, collection_with_geoparquet: Path
    ) -> None:
        """A skip with everything already tracked creates NO new version (Issue #519)."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        # Generate + track.
        generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet, catalog_root=catalog_root
        )
        after_generate = json.loads((collection_with_geoparquet / "versions.json").read_text())

        # Re-run: PMTiles up-to-date, everything tracked -> no version bump.
        generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet, catalog_root=catalog_root
        )
        after_skip = json.loads((collection_with_geoparquet / "versions.json").read_text())

        assert len(after_skip["versions"]) == len(after_generate["versions"]), (
            "Idempotent skip must not bump a version when everything is tracked"
        )
        assert after_skip["current_version"] == after_generate["current_version"]

    def test_each_geoparquet_asset_gets_its_own_version(
        self, collection_with_two_geoparquet: Path
    ) -> None:
        """Each source asset's side-step is one version; the latest is cumulative.

        Two GeoParquet assets -> two side-steps -> two new version snapshots
        (one per asset, each bundling that asset's PMTiles + thumbnail), and the
        final snapshot carries every generated artifact (Issue #519).
        """
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        collection = collection_with_two_geoparquet
        catalog_root = collection.parent

        before = json.loads((collection / "versions.json").read_text())
        result = generate_pmtiles_for_collection(
            collection_path=collection, catalog_root=catalog_root
        )
        after = json.loads((collection / "versions.json").read_text())

        assert len(result.generated) == 2
        # One version per source asset (not per artifact): +2, not +4.
        assert len(after["versions"]) == len(before["versions"]) + 2

        # The latest snapshot is cumulative across both assets.
        latest = after["versions"][-1]["assets"]
        assert "a.pmtiles" in latest
        assert "b.pmtiles" in latest
        # Thumbnails are bundled into their asset's snapshot when rendered.
        if (collection / "a.thumb.jpg").exists():
            assert "a.thumb.jpg" in latest
        if (collection / "b.thumb.jpg").exists():
            assert "b.thumb.jpg" in latest


class TestPMTilesZoomLevels:
    """Test PMTiles zoom level configuration."""

    def test_custom_zoom_levels(self, collection_with_geoparquet: Path) -> None:
        """Custom min/max zoom levels are passed to tippecanoe."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        result = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
            min_zoom=0,
            max_zoom=8,
        )

        assert len(result.generated) == 1
        assert result.success is True

        # PMTiles was created (zoom params accepted)
        pmtiles_path = collection_with_geoparquet / "roads.pmtiles"
        assert pmtiles_path.exists()


class TestPMTilesMultipleAssets:
    """Test PMTiles generation for collections with multiple assets."""

    def test_generates_pmtiles_for_all_geoparquet_assets(
        self, tmp_path: Path, fixtures_dir: Path
    ) -> None:
        """All GeoParquet assets in collection get PMTiles generated."""
        collection_dir = tmp_path / "multi"
        collection_dir.mkdir()

        # Copy two different GeoParquet files
        roads_src = fixtures_dir / "realdata" / "road-detections.parquet"
        buildings_src = fixtures_dir / "realdata" / "open-buildings.parquet"

        shutil.copy(roads_src, collection_dir / "roads.parquet")
        shutil.copy(buildings_src, collection_dir / "buildings.parquet")

        # Create collection.json with both assets
        collection_json = {
            "type": "Collection",
            "id": "multi",
            "stac_version": "1.1.0",
            "description": "Multiple assets",
            "links": [],
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "license": "proprietary",
            "assets": {
                "roads": {
                    "href": "./roads.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                },
                "buildings": {
                    "href": "./buildings.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                },
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Create versions.json
        versions_json: dict[str, Any] = {
            "spec_version": "1.0.0",
            "current_version": None,
            "versions": [],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

        # Create catalog root
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("catalog_id: test\n")

        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        result = generate_pmtiles_for_collection(
            collection_path=collection_dir,
            catalog_root=tmp_path,
        )

        assert len(result.generated) == 2
        assert result.success is True

        # Both PMTiles files exist
        assert (collection_dir / "roads.pmtiles").exists()
        assert (collection_dir / "buildings.pmtiles").exists()

        # Both registered in collection.json
        updated = json.loads((collection_dir / "collection.json").read_text())
        assert "roads-tiles" in updated["assets"]
        assert "buildings-tiles" in updated["assets"]


class TestPMTilesAdvancedParameters:
    """Test PMTiles generation with advanced gpio-pmtiles parameters."""

    def test_precision_parameter_accepted(self, collection_with_geoparquet: Path) -> None:
        """Custom precision parameter is accepted by tippecanoe."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        result = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
            precision=4,
        )

        assert len(result.generated) == 1
        assert result.success is True
        assert (collection_with_geoparquet / "roads.pmtiles").exists()

    def test_layer_parameter_accepted(self, collection_with_geoparquet: Path) -> None:
        """Custom layer name is accepted."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        result = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
            layer="custom-layer-name",
        )

        assert len(result.generated) == 1
        assert result.success is True

    def test_attribution_parameter_accepted(self, collection_with_geoparquet: Path) -> None:
        """Custom attribution HTML is accepted."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        result = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
            attribution="© Test Attribution",
        )

        assert len(result.generated) == 1
        assert result.success is True

    def test_all_parameters_together(self, collection_with_geoparquet: Path) -> None:
        """All advanced parameters work together."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        catalog_root = collection_with_geoparquet.parent

        result = generate_pmtiles_for_collection(
            collection_path=collection_with_geoparquet,
            catalog_root=catalog_root,
            min_zoom=0,
            max_zoom=10,
            layer="roads",
            precision=5,
            attribution="© OpenStreetMap",
        )

        assert len(result.generated) == 1
        assert result.success is True
        assert (collection_with_geoparquet / "roads.pmtiles").exists()
