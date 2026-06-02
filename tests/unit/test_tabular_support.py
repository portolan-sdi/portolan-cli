"""Unit tests for non-geo tabular data support (Issue #432).

Tests the classification, configuration, and handling of tabular data
(CSV, TSV, XLSX, plain Parquet) as collection-level assets.

Design decisions tested:
- GeoParquet vs plain Parquet detection via metadata peeking
- tabular.enabled config (default: false)
- tabular.convert config (default: true)
- Error with hint when tabular.enabled is false
- XLSX/XLS basic support (single sheet)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.scan_classify import (
    FileCategory,
    classify_file,
)

if TYPE_CHECKING:
    pass


@pytest.mark.unit
class TestParquetClassification:
    """Tests for distinguishing GeoParquet from plain Parquet."""

    def test_geoparquet_classified_as_geo_asset(self, tmp_path: Path) -> None:
        """GeoParquet files (with geo metadata) should be GEO_ASSET."""
        parquet_file = tmp_path / "data.parquet"

        # Create a minimal GeoParquet file with geo metadata
        table = pa.table({"id": [1, 2], "geometry": [b"wkb1", b"wkb2"]})
        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": json.dumps(geo_metadata).encode()}
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, parquet_file)

        category, skip_type, message = classify_file(parquet_file)

        assert category == FileCategory.GEO_ASSET
        assert skip_type is None
        assert message is None

    def test_plain_parquet_classified_as_tabular(self, tmp_path: Path) -> None:
        """Plain Parquet files (no geo metadata) should be TABULAR_DATA."""
        parquet_file = tmp_path / "demographics.parquet"

        # Create a plain Parquet file without geo metadata
        table = pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]})
        pq.write_table(table, parquet_file)

        category, skip_type, message = classify_file(parquet_file)

        assert category == FileCategory.TABULAR_DATA
        assert skip_type is not None
        assert "tabular" in message.lower() or "parquet" in message.lower()

    def test_parquet_with_geometry_column_but_no_geo_metadata(self, tmp_path: Path) -> None:
        """Parquet with geometry column but no geo metadata should be TABULAR_DATA.

        This is an edge case where someone wrote a Parquet file with a geometry
        column but didn't include GeoParquet metadata. We classify based on
        metadata, not column names, to be consistent and fast.
        """
        parquet_file = tmp_path / "maybe_geo.parquet"

        # Create a Parquet with a geometry-like column but NO geo metadata
        table = pa.table({"id": [1], "geometry": [b"some_wkb_bytes"]})
        pq.write_table(table, parquet_file)

        category, _, _ = classify_file(parquet_file)

        # Without geo metadata, it's tabular (gpio will detect on read if needed)
        assert category == FileCategory.TABULAR_DATA


@pytest.mark.unit
class TestTabularExtensions:
    """Tests for tabular file extension classification."""

    @pytest.mark.parametrize(
        "filename,expected_category",
        [
            ("data.csv", FileCategory.TABULAR_DATA),
            ("data.tsv", FileCategory.TABULAR_DATA),
            ("data.xlsx", FileCategory.TABULAR_DATA),
            ("data.xls", FileCategory.TABULAR_DATA),
        ],
    )
    def test_tabular_extensions_classified_correctly(
        self, tmp_path: Path, filename: str, expected_category: FileCategory
    ) -> None:
        """CSV, TSV, XLSX, XLS should all be classified as TABULAR_DATA."""
        file_path = tmp_path / filename
        file_path.touch()

        category, skip_type, message = classify_file(file_path)

        assert category == expected_category
        assert skip_type is not None  # Tabular files have skip reasons


@pytest.mark.unit
class TestTabularConfig:
    """Tests for tabular configuration settings."""

    def test_tabular_enabled_default_is_false(self) -> None:
        """tabular.enabled should default to false."""
        from portolan_cli.config import DEFAULT_SETTINGS

        assert DEFAULT_SETTINGS.get("tabular.enabled") is False

    def test_tabular_convert_default_is_true(self) -> None:
        """tabular.convert should default to true."""
        from portolan_cli.config import DEFAULT_SETTINGS

        assert DEFAULT_SETTINGS.get("tabular.convert") is True

    def test_tabular_settings_in_known_settings(self) -> None:
        """Tabular settings should be in KNOWN_SETTINGS."""
        from portolan_cli.config import KNOWN_SETTINGS

        assert "tabular.enabled" in KNOWN_SETTINGS
        assert "tabular.convert" in KNOWN_SETTINGS


@pytest.mark.unit
class TestConstantsUnification:
    """Tests that tabular constants are unified across modules."""

    def test_constants_tabular_includes_parquet(self) -> None:
        """constants.py TABULAR_EXTENSIONS should include .parquet."""
        from portolan_cli.constants import TABULAR_EXTENSIONS

        assert ".parquet" in TABULAR_EXTENSIONS

    def test_constants_tabular_includes_excel(self) -> None:
        """constants.py TABULAR_EXTENSIONS should include Excel formats."""
        from portolan_cli.constants import TABULAR_EXTENSIONS

        assert ".xlsx" in TABULAR_EXTENSIONS
        assert ".xls" in TABULAR_EXTENSIONS

    def test_scan_classify_tabular_matches_constants(self) -> None:
        """scan_classify.py TABULAR_EXTENSIONS should match constants.py."""
        from portolan_cli.constants import TABULAR_EXTENSIONS as CONST_TABULAR
        from portolan_cli.scan_classify import TABULAR_EXTENSIONS as SCAN_TABULAR

        # scan_classify should have at least the same extensions
        # (it may have more if needed for classification)
        assert CONST_TABULAR <= SCAN_TABULAR

    def test_parquet_not_in_geo_asset_extensions(self) -> None:
        """Parquet should NOT be in GEO_ASSET_EXTENSIONS (peeking required)."""
        from portolan_cli.scan_classify import GEO_ASSET_EXTENSIONS

        # .parquet requires metadata peeking, so it shouldn't be in the simple
        # extension-based GEO_ASSET list
        assert ".parquet" not in GEO_ASSET_EXTENSIONS


@pytest.mark.unit
class TestIsGeoParquet:
    """Tests for the is_geoparquet() helper function."""

    def test_is_geoparquet_with_geo_metadata(self, tmp_path: Path) -> None:
        """Files with geo metadata should return True."""
        from portolan_cli.scan_classify import is_geoparquet

        parquet_file = tmp_path / "geo.parquet"
        table = pa.table({"id": [1], "geometry": [b"wkb"]})
        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB"}},
        }
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": json.dumps(geo_metadata).encode()}
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, parquet_file)

        assert is_geoparquet(parquet_file) is True

    def test_is_geoparquet_without_geo_metadata(self, tmp_path: Path) -> None:
        """Files without geo metadata should return False."""
        from portolan_cli.scan_classify import is_geoparquet

        parquet_file = tmp_path / "plain.parquet"
        table = pa.table({"id": [1], "value": [100]})
        pq.write_table(table, parquet_file)

        assert is_geoparquet(parquet_file) is False

    def test_is_geoparquet_handles_read_error(self, tmp_path: Path) -> None:
        """Should return False on read errors (not crash)."""
        from portolan_cli.scan_classify import is_geoparquet

        bad_file = tmp_path / "not_parquet.parquet"
        bad_file.write_text("this is not a parquet file")

        # Should not raise, should return False
        assert is_geoparquet(bad_file) is False

    def test_is_geoparquet_nonexistent_file(self, tmp_path: Path) -> None:
        """Should return False for nonexistent files."""
        from portolan_cli.scan_classify import is_geoparquet

        missing = tmp_path / "missing.parquet"

        assert is_geoparquet(missing) is False


def _setup_test_catalog(path: Path) -> None:
    """Create an initialized Portolan catalog for tests."""
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test Portolan catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


@pytest.mark.unit
class TestTabularEnabledCheck:
    """Tests for tabular.enabled config enforcement in add workflow."""

    def test_tabular_disabled_standalone_parquet_fails_with_hint(self, tmp_path: Path) -> None:
        """When tabular.enabled=false, standalone tabular files should fail with hint.

        This tests the core Issue #432 requirement: when a user tries to add a
        plain Parquet file (no geo metadata) without a companion geo file,
        and tabular.enabled is false (default), they should get a clear error
        message telling them how to enable tabular support.
        """
        from portolan_cli.dataset import add_files

        # Create catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_test_catalog(catalog_root)

        # Create collection with ONLY a plain parquet file (no geo file)
        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()

        # Create plain Parquet (no geo metadata)
        parquet_file = collection_dir / "census.parquet"
        table = pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]})
        pq.write_table(table, parquet_file)

        # Config: tabular.enabled = false (default)
        config_file = catalog_root / ".portolan" / "config.yaml"
        config_file.write_text("tabular:\n  enabled: false\n")

        # Add the file - should fail with hint
        added, skipped, failures = add_files(
            paths=[parquet_file],
            catalog_root=catalog_root,
        )

        # Should have a failure with helpful message
        assert len(failures) == 1
        failure = failures[0]
        assert "tabular" in failure.error.lower()
        assert "enabled" in failure.error.lower() or "config" in failure.error.lower()

    def test_tabular_enabled_standalone_parquet_succeeds(self, tmp_path: Path) -> None:
        """When tabular.enabled=true, standalone tabular files should be tracked.

        This tests that when users opt-in to tabular support, plain Parquet
        files are tracked as collection-level assets even without a companion
        geo file.
        """
        from portolan_cli.dataset import add_files

        # Create catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_test_catalog(catalog_root)

        # Create collection with ONLY a plain parquet file (no geo file)
        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()

        # Create plain Parquet (no geo metadata)
        parquet_file = collection_dir / "census.parquet"
        table = pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]})
        pq.write_table(table, parquet_file)

        # Config: tabular.enabled = true (opt-in)
        config_file = catalog_root / ".portolan" / "config.yaml"
        config_file.write_text("tabular:\n  enabled: true\n")

        # Add the file - should succeed
        added, skipped, failures = add_files(
            paths=[parquet_file],
            catalog_root=catalog_root,
        )

        # Should have no failures
        assert len(failures) == 0

        # File should be in skipped (tracked as collection-level tabular asset)
        # When tabular.enabled=true, standalone tabular files go to skipped
        # (like other tracked-but-not-converted files per ADR-0028)
        assert parquet_file in skipped

        # collection.json should exist and have the asset
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists(), "collection.json should be created"

        import json as json_mod

        collection_data = json_mod.loads(collection_json.read_text())
        assert "assets" in collection_data, "collection.json should have assets"
        assert len(collection_data["assets"]) > 0, "Should have at least one asset"

    def test_tabular_companion_asset_works_regardless_of_config(self, tmp_path: Path) -> None:
        """Tabular files WITH a companion geo file work regardless of tabular.enabled.

        This tests the ADR-0028 behavior: when a tabular file is in the same
        directory as a geo file, it's tracked as a companion asset. This should
        work whether tabular.enabled is true or false.

        Note: This requires geopandas for creating valid GeoParquet test data.
        """
        geopandas = pytest.importorskip("geopandas")
        pytest.importorskip("shapely")
        from shapely.geometry import Point

        from portolan_cli.dataset import add_files

        # Create catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_test_catalog(catalog_root)

        # Create collection with both geo and tabular files
        collection_dir = catalog_root / "census"
        collection_dir.mkdir()

        # Create valid GeoParquet using geopandas
        geo_file = collection_dir / "boundaries.parquet"
        gdf = geopandas.GeoDataFrame(
            {"id": [1, 2], "name": ["A", "B"]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326",
        )
        gdf.to_parquet(geo_file)

        # Create plain Parquet (companion tabular data)
        tabular_file = collection_dir / "demographics.parquet"
        tabular_table = pa.table({"tract_id": ["001"], "population": [5000]})
        pq.write_table(tabular_table, tabular_file)

        # Config: tabular.enabled = false (but companion should still work)
        config_file = catalog_root / ".portolan" / "config.yaml"
        config_file.write_text("tabular:\n  enabled: false\n")

        # Add both files - should work for both
        added, skipped, failures = add_files(
            paths=[geo_file, tabular_file],
            catalog_root=catalog_root,
        )

        # Geo file should be added, tabular should be tracked (as companion)
        # No failures expected - tabular companion works regardless of config
        assert len(failures) == 0


@pytest.mark.unit
class TestGpioRoutingForTabular:
    """Tests for routing tabular files through geoparquet-io (Issue #432).

    When tabular.convert=true, CSV/TSV files should be converted to Parquet
    using gpio.convert().write() — the same pipeline as geo files but without
    geometry operations.
    """

    def test_csv_converted_to_parquet_via_gpio(self, tmp_path: Path) -> None:
        """CSV files should be converted to Parquet when tabular.convert=true.

        This verifies the gpio routing: csv -> gpio.convert() -> .write() -> parquet
        """
        from portolan_cli.dataset import convert_tabular

        # Create a simple CSV
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("id,name,value\n1,foo,100\n2,bar,200\n")

        # Convert using the new function
        output = convert_tabular(csv_path, tmp_path)

        # Should produce a Parquet file
        assert output.exists()
        assert output.suffix == ".parquet"
        assert output.stem == "data"

        # Verify it's valid Parquet without geo metadata
        schema = pq.read_schema(output)
        assert schema.metadata is None or b"geo" not in schema.metadata

        # Verify data integrity
        table = pq.read_table(output)
        assert table.num_rows == 2
        assert "id" in table.column_names
        assert "name" in table.column_names
        assert "value" in table.column_names

    def test_tsv_converted_to_parquet_via_gpio(self, tmp_path: Path) -> None:
        """TSV files should be converted to Parquet when tabular.convert=true."""
        from portolan_cli.dataset import convert_tabular

        # Create a simple TSV
        tsv_path = tmp_path / "data.tsv"
        tsv_path.write_text("id\tname\tvalue\n1\tfoo\t100\n2\tbar\t200\n")

        output = convert_tabular(tsv_path, tmp_path)

        assert output.exists()
        assert output.suffix == ".parquet"

        # Verify data integrity
        table = pq.read_table(output)
        assert table.num_rows == 2

    def test_parquet_file_copied_not_converted(self, tmp_path: Path) -> None:
        """Plain Parquet files should be copied, not re-converted."""
        from portolan_cli.dataset import convert_tabular

        # Create a plain Parquet file
        src_parquet = tmp_path / "source" / "data.parquet"
        src_parquet.parent.mkdir()
        table = pa.table({"id": [1, 2], "value": [100, 200]})
        pq.write_table(table, src_parquet)

        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        output = convert_tabular(src_parquet, dest_dir)

        assert output.exists()
        assert output == dest_dir / "data.parquet"

        # Verify data integrity
        result_table = pq.read_table(output)
        assert result_table.num_rows == 2

    def test_xlsx_converted_to_parquet_via_gpio(self, tmp_path: Path) -> None:
        """XLSX files should be converted to Parquet when tabular.convert=true."""
        pytest.importorskip("openpyxl")
        from openpyxl import Workbook

        from portolan_cli.dataset import convert_tabular

        # Create a simple XLSX
        xlsx_path = tmp_path / "data.xlsx"
        wb = Workbook()
        ws = wb.active
        ws.append(["id", "name", "value"])
        ws.append([1, "foo", 100])
        ws.append([2, "bar", 200])
        wb.save(xlsx_path)

        output = convert_tabular(xlsx_path, tmp_path)

        assert output.exists()
        assert output.suffix == ".parquet"

        # Verify data integrity
        table = pq.read_table(output)
        assert table.num_rows == 2


@pytest.mark.unit
class TestAoiInheritance:
    """Tests for AOI inheritance in tabular collections (Issue #432).

    Tabular collections should inherit their spatial extent from sibling
    geo collections, not use a hardcoded global bbox placeholder.
    """

    def test_tabular_collection_inherits_bbox_from_sibling(self, tmp_path: Path) -> None:
        """Tabular collection should inherit bbox from sibling geo collection.

        When a tabular-only collection is created, it should scan sibling
        collections for their spatial extents and use the union.
        """
        from portolan_cli.dataset import add_files

        # Create catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_test_catalog(catalog_root)

        # Create a geo collection FIRST with a specific bbox
        geo_collection_dir = catalog_root / "boundaries"
        geo_collection_dir.mkdir()

        # Create a geo collection with known extent
        geo_collection_json = geo_collection_dir / "collection.json"
        geo_collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "boundaries",
            "description": "Boundary polygons",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-75.5, 39.5, -74.5, 40.5]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
            "links": [],
        }
        geo_collection_json.write_text(json.dumps(geo_collection_data, indent=2))

        # Update catalog to link to geo collection
        catalog_path = catalog_root / "catalog.json"
        catalog_data = json.loads(catalog_path.read_text())
        catalog_data["links"].append(
            {"rel": "child", "href": "./boundaries/collection.json", "type": "application/json"}
        )
        catalog_path.write_text(json.dumps(catalog_data, indent=2))

        # Now create a tabular collection
        tabular_dir = catalog_root / "demographics"
        tabular_dir.mkdir()

        # Create plain Parquet (no geo metadata)
        parquet_file = tabular_dir / "census.parquet"
        table = pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]})
        pq.write_table(table, parquet_file)

        # Config: tabular.enabled = true
        config_file = catalog_root / ".portolan" / "config.yaml"
        config_file.write_text("tabular:\n  enabled: true\n")

        # Add the tabular file
        added, skipped, failures = add_files(
            paths=[parquet_file],
            catalog_root=catalog_root,
        )

        assert len(failures) == 0

        # Check the created tabular collection's extent
        tabular_collection = tabular_dir / "collection.json"
        assert tabular_collection.exists()

        collection_data = json.loads(tabular_collection.read_text())
        bbox = collection_data["extent"]["spatial"]["bbox"][0]

        # Should inherit from sibling, NOT use global [-180, -90, 180, 90]
        assert bbox == [-75.5, 39.5, -74.5, 40.5], f"Expected sibling bbox, got {bbox}"

    def test_tabular_collection_uses_union_of_multiple_siblings(self, tmp_path: Path) -> None:
        """Tabular collection should use union bbox when multiple siblings exist."""
        from portolan_cli.dataset import add_files

        # Create catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_test_catalog(catalog_root)

        # Create two geo collections with different extents
        for coll_id, bbox in [
            ("parcels", [-75.5, 39.5, -75.0, 40.0]),
            ("buildings", [-75.2, 39.8, -74.5, 40.5]),
        ]:
            coll_dir = catalog_root / coll_id
            coll_dir.mkdir()
            coll_json = coll_dir / "collection.json"
            coll_data = {
                "type": "Collection",
                "stac_version": "1.0.0",
                "id": coll_id,
                "description": f"{coll_id} collection",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [bbox]},
                    "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
                },
                "links": [],
            }
            coll_json.write_text(json.dumps(coll_data, indent=2))

        # Update catalog to link to both collections
        catalog_path = catalog_root / "catalog.json"
        catalog_data = json.loads(catalog_path.read_text())
        catalog_data["links"].extend(
            [
                {"rel": "child", "href": "./parcels/collection.json", "type": "application/json"},
                {"rel": "child", "href": "./buildings/collection.json", "type": "application/json"},
            ]
        )
        catalog_path.write_text(json.dumps(catalog_data, indent=2))

        # Create tabular collection
        tabular_dir = catalog_root / "demographics"
        tabular_dir.mkdir()
        parquet_file = tabular_dir / "census.parquet"
        table = pa.table({"tract_id": ["001"], "population": [5000]})
        pq.write_table(table, parquet_file)

        config_file = catalog_root / ".portolan" / "config.yaml"
        config_file.write_text("tabular:\n  enabled: true\n")

        added, skipped, failures = add_files(
            paths=[parquet_file],
            catalog_root=catalog_root,
        )

        assert len(failures) == 0

        # Check the tabular collection's extent
        tabular_collection = tabular_dir / "collection.json"
        collection_data = json.loads(tabular_collection.read_text())
        bbox = collection_data["extent"]["spatial"]["bbox"][0]

        # Should be union: min(west), min(south), max(east), max(north)
        # Union of [-75.5, 39.5, -75.0, 40.0] and [-75.2, 39.8, -74.5, 40.5]
        # = [-75.5, 39.5, -74.5, 40.5]
        expected_union = [-75.5, 39.5, -74.5, 40.5]
        assert bbox == expected_union, f"Expected union bbox {expected_union}, got {bbox}"

    def test_tabular_collection_fallback_to_global_when_no_siblings(self, tmp_path: Path) -> None:
        """Tabular collection should fallback to global bbox when no geo siblings exist."""
        from portolan_cli.dataset import add_files

        # Create catalog structure with NO geo collections
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_test_catalog(catalog_root)

        # Create tabular collection directly (no siblings)
        tabular_dir = catalog_root / "demographics"
        tabular_dir.mkdir()
        parquet_file = tabular_dir / "census.parquet"
        table = pa.table({"tract_id": ["001"], "population": [5000]})
        pq.write_table(table, parquet_file)

        config_file = catalog_root / ".portolan" / "config.yaml"
        config_file.write_text("tabular:\n  enabled: true\n")

        added, skipped, failures = add_files(
            paths=[parquet_file],
            catalog_root=catalog_root,
        )

        assert len(failures) == 0

        # Check the tabular collection's extent
        tabular_collection = tabular_dir / "collection.json"
        collection_data = json.loads(tabular_collection.read_text())
        bbox = collection_data["extent"]["spatial"]["bbox"][0]

        # With no siblings, should fallback to global bbox
        assert bbox == [-180.0, -90.0, 180.0, 90.0]

    def test_existing_tabular_collection_keeps_its_extent(self, tmp_path: Path) -> None:
        """If tabular collection already exists, its extent should not change."""
        from portolan_cli.dataset import add_files

        # Create catalog structure
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_test_catalog(catalog_root)

        # Pre-create tabular collection with custom extent
        tabular_dir = catalog_root / "demographics"
        tabular_dir.mkdir()
        existing_collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "demographics",
            "description": "Demographics data",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-80.0, 35.0, -70.0, 45.0]]},  # Custom extent
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
            "links": [],
        }
        (tabular_dir / "collection.json").write_text(json.dumps(existing_collection, indent=2))

        # Add a new tabular file to this collection
        parquet_file = tabular_dir / "census.parquet"
        table = pa.table({"tract_id": ["001"], "population": [5000]})
        pq.write_table(table, parquet_file)

        config_file = catalog_root / ".portolan" / "config.yaml"
        config_file.write_text("tabular:\n  enabled: true\n")

        added, skipped, failures = add_files(
            paths=[parquet_file],
            catalog_root=catalog_root,
        )

        assert len(failures) == 0

        # Existing extent should be preserved
        collection_data = json.loads((tabular_dir / "collection.json").read_text())
        bbox = collection_data["extent"]["spatial"]["bbox"][0]

        assert bbox == [-80.0, 35.0, -70.0, 45.0], "Existing extent should be preserved"
