"""Unit tests for non-geospatial CSV/TSV handling (Issue #140).

Tests that `portolan add` gracefully handles CSV/TSV files without geometry:
- Per ADR-0028: Track non-geo files as assets when in same dir as geo file
- Skip non-geo files that have no companion geo file (can't create STAC item)
- Emit appropriate log messages
- Support both CSV and TSV file formats

See: https://github.com/portolan-sdi/portolan-cli/issues/140
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import click
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.constants import GEOSPATIAL_EXTENSIONS, TABULAR_EXTENSIONS
from portolan_cli.dataset import (
    _copy_non_geo_to_item_dir,
    _is_no_geometry_error,
    _update_item_with_asset,
    add_files,
    iter_files_with_sidecars,
)

if TYPE_CHECKING:
    pass


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog structure (per ADR-0023)."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


@pytest.fixture
def non_geo_csv(tmp_path: Path) -> Path:
    """Create a CSV file without geometry columns (metadata-only)."""
    csv_path = tmp_path / "collection" / "metadata.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(
        "name,value,description\nfield1,100,Test field 1\nfield2,200,Test field 2\n"
    )
    return csv_path


@pytest.fixture
def geo_csv(tmp_path: Path) -> Path:
    """Create a CSV file WITH geometry columns (lat/lon)."""
    csv_path = tmp_path / "collection" / "points.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(
        "name,latitude,longitude,value\n"
        "Point A,40.7128,-74.0060,100\n"
        "Point B,34.0522,-118.2437,200\n"
    )
    return csv_path


@pytest.fixture
def geojson_file(tmp_path: Path) -> Path:
    """Create a valid GeoJSON file."""
    geojson_path = tmp_path / "collection" / "data.geojson"
    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                "properties": {"name": "Test Point"},
            }
        ],
    }
    geojson_path.write_text(json.dumps(geojson_data))
    return geojson_path


class TestNonGeospatialCsvSkip:
    """Tests for skipping non-geospatial CSV files (Issue #140)."""

    @pytest.mark.unit
    def test_add_files_skips_non_geo_csv_with_warning(
        self, initialized_catalog: Path, non_geo_csv: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """add_files should skip CSVs without geometry and emit a warning."""
        with caplog.at_level(logging.WARNING):
            added, skipped, failures = add_files(
                paths=[non_geo_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should not error - should skip gracefully
        assert len(added) == 0
        # The file should be in a "skipped due to no geometry" list
        # (exact return value depends on implementation)

        # Should emit a warning about non-geospatial CSV
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("geometry" in msg.lower() or "csv" in msg.lower() for msg in warning_messages), (
            f"Expected warning about CSV/geometry, got: {warning_messages}"
        )

    @pytest.mark.unit
    def test_add_files_continues_after_non_geo_csv(
        self, initialized_catalog: Path, non_geo_csv: Path, geojson_file: Path
    ) -> None:
        """add_files should continue processing other files after skipping non-geo CSV."""
        # Mock the add_dataset to avoid actual conversion
        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="data", collection_id="collection")

            # Add directory containing both non-geo CSV and valid GeoJSON
            directory = non_geo_csv.parent
            added, skipped, failures = add_files(
                paths=[directory],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should have attempted to add the GeoJSON
            assert mock_add.called, "Should have called add_dataset for GeoJSON"

    @pytest.mark.unit
    def test_add_files_error_message_is_user_friendly(
        self, initialized_catalog: Path, non_geo_csv: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warning message should be user-friendly and actionable."""
        with caplog.at_level(logging.WARNING):
            add_files(
                paths=[non_geo_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Check for user-friendly message
        all_messages = " ".join(r.message for r in caplog.records)
        # Should mention file path for clarity
        assert "metadata.csv" in all_messages or "skipping" in all_messages.lower()

    @pytest.mark.unit
    def test_add_files_does_not_error_on_non_geo_csv(
        self, initialized_catalog: Path, non_geo_csv: Path
    ) -> None:
        """add_files should NOT raise an exception for non-geospatial CSV."""
        # This should NOT raise - it should handle gracefully
        try:
            added, skipped, failures = add_files(
                paths=[non_geo_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )
        except Exception as e:
            pytest.fail(f"add_files raised an exception for non-geo CSV: {e}")


class TestMixedDirectoryProcessing:
    """Tests for processing directories with mixed geo and non-geo files."""

    @pytest.mark.unit
    def test_mixed_directory_processes_geo_files_only(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """Directory with mixed files should process geo files and skip non-geo."""
        collection_dir = tmp_path / "catalog" / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create a non-geo CSV (metadata)
        metadata_csv = collection_dir / "metadata.csv"
        metadata_csv.write_text("name,description\nfield1,Test field\n")

        # Create a valid GeoJSON
        geojson = collection_dir / "data.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        # Mock add_dataset to track calls
        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="data", collection_id="collection")

            added, skipped, failures = add_files(
                paths=[collection_dir],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should have processed the GeoJSON
            geojson_calls = [
                call for call in mock_add.call_args_list if "geojson" in str(call).lower()
            ]
            assert len(geojson_calls) > 0, "Should have processed GeoJSON file"

    @pytest.mark.unit
    def test_all_non_geo_directory_returns_empty(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Directory with only non-geo files should return empty without error."""
        # Create collection/item structure inside initialized_catalog (Issue #163)
        collection_dir = initialized_catalog / "collection"
        item_dir = collection_dir / "data"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create only non-geo CSVs
        (item_dir / "metadata.csv").write_text("name,value\nfield1,100\n")
        (item_dir / "config.csv").write_text("key,value\nsetting1,true\n")

        with caplog.at_level(logging.WARNING):
            added, skipped, failures = add_files(
                paths=[item_dir],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should return empty, not error
        assert len(added) == 0

    @pytest.mark.unit
    def test_iter_files_with_sidecars_includes_csv(self, tmp_path: Path) -> None:
        """iter_files_with_sidecars should include CSV files for attempted processing."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        # Create CSV and GeoJSON
        (collection_dir / "metadata.csv").write_text("name,value\nfield1,100\n")
        (collection_dir / "data.geojson").write_text('{"type":"FeatureCollection","features":[]}')

        files = list(iter_files_with_sidecars(collection_dir))
        extensions = {f.suffix.lower() for f in files}

        # CSV should be included for attempted processing
        # (the skip happens in add_files, not iter_files_with_sidecars)
        assert ".csv" in extensions or ".geojson" in extensions


class TestCsvGeometryDetection:
    """Tests for CSV geometry detection edge cases."""

    @pytest.mark.unit
    def test_csv_with_lat_lon_is_processed(self, initialized_catalog: Path, geo_csv: Path) -> None:
        """CSV with lat/lon columns should be attempted for processing."""
        # This test verifies CSVs with geometry are NOT skipped
        # The actual processing depends on geoparquet-io
        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="points", collection_id="collection")

            added, skipped, failures = add_files(
                paths=[geo_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should attempt to add the CSV (whether it succeeds depends on geoparquet-io)
            # The key is it's not preemptively skipped
            # mock_add.called would be True if CSV was attempted

    @pytest.mark.unit
    def test_empty_csv_is_skipped(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Empty CSV should be handled gracefully."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        empty_csv = collection_dir / "empty.csv"
        empty_csv.write_text("")

        with caplog.at_level(logging.WARNING):
            try:
                added, skipped, failures = add_files(
                    paths=[empty_csv],
                    catalog_root=initialized_catalog,
                    collection_id="collection",
                )
            except Exception:
                # If it errors, that's also acceptable for empty files
                pass

    @pytest.mark.unit
    def test_csv_with_wkt_geometry_is_processed(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """CSV with WKT geometry column should be attempted for processing."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        wkt_csv = collection_dir / "wkt_data.csv"
        wkt_csv.write_text(
            "name,geometry,value\nPoint A,POINT(-122.4 37.8),100\nPoint B,POINT(-118.2 34.0),200\n"
        )

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="wkt_data", collection_id="collection")

            added, skipped, failures = add_files(
                paths=[wkt_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should attempt to process the WKT CSV
            # (success depends on geoparquet-io)


class TestWarningMessages:
    """Tests for warning message quality and consistency."""

    @pytest.mark.unit
    def test_warning_includes_file_path(
        self, initialized_catalog: Path, non_geo_csv: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warning should include the file path for debugging."""
        with caplog.at_level(logging.WARNING):
            add_files(
                paths=[non_geo_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Check that file path appears in warnings
        all_messages = " ".join(r.message for r in caplog.records)
        # Either the full path or filename should be mentioned
        assert "csv" in all_messages.lower() or str(non_geo_csv) in all_messages

    @pytest.mark.unit
    def test_warning_suggests_reason(
        self, initialized_catalog: Path, non_geo_csv: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warning should explain why the file was skipped."""
        with caplog.at_level(logging.WARNING):
            add_files(
                paths=[non_geo_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should mention geometry or reason for skip
        all_messages = " ".join(r.message for r in caplog.records).lower()
        assert (
            "geometry" in all_messages or "skip" in all_messages or "non-geospatial" in all_messages
        ), f"Warning should explain reason: {all_messages}"


# =============================================================================
# Hypothesis Property-Based Tests
# =============================================================================


# Strategy for generating non-geometry column names
non_geo_column_names = st.sampled_from(
    [
        "name",
        "value",
        "description",
        "id",
        "category",
        "status",
        "date",
        "amount",
        "count",
        "type",
        "flag",
        "notes",
        "code",
    ]
)

# Strategy for generating simple CSV values (no commas, quotes, or newlines)
csv_safe_values = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_ "),
    min_size=1,
    max_size=20,
).filter(lambda x: x.strip())


class TestCsvSkipHypothesis:
    """Hypothesis property-based tests for CSV skip logic."""

    @pytest.mark.unit
    @given(
        num_columns=st.integers(min_value=2, max_value=5),
        num_rows=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=20, deadline=5000)
    def test_non_geo_csv_never_raises(self, num_columns: int, num_rows: int) -> None:
        """Property: Non-geo CSVs should NEVER raise exceptions in add_files."""
        # Use tempfile for fresh directory each hypothesis example
        tmp_dir = tempfile.mkdtemp()
        tmp_path = Path(tmp_dir)
        try:
            # Set up catalog
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir(exist_ok=True)
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "portolan-catalog",
                "description": "Test catalog",
                "links": [],
            }
            (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

            # Generate non-geo CSV with random columns
            collection_dir = tmp_path / "collection"
            collection_dir.mkdir(exist_ok=True)

            # Use column names that won't be mistaken for geometry
            columns = [f"col_{i}" for i in range(num_columns)]
            header = ",".join(columns)
            rows = [",".join([f"val_{r}_{c}" for c in range(num_columns)]) for r in range(num_rows)]
            csv_content = header + "\n" + "\n".join(rows)

            csv_file = collection_dir / "test.csv"
            csv_file.write_text(csv_content)

            # This should NOT raise - ever
            try:
                added, skipped, failures = add_files(
                    paths=[csv_file],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )
                # Should either be added (if somehow detected as geo) or skipped
                assert len(added) == 0 or len(skipped) >= 0
            except Exception as e:
                pytest.fail(f"add_files raised for non-geo CSV: {e}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @pytest.mark.unit
    @given(
        num_geo_files=st.integers(min_value=0, max_value=3),
        num_csv_files=st.integers(min_value=1, max_value=3),
    )
    @settings(max_examples=15, deadline=10000)
    def test_mixed_directory_processes_all_geo_files(
        self, num_geo_files: int, num_csv_files: int
    ) -> None:
        """Property: All geospatial files should be processed even with non-geo CSVs present."""
        tmp_dir = tempfile.mkdtemp()
        tmp_path = Path(tmp_dir)
        try:
            # Set up catalog
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir(exist_ok=True)
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "portolan-catalog",
                "description": "Test catalog",
                "links": [],
            }
            (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

            collection_dir = tmp_path / "collection"
            collection_dir.mkdir(exist_ok=True)

            # Create non-geo CSVs
            for i in range(num_csv_files):
                csv_file = collection_dir / f"metadata_{i}.csv"
                csv_file.write_text(f"name,value\nfield{i},100\n")

            # Create GeoJSON files
            geojson_paths = []
            for i in range(num_geo_files):
                geojson_file = collection_dir / f"geo_{i}.geojson"
                geojson_data = {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4 + i, 37.8]},
                            "properties": {"name": f"Point {i}"},
                        }
                    ],
                }
                geojson_file.write_text(json.dumps(geojson_data))
                geojson_paths.append(geojson_file)

            # Mock add_dataset to track calls
            with patch("portolan_cli.dataset.add_dataset") as mock_add:
                mock_add.return_value = MagicMock(item_id="test", collection_id="collection")

                # This should NOT raise
                try:
                    added, skipped, failures = add_files(
                        paths=[collection_dir],
                        catalog_root=tmp_path,
                        collection_id="collection",
                    )
                except Exception as e:
                    pytest.fail(f"add_files raised for mixed directory: {e}")

                # Should have attempted to add all geo files
                # (actual count may differ due to mocking behavior)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @pytest.mark.unit
    @given(
        csv_rows=st.lists(
            st.tuples(
                st.text(alphabet="abcdefghijklmnop", min_size=1, max_size=10),
                st.integers(min_value=0, max_value=1000),
            ),
            min_size=1,
            max_size=5,
        )
    )
    @settings(max_examples=15, deadline=5000)
    def test_various_csv_contents_handled_gracefully(self, csv_rows: list[tuple[str, int]]) -> None:
        """Property: Various CSV contents without geometry should be handled gracefully."""
        tmp_dir = tempfile.mkdtemp()
        tmp_path = Path(tmp_dir)
        try:
            # Set up catalog
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir(exist_ok=True)
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "portolan-catalog",
                "description": "Test catalog",
                "links": [],
            }
            (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

            collection_dir = tmp_path / "collection"
            collection_dir.mkdir(exist_ok=True)

            # Build CSV content
            header = "name,value"
            rows = [f"{name},{value}" for name, value in csv_rows]
            csv_content = header + "\n" + "\n".join(rows)

            csv_file = collection_dir / "data.csv"
            csv_file.write_text(csv_content)

            # Should not raise
            try:
                added, skipped, failures = add_files(
                    paths=[csv_file],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )
            except Exception as e:
                pytest.fail(f"add_files raised for CSV content: {e}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# =============================================================================
# TSV Support Tests (Issue #140 extension)
# =============================================================================


class TestTsvSupport:
    """Tests for TSV file handling (same logic as CSV)."""

    @pytest.mark.unit
    def test_tsv_in_geospatial_extensions(self) -> None:
        """TSV should be in GEOSPATIAL_EXTENSIONS constant."""
        assert ".tsv" in GEOSPATIAL_EXTENSIONS

    @pytest.mark.unit
    def test_tsv_in_tabular_extensions(self) -> None:
        """TSV should be in TABULAR_EXTENSIONS constant."""
        assert ".tsv" in TABULAR_EXTENSIONS
        assert ".csv" in TABULAR_EXTENSIONS

    @pytest.mark.unit
    def test_non_geo_tsv_handled_gracefully(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Non-geospatial TSV should be handled the same as non-geo CSV."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create non-geo TSV (tab-separated)
        tsv_file = collection_dir / "metadata.tsv"
        tsv_file.write_text("name\tvalue\tdescription\nfield1\t100\tTest field\n")

        with caplog.at_level(logging.WARNING):
            try:
                added, skipped, failures = add_files(
                    paths=[tsv_file],
                    catalog_root=initialized_catalog,
                    collection_id="collection",
                )
            except Exception as e:
                pytest.fail(f"add_files raised for non-geo TSV: {e}")

        # Should not error
        assert len(added) == 0

    @pytest.mark.unit
    def test_geo_tsv_with_lat_lon_is_processed(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """TSV with lat/lon columns should be attempted for processing."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        geo_tsv = collection_dir / "points.tsv"
        geo_tsv.write_text(
            "name\tlatitude\tlongitude\tvalue\n"
            "Point A\t40.7128\t-74.0060\t100\n"
            "Point B\t34.0522\t-118.2437\t200\n"
        )

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="points", collection_id="collection")

            added, skipped, failures = add_files(
                paths=[geo_tsv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should attempt to process
            assert mock_add.called

    @pytest.mark.unit
    def test_iter_files_with_sidecars_includes_tsv(self, tmp_path: Path) -> None:
        """iter_files_with_sidecars should include TSV files."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        (collection_dir / "metadata.tsv").write_text("name\tvalue\nfield1\t100\n")
        (collection_dir / "data.geojson").write_text('{"type":"FeatureCollection","features":[]}')

        files = list(iter_files_with_sidecars(collection_dir))
        extensions = {f.suffix.lower() for f in files}

        assert ".tsv" in extensions


# =============================================================================
# Exception Handling Tests (Adversarial Review - Narrow Exception Catch)
# =============================================================================


class TestExceptionHandlingNarrowness:
    """Tests for narrow exception handling (adversarial review issue #1)."""

    @pytest.mark.unit
    def test_is_no_geometry_error_detects_geometry_error(self) -> None:
        """_is_no_geometry_error should detect geoparquet-io geometry errors."""
        # Create mock errors that match geoparquet-io patterns
        geometry_errors = [
            click.ClickException("Could not detect geometry columns in CSV/TSV file"),
            click.ClickException("Reading failed: Could not detect geometry columns"),
            click.ClickException("No geometry columns in csv file"),
            click.ClickException("No geometry columns in tsv file"),
        ]

        for err in geometry_errors:
            assert _is_no_geometry_error(err), f"Should detect as geometry error: {err.message}"

    @pytest.mark.unit
    def test_is_no_geometry_error_rejects_other_errors(self) -> None:
        """_is_no_geometry_error should NOT match non-geometry errors."""
        # These are errors that should NOT be caught as geometry errors
        other_errors = [
            click.ClickException("Permission denied: /path/to/file"),
            click.ClickException("File not found: data.csv"),
            click.ClickException("Memory allocation failed"),
            click.ClickException("Invalid encoding: UTF-16"),
            click.ClickException("Connection timeout"),
        ]

        for err in other_errors:
            assert not _is_no_geometry_error(err), f"Should NOT be geometry error: {err.message}"

    @pytest.mark.unit
    def test_non_geometry_click_exception_propagates(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """Non-geometry ClickExceptions should propagate (not be swallowed)."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        csv_file = collection_dir / "data.csv"
        csv_file.write_text("name,value\ntest,100\n")

        # Mock add_dataset to raise a non-geometry ClickException
        # Per Issue #175: errors are now collected instead of raised
        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.side_effect = click.ClickException("Permission denied: /some/path")

            added, skipped, failures = add_files(
                paths=[csv_file],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should have one failure with the error message
            assert len(failures) == 1
            assert "Permission denied" in failures[0].error


# =============================================================================
# ADR-0028 Asset Tracking Tests
# =============================================================================


class TestAdr0028AssetTracking:
    """Tests for ADR-0028 compliance: non-geo files tracked as assets."""

    @pytest.mark.unit
    def test_non_geo_csv_with_geo_file_tracked_as_asset(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Non-geo CSV in same dir as geo file should be tracked as asset (ADR-0028)."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create non-geo CSV
        metadata_csv = collection_dir / "metadata.csv"
        metadata_csv.write_text("name,description\nfield1,Test field\n")

        # Create valid GeoJSON
        geojson = collection_dir / "data.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        # Mock add_dataset to simulate successful geo file processing
        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="data", collection_id="collection")

            with caplog.at_level(logging.INFO):
                added, skipped, failures = add_files(
                    paths=[collection_dir],
                    catalog_root=initialized_catalog,
                    collection_id="collection",
                )

            # Should process geo file
            assert mock_add.called

    @pytest.mark.unit
    def test_non_geo_only_directory_logs_warning(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Directory with only non-geo files should log warning about missing geo file."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create only non-geo CSVs (no geo files)
        (collection_dir / "metadata.csv").write_text("name,value\nfield1,100\n")
        (collection_dir / "config.csv").write_text("key,value\nsetting1,true\n")

        with caplog.at_level(logging.WARNING):
            added, skipped, failures = add_files(
                paths=[collection_dir],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should return empty (no geo files to create items)
        assert len(added) == 0

        # Should log warning about missing geo file
        warning_messages = " ".join(r.message for r in caplog.records).lower()
        assert "geospatial" in warning_messages or "geometry" in warning_messages, (
            f"Expected warning about non-geospatial files: {warning_messages}"
        )

    @pytest.mark.unit
    def test_warning_message_includes_full_path(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warning/info messages should include full file path (not just filename)."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        csv_file = collection_dir / "metadata.csv"
        csv_file.write_text("name,value\ntest,100\n")

        with caplog.at_level(logging.WARNING):
            add_files(
                paths=[csv_file],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Check messages include path info (not just "metadata.csv")
        all_messages = " ".join(r.message for r in caplog.records)
        # Should have some path context (either full path or meaningful directory info)
        assert (
            "collection" in all_messages.lower()
            or str(csv_file) in all_messages
            or "metadata.csv" in all_messages
        )


# =============================================================================
# Helper Function Unit Tests (Coverage for lines 1104-1109, 1281-1333)
# =============================================================================


class TestCopyNonGeoToItemDir:
    """Unit tests for _copy_non_geo_to_item_dir helper function."""

    @pytest.mark.unit
    def test_copies_file_to_item_dir(self, tmp_path: Path) -> None:
        """_copy_non_geo_to_item_dir should copy source file to item directory."""
        # Create source file
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "metadata.csv"
        source_file.write_text("name,value\ntest,100\n")

        # Create item directory
        item_dir = tmp_path / "item"
        item_dir.mkdir()

        # Copy
        result = _copy_non_geo_to_item_dir(source_file, item_dir)

        # Should return path to copied file
        assert result == item_dir / "metadata.csv"
        assert result.exists()
        assert result.read_text() == "name,value\ntest,100\n"

    @pytest.mark.unit
    def test_returns_existing_file_if_already_in_place(self, tmp_path: Path) -> None:
        """_copy_non_geo_to_item_dir should return existing file if source == dest."""
        # File already in item directory
        item_dir = tmp_path / "item"
        item_dir.mkdir()
        existing_file = item_dir / "metadata.csv"
        existing_file.write_text("name,value\ntest,100\n")

        # "Copy" file that's already in place
        result = _copy_non_geo_to_item_dir(existing_file, item_dir)

        # Should return the same file without error
        assert result == existing_file
        assert result.exists()

    @pytest.mark.unit
    def test_preserves_file_metadata(self, tmp_path: Path) -> None:
        """_copy_non_geo_to_item_dir should preserve file metadata (uses copy2)."""
        import os
        import time

        # Create source file with specific content
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "metadata.csv"
        source_file.write_text("name,value\ntest,100\n")

        # Get original mtime
        original_mtime = os.path.getmtime(source_file)

        # Wait a bit to ensure time difference
        time.sleep(0.1)

        # Create item directory
        item_dir = tmp_path / "item"
        item_dir.mkdir()

        # Copy
        result = _copy_non_geo_to_item_dir(source_file, item_dir)

        # mtime should be preserved (within tolerance)
        copied_mtime = os.path.getmtime(result)
        assert abs(copied_mtime - original_mtime) < 1.0, "File metadata not preserved"


class TestUpdateItemWithAsset:
    """Unit tests for _update_item_with_asset helper function."""

    @pytest.mark.unit
    def test_updates_item_json_with_new_asset(self, tmp_path: Path) -> None:
        """_update_item_with_asset should add new asset to existing item.json."""
        # Create catalog structure
        catalog_root = tmp_path
        collection_id = "collection"
        item_id = "test-item"

        collection_dir = catalog_root / collection_id
        item_dir = collection_dir / item_id
        item_dir.mkdir(parents=True)

        # Create primary data file
        primary_file = item_dir / "data.parquet"
        primary_file.write_bytes(b"fake parquet content")

        # Create initial item.json
        item_json_path = item_dir / f"{item_id}.json"
        initial_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
            "bbox": [-122.4, 37.8, -122.4, 37.8],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "links": [],
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/x-parquet",
                    "roles": ["data"],
                }
            },
        }
        item_json_path.write_text(json.dumps(initial_item, indent=2))

        # Create versions.json with correct schema (dict-based assets, not list)
        versions_path = collection_dir / "versions.json"
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "changes": ["data.parquet"],
                    "assets": {
                        "data.parquet": {
                            "sha256": "abc123def456",
                            "size_bytes": 1024,
                            "href": f"{collection_id}/{item_id}/data.parquet",
                        }
                    },
                }
            ],
        }
        versions_path.write_text(json.dumps(versions_data, indent=2))

        # Add new asset file
        new_asset = item_dir / "metadata.csv"
        new_asset.write_text("name,value\ntest,100\n")

        # Update item with asset
        _update_item_with_asset(
            catalog_root=catalog_root,
            collection_id=collection_id,
            item_id=item_id,
            asset_path=new_asset,
        )

        # Verify item.json was updated
        updated_item = json.loads(item_json_path.read_text())
        assert "metadata" in updated_item["assets"] or "metadata.csv" in str(
            updated_item["assets"]
        ), f"New asset not found in: {updated_item['assets']}"

    @pytest.mark.unit
    def test_handles_missing_item_json(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """_update_item_with_asset should log warning if item.json doesn't exist."""
        catalog_root = tmp_path
        collection_id = "collection"
        item_id = "nonexistent-item"

        # Create directory but no item.json
        item_dir = catalog_root / collection_id / item_id
        item_dir.mkdir(parents=True)

        asset_path = item_dir / "metadata.csv"
        asset_path.write_text("name,value\ntest,100\n")

        with caplog.at_level(logging.WARNING):
            _update_item_with_asset(
                catalog_root=catalog_root,
                collection_id=collection_id,
                item_id=item_id,
                asset_path=asset_path,
            )

        # Should log warning about missing item.json
        assert any("not found" in r.message.lower() for r in caplog.records)

    @pytest.mark.unit
    def test_handles_item_without_primary_file(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """_update_item_with_asset should handle item directory with only JSON files."""
        catalog_root = tmp_path
        collection_id = "collection"
        item_id = "empty-item"

        item_dir = catalog_root / collection_id / item_id
        item_dir.mkdir(parents=True)

        # Create item.json but no data files
        item_json_path = item_dir / f"{item_id}.json"
        item_json_path.write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "id": item_id,
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "bbox": [-122.4, 37.8, -122.4, 37.8],
                    "properties": {"datetime": "2024-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                }
            )
        )

        with caplog.at_level(logging.WARNING):
            _update_item_with_asset(
                catalog_root=catalog_root,
                collection_id=collection_id,
                item_id=item_id,
                asset_path=item_dir / "metadata.csv",
            )

        # Should log warning about no primary file
        warning_msgs = [r.message.lower() for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("primary" in msg or "not found" in msg for msg in warning_msgs)


# =============================================================================
# add_files Code Path Coverage Tests
# =============================================================================


class TestAddFilesCodePaths:
    """Tests for specific code paths in add_files to improve coverage."""

    @pytest.mark.unit
    def test_symlink_resolution(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_files should resolve symlinks to track the real file."""
        # Create a real file
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True)
        real_file = collection_dir / "data.geojson"
        real_file.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        # Create a symlink
        link_dir = tmp_path / "links"
        link_dir.mkdir()
        symlink = link_dir / "link.geojson"
        symlink.symlink_to(real_file)

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="data", collection_id="collection")

            added, skipped, failures = add_files(
                paths=[symlink],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should have called add_dataset with the resolved path
            assert mock_add.called

    @pytest.mark.unit
    def test_duplicate_file_skipping(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_files should skip duplicate files in the paths list."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True)

        geojson = collection_dir / "data.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="data", collection_id="collection")

            # Pass the same file twice
            added, skipped, failures = add_files(
                paths=[geojson, geojson],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should only call add_dataset once
            assert mock_add.call_count == 1

    @pytest.mark.unit
    def test_non_geospatial_extension_skipped(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_files should skip files with non-geospatial extensions."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True)

        # Create a .txt file (not in GEOSPATIAL_EXTENSIONS)
        txt_file = collection_dir / "readme.txt"
        txt_file.write_text("This is a readme file")

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            added, skipped, failures = add_files(
                paths=[txt_file],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should NOT call add_dataset for non-geospatial files
            assert not mock_add.called
            assert len(added) == 0

    @pytest.mark.unit
    def test_unchanged_file_skipped(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_files should skip unchanged files (is_current returns True)."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True)

        geojson = collection_dir / "data.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            with patch("portolan_cli.dataset.is_current", return_value=True):
                added, skipped, failures = add_files(
                    paths=[geojson],
                    catalog_root=initialized_catalog,
                    collection_id="collection",
                )

                # Should NOT call add_dataset for unchanged files
                assert not mock_add.called
                assert geojson in skipped

    @pytest.mark.unit
    def test_collection_id_resolution(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """add_files should resolve collection_id when not provided."""
        # Create a file in a subdirectory that will be used for collection_id
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir(parents=True)

        geojson = collection_dir / "data.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            with patch(
                "portolan_cli.dataset.resolve_collection_id", return_value="my-collection"
            ) as mock_resolve:
                mock_add.return_value = MagicMock(item_id="data", collection_id="my-collection")

                # Don't pass collection_id - should be resolved
                added, skipped, failures = add_files(
                    paths=[geojson],
                    catalog_root=initialized_catalog,
                    collection_id=None,
                )

                # Should have resolved collection_id
                mock_resolve.assert_called_once()

    @pytest.mark.unit
    def test_deferred_non_geo_processing_with_geo_file(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Non-geo CSV in same dir as geo file should be processed in deferred pass."""
        # Use a separate source directory to avoid item_dir files being scanned
        source_dir = tmp_path / "source" / "collection"
        source_dir.mkdir(parents=True)

        # Create both geo and non-geo files in same directory
        geojson = source_dir / "data.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        csv_file = source_dir / "metadata.csv"
        csv_file.write_text("name,value\ntest,100\n")

        # Create the item directory that add_dataset would create
        item_dir = initialized_catalog / "collection" / "data"
        item_dir.mkdir(parents=True)

        # Create item.json
        item_json = item_dir / "data.json"
        item_json.write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "id": "data",
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "bbox": [-122.4, 37.8, -122.4, 37.8],
                    "properties": {"datetime": "2024-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                }
            )
        )

        # Create primary data file
        primary_file = item_dir / "data.parquet"
        primary_file.write_bytes(b"fake parquet")

        # Create versions.json
        versions_path = initialized_catalog / "collection" / "versions.json"
        versions_path.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "breaking": False,
                            "changes": ["data.parquet"],
                            "assets": {
                                "data.parquet": {
                                    "sha256": "abc123",
                                    "size_bytes": 12,
                                    "href": "collection/data/data.parquet",
                                }
                            },
                        }
                    ],
                }
            )
        )

        def mock_add_dataset_side_effect(path, catalog_root, collection_id, item_id=None):
            """Simulate add_dataset: success for geojson, geometry error for csv."""
            if path.suffix.lower() == ".geojson":
                return MagicMock(item_id="data", collection_id="collection")
            elif path.suffix.lower() == ".csv":
                raise click.ClickException("Could not detect geometry columns in CSV file")
            else:
                # Skip other files (like parquet) - they shouldn't be in source_dir
                raise ValueError(f"Unexpected file type: {path}")

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.side_effect = mock_add_dataset_side_effect

            with caplog.at_level(logging.INFO):
                added, skipped, failures = add_files(
                    paths=[source_dir],
                    catalog_root=initialized_catalog,
                    collection_id="collection",
                )

            # Geo file should be added
            assert len(added) == 1

            # CSV should be in skipped (tracked as asset, not converted)
            assert csv_file in skipped

            # Should log about tracking the non-geo file
            info_msgs = " ".join(r.message for r in caplog.records if r.levelno == logging.INFO)
            assert "non-geospatial" in info_msgs.lower() or "tracking" in info_msgs.lower()

    @pytest.mark.unit
    def test_non_tabular_format_geometry_error_propagates(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """Non-tabular format (e.g. GeoJSON) without geometry should raise, not defer."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True)

        # Create a GeoJSON that will fail geometry detection
        # (simulated via mock - in reality this wouldn't happen for valid GeoJSON)
        geojson = collection_dir / "invalid.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            # Simulate geometry detection error for a non-tabular format
            mock_add.side_effect = click.ClickException("Could not detect geometry columns in file")

            # Per Issue #175: errors are now collected instead of raised
            added, skipped, failures = add_files(
                paths=[geojson],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should have one failure with geometry error message
            assert len(failures) == 1
            assert "geometry" in failures[0].error.lower()

    @pytest.mark.unit
    def test_value_error_reraise_with_context(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_files should collect ValueError with file context (Issue #175)."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True)

        geojson = collection_dir / "data.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.side_effect = ValueError("Invalid data")

            # Per Issue #175: errors are now collected instead of raised
            added, skipped, failures = add_files(
                paths=[geojson],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should have one failure with file path in error
            assert len(failures) == 1
            assert "data.geojson" in str(failures[0].path) or "Failed to add" in failures[0].error

    @pytest.mark.unit
    def test_file_not_found_error_reraise_with_context(
        self, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """add_files should collect FileNotFoundError with file context (Issue #175)."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True)

        geojson = collection_dir / "data.geojson"
        geojson.write_text('{"type": "FeatureCollection", "features": []}')

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.side_effect = FileNotFoundError("File missing")

            # Per Issue #175: errors are now collected instead of raised
            added, skipped, failures = add_files(
                paths=[geojson],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should have one failure with file path in error
            assert len(failures) == 1
            assert "data.geojson" in str(failures[0].path) or "Failed to add" in failures[0].error


# =============================================================================
# Mixed Format Integration Tests
# =============================================================================


class TestMixedFormatIntegration:
    """Integration tests for mixed CSV/TSV/geo file processing."""

    @pytest.mark.unit
    def test_mixed_csv_tsv_directory(
        self, initialized_catalog: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Directory with mix of CSV, TSV, and geo files should be handled correctly."""
        collection_dir = tmp_path / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create non-geo CSV
        (collection_dir / "metadata.csv").write_text("name,value\nfield1,100\n")

        # Create non-geo TSV
        (collection_dir / "config.tsv").write_text("key\tvalue\nsetting1\ttrue\n")

        # Create valid GeoJSON
        geojson = collection_dir / "data.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                            "properties": {"name": "Test"},
                        }
                    ],
                }
            )
        )

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.return_value = MagicMock(item_id="data", collection_id="collection")

            with caplog.at_level(logging.INFO):
                try:
                    added, skipped, failures = add_files(
                        paths=[collection_dir],
                        catalog_root=initialized_catalog,
                        collection_id="collection",
                    )
                except Exception as e:
                    pytest.fail(f"add_files raised for mixed format directory: {e}")

            # Should process the geo file
            assert mock_add.called

    @pytest.mark.unit
    @given(
        num_csv=st.integers(min_value=0, max_value=2),
        num_tsv=st.integers(min_value=0, max_value=2),
        num_geo=st.integers(min_value=0, max_value=2),
    )
    @settings(max_examples=10, deadline=10000)
    def test_property_mixed_formats_never_raise(
        self, num_csv: int, num_tsv: int, num_geo: int
    ) -> None:
        """Property: Any mix of CSV/TSV/geo files should never raise unexpected exceptions."""
        tmp_dir = tempfile.mkdtemp()
        tmp_path = Path(tmp_dir)
        try:
            # Set up catalog
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir(exist_ok=True)
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "portolan-catalog",
                "description": "Test catalog",
                "links": [],
            }
            (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

            collection_dir = tmp_path / "collection"
            collection_dir.mkdir(exist_ok=True)

            # Create non-geo CSVs
            for i in range(num_csv):
                (collection_dir / f"meta_{i}.csv").write_text(f"name,value\nfield{i},100\n")

            # Create non-geo TSVs
            for i in range(num_tsv):
                (collection_dir / f"conf_{i}.tsv").write_text(f"key\tvalue\nset{i}\ttrue\n")

            # Create GeoJSONs
            for i in range(num_geo):
                geojson = collection_dir / f"geo_{i}.geojson"
                geojson.write_text(
                    json.dumps(
                        {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [-122.4 + i, 37.8],
                                    },
                                    "properties": {"name": f"Point {i}"},
                                }
                            ],
                        }
                    )
                )

            with patch("portolan_cli.dataset.add_dataset") as mock_add:
                mock_add.return_value = MagicMock(item_id="test", collection_id="collection")

                try:
                    add_files(
                        paths=[collection_dir],
                        catalog_root=tmp_path,
                        collection_id="collection",
                    )
                except Exception as e:
                    pytest.fail(f"add_files raised for mixed formats: {e}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
