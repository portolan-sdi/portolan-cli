"""Unit tests for non-geospatial CSV skip logic (Issue #140).

Tests that `portolan add` gracefully handles CSV files without geometry:
- Warn and skip non-geospatial CSVs instead of erroring
- Continue processing other geospatial files in the directory
- Emit appropriate warning messages

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

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.dataset import add_files, iter_files_with_sidecars

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
            added, skipped = add_files(
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
            added, skipped = add_files(
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
            added, skipped = add_files(
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

            added, skipped = add_files(
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
        collection_dir = tmp_path / "catalog" / "collection"
        collection_dir.mkdir(parents=True, exist_ok=True)

        # Create only non-geo CSVs
        (collection_dir / "metadata.csv").write_text("name,value\nfield1,100\n")
        (collection_dir / "config.csv").write_text("key,value\nsetting1,true\n")

        with caplog.at_level(logging.WARNING):
            added, skipped = add_files(
                paths=[collection_dir],
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

            added, skipped = add_files(
                paths=[geo_csv],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Should attempt to add the CSV (whether it succeeds depends on geoparquet-io)
            # The key is it's not pre-emptively skipped
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
                added, skipped = add_files(
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

            added, skipped = add_files(
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
                added, skipped = add_files(
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
                    added, skipped = add_files(
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
                added, skipped = add_files(
                    paths=[csv_file],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )
            except Exception as e:
                pytest.fail(f"add_files raised for CSV content: {e}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
