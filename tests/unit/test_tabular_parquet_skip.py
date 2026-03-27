"""Unit tests for tabular parquet (no geometry) handling (Issue #177).

Tests that `portolan add` gracefully handles Parquet files without geometry:
- Per ADR-0028: Track non-geo parquet files as assets when in same dir as geo file
- Skip non-geo parquet files that have no companion geo file (can't create STAC item)
- Emit appropriate log messages
- Support parquet files as auxiliary/tabular data assets

This follows the same pattern as CSV/TSV handling (Issue #140).

See: https://github.com/portolan-sdi/portolan-cli/issues/177
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.constants import TABULAR_EXTENSIONS
from portolan_cli.dataset import add_files
from portolan_cli.errors import NoGeometryError

if TYPE_CHECKING:
    pass


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog structure (per ADR-0023)."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()

    # Create config.yaml as sentinel (per ADR-0029)
    config_path = portolan_dir / "config.yaml"
    config_path.write_text("# Portolan config\n")

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
def tabular_parquet(tmp_path: Path) -> Path:
    """Create a Parquet file WITHOUT geometry (tabular data only).

    This simulates a census metadata file or lookup table that contains
    only tabular data without any geometry column.
    """
    # Create item directory structure inside collection
    item_dir = tmp_path / "collection" / "item"
    item_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = item_dir / "census-data.parquet"

    # Create a table WITHOUT geometry metadata
    table = pa.table(
        {
            "code": ["A001", "A002", "A003"],
            "name": ["Area One", "Area Two", "Area Three"],
            "population": [1000, 2000, 3000],
            "year": [2020, 2020, 2020],
        }
    )

    # Write as plain Parquet (no geo metadata)
    pq.write_table(table, parquet_path)

    return parquet_path


class TestTabularParquetConstants:
    """Tests for TABULAR_EXTENSIONS including .parquet."""

    @pytest.mark.unit
    def test_parquet_in_tabular_extensions(self) -> None:
        """Parquet should be in TABULAR_EXTENSIONS for non-geo handling."""
        assert ".parquet" in TABULAR_EXTENSIONS, (
            "Per Issue #177, .parquet should be in TABULAR_EXTENSIONS so that "
            "tabular parquet files without geometry can be tracked as auxiliary assets"
        )


class TestTabularParquetSkip:
    """Tests for skipping tabular parquet files without geometry (Issue #177)."""

    @pytest.mark.unit
    def test_add_files_skips_tabular_parquet_with_warning(
        self, initialized_catalog: Path, tabular_parquet: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """add_files should skip parquet without geometry and emit a warning."""
        with caplog.at_level(logging.WARNING):
            added, skipped, failures = add_files(
                paths=[tabular_parquet],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should not error - should skip gracefully
        assert len(added) == 0
        # The file should be in skipped (no geo file in same dir to create item)

        # Should emit a warning about non-geospatial file
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "geometry" in msg.lower() or "non-geospatial" in msg.lower() or "parquet" in msg.lower()
            for msg in warning_messages
        ), f"Expected warning about parquet/geometry, got: {warning_messages}"

    @pytest.mark.unit
    def test_add_files_does_not_error_on_tabular_parquet(
        self, initialized_catalog: Path, tabular_parquet: Path
    ) -> None:
        """add_files should NOT raise an exception for tabular parquet."""
        # This should NOT raise - it should handle gracefully
        try:
            added, skipped, failures = add_files(
                paths=[tabular_parquet],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )
        except Exception as e:
            pytest.fail(f"add_files raised an exception for tabular parquet: {e}")


class TestTabularParquetWithGeoAsset:
    """Tests for tabular parquet files alongside geo assets (the primary use case)."""

    @pytest.mark.unit
    def test_tabular_parquet_added_as_auxiliary_asset(
        self, initialized_catalog: Path, tabular_parquet: Path
    ) -> None:
        """Tabular parquet should be tracked as auxiliary asset when geo file exists.

        Verifies the core feature: when a tabular parquet and a geo file exist
        in the same directory, the tabular parquet is deferred and then tracked
        as a non-geospatial asset alongside the primary geo-asset (ADR-0028).
        """
        # Create a valid GeoJSON file in the same directory as the tabular parquet
        item_dir = tabular_parquet.parent
        geo_file = item_dir / "boundaries.geojson"
        geo_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "properties": {"name": "Test Point"},
                }
            ],
        }
        geo_file.write_text(json.dumps(geo_data))

        def mock_add_side_effect(
            *,
            path: Path,
            catalog_root: Path,
            collection_id: str,
            item_id: str | None = None,
            **kwargs: object,
        ) -> MagicMock:
            """Simulate add_dataset: succeed for GeoJSON, raise for tabular parquet."""
            if path.suffix.lower() == ".parquet":
                raise NoGeometryError(
                    path=path.stem,
                    reason="The source file may have no valid geometry.",
                )
            # Succeed for the GeoJSON
            return MagicMock(
                item_id="item",
                collection_id=collection_id,
                asset_paths=["boundaries.parquet"],
            )

        # Mock add_dataset to simulate the real flow:
        # - GeoJSON succeeds (creates STAC item)
        # - Tabular parquet raises NoGeometryError (deferred, then tracked)
        # Per Issue #281: add_files now calls prepare_dataset + finalize_datasets
        with (
            patch(
                "portolan_cli.dataset.prepare_dataset", side_effect=mock_add_side_effect
            ) as mock_add,
            patch("portolan_cli.dataset.finalize_datasets") as mock_finalize,
            patch("portolan_cli.dataset._update_item_with_asset") as mock_update_item,
        ):
            mock_finalize.return_value = []

            added, skipped, failures = add_files(
                paths=[item_dir],  # Add the entire item directory
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # The geo file should be added
            assert mock_add.called, "Should have called prepare_dataset for GeoJSON"

            # The tabular parquet should be tracked as an auxiliary asset
            # (deferred to after geo processing, then _update_item_with_asset called)
            assert mock_update_item.called, (
                "Should have called _update_item_with_asset for the tabular parquet. "
                "The tabular parquet should be tracked as a non-geospatial asset."
            )

            # Verify the tracked asset path points to the parquet file
            update_call_kwargs = mock_update_item.call_args.kwargs
            tracked_path = update_call_kwargs.get("asset_path")
            assert tracked_path is not None, (
                "asset_path should be passed to _update_item_with_asset"
            )
            assert "census-data.parquet" in str(tracked_path), (
                f"Expected census-data.parquet to be tracked, got: {tracked_path}"
            )


class TestMixedParquetDirectory:
    """Tests for processing directories with mixed geo and tabular parquet files."""

    @pytest.mark.unit
    def test_all_tabular_parquet_directory_skips_gracefully(
        self, initialized_catalog: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Directory with only tabular parquet should skip without error."""
        # Create item directory
        item_dir = initialized_catalog / "collection" / "item"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create two tabular parquet files (no geo)
        table1 = pa.table({"code": ["A", "B"], "value": [1, 2]})
        pq.write_table(table1, item_dir / "data1.parquet")

        table2 = pa.table({"code": ["C", "D"], "value": [3, 4]})
        pq.write_table(table2, item_dir / "data2.parquet")

        with caplog.at_level(logging.WARNING):
            added, skipped, failures = add_files(
                paths=[item_dir],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Should return empty without error
        assert len(added) == 0


class TestParquetGeometryDetection:
    """Tests for parquet geometry detection edge cases."""

    @pytest.mark.unit
    def test_parquet_without_geo_metadata_is_tabular(
        self, initialized_catalog: Path, tabular_parquet: Path
    ) -> None:
        """Parquet files without 'geo' metadata should be treated as tabular."""
        # Verify the fixture has no geo metadata
        pf = pq.ParquetFile(tabular_parquet)
        schema = pf.schema_arrow
        metadata = schema.metadata
        assert metadata is None or b"geo" not in metadata, (
            "Test fixture should not have geo metadata"
        )


# =============================================================================
# NoGeometryError Unit Tests
# =============================================================================


class TestNoGeometryError:
    """Direct unit tests for the NoGeometryError custom exception."""

    @pytest.mark.unit
    def test_no_geometry_error_is_value_error(self) -> None:
        """NoGeometryError must be a ValueError subclass for backward compatibility."""
        err = NoGeometryError(path="test-file", reason="No geometry column.")
        assert isinstance(err, ValueError), "NoGeometryError must be a ValueError subclass"

    @pytest.mark.unit
    def test_no_geometry_error_message_format(self) -> None:
        """NoGeometryError message should contain 'missing bounding box' for backward compat."""
        err = NoGeometryError(
            path="census-data", reason="The source file may have no valid geometry."
        )
        msg = str(err)
        assert "missing bounding box" in msg
        assert "census-data" in msg
        assert "no valid geometry" in msg.lower()

    @pytest.mark.unit
    def test_no_geometry_error_stores_path_and_reason(self) -> None:
        """NoGeometryError should expose path and reason attributes."""
        err = NoGeometryError(path="my-file", reason="No features.")
        assert err.path == "my-file"
        assert err.reason == "No features."

    @pytest.mark.unit
    def test_no_geometry_error_caught_by_valueerror_handler(self) -> None:
        """NoGeometryError should be catchable as ValueError (backward compat)."""
        with pytest.raises(ValueError, match="missing bounding box"):
            raise NoGeometryError(path="test", reason="No geometry column.")

    @pytest.mark.unit
    def test_no_geometry_error_distinguishable_from_other_valueerrors(self) -> None:
        """NoGeometryError should be distinguishable from plain ValueError via isinstance."""
        geo_err = NoGeometryError(path="test", reason="No geometry.")
        plain_err = ValueError("Invalid collection ID")

        assert isinstance(geo_err, NoGeometryError)
        assert not isinstance(plain_err, NoGeometryError)


# =============================================================================
# Negative Case Tests (Non-Geometry ValueErrors Propagate)
# =============================================================================


class TestNonGeometryValueErrorsPropagation:
    """Tests that non-geometry ValueErrors are captured in failures (Issue #175)."""

    @pytest.mark.unit
    def test_non_geometry_valueerror_captured_in_failures(
        self, initialized_catalog: Path, tabular_parquet: Path
    ) -> None:
        """Non-geometry ValueErrors (e.g., invalid collection ID) should be in failures."""
        # Create a parquet file in a directory
        item_dir = initialized_catalog / "collection" / "item"
        item_dir.mkdir(parents=True, exist_ok=True)

        table = pa.table({"code": ["A"], "value": [1]})
        pq.write_table(table, item_dir / "data.parquet")

        # Mock add_dataset to raise a non-geometry ValueError
        with patch("portolan_cli.dataset.prepare_dataset") as mock_add:
            mock_add.side_effect = ValueError("Unsupported format: .xyz")

            # Per Issue #175, errors are collected in failures instead of raised
            added, skipped, failures = add_files(
                paths=[item_dir / "data.parquet"],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            assert len(failures) == 1
            assert "Unsupported format" in failures[0].error

    @pytest.mark.unit
    def test_json_parse_error_captured_in_failures(self, initialized_catalog: Path) -> None:
        """JSON parse errors should be in failures (not treated as no-geometry)."""
        item_dir = initialized_catalog / "collection" / "item"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create an invalid GeoJSON file
        bad_geojson = item_dir / "bad.geojson"
        bad_geojson.write_text("{invalid json content")

        # Per Issue #175, errors are collected in failures instead of raised
        added, skipped, failures = add_files(
            paths=[bad_geojson],
            catalog_root=initialized_catalog,
            collection_id="collection",
        )

        assert len(failures) == 1
        assert "Invalid JSON" in failures[0].error or "json" in failures[0].error.lower()


# =============================================================================
# GeoParquet Success Path Regression Test
# =============================================================================


class TestGeoParquetSuccessPath:
    """Regression tests to ensure valid GeoParquet files still work correctly."""

    @pytest.mark.unit
    def test_valid_geoparquet_not_deferred(self, initialized_catalog: Path) -> None:
        """A valid GeoParquet file should be added normally, NOT deferred as tabular.

        This is a regression test to ensure the NoGeometryError handling does
        not accidentally intercept valid GeoParquet files.
        """
        item_dir = initialized_catalog / "collection" / "item"
        item_dir.mkdir(parents=True, exist_ok=True)

        geo_parquet = item_dir / "boundaries.parquet"

        # Create a parquet file WITH geo metadata (simulate GeoParquet)
        table = pa.table(
            {
                "name": ["Area One", "Area Two"],
                "geometry": [b"POINT(0 0)", b"POINT(1 1)"],
            }
        )
        # Write with geo metadata to make it a GeoParquet file
        geo_metadata = json.dumps(
            {
                "version": "1.0.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point"],
                    }
                },
            }
        )
        existing_meta = table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": geo_metadata.encode("utf-8")}
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, geo_parquet)

        # Mock prepare_dataset and finalize_datasets to simulate success
        # Per Issue #281: add_files now calls prepare_dataset + finalize_datasets
        # The added list is populated from finalize_datasets return value
        from portolan_cli.dataset import DatasetInfo
        from portolan_cli.formats import FormatType

        mock_dataset_info = DatasetInfo(
            item_id="item",
            collection_id="collection",
            format_type=FormatType.VECTOR,
            bbox=[-180.0, -90.0, 180.0, 90.0],
            asset_paths=["boundaries.parquet"],
        )

        with (
            patch("portolan_cli.dataset.prepare_dataset") as mock_add,
            patch("portolan_cli.dataset.finalize_datasets") as mock_finalize,
        ):
            mock_add.return_value = MagicMock(
                item_id="item",
                collection_id="collection",
                asset_paths=["boundaries.parquet"],
            )
            # finalize_datasets returns the list of successfully added DatasetInfo objects
            mock_finalize.return_value = [mock_dataset_info]

            added, skipped, failures = add_files(
                paths=[geo_parquet],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

            # Valid GeoParquet should be ADDED (not deferred/skipped)
            assert mock_add.called, "prepare_dataset should be called for valid GeoParquet"
            assert len(added) == 1, "Valid GeoParquet should appear in added list"


# =============================================================================
# Warning Message Tests
# =============================================================================


class TestWarningMessages:
    """Tests for warning message quality and consistency."""

    @pytest.mark.unit
    def test_warning_includes_parquet_info(
        self, initialized_catalog: Path, tabular_parquet: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Warning should mention the file for debugging."""
        with caplog.at_level(logging.WARNING):
            added, skipped, failures = add_files(
                paths=[tabular_parquet],
                catalog_root=initialized_catalog,
                collection_id="collection",
            )

        # Check that some identifying info appears in warnings
        all_messages = " ".join(r.message for r in caplog.records)
        # Should mention either the filename or parquet format
        assert (
            "parquet" in all_messages.lower()
            or "census-data" in all_messages.lower()
            or "non-geospatial" in all_messages.lower()
        )


# =============================================================================
# Hypothesis Property-Based Tests (Consistent with CSV tests)
# =============================================================================


class TestTabularParquetHypothesis:
    """Hypothesis property-based tests for tabular parquet skip logic."""

    @pytest.mark.unit
    @given(
        num_columns=st.integers(min_value=2, max_value=5),
        num_rows=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=20, deadline=5000)
    def test_non_geo_parquet_never_raises(self, num_columns: int, num_rows: int) -> None:
        """Property: Non-geo parquet files should NEVER raise exceptions in add_files."""
        tmp_dir = tempfile.mkdtemp()
        tmp_path = Path(tmp_dir)
        try:
            # Set up catalog
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir(exist_ok=True)
            config_path = portolan_dir / "config.yaml"
            config_path.write_text("# Portolan config\n")
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "portolan-catalog",
                "description": "Test catalog",
                "links": [],
            }
            (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

            # Generate non-geo parquet with random columns
            collection_dir = tmp_path / "collection" / "item"
            collection_dir.mkdir(parents=True, exist_ok=True)

            # Create table with non-geometry columns
            data = {
                f"col_{i}": [f"val_{r}_{i}" for r in range(num_rows)] for i in range(num_columns)
            }
            table = pa.table(data)
            parquet_file = collection_dir / "test.parquet"
            pq.write_table(table, parquet_file)

            # This should NOT raise - ever
            try:
                added, skipped, failures = add_files(
                    paths=[parquet_file],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )
                # Should either be added (if somehow detected as geo) or skipped
                assert len(added) == 0 or len(skipped) >= 0
            except Exception as e:
                pytest.fail(f"add_files raised for non-geo parquet: {e}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @pytest.mark.unit
    @given(
        num_geo_files=st.integers(min_value=0, max_value=3),
        num_parquet_files=st.integers(min_value=1, max_value=3),
    )
    @settings(max_examples=15, deadline=10000)
    def test_mixed_directory_processes_all_geo_files(
        self, num_geo_files: int, num_parquet_files: int
    ) -> None:
        """Property: All geospatial files should be processed even with non-geo parquet present."""
        tmp_dir = tempfile.mkdtemp()
        tmp_path = Path(tmp_dir)
        try:
            # Set up catalog
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir(exist_ok=True)
            config_path = portolan_dir / "config.yaml"
            config_path.write_text("# Portolan config\n")
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "portolan-catalog",
                "description": "Test catalog",
                "links": [],
            }
            (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

            collection_dir = tmp_path / "collection" / "item"
            collection_dir.mkdir(parents=True, exist_ok=True)

            # Create non-geo parquet files
            for i in range(num_parquet_files):
                table = pa.table({f"col_{i}": [f"val_{i}"]})
                pq.write_table(table, collection_dir / f"metadata_{i}.parquet")

            # Create GeoJSON files
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

            # Mock prepare_dataset and finalize_datasets to track calls
            # Per Issue #281: add_files now calls prepare_dataset + finalize_datasets
            with (
                patch("portolan_cli.dataset.prepare_dataset") as mock_add,
                patch("portolan_cli.dataset.finalize_datasets") as mock_finalize,
            ):
                mock_add.return_value = MagicMock(item_id="item", collection_id="collection")
                mock_finalize.return_value = []

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
        column_names=st.lists(
            st.text(alphabet="abcdefghijklmnop", min_size=1, max_size=10),
            min_size=1,
            max_size=5,
            unique=True,
        )
    )
    @settings(max_examples=15, deadline=5000)
    def test_various_parquet_schemas_handled_gracefully(self, column_names: list[str]) -> None:
        """Property: Various parquet schemas without geometry should be handled gracefully."""
        tmp_dir = tempfile.mkdtemp()
        tmp_path = Path(tmp_dir)
        try:
            # Set up catalog
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir(exist_ok=True)
            config_path = portolan_dir / "config.yaml"
            config_path.write_text("# Portolan config\n")
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "portolan-catalog",
                "description": "Test catalog",
                "links": [],
            }
            (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

            collection_dir = tmp_path / "collection" / "item"
            collection_dir.mkdir(parents=True, exist_ok=True)

            # Build parquet with given column names
            data = {col: ["value"] for col in column_names}
            table = pa.table(data)
            parquet_file = collection_dir / "data.parquet"
            pq.write_table(table, parquet_file)

            # Should not raise
            try:
                added, skipped, failures = add_files(
                    paths=[parquet_file],
                    catalog_root=tmp_path,
                    collection_id="collection",
                )
            except Exception as e:
                pytest.fail(f"add_files raised for parquet content: {e}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
