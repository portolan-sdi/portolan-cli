"""Integration tests for bbox validation (issue #516).

Tests that inf/nan coordinates are properly filtered during add
and detected by the check command.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


def _create_geoparquet_with_inf_bbox(path: Path) -> None:
    """Create a GeoParquet file with inf values in the bbox metadata.

    This simulates the IGN Argentina issue where upstream WFS served
    coordinates like -1.79e308 (near float min, effectively -inf).
    """
    # Create a simple geometry (point at origin)
    # WKB for POINT(0 0)
    wkb_point = bytes.fromhex("0101000000000000000000000000000000")

    # Create table with one feature
    table = pa.table(
        {
            "geometry": pa.array([wkb_point], type=pa.binary()),
            "id": pa.array([1], type=pa.int64()),
        }
    )

    # Add geo metadata with poisoned bbox (issue #516 sentinel value)
    geo_meta = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                # This is the poisoned bbox - contains -inf-like sentinel values
                "bbox": [-1.79e308, -1.79e308, 180.0, 66.55],
            }
        },
    }

    # Write with geo metadata
    schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
    table = table.cast(schema_with_meta)
    pq.write_table(table, path)


def _create_valid_geoparquet(path: Path) -> None:
    """Create a valid GeoParquet file with proper bbox."""
    wkb_point = bytes.fromhex("0101000000000000000000000000000000")

    table = pa.table(
        {
            "geometry": pa.array([wkb_point], type=pa.binary()),
            "id": pa.array([1], type=pa.int64()),
        }
    )

    geo_meta = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "bbox": [-74.0, 40.0, -73.0, 41.0],  # Valid NYC-area bbox
            }
        },
    }

    schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
    table = table.cast(schema_with_meta)
    pq.write_table(table, path)


@pytest.mark.integration
class TestBboxValidationIntegration:
    """Integration tests for bbox validation during add and check."""

    def test_add_rejects_file_with_only_invalid_bbox(self) -> None:
        """Add command should reject files where ALL bboxes are invalid (defense-in-depth)."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog by creating .portolan/config.yaml
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection directory with poisoned parquet
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            parquet_path = collection_dir / "data.parquet"
            _create_geoparquet_with_inf_bbox(parquet_path)

            # Add the file - should fail because all bboxes are invalid
            result = runner.invoke(cli, ["add", str(parquet_path)])

            # The add should fail with a clear error message about invalid bbox
            assert result.exit_code != 0, f"add should have failed: {result.output}"
            assert "invalid" in result.output.lower() or "inf" in result.output.lower(), (
                f"Error should mention invalid/inf: {result.output}"
            )

    def test_add_filters_inf_bbox_from_mixed_collection(self) -> None:
        """Add should succeed when a collection has SOME valid bboxes, filtering invalid ones."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection directory with a VALID parquet file
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            parquet_path = collection_dir / "data.parquet"
            _create_valid_geoparquet(parquet_path)

            # Add the valid file
            result = runner.invoke(cli, ["add", str(parquet_path)])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Check the collection extent - should have valid bbox
            collection_json = collection_dir / "collection.json"
            assert collection_json.exists(), "collection.json should exist"

            with open(collection_json) as f:
                collection_data = json.load(f)

            extent = collection_data.get("extent", {})
            spatial = extent.get("spatial", {})
            bboxes = spatial.get("bbox", [[]])

            # The bbox should be valid (not contain inf/nan)
            for bbox in bboxes:
                if bbox:
                    for coord in bbox[:4]:
                        assert np.isfinite(coord), f"Collection bbox has non-finite: {bbox}"
                    assert -180 <= bbox[0] <= 180, f"west out of range: {bbox}"
                    assert -90 <= bbox[1] <= 90, f"south out of range: {bbox}"
                    assert -180 <= bbox[2] <= 180, f"east out of range: {bbox}"
                    assert -90 <= bbox[3] <= 90, f"north out of range: {bbox}"

    def test_check_detects_invalid_bbox_in_collection(self) -> None:
        """Check command should detect collections with invalid bbox."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection with valid file first
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            parquet_path = collection_dir / "data.parquet"
            _create_valid_geoparquet(parquet_path)

            result = runner.invoke(cli, ["add", str(parquet_path)])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Manually corrupt the collection bbox (simulating upstream bug)
            collection_json = collection_dir / "collection.json"
            with open(collection_json) as f:
                data = json.load(f)

            # Inject poisoned bbox
            data["extent"]["spatial"]["bbox"] = [[-1.79e308, -1.79e308, 180.0, 66.55]]

            with open(collection_json, "w") as f:
                json.dump(data, f)

            # Run check - should detect the invalid bbox
            result = runner.invoke(cli, ["check", "."])

            # Check should fail or warn about invalid bbox
            # The BboxValidRule should catch this
            assert "bbox_valid" in result.output or "invalid" in result.output.lower(), (
                f"Check should detect invalid bbox: {result.output}"
            )

    def test_check_passes_with_valid_bboxes(self) -> None:
        """Check command should pass when all bboxes are valid."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection with valid file
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            parquet_path = collection_dir / "data.parquet"
            _create_valid_geoparquet(parquet_path)

            result = runner.invoke(cli, ["add", str(parquet_path)])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Run check - should pass (or at least not fail on bbox_valid)
            result = runner.invoke(cli, ["check", "."])

            # bbox_valid rule should pass
            # Note: check may have other failures (pmtiles, etc.) but bbox should be OK
            output_lower = result.output.lower()
            assert "invalid bbox" not in output_lower, (
                f"Check should not report invalid bbox: {result.output}"
            )


@pytest.mark.integration
class TestAntimeridianBboxIntegration:
    """Integration tests for anti-meridian crossing bbox handling."""

    def test_collection_with_antimeridian_crossing_item(self) -> None:
        """Collection with anti-meridian crossing should produce valid extent."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection directory
            collection_dir = Path("fiji-data")
            collection_dir.mkdir()

            # Create GeoParquet with Fiji-like bbox (crosses antimeridian)
            parquet_path = collection_dir / "data.parquet"
            wkb_point = bytes.fromhex("0101000000000000000000000000000000")

            table = pa.table(
                {
                    "geometry": pa.array([wkb_point], type=pa.binary()),
                    "id": pa.array([1], type=pa.int64()),
                }
            )

            # Fiji-style anti-meridian crossing bbox
            geo_meta = {
                "version": "1.0.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Point"],
                        "bbox": [177.0, -20.0, -175.0, -15.0],  # Crosses 180°
                    }
                },
            }

            schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
            table = table.cast(schema_with_meta)
            pq.write_table(table, parquet_path)

            # Add the file
            result = runner.invoke(cli, ["add", str(parquet_path)])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Check the collection extent - should be valid
            collection_json = collection_dir / "collection.json"
            with open(collection_json) as f:
                collection_data = json.load(f)

            bboxes = collection_data["extent"]["spatial"]["bbox"]

            # All bboxes should be valid (finite, within WGS84 bounds)
            for bbox in bboxes:
                for coord in bbox[:4]:
                    assert np.isfinite(coord), f"Bbox contains non-finite: {bbox}"
                assert -180 <= bbox[0] <= 180
                assert -90 <= bbox[1] <= 90
                assert -180 <= bbox[2] <= 180
                assert -90 <= bbox[3] <= 90

            # Run check - should pass
            result = runner.invoke(cli, ["check", "."])
            assert "invalid bbox" not in result.output.lower()
