"""Tests for FlatGeobuf metadata extraction.

Tests the FlatGeobufMetadata dataclass and extract_flatgeobuf_metadata() function.
FlatGeobuf files contain CRS, bounds, schema, and feature count in their header.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Will be implemented
from portolan_cli.metadata.flatgeobuf import (
    FlatGeobufMetadata,
    extract_flatgeobuf_metadata,
)


@pytest.fixture
def sample_fgb(tmp_path: Path) -> Path:
    """Use the cloud_native sample.fgb fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "cloud_native" / "sample.fgb"
    if not fixture_path.exists():
        pytest.skip("FlatGeobuf fixture not found")
    return fixture_path


class TestFlatGeobufMetadata:
    """Tests for FlatGeobufMetadata dataclass."""

    def test_to_dict(self) -> None:
        """to_dict() returns JSON-serializable dict."""
        meta = FlatGeobufMetadata(
            bbox=(-122.4, 37.8, -73.9, 41.9),
            crs="EPSG:4326",
            geometry_type="Point",
            feature_count=3,
            schema={"name": "string", "value": "int64"},
        )
        result = meta.to_dict()

        assert result["bbox"] == [-122.4, 37.8, -73.9, 41.9]
        assert result["crs"] == "EPSG:4326"
        assert result["geometry_type"] == "Point"
        assert result["feature_count"] == 3
        assert result["schema"] == {"name": "string", "value": "int64"}

    def test_to_stac_properties(self) -> None:
        """to_stac_properties() returns STAC-compatible properties."""
        meta = FlatGeobufMetadata(
            bbox=(-122.4, 37.8, -73.9, 41.9),
            crs="EPSG:4326",
            geometry_type="Point",
            feature_count=3,
            schema={"name": "string", "value": "int64"},
        )
        props = meta.to_stac_properties()

        # CRS should be proj:epsg for EPSG codes
        assert props["proj:epsg"] == 4326
        assert props["flatgeobuf:geometry_type"] == "Point"
        assert props["flatgeobuf:feature_count"] == 3


class TestExtractFlatGeobufMetadata:
    """Tests for extract_flatgeobuf_metadata() function."""

    @pytest.mark.unit
    def test_extracts_bbox(self, sample_fgb: Path) -> None:
        """Extracts bounding box."""
        meta = extract_flatgeobuf_metadata(sample_fgb)

        assert meta.bbox is not None
        # Fixture has SF, NYC, Chicago points
        assert meta.bbox[0] == pytest.approx(-122.4, abs=0.01)  # min_x
        assert meta.bbox[1] == pytest.approx(37.8, abs=0.01)  # min_y
        assert meta.bbox[2] == pytest.approx(-73.9, abs=0.01)  # max_x
        assert meta.bbox[3] == pytest.approx(41.9, abs=0.01)  # max_y

    @pytest.mark.unit
    def test_extracts_crs(self, sample_fgb: Path) -> None:
        """Extracts CRS from file header."""
        meta = extract_flatgeobuf_metadata(sample_fgb)

        assert meta.crs == "EPSG:4326"

    @pytest.mark.unit
    def test_extracts_geometry_type(self, sample_fgb: Path) -> None:
        """Extracts geometry type."""
        meta = extract_flatgeobuf_metadata(sample_fgb)

        assert meta.geometry_type == "Point"

    @pytest.mark.unit
    def test_extracts_feature_count(self, sample_fgb: Path) -> None:
        """Extracts feature count."""
        meta = extract_flatgeobuf_metadata(sample_fgb)

        assert meta.feature_count == 3

    @pytest.mark.unit
    def test_extracts_schema(self, sample_fgb: Path) -> None:
        """Extracts field schema."""
        meta = extract_flatgeobuf_metadata(sample_fgb)

        assert "name" in meta.schema
        assert "value" in meta.schema

    @pytest.mark.unit
    def test_file_not_found(self, tmp_path: Path) -> None:
        """Raises FileNotFoundError for missing files."""
        with pytest.raises(FileNotFoundError):
            extract_flatgeobuf_metadata(tmp_path / "nonexistent.fgb")

    @pytest.mark.unit
    def test_invalid_file(self, tmp_path: Path) -> None:
        """Raises ValueError for invalid FlatGeobuf files."""
        invalid = tmp_path / "invalid.fgb"
        invalid.write_bytes(b"not a flatgeobuf file")

        with pytest.raises(ValueError, match="Invalid FlatGeobuf"):
            extract_flatgeobuf_metadata(invalid)
