"""Tests for PMTiles metadata extraction.

Tests the PMTilesMetadata dataclass and extract_pmtiles_metadata() function.
PMTiles store bounds in WGS84 (4326) but tiles are Web Mercator (3857).
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Will be implemented
from portolan_cli.metadata.pmtiles import (
    PMTilesMetadata,
    extract_pmtiles_metadata,
)


@pytest.fixture
def sample_pmtiles(tmp_path: Path) -> Path:
    """Use the cloud_native sample.pmtiles fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "cloud_native" / "sample.pmtiles"
    if not fixture_path.exists():
        pytest.skip("PMTiles fixture not found")
    return fixture_path


class TestPMTilesMetadata:
    """Tests for PMTilesMetadata dataclass."""

    def test_to_dict(self) -> None:
        """to_dict() returns JSON-serializable dict."""
        meta = PMTilesMetadata(
            bbox=(-122.4, 37.8, -73.9, 41.9),
            min_zoom=4,
            max_zoom=8,
            tile_type="mvt",
            center=((-122.4 + -73.9) / 2, (37.8 + 41.9) / 2, 6),
        )
        result = meta.to_dict()

        assert result["bbox"] == [-122.4, 37.8, -73.9, 41.9]
        assert result["min_zoom"] == 4
        assert result["max_zoom"] == 8
        assert result["tile_type"] == "mvt"
        assert result["center"] is not None

    def test_to_stac_properties(self) -> None:
        """to_stac_properties() returns STAC-compatible properties."""
        meta = PMTilesMetadata(
            bbox=(-122.4, 37.8, -73.9, 41.9),
            min_zoom=4,
            max_zoom=8,
            tile_type="mvt",
            center=None,
        )
        props = meta.to_stac_properties()

        # PMTiles are always 3857 (Web Mercator) internally
        assert props["proj:epsg"] == 3857
        # Zoom levels should be included
        assert props["pmtiles:min_zoom"] == 4
        assert props["pmtiles:max_zoom"] == 8
        assert props["pmtiles:tile_type"] == "mvt"


class TestExtractPMTilesMetadata:
    """Tests for extract_pmtiles_metadata() function."""

    @pytest.mark.unit
    def test_extracts_bbox(self, sample_pmtiles: Path) -> None:
        """Extracts bounding box in WGS84 (4326)."""
        meta = extract_pmtiles_metadata(sample_pmtiles)

        assert meta.bbox is not None
        # Fixture has SF, NYC, Chicago points
        assert meta.bbox[0] == pytest.approx(-122.4, abs=0.01)  # min_lon
        assert meta.bbox[1] == pytest.approx(37.8, abs=0.01)  # min_lat
        assert meta.bbox[2] == pytest.approx(-73.9, abs=0.01)  # max_lon
        assert meta.bbox[3] == pytest.approx(41.9, abs=0.01)  # max_lat

    @pytest.mark.unit
    def test_extracts_zoom_levels(self, sample_pmtiles: Path) -> None:
        """Extracts min/max zoom levels."""
        meta = extract_pmtiles_metadata(sample_pmtiles)

        assert meta.min_zoom == 4
        assert meta.max_zoom == 8

    @pytest.mark.unit
    def test_extracts_tile_type(self, sample_pmtiles: Path) -> None:
        """Extracts tile type (mvt for vector tiles)."""
        meta = extract_pmtiles_metadata(sample_pmtiles)

        assert meta.tile_type == "mvt"

    @pytest.mark.unit
    def test_file_not_found(self, tmp_path: Path) -> None:
        """Raises FileNotFoundError for missing files."""
        with pytest.raises(FileNotFoundError):
            extract_pmtiles_metadata(tmp_path / "nonexistent.pmtiles")

    @pytest.mark.unit
    def test_invalid_file(self, tmp_path: Path) -> None:
        """Raises ValueError for invalid PMTiles files."""
        invalid = tmp_path / "invalid.pmtiles"
        invalid.write_bytes(b"not a pmtiles file")

        with pytest.raises(ValueError, match="Invalid PMTiles"):
            extract_pmtiles_metadata(invalid)
