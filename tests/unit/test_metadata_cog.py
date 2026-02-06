"""Tests for COG metadata extraction."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.metadata.cog import COGMetadata, extract_cog_metadata


class TestExtractCOGMetadata:
    """Tests for extract_cog_metadata()."""

    @pytest.mark.unit
    def test_returns_cog_metadata(self, valid_rgb_cog: Path) -> None:
        """Should return COGMetadata dataclass."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert isinstance(metadata, COGMetadata)

    @pytest.mark.unit
    def test_extracts_bbox(self, valid_rgb_cog: Path) -> None:
        """Should extract bounding box as (minx, miny, maxx, maxy)."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.bbox is not None
        assert len(metadata.bbox) == 4
        minx, miny, maxx, maxy = metadata.bbox
        assert minx <= maxx
        assert miny <= maxy

    @pytest.mark.unit
    def test_extracts_crs(self, valid_rgb_cog: Path) -> None:
        """Should extract CRS."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.crs is not None

    @pytest.mark.unit
    def test_extracts_dimensions(self, valid_rgb_cog: Path) -> None:
        """Should extract width and height."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.width is not None
        assert metadata.height is not None
        assert metadata.width > 0
        assert metadata.height > 0

    @pytest.mark.unit
    def test_extracts_band_count(self, valid_rgb_cog: Path) -> None:
        """Should extract number of bands."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.band_count is not None
        assert metadata.band_count > 0

    @pytest.mark.unit
    def test_extracts_dtype(self, valid_rgb_cog: Path) -> None:
        """Should extract data type."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.dtype is not None

    @pytest.mark.unit
    def test_extracts_nodata(self, valid_nodata_cog: Path) -> None:
        """Should extract nodata value if present."""
        metadata = extract_cog_metadata(valid_nodata_cog)
        # nodata may or may not be set, just verify it doesn't crash
        assert hasattr(metadata, "nodata")

    @pytest.mark.unit
    def test_extracts_resolution(self, valid_rgb_cog: Path) -> None:
        """Should extract pixel resolution."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.resolution is not None

    @pytest.mark.unit
    def test_raises_for_nonexistent_file(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            extract_cog_metadata(tmp_path / "missing.tif")

    @pytest.mark.unit
    def test_raises_for_non_raster(self, tmp_path: Path) -> None:
        """Should raise error for non-raster file."""
        import rasterio

        fake_file = tmp_path / "fake.tif"
        fake_file.write_bytes(b"not a tiff file")

        with pytest.raises(rasterio.errors.RasterioIOError):
            extract_cog_metadata(fake_file)


class TestCOGMetadata:
    """Tests for COGMetadata dataclass."""

    @pytest.mark.unit
    def test_to_dict(self, valid_rgb_cog: Path) -> None:
        """to_dict() returns complete metadata dict."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        d = metadata.to_dict()

        assert "bbox" in d
        assert "crs" in d
        assert "width" in d
        assert "height" in d
        assert "band_count" in d

    @pytest.mark.unit
    def test_to_stac_properties(self, valid_rgb_cog: Path) -> None:
        """to_stac_properties() returns STAC-compatible dict."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        props = metadata.to_stac_properties()

        assert isinstance(props, dict)
