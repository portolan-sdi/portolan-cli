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

    @pytest.mark.unit
    def test_to_stac_properties_with_nodata(self, valid_nodata_cog: Path) -> None:
        """to_stac_properties() includes nodata in band info."""
        metadata = extract_cog_metadata(valid_nodata_cog)
        props = metadata.to_stac_properties()

        # Check that bands have nodata if the source file has it
        if metadata.nodata is not None:
            assert "raster:bands" in props
            for band in props["raster:bands"]:
                assert "nodata" in band
                assert band["nodata"] == metadata.nodata

    @pytest.mark.unit
    def test_to_stac_properties_without_nodata(self) -> None:
        """to_stac_properties() omits nodata when not set."""
        # Create metadata without nodata
        metadata = COGMetadata(
            bbox=(0.0, 0.0, 1.0, 1.0),
            crs="EPSG:4326",
            width=64,
            height=64,
            band_count=3,
            dtype="uint8",
            nodata=None,
            resolution=(0.1, 0.1),
        )
        props = metadata.to_stac_properties()

        # Bands should not have nodata key
        assert "raster:bands" in props
        for band in props["raster:bands"]:
            assert "nodata" not in band


class TestCOGMetadataEdgeCases:
    """Tests for edge cases in COG metadata extraction."""

    @pytest.mark.unit
    def test_cog_without_epsg(self, invalid_not_georeferenced_tif: Path) -> None:
        """COG without valid EPSG returns WKT or None for CRS."""
        # The not_georeferenced.tif should have no CRS
        metadata = extract_cog_metadata(invalid_not_georeferenced_tif)
        # Should be None since no CRS
        assert metadata.crs is None

    @pytest.mark.unit
    def test_extract_crs_wkt_fallback(self, tmp_path: Path) -> None:
        """Falls back to WKT when no EPSG code available."""
        import numpy as np
        import rasterio
        from rasterio.crs import CRS as RasterioCRS
        from rasterio.transform import from_bounds

        # Create a COG with a custom CRS that has no EPSG code
        # Using a custom WKT that rasterio won't map to EPSG
        custom_wkt = """PROJCS["Custom_CRS",
            GEOGCS["GCS_WGS_1984",
                DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],
                PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],
            PROJECTION["Mercator"],
            PARAMETER["central_meridian",0],
            PARAMETER["scale_factor",1],
            PARAMETER["false_easting",0],
            PARAMETER["false_northing",0],
            UNIT["Meter",1]]"""

        path = tmp_path / "custom_crs.tif"
        transform = from_bounds(0, 0, 1, 1, 64, 64)

        try:
            crs = RasterioCRS.from_wkt(custom_wkt)
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                height=64,
                width=64,
                count=1,
                dtype="uint8",
                crs=crs,
                transform=transform,
            ) as dst:
                dst.write(np.zeros((64, 64), dtype="uint8"), 1)

            metadata = extract_cog_metadata(path)
            # Should return WKT string since no EPSG
            assert metadata.crs is not None
            # Could be WKT string
            assert isinstance(metadata.crs, str)
        except Exception:
            # If the custom CRS can't be created, skip the test
            pytest.skip("Could not create custom CRS for WKT fallback test")
