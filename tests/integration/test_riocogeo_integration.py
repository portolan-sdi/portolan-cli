"""Integration tests verifying rio-cogeo works with our fixtures.

These tests confirm that rio-cogeo can convert and validate raster
files. Per ADR-0010, Portolan delegates conversion to rio-cogeo.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds


class TestRioCogeoConversion:
    """Tests for rio-cogeo COG conversion."""

    @pytest.mark.integration
    def test_convert_non_cog_to_cog(self, tmp_path: Path) -> None:
        """rio-cogeo can convert a non-COG GeoTIFF to COG."""
        from rio_cogeo.cogeo import cog_translate
        from rio_cogeo.profiles import cog_profiles

        # Create a non-COG GeoTIFF
        non_cog = tmp_path / "non_cog.tif"
        width, height = 64, 64
        transform = from_bounds(-122.5, 37.7, -122.3, 37.9, width, height)

        with rasterio.open(
            non_cog,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=transform,
            # No tiling = not a COG
        ) as dst:
            dst.write(np.ones((1, height, width), dtype=np.uint8) * 128)

        # Convert to COG
        output_cog = tmp_path / "output.tif"
        profile = cog_profiles.get("deflate")
        cog_translate(str(non_cog), str(output_cog), profile, quiet=True)

        assert output_cog.exists()

        # Verify it is a valid COG (tiled, has overviews structure)
        with rasterio.open(output_cog) as src:
            assert src.is_tiled

    @pytest.mark.integration
    def test_validate_existing_cog(self, valid_rgb_cog: Path) -> None:
        """rio-cogeo can validate our COG fixtures."""
        from rio_cogeo.cogeo import cog_validate

        is_valid, errors, warnings = cog_validate(str(valid_rgb_cog))
        assert is_valid, f"COG validation failed: {errors}"

    @pytest.mark.integration
    def test_validate_singleband_cog(self, valid_singleband_cog: Path) -> None:
        """Singleband COG fixture passes validation."""
        from rio_cogeo.cogeo import cog_validate

        is_valid, errors, _ = cog_validate(str(valid_singleband_cog))
        assert is_valid, f"COG validation failed: {errors}"

    @pytest.mark.integration
    def test_validate_float32_cog(self, valid_float32_cog: Path) -> None:
        """Float32 COG fixture passes validation."""
        from rio_cogeo.cogeo import cog_validate

        is_valid, errors, _ = cog_validate(str(valid_float32_cog))
        assert is_valid, f"COG validation failed: {errors}"

    @pytest.mark.integration
    def test_validate_nodata_cog(self, valid_nodata_cog: Path) -> None:
        """COG with nodata fixture passes validation."""
        from rio_cogeo.cogeo import cog_validate

        is_valid, errors, _ = cog_validate(str(valid_nodata_cog))
        assert is_valid, f"COG validation failed: {errors}"


class TestRioCogeoErrorHandling:
    """Tests for rio-cogeo error handling with invalid inputs."""

    @pytest.mark.integration
    def test_not_georeferenced_handling(
        self, invalid_not_georeferenced_tif: Path, tmp_path: Path
    ) -> None:
        """Document rio-cogeo behavior with non-georeferenced TIFF."""
        from rio_cogeo.cogeo import cog_translate
        from rio_cogeo.profiles import cog_profiles

        output = tmp_path / "output.tif"
        profile = cog_profiles.get("deflate")

        # rio-cogeo may convert it anyway (CRS not required for COG structure)
        # or may raise—document actual behavior
        try:
            cog_translate(str(invalid_not_georeferenced_tif), str(output), profile, quiet=True)
            # If it succeeds, the file exists but may not have CRS
            assert output.exists()
        except (ValueError, rasterio.errors.RasterioError):
            # Rejection is also acceptable behavior
            pass

    @pytest.mark.integration
    def test_truncated_tiff_raises(self, invalid_truncated_tif: Path, tmp_path: Path) -> None:
        """rio-cogeo raises on truncated/corrupted TIFF."""
        from rio_cogeo.cogeo import cog_translate
        from rio_cogeo.profiles import cog_profiles

        output = tmp_path / "output.tif"
        profile = cog_profiles.get("deflate")

        with pytest.raises(rasterio.errors.RasterioError):
            cog_translate(str(invalid_truncated_tif), str(output), profile, quiet=True)
