"""Tests for STAC v1.1.0 unified bands array migration.

Tests that COGMetadata.to_stac_properties() emits `bands` instead of `raster:bands`.
This is a breaking change from STAC v1.0 to v1.1.
"""

from __future__ import annotations

from portolan_cli.metadata.cog import COGMetadata


class TestUnifiedBandsArray:
    """Tests for unified bands array (STAC v1.1.0)."""

    def test_emits_bands_not_raster_bands(self) -> None:
        """Should emit 'bands' key, not 'raster:bands'."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=3,
            dtype="uint8",
            nodata=0,
            resolution=(1.0, 1.0),
        )

        props = metadata.to_stac_properties()

        assert "bands" in props
        assert "raster:bands" not in props

    def test_bands_array_has_correct_structure(self) -> None:
        """Bands array should have name, data_type, and nodata per band."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=2,
            dtype="float32",
            nodata=-9999.0,
            resolution=(10.0, 10.0),
        )

        props = metadata.to_stac_properties()

        assert len(props["bands"]) == 2
        assert props["bands"][0]["name"] == "band_1"
        assert props["bands"][0]["data_type"] == "float32"
        assert props["bands"][0]["nodata"] == -9999.0
        assert props["bands"][1]["name"] == "band_2"

    def test_bands_with_per_band_nodata(self) -> None:
        """Should use per-band nodata values when available."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=3,
            dtype="uint16",
            nodata=None,
            resolution=(1.0, 1.0),
            nodatavals=(0, 255, None),
        )

        props = metadata.to_stac_properties()

        assert props["bands"][0]["nodata"] == 0
        assert props["bands"][1]["nodata"] == 255
        assert "nodata" not in props["bands"][2]

    def test_bands_without_nodata(self) -> None:
        """Should omit nodata when not set."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=1,
            dtype="uint8",
            nodata=None,
            resolution=(1.0, 1.0),
        )

        props = metadata.to_stac_properties()

        assert "nodata" not in props["bands"][0]


class TestSpatialResolution:
    """Tests for raster:spatial_resolution field."""

    def test_includes_spatial_resolution(self) -> None:
        """Should include raster:spatial_resolution from resolution tuple."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=1,
            dtype="uint8",
            nodata=None,
            resolution=(10.0, 10.0),
        )

        props = metadata.to_stac_properties()

        assert "raster:spatial_resolution" in props
        assert props["raster:spatial_resolution"] == 10.0

    def test_spatial_resolution_averages_non_square_pixels(self) -> None:
        """Should average x and y resolution for non-square pixels."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=1,
            dtype="uint8",
            nodata=None,
            resolution=(10.0, 20.0),  # Non-square
        )

        props = metadata.to_stac_properties()

        assert props["raster:spatial_resolution"] == 15.0  # (10 + 20) / 2
