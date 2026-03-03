"""Unit tests for get_effective_status() with conversion overrides.

Tests the integration between format detection and conversion config:
- Force-convert cloud-native formats (e.g., FlatGeobuf -> CONVERTIBLE)
- Preserve convertible formats (e.g., Shapefile -> CLOUD_NATIVE)
- Path pattern overrides

See GitHub Issue #75 and #103 for context.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.conversion_config import ConversionOverrides
from portolan_cli.formats import CloudNativeStatus, get_effective_status


class TestGetEffectiveStatusBasic:
    """Tests for get_effective_status() without overrides."""

    @pytest.mark.unit
    def test_no_overrides_returns_original_status(self, tmp_path: Path) -> None:
        """Without overrides, returns the same status as get_cloud_native_status()."""
        fgb_file = tmp_path / "test.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        result = get_effective_status(fgb_file)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "FlatGeobuf"

    @pytest.mark.unit
    def test_empty_overrides_returns_original_status(self, tmp_path: Path) -> None:
        """Empty overrides return the same status as original."""
        fgb_file = tmp_path / "test.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        overrides = ConversionOverrides()
        result = get_effective_status(fgb_file, overrides=overrides)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE


class TestGetEffectiveStatusForceConvert:
    """Tests for force-converting cloud-native formats."""

    @pytest.mark.unit
    def test_flatgeobuf_force_convert_to_geoparquet(self, tmp_path: Path) -> None:
        """FlatGeobuf in convert list returns CONVERTIBLE with GeoParquet target."""
        fgb_file = tmp_path / "test.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        overrides = ConversionOverrides(extensions_convert=frozenset({".fgb"}))
        result = get_effective_status(fgb_file, overrides=overrides)

        assert result.status == CloudNativeStatus.CONVERTIBLE
        assert result.display_name == "FlatGeobuf"
        assert result.target_format == "GeoParquet"
        assert result.error_message is None

    @pytest.mark.unit
    def test_unlisted_cloud_native_not_affected(self, tmp_path: Path) -> None:
        """Cloud-native formats not in convert list remain CLOUD_NATIVE."""
        # Use PMTiles as an example of a cloud-native format
        pmtiles_file = tmp_path / "test.pmtiles"
        pmtiles_file.write_bytes(b"\x00\x00\x00\x00")

        # Only FlatGeobuf in convert list, not PMTiles
        overrides = ConversionOverrides(extensions_convert=frozenset({".fgb"}))
        result = get_effective_status(pmtiles_file, overrides=overrides)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE


class TestGetEffectiveStatusPreserve:
    """Tests for preserving convertible formats."""

    @pytest.mark.unit
    def test_shapefile_preserve_returns_cloud_native(self, tmp_path: Path) -> None:
        """Shapefile in preserve list returns CLOUD_NATIVE (skip conversion)."""
        # Create minimal shapefile set
        shp_file = tmp_path / "test.shp"
        shx_file = tmp_path / "test.shx"
        dbf_file = tmp_path / "test.dbf"
        shp_file.write_bytes(b"\x00\x00\x00\x00")
        shx_file.write_bytes(b"\x00\x00\x00\x00")
        dbf_file.write_bytes(b"\x00\x00\x00\x00")

        overrides = ConversionOverrides(extensions_preserve=frozenset({".shp"}))
        result = get_effective_status(shp_file, overrides=overrides)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "SHP"
        assert result.target_format is None

    @pytest.mark.unit
    def test_geopackage_preserve(self, tmp_path: Path) -> None:
        """GeoPackage in preserve list returns CLOUD_NATIVE."""
        gpkg_file = tmp_path / "test.gpkg"
        gpkg_file.write_bytes(b"\x00\x00\x00\x00")

        overrides = ConversionOverrides(extensions_preserve=frozenset({".gpkg"}))
        result = get_effective_status(gpkg_file, overrides=overrides)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "GPKG"

    @pytest.mark.unit
    def test_geojson_preserve(self, tmp_path: Path) -> None:
        """GeoJSON in preserve list returns CLOUD_NATIVE."""
        geojson_file = tmp_path / "test.geojson"
        geojson_file.write_text('{"type": "FeatureCollection", "features": []}')

        overrides = ConversionOverrides(extensions_preserve=frozenset({".geojson"}))
        result = get_effective_status(geojson_file, overrides=overrides)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "GeoJSON"

    @pytest.mark.unit
    def test_unlisted_convertible_not_affected(self, tmp_path: Path) -> None:
        """Convertible formats not in preserve list remain CONVERTIBLE."""
        geojson_file = tmp_path / "test.geojson"
        geojson_file.write_text('{"type": "FeatureCollection", "features": []}')

        # Only shapefile in preserve list
        overrides = ConversionOverrides(extensions_preserve=frozenset({".shp"}))
        result = get_effective_status(geojson_file, overrides=overrides)

        assert result.status == CloudNativeStatus.CONVERTIBLE


class TestGetEffectiveStatusPathPatterns:
    """Tests for path-based overrides."""

    @pytest.mark.unit
    def test_path_preserve_overrides_extension_convert(self, tmp_path: Path) -> None:
        """Path preserve pattern overrides extension convert rule."""
        # Create archive directory with FlatGeobuf
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()
        fgb_file = archive_dir / "data.fgb"
        fgb_file.write_bytes(b"\x00\x00\x00\x00")

        # FlatGeobuf would be force-converted, but archive/** preserves it
        overrides = ConversionOverrides(
            extensions_convert=frozenset({".fgb"}),
            paths_preserve=("archive/**",),
        )
        result = get_effective_status(fgb_file, overrides=overrides, root=tmp_path)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "FlatGeobuf"

    @pytest.mark.unit
    def test_path_preserve_with_extension_pattern(self, tmp_path: Path) -> None:
        """Path pattern with extension (e.g., regulatory/*.shp) works."""
        regulatory_dir = tmp_path / "regulatory"
        regulatory_dir.mkdir()
        shp_file = regulatory_dir / "boundaries.shp"
        shp_file.write_bytes(b"\x00\x00\x00\x00")

        overrides = ConversionOverrides(
            paths_preserve=("regulatory/*.shp",),
        )
        result = get_effective_status(shp_file, overrides=overrides, root=tmp_path)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE

    @pytest.mark.unit
    def test_path_outside_pattern_not_preserved(self, tmp_path: Path) -> None:
        """Files outside path patterns are not affected."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        shp_file = data_dir / "boundaries.shp"
        shp_file.write_bytes(b"\x00\x00\x00\x00")

        # Only regulatory/ is preserved, not data/
        overrides = ConversionOverrides(
            paths_preserve=("regulatory/**",),
        )
        result = get_effective_status(shp_file, overrides=overrides, root=tmp_path)

        # Shapefile outside preserved paths remains CONVERTIBLE
        assert result.status == CloudNativeStatus.CONVERTIBLE


class TestGetEffectiveStatusUnsupported:
    """Tests for unsupported format handling."""

    @pytest.mark.unit
    def test_unsupported_format_unchanged(self, tmp_path: Path) -> None:
        """Unsupported formats remain UNSUPPORTED regardless of overrides."""
        netcdf_file = tmp_path / "data.nc"
        netcdf_file.write_bytes(b"\x00\x00\x00\x00")

        # Even with preserve override, unsupported remains unsupported
        overrides = ConversionOverrides(extensions_preserve=frozenset({".nc"}))
        result = get_effective_status(netcdf_file, overrides=overrides)

        assert result.status == CloudNativeStatus.UNSUPPORTED

    @pytest.mark.unit
    def test_unsupported_with_path_preserve_unchanged(self, tmp_path: Path) -> None:
        """Unsupported formats in preserved paths remain UNSUPPORTED."""
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()
        netcdf_file = archive_dir / "data.nc"
        netcdf_file.write_bytes(b"\x00\x00\x00\x00")

        overrides = ConversionOverrides(paths_preserve=("archive/**",))
        result = get_effective_status(netcdf_file, overrides=overrides, root=tmp_path)

        # Even in archive/, NetCDF remains unsupported
        assert result.status == CloudNativeStatus.UNSUPPORTED


class TestGetEffectiveStatusWithRealFixtures:
    """Tests using real fixture files."""

    @pytest.mark.realdata
    def test_real_shapefile_preserve(self) -> None:
        """Real shapefile fixture with preserve override."""
        # Use the complete_shapefile fixture
        shp_path = Path("tests/fixtures/scan/complete_shapefile/radios_sample.shp")
        if not shp_path.exists():
            pytest.skip("Shapefile fixture not found")

        overrides = ConversionOverrides(extensions_preserve=frozenset({".shp"}))
        result = get_effective_status(shp_path, overrides=overrides)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "SHP"

    @pytest.mark.unit
    def test_real_geoparquet_unaffected_by_overrides(self, valid_points_parquet: Path) -> None:
        """Real GeoParquet fixture remains CLOUD_NATIVE with preserve override."""
        # GeoParquet is already cloud-native; preserve list shouldn't affect it
        overrides = ConversionOverrides(extensions_preserve=frozenset({".shp"}))
        result = get_effective_status(valid_points_parquet, overrides=overrides)

        assert result.status == CloudNativeStatus.CLOUD_NATIVE
        assert result.display_name == "GeoParquet"
