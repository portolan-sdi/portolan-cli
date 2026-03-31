"""Unit tests for multi-layer format support.

Tests that Portolan correctly detects and handles multi-layer files
(GeoPackage, FileGDB) per GitHub issue #265.

These tests verify:
1. Layer enumeration - detecting all layers in a multi-layer file
2. Per-layer conversion - each layer becomes a separate output file
3. Progress reporting - all layers reflected in output
"""

from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "multilayer"


class TestLayerEnumeration:
    """Tests for detecting layers in multi-layer files."""

    @pytest.mark.unit
    def test_geopackage_lists_all_layers(self) -> None:
        """GeoPackage with 3 layers returns all layer names."""
        from portolan_cli.formats import list_layers

        gpkg_path = FIXTURES_DIR / "multilayer.gpkg"
        layers = list_layers(gpkg_path)

        assert layers is not None
        assert len(layers) == 3
        assert set(layers) == {"points", "lines", "polygons"}

    @pytest.mark.unit
    def test_filegdb_lists_all_layers(self) -> None:
        """FileGDB with 4 layers (in feature datasets) returns all layer names."""
        from portolan_cli.formats import list_layers

        gdb_path = FIXTURES_DIR / "featuredataset.gdb"
        layers = list_layers(gdb_path)

        assert layers is not None
        assert len(layers) == 4
        assert set(layers) == {"standalone", "fd1_lyr1", "fd1_lyr2", "fd2_lyr"}

    @pytest.mark.unit
    def test_single_layer_file_returns_one_layer(self) -> None:
        """Single-layer GeoJSON returns one layer (or None for single-layer formats)."""
        from portolan_cli.formats import list_layers

        geojson_path = (
            Path(__file__).parent.parent / "fixtures" / "vector" / "valid" / "points.geojson"
        )
        layers = list_layers(geojson_path)

        # Single-layer formats return a list with one item or None
        assert layers is None or len(layers) == 1

    @pytest.mark.unit
    def test_list_layers_nonexistent_file_raises(self) -> None:
        """list_layers raises FileNotFoundError for missing file."""
        from portolan_cli.formats import list_layers

        with pytest.raises(FileNotFoundError):
            list_layers(Path("/nonexistent/file.gpkg"))


class TestMultiLayerDetection:
    """Tests for detecting if a file has multiple layers."""

    @pytest.mark.unit
    def test_geopackage_is_multilayer(self) -> None:
        """GeoPackage with 3 layers is detected as multi-layer."""
        from portolan_cli.formats import is_multilayer

        gpkg_path = FIXTURES_DIR / "multilayer.gpkg"
        assert is_multilayer(gpkg_path) is True

    @pytest.mark.unit
    def test_filegdb_is_multilayer(self) -> None:
        """FileGDB with 4 layers is detected as multi-layer."""
        from portolan_cli.formats import is_multilayer

        gdb_path = FIXTURES_DIR / "featuredataset.gdb"
        assert is_multilayer(gdb_path) is True

    @pytest.mark.unit
    def test_geojson_is_not_multilayer(self) -> None:
        """Single-layer GeoJSON is not detected as multi-layer."""
        from portolan_cli.formats import is_multilayer

        geojson_path = (
            Path(__file__).parent.parent / "fixtures" / "vector" / "valid" / "points.geojson"
        )
        assert is_multilayer(geojson_path) is False


class TestMultiLayerConversion:
    """Tests for converting multi-layer files."""

    @pytest.mark.unit
    def test_convert_multilayer_returns_multiple_results(self, tmp_path: Path) -> None:
        """Converting a multi-layer file returns one result per layer."""
        from portolan_cli.convert import convert_multilayer_file

        gpkg_path = FIXTURES_DIR / "multilayer.gpkg"
        results = convert_multilayer_file(gpkg_path, output_dir=tmp_path)

        # Should have 3 results (one per layer)
        assert len(results) == 3

        # Each result should have a unique output file
        output_files = [r.output for r in results if r.output]
        assert len(output_files) == 3
        assert len(set(output_files)) == 3  # All unique

    @pytest.mark.unit
    def test_convert_multilayer_output_naming(self, tmp_path: Path) -> None:
        """Output files are named with layer suffix: source_layername.parquet."""
        from portolan_cli.convert import convert_multilayer_file

        gpkg_path = FIXTURES_DIR / "multilayer.gpkg"
        results = convert_multilayer_file(gpkg_path, output_dir=tmp_path)

        expected_names = {
            "multilayer_points.parquet",
            "multilayer_lines.parquet",
            "multilayer_polygons.parquet",
        }
        actual_names = {r.output.name for r in results if r.output}

        assert actual_names == expected_names

    @pytest.mark.unit
    def test_convert_multilayer_filegdb(self, tmp_path: Path) -> None:
        """FileGDB multi-layer conversion produces one file per layer."""
        from portolan_cli.convert import convert_multilayer_file

        gdb_path = FIXTURES_DIR / "featuredataset.gdb"
        results = convert_multilayer_file(gdb_path, output_dir=tmp_path)

        # Should have 4 results
        assert len(results) == 4

        expected_names = {
            "featuredataset_standalone.parquet",
            "featuredataset_fd1_lyr1.parquet",
            "featuredataset_fd1_lyr2.parquet",
            "featuredataset_fd2_lyr.parquet",
        }
        actual_names = {r.output.name for r in results if r.output}

        assert actual_names == expected_names

    @pytest.mark.unit
    def test_convert_multilayer_validates_output(self, tmp_path: Path) -> None:
        """Each converted layer produces valid GeoParquet."""
        from portolan_cli.convert import convert_multilayer_file

        gpkg_path = FIXTURES_DIR / "multilayer.gpkg"
        results = convert_multilayer_file(gpkg_path, output_dir=tmp_path)

        # All results should be successful
        for result in results:
            assert result.success, f"Conversion failed for {result.layer}: {result.error}"
            assert result.output is not None
            assert result.output.exists()
            assert result.output.suffix == ".parquet"

    @pytest.mark.unit
    def test_convert_multilayer_preserves_layer_name(self, tmp_path: Path) -> None:
        """Each result includes the original layer name."""
        from portolan_cli.convert import convert_multilayer_file

        gpkg_path = FIXTURES_DIR / "multilayer.gpkg"
        results = convert_multilayer_file(gpkg_path, output_dir=tmp_path)

        layer_names = {r.layer for r in results}
        assert layer_names == {"points", "lines", "polygons"}
