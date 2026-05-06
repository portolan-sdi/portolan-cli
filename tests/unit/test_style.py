"""Unit tests for vector style generation (Issue #13).

Tests style generation for PMTiles assets and render extension for rasters.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# =============================================================================
# Phase 1: VectorStyleConfig Tests
# =============================================================================


class TestVectorStyleConfig:
    """Tests for VectorStyleConfig dataclass."""

    @pytest.mark.unit
    def test_default_values(self) -> None:
        """VectorStyleConfig has sensible defaults per geometry type."""
        from portolan_cli.style import VectorStyleConfig

        config = VectorStyleConfig()
        assert config.point_color == "#3388ff"
        assert config.point_radius == 4
        assert config.point_opacity == 0.8
        assert config.line_color == "#3388ff"
        assert config.line_width == 2
        assert config.line_opacity == 0.8
        assert config.polygon_fill_color == "#3388ff"
        assert config.polygon_fill_opacity == 0.6
        assert config.polygon_outline_color == "#2266cc"

    @pytest.mark.unit
    def test_custom_values(self) -> None:
        """VectorStyleConfig accepts custom values."""
        from portolan_cli.style import VectorStyleConfig

        config = VectorStyleConfig(
            point_color="#ff0000",
            point_radius=8,
            polygon_fill_color="#00ff00",
        )
        assert config.point_color == "#ff0000"
        assert config.point_radius == 8
        assert config.polygon_fill_color == "#00ff00"

    @pytest.mark.unit
    def test_frozen_dataclass(self) -> None:
        """VectorStyleConfig is immutable (frozen)."""
        from portolan_cli.style import VectorStyleConfig

        config = VectorStyleConfig()
        with pytest.raises(AttributeError):
            config.point_color = "#ff0000"  # type: ignore[misc]


# =============================================================================
# Phase 2: Style Building Tests
# =============================================================================


class TestBuildPmtilesStyle:
    """Tests for build_pmtiles_style function."""

    @pytest.mark.unit
    def test_polygon_style(self) -> None:
        """Builds fill layer for polygon geometry."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("Polygon", "layer_name", config)

        assert style["version"] == 8
        assert len(style["layers"]) == 1

        layer = style["layers"][0]
        assert layer["type"] == "fill"
        assert layer["source-layer"] == "layer_name"
        assert layer["paint"]["fill-color"] == "#3388ff"
        assert layer["paint"]["fill-opacity"] == 0.6
        assert layer["paint"]["fill-outline-color"] == "#2266cc"

    @pytest.mark.unit
    def test_multipolygon_style(self) -> None:
        """MultiPolygon uses same style as Polygon."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("MultiPolygon", "data", config)

        layer = style["layers"][0]
        assert layer["type"] == "fill"

    @pytest.mark.unit
    def test_linestring_style(self) -> None:
        """Builds line layer for line geometry."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("LineString", "roads", config)

        layer = style["layers"][0]
        assert layer["type"] == "line"
        assert layer["paint"]["line-color"] == "#3388ff"
        assert layer["paint"]["line-width"] == 2
        assert layer["paint"]["line-opacity"] == 0.8

    @pytest.mark.unit
    def test_multilinestring_style(self) -> None:
        """MultiLineString uses same style as LineString."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("MultiLineString", "rivers", config)

        layer = style["layers"][0]
        assert layer["type"] == "line"

    @pytest.mark.unit
    def test_point_style(self) -> None:
        """Builds circle layer for point geometry."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("Point", "cities", config)

        layer = style["layers"][0]
        assert layer["type"] == "circle"
        assert layer["paint"]["circle-color"] == "#3388ff"
        assert layer["paint"]["circle-radius"] == 4
        assert layer["paint"]["circle-opacity"] == 0.8

    @pytest.mark.unit
    def test_multipoint_style(self) -> None:
        """MultiPoint uses same style as Point."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("MultiPoint", "events", config)

        layer = style["layers"][0]
        assert layer["type"] == "circle"

    @pytest.mark.unit
    def test_geometry_collection_fallback(self) -> None:
        """GeometryCollection falls back to polygon style."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("GeometryCollection", "mixed", config)

        layer = style["layers"][0]
        assert layer["type"] == "fill"

    @pytest.mark.unit
    def test_unknown_geometry_fallback(self) -> None:
        """Unknown geometry type falls back to polygon style."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("Unknown", "data", config)

        layer = style["layers"][0]
        assert layer["type"] == "fill"

    @pytest.mark.unit
    def test_custom_config_applied(self) -> None:
        """Custom config values are applied to style."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig(
            polygon_fill_color="#ff0000",
            polygon_fill_opacity=0.9,
            polygon_outline_color="#000000",
        )
        style = build_pmtiles_style("Polygon", "parcels", config)

        layer = style["layers"][0]
        assert layer["paint"]["fill-color"] == "#ff0000"
        assert layer["paint"]["fill-opacity"] == 0.9
        assert layer["paint"]["fill-outline-color"] == "#000000"

    @pytest.mark.unit
    def test_layer_id_generated(self) -> None:
        """Layer ID is auto-generated from source-layer."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("Polygon", "parcels", config)

        layer = style["layers"][0]
        assert layer["id"] == "parcels-fill"

    @pytest.mark.unit
    def test_line_layer_id(self) -> None:
        """Line layer ID uses -line suffix."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("LineString", "roads", config)

        layer = style["layers"][0]
        assert layer["id"] == "roads-line"

    @pytest.mark.unit
    def test_circle_layer_id(self) -> None:
        """Circle layer ID uses -circle suffix."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("Point", "points", config)

        layer = style["layers"][0]
        assert layer["id"] == "points-circle"


# =============================================================================
# Phase 3: Raster Style Tests
# =============================================================================


class TestBuildRasterStyle:
    """Tests for build_raster_style function (render extension)."""

    @pytest.mark.unit
    def test_default_colormap(self) -> None:
        """Default colormap is viridis."""
        from portolan_cli.style import RasterStyleConfig, build_raster_style

        config = RasterStyleConfig()
        style = build_raster_style(config)

        assert style["render:colormap_name"] == "viridis"

    @pytest.mark.unit
    def test_auto_rescale(self) -> None:
        """Auto rescale uses None (viewer determines)."""
        from portolan_cli.style import RasterStyleConfig, build_raster_style

        config = RasterStyleConfig()
        style = build_raster_style(config)

        assert "render:rescale" not in style or style["render:rescale"] is None

    @pytest.mark.unit
    def test_explicit_rescale(self) -> None:
        """Explicit rescale is included in style."""
        from portolan_cli.style import RasterStyleConfig, build_raster_style

        config = RasterStyleConfig(rescale_min=0, rescale_max=255)
        style = build_raster_style(config)

        assert style["render:rescale"] == [[0, 255]]

    @pytest.mark.unit
    def test_custom_colormap(self) -> None:
        """Custom colormap is applied."""
        from portolan_cli.style import RasterStyleConfig, build_raster_style

        config = RasterStyleConfig(colormap="terrain")
        style = build_raster_style(config)

        assert style["render:colormap_name"] == "terrain"


# =============================================================================
# Phase 4: Config Loading Tests
# =============================================================================


class TestGetStyleConfig:
    """Tests for loading style config from catalog config."""

    @pytest.mark.unit
    def test_returns_defaults_when_no_config(self, tmp_path: Path) -> None:
        """Returns default config when no styles section exists."""
        from portolan_cli.style import VectorStyleConfig, get_vector_style_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("conversion:\n  cog: {}\n")

        config = get_vector_style_config(tmp_path)

        assert config == VectorStyleConfig()

    @pytest.mark.unit
    def test_loads_custom_vector_config(self, tmp_path: Path) -> None:
        """Loads custom vector style config from YAML."""
        from portolan_cli.style import get_vector_style_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
styles:
  vector:
    point:
      circle-color: "#ff0000"
      circle-radius: 8
    polygon:
      fill-color: "#00ff00"
      fill-opacity: 0.8
""")

        config = get_vector_style_config(tmp_path)

        assert config.point_color == "#ff0000"
        assert config.point_radius == 8
        assert config.polygon_fill_color == "#00ff00"
        assert config.polygon_fill_opacity == 0.8

    @pytest.mark.unit
    def test_loads_raster_config(self, tmp_path: Path) -> None:
        """Loads raster style config from YAML."""
        from portolan_cli.style import get_raster_style_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
styles:
  raster:
    colormap: terrain
    rescale: [0, 1000]
""")

        config = get_raster_style_config(tmp_path)

        assert config.colormap == "terrain"
        assert config.rescale_min == 0
        assert config.rescale_max == 1000


# =============================================================================
# Phase 5: STAC Asset Property Tests
# =============================================================================


class TestStyleInAssetProperties:
    """Tests for adding style to STAC asset properties."""

    @pytest.mark.unit
    def test_pmtiles_style_in_properties(self) -> None:
        """PMTiles style is serialized to pmtiles:style property."""
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("Polygon", "parcels", config)

        # Style should be a complete Mapbox GL style spec subset
        assert "version" in style
        assert "layers" in style
        assert isinstance(style["layers"], list)
        assert len(style["layers"]) > 0

    @pytest.mark.unit
    def test_style_is_json_serializable(self) -> None:
        """Style dict is JSON-serializable for STAC asset storage."""
        import json

        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        config = VectorStyleConfig()
        style = build_pmtiles_style("Polygon", "layer", config)

        # Should not raise
        serialized = json.dumps(style)
        assert isinstance(serialized, str)

        # Should round-trip
        deserialized = json.loads(serialized)
        assert deserialized == style


# =============================================================================
# Phase 6: Style Fixture Tests
# =============================================================================


class TestStyleFixtures:
    """Tests using style fixtures."""

    @pytest.fixture
    def valid_style_dir(self, fixtures_dir: Path) -> Path:
        """Path to valid style fixtures."""
        return fixtures_dir / "metadata" / "style" / "valid"

    @pytest.fixture
    def invalid_style_dir(self, fixtures_dir: Path) -> Path:
        """Path to invalid style fixtures."""
        return fixtures_dir / "metadata" / "style" / "invalid"

    @pytest.mark.unit
    def test_valid_point_style_loads(self, valid_style_dir: Path) -> None:
        """Valid point style fixture loads correctly."""
        import json

        style_path = valid_style_dir / "style_point.json"

        style = json.loads(style_path.read_text())

        assert style["version"] == 8
        assert len(style["layers"]) == 1
        assert style["layers"][0]["type"] == "circle"

    @pytest.mark.unit
    def test_valid_polygon_style_loads(self, valid_style_dir: Path) -> None:
        """Valid polygon style fixture loads correctly."""
        import json

        style_path = valid_style_dir / "style_polygon.json"

        style = json.loads(style_path.read_text())

        assert style["version"] == 8
        assert len(style["layers"]) == 1
        assert style["layers"][0]["type"] == "fill"

    @pytest.mark.unit
    def test_valid_line_style_loads(self, valid_style_dir: Path) -> None:
        """Valid line style fixture loads correctly."""
        import json

        style_path = valid_style_dir / "style_line.json"

        style = json.loads(style_path.read_text())

        assert style["version"] == 8
        assert len(style["layers"]) == 1
        assert style["layers"][0]["type"] == "line"

    @pytest.mark.unit
    def test_categorical_style_has_match_expression(self, valid_style_dir: Path) -> None:
        """Categorical style uses match expression."""
        import json

        style_path = valid_style_dir / "style_categorical.json"

        style = json.loads(style_path.read_text())
        paint = style["layers"][0]["paint"]

        # fill-color should be a match expression (list starting with "match")
        fill_color = paint["fill-color"]
        assert isinstance(fill_color, list)
        assert fill_color[0] == "match"

    @pytest.mark.unit
    def test_graduated_style_has_interpolate_expression(self, valid_style_dir: Path) -> None:
        """Graduated style uses interpolate expression."""
        import json

        style_path = valid_style_dir / "style_graduated.json"

        style = json.loads(style_path.read_text())
        paint = style["layers"][0]["paint"]

        # fill-color should be an interpolate expression
        fill_color = paint["fill-color"]
        assert isinstance(fill_color, list)
        assert fill_color[0] == "interpolate"

    @pytest.mark.unit
    def test_bad_syntax_fixture_fails_parse(self, invalid_style_dir: Path) -> None:
        """Bad syntax fixture fails JSON parse."""
        import json

        style_path = invalid_style_dir / "style_bad_syntax.json"

        with pytest.raises(json.JSONDecodeError):
            json.loads(style_path.read_text())

    @pytest.mark.unit
    def test_missing_layers_fixture_lacks_layers(self, invalid_style_dir: Path) -> None:
        """Missing layers fixture has no layers key."""
        import json

        style_path = invalid_style_dir / "style_missing_layers.json"

        style = json.loads(style_path.read_text())
        assert "layers" not in style
