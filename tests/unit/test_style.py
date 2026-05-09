"""Unit tests for vector style generation (Issue #13).

Tests style generation for PMTiles assets and render extension for rasters.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from portolan_cli.style import StyleInfo

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


# =============================================================================
# Phase 5: Style Discovery Tests
# =============================================================================


class TestDiscoverStyles:
    """Tests for discover_styles function."""

    @pytest.mark.unit
    def test_discovers_style_files(self, tmp_path: Path) -> None:
        """Creates 2 style files and verifies both found with correct keys."""
        import json

        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()

        # Create two style files
        (styles_dir / "default.json").write_text(json.dumps({"version": 8, "layers": []}))
        (styles_dir / "custom.json").write_text(json.dumps({"version": 8, "layers": []}))

        styles = discover_styles(tmp_path)

        assert len(styles) == 2
        keys = {s.key for s in styles}
        assert keys == {"styles/default", "styles/custom"}

    @pytest.mark.unit
    def test_extracts_name_as_title(self, tmp_path: Path) -> None:
        """Style with 'name' field returns that as title."""
        import json

        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()

        (styles_dir / "buildings.json").write_text(
            json.dumps({"version": 8, "name": "Buildings by Age", "layers": []})
        )

        styles = discover_styles(tmp_path)

        assert len(styles) == 1
        assert styles[0].title == "Buildings by Age"

    @pytest.mark.unit
    def test_returns_empty_when_no_styles_dir(self, tmp_path: Path) -> None:
        """No styles/ directory returns empty list."""
        from portolan_cli.style import discover_styles

        styles = discover_styles(tmp_path)

        assert styles == []

    @pytest.mark.unit
    def test_skips_non_json_files(self, tmp_path: Path) -> None:
        """Non-JSON files in styles/ directory are ignored."""
        import json

        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()

        # Create a valid JSON file and a .md file
        (styles_dir / "default.json").write_text(json.dumps({"version": 8, "layers": []}))
        (styles_dir / "README.md").write_text("# Styles\n")

        styles = discover_styles(tmp_path)

        assert len(styles) == 1
        assert styles[0].key == "styles/default"

    @pytest.mark.unit
    def test_skips_invalid_json(self, tmp_path: Path) -> None:
        """Malformed JSON is skipped, valid files still returned."""
        import json

        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()

        # Create valid and invalid JSON files
        (styles_dir / "default.json").write_text(json.dumps({"version": 8, "layers": []}))
        (styles_dir / "broken.json").write_text("{invalid json")
        (styles_dir / "custom.json").write_text(json.dumps({"version": 8, "layers": []}))

        styles = discover_styles(tmp_path)

        # Should only find the two valid files
        assert len(styles) == 2
        keys = {s.key for s in styles}
        assert keys == {"styles/default", "styles/custom"}

    @pytest.mark.unit
    def test_skips_non_dict_json(self, tmp_path: Path) -> None:
        """JSON files that parse to non-dict values are skipped."""
        import json

        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()

        (styles_dir / "array.json").write_text(json.dumps([1, 2, 3]))
        (styles_dir / "string.json").write_text(json.dumps("not a style"))
        (styles_dir / "valid.json").write_text(json.dumps({"version": 8, "layers": []}))

        styles = discover_styles(tmp_path)

        assert len(styles) == 1
        assert styles[0].key == "styles/valid"

    @pytest.mark.unit
    def test_fallback_title_from_filename(self, tmp_path: Path) -> None:
        """Style without 'name' field uses filename stem as title."""
        import json

        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()

        # Create style without name field
        (styles_dir / "custom-theme.json").write_text(json.dumps({"version": 8, "layers": []}))

        styles = discover_styles(tmp_path)

        assert len(styles) == 1
        assert styles[0].title == "custom-theme"


class TestBuildStylesManifest:
    """Tests for build_styles_manifest function."""

    @staticmethod
    def _style_info(key: str) -> StyleInfo:
        from portolan_cli.style import StyleInfo

        return StyleInfo(key=key, href="", title="", description="", path=Path())

    @pytest.mark.unit
    def test_default_first(self) -> None:
        """Default style always first regardless of input order."""
        from portolan_cli.style import build_styles_manifest

        styles = [
            self._style_info("styles/zebra"),
            self._style_info("styles/default"),
            self._style_info("styles/alpha"),
        ]

        manifest = build_styles_manifest(styles)

        assert manifest[0] == "styles/default"
        assert len(manifest) == 3

    @pytest.mark.unit
    def test_alphabetical_after_default(self) -> None:
        """Non-default styles sorted alphabetically."""
        from portolan_cli.style import build_styles_manifest

        styles = [
            self._style_info("styles/zebra"),
            self._style_info("styles/default"),
            self._style_info("styles/alpha"),
            self._style_info("styles/beta"),
        ]

        manifest = build_styles_manifest(styles)

        assert manifest == ["styles/default", "styles/alpha", "styles/beta", "styles/zebra"]

    @pytest.mark.unit
    def test_no_default(self) -> None:
        """Works without a style named 'default'."""
        from portolan_cli.style import build_styles_manifest

        styles = [
            self._style_info("styles/zebra"),
            self._style_info("styles/alpha"),
        ]

        manifest = build_styles_manifest(styles)

        assert manifest == ["styles/alpha", "styles/zebra"]

    @pytest.mark.unit
    def test_empty_list(self) -> None:
        """Empty input returns empty output."""
        from portolan_cli.style import build_styles_manifest

        manifest = build_styles_manifest([])

        assert manifest == []

    @pytest.mark.unit
    def test_single_style(self) -> None:
        """Single style returns single-element list."""
        from portolan_cli.style import build_styles_manifest

        styles = [self._style_info("styles/custom")]

        manifest = build_styles_manifest(styles)

        assert manifest == ["styles/custom"]


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
        assert "sources" in style
        assert "data" in style["sources"]
        assert style["layers"][0]["source"] == "data"

    @pytest.mark.unit
    def test_valid_polygon_style_loads(self, valid_style_dir: Path) -> None:
        """Valid polygon style fixture loads correctly."""
        import json

        style_path = valid_style_dir / "style_polygon.json"

        style = json.loads(style_path.read_text())

        assert style["version"] == 8
        assert len(style["layers"]) == 1
        assert style["layers"][0]["type"] == "fill"
        assert "sources" in style
        assert "data" in style["sources"]
        assert style["layers"][0]["source"] == "data"

    @pytest.mark.unit
    def test_valid_line_style_loads(self, valid_style_dir: Path) -> None:
        """Valid line style fixture loads correctly."""
        import json

        style_path = valid_style_dir / "style_line.json"

        style = json.loads(style_path.read_text())

        assert style["version"] == 8
        assert len(style["layers"]) == 1
        assert style["layers"][0]["type"] == "line"
        assert "sources" in style
        assert "data" in style["sources"]
        assert style["layers"][0]["source"] == "data"

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
        assert "sources" in style
        assert "data" in style["sources"]
        assert style["layers"][0]["source"] == "data"

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
        assert "sources" in style
        assert "data" in style["sources"]
        assert style["layers"][0]["source"] == "data"

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


# =============================================================================
# Phase 7: Full Style Building Tests
# =============================================================================


class TestBuildFullStyle:
    """Tests for build_full_style function."""

    @pytest.mark.unit
    def test_polygon_full_style(self) -> None:
        """Builds complete Mapbox GL style for Polygon geometry."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Default",
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        # Check version and name
        assert style["version"] == 8
        assert style["name"] == "Default"

        # Check sources
        assert "sources" in style
        assert "data" in style["sources"]
        assert style["sources"]["data"]["type"] == "vector"
        assert style["sources"]["data"]["url"] == "../data.pmtiles"

        # Check layers
        assert "layers" in style
        assert len(style["layers"]) == 1
        layer = style["layers"][0]
        assert layer["type"] == "fill"
        assert layer["source"] == "data"
        assert layer["source-layer"] == "parcels"
        assert layer["paint"]["fill-color"] == "#3388ff"
        assert layer["paint"]["fill-opacity"] == 0.6
        assert layer["paint"]["fill-outline-color"] == "#2266cc"

    @pytest.mark.unit
    def test_linestring_full_style(self) -> None:
        """Builds complete Mapbox GL style for LineString geometry."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Roads",
            geometry_type="LineString",
            source_layer="roads",
            pmtiles_relative_path="../roads.pmtiles",
            config=config,
        )

        # Check layer type is line
        assert style["layers"][0]["type"] == "line"
        assert style["layers"][0]["source"] == "data"
        assert style["layers"][0]["paint"]["line-color"] == "#3388ff"

    @pytest.mark.unit
    def test_point_full_style(self) -> None:
        """Builds complete Mapbox GL style for Point geometry."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Cities",
            geometry_type="Point",
            source_layer="cities",
            pmtiles_relative_path="../cities.pmtiles",
            config=config,
        )

        # Check layer type is circle
        assert style["layers"][0]["type"] == "circle"
        assert style["layers"][0]["source"] == "data"
        assert style["layers"][0]["paint"]["circle-color"] == "#3388ff"

    @pytest.mark.unit
    def test_custom_config_applied(self) -> None:
        """Custom VectorStyleConfig values appear in paint properties."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig(
            polygon_fill_color="#ff0000",
            polygon_fill_opacity=0.9,
            polygon_outline_color="#000000",
        )
        style = build_full_style(
            name="Custom",
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        paint = style["layers"][0]["paint"]
        assert paint["fill-color"] == "#ff0000"
        assert paint["fill-opacity"] == 0.9
        assert paint["fill-outline-color"] == "#000000"

    @pytest.mark.unit
    def test_full_style_is_json_serializable(self) -> None:
        """Full style dict is JSON-serializable."""
        import json

        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Test",
            geometry_type="Polygon",
            source_layer="layer",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        # Should round-trip through JSON
        serialized = json.dumps(style)
        deserialized = json.loads(serialized)
        assert deserialized == style


# =============================================================================
# Phase 8: Style File Writing Tests
# =============================================================================


class TestWriteStyleFile:
    """Tests for write_style_file function."""

    @pytest.mark.unit
    def test_writes_style_to_disk(self, tmp_path: Path) -> None:
        """Writes style dict to disk as JSON."""
        from portolan_cli.style import write_style_file

        style_dir = tmp_path / "styles"
        style_dict = {
            "version": 8,
            "name": "Test",
            "sources": {"data": {"type": "vector", "url": "../data.pmtiles"}},
            "layers": [],
        }

        result_path = write_style_file(style_dir, "default", style_dict)

        assert result_path == style_dir / "default.json"
        assert result_path.exists()

        # Verify content
        import json

        written = json.loads(result_path.read_text())
        assert written == style_dict

    @pytest.mark.unit
    def test_creates_styles_directory(self, tmp_path: Path) -> None:
        """Creates styles directory if it doesn't exist."""
        from portolan_cli.style import write_style_file

        style_dir = tmp_path / "styles"
        assert not style_dir.exists()

        style_dict = {"version": 8}
        write_style_file(style_dir, "test", style_dict)

        assert style_dir.exists()
        assert style_dir.is_dir()

    @pytest.mark.unit
    def test_overwrites_existing_file(self, tmp_path: Path) -> None:
        """Overwrites existing style file."""
        from portolan_cli.style import write_style_file

        style_dir = tmp_path / "styles"
        style_dir.mkdir()

        # Write original
        original = {"version": 7}
        write_style_file(style_dir, "default", original)

        # Overwrite with new
        new = {"version": 8}
        write_style_file(style_dir, "default", new)

        # Verify new content
        import json

        written = json.loads((style_dir / "default.json").read_text())
        assert written == new

    @pytest.mark.unit
    def test_rejects_path_traversal_slash(self, tmp_path: Path) -> None:
        """Rejects style names containing forward slash."""
        from portolan_cli.style import write_style_file

        with pytest.raises(ValueError, match="path separators"):
            write_style_file(tmp_path / "styles", "../evil", {"version": 8})

    @pytest.mark.unit
    def test_rejects_path_traversal_backslash(self, tmp_path: Path) -> None:
        """Rejects style names containing backslash."""
        from portolan_cli.style import write_style_file

        with pytest.raises(ValueError, match="path separators"):
            write_style_file(tmp_path / "styles", "..\\evil", {"version": 8})

    @pytest.mark.unit
    def test_rejects_path_traversal_dotdot(self, tmp_path: Path) -> None:
        """Rejects style names containing '..'."""
        from portolan_cli.style import write_style_file

        with pytest.raises(ValueError, match="path separators"):
            write_style_file(tmp_path / "styles", "..", {"version": 8})


# =============================================================================
# Phase 9: Default Style Writing Tests
# =============================================================================


class TestWriteDefaultStyle:
    """Tests for write_default_style function."""

    @pytest.mark.unit
    def test_writes_default_style_file(self, tmp_path: Path) -> None:
        """Creates styles/default.json with correct content."""
        from portolan_cli.style import VectorStyleConfig, write_default_style

        config = VectorStyleConfig()
        result_path = write_default_style(
            collection_path=tmp_path,
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="data.pmtiles",
            config=config,
        )

        assert result_path is not None
        assert result_path == tmp_path / "styles" / "default.json"
        assert result_path.exists()

        # Verify content
        import json

        style = json.loads(result_path.read_text())
        assert style["version"] == 8
        assert style["name"] == "Default"
        assert style["sources"]["data"]["url"] == "../data.pmtiles"
        assert style["layers"][0]["type"] == "fill"
        assert style["layers"][0]["source"] == "data"
        assert style["layers"][0]["source-layer"] == "parcels"

    @pytest.mark.unit
    def test_uses_custom_config(self, tmp_path: Path) -> None:
        """Custom VectorStyleConfig affects output."""
        from portolan_cli.style import VectorStyleConfig, write_default_style

        config = VectorStyleConfig(
            polygon_fill_color="#ff0000",
            polygon_fill_opacity=0.9,
        )
        result_path = write_default_style(
            collection_path=tmp_path,
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="data.pmtiles",
            config=config,
        )

        import json

        style = json.loads(result_path.read_text())  # type: ignore[union-attr]
        paint = style["layers"][0]["paint"]
        assert paint["fill-color"] == "#ff0000"
        assert paint["fill-opacity"] == 0.9

    @pytest.mark.unit
    def test_does_not_overwrite_existing(self, tmp_path: Path) -> None:
        """Returns None if default.json already exists, doesn't overwrite."""
        from portolan_cli.style import write_default_style

        # Create existing default.json
        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        default_path = styles_dir / "default.json"
        original_content = {"version": 7, "name": "Original"}

        import json

        default_path.write_text(json.dumps(original_content))

        # Try to write default style
        result = write_default_style(
            collection_path=tmp_path,
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="data.pmtiles",
        )

        # Should return None
        assert result is None

        # Original content should be unchanged
        written = json.loads(default_path.read_text())
        assert written == original_content


# =============================================================================
# Phase 10: Style Registration Tests
# =============================================================================


class TestRegisterStyleAssets:
    """Tests for registering discovered styles as STAC assets."""

    @pytest.mark.unit
    def test_registers_style_assets_in_collection(self, tmp_path: Path) -> None:
        """Discovered styles are added as assets in collection.json."""
        import json

        from portolan_cli.style import discover_styles, register_style_assets

        collection_data = {
            "type": "Collection",
            "id": "test",
            "assets": {"data": {"href": "./data.parquet", "type": "application/x-parquet"}},
        }
        (tmp_path / "collection.json").write_text(json.dumps(collection_data))

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        (styles_dir / "default.json").write_text(
            '{"version":8,"name":"Default","sources":{},"layers":[]}'
        )
        (styles_dir / "by-age.json").write_text(
            '{"version":8,"name":"By Age","description":"Buildings colored by construction year","sources":{},"layers":[]}'
        )

        styles = discover_styles(tmp_path)
        register_style_assets(tmp_path, styles)

        updated = json.loads((tmp_path / "collection.json").read_text())

        assert "styles/default" in updated["assets"]
        assert "styles/by-age" in updated["assets"]

        default_asset = updated["assets"]["styles/default"]
        assert default_asset["type"] == "application/json"
        assert default_asset["roles"] == ["style"]
        assert default_asset["title"] == "Default"

        by_age_asset = updated["assets"]["styles/by-age"]
        assert by_age_asset["title"] == "By Age"
        assert by_age_asset["description"] == "Buildings colored by construction year"

        assert updated["portolan:styles"] == ["styles/default", "styles/by-age"]

    @pytest.mark.unit
    def test_no_styles_no_manifest(self, tmp_path: Path) -> None:
        """No portolan:styles property when no styles exist."""
        import json

        from portolan_cli.style import register_style_assets

        collection_data = {"type": "Collection", "id": "test", "assets": {}}
        (tmp_path / "collection.json").write_text(json.dumps(collection_data))

        register_style_assets(tmp_path, [])

        updated = json.loads((tmp_path / "collection.json").read_text())
        assert "portolan:styles" not in updated

    @pytest.mark.unit
    def test_no_op_without_collection_json(self, tmp_path: Path) -> None:
        """Does nothing when collection.json doesn't exist."""
        from portolan_cli.style import StyleInfo, register_style_assets

        styles = [
            StyleInfo(
                key="styles/default",
                href="./styles/default.json",
                title="Default",
                description="",
                path=tmp_path / "styles" / "default.json",
            )
        ]
        register_style_assets(tmp_path, styles)

        assert not (tmp_path / "collection.json").exists()

    @pytest.mark.unit
    def test_removes_stale_style_assets(self, tmp_path: Path) -> None:
        """Removes style assets that no longer have files on disk."""
        import json

        from portolan_cli.style import register_style_assets

        collection_data = {
            "type": "Collection",
            "id": "test",
            "portolan:styles": ["styles/default", "styles/old"],
            "assets": {
                "styles/default": {
                    "href": "./styles/default.json",
                    "type": "application/json",
                    "roles": ["style"],
                },
                "styles/old": {
                    "href": "./styles/old.json",
                    "type": "application/json",
                    "roles": ["style"],
                },
            },
        }
        (tmp_path / "collection.json").write_text(json.dumps(collection_data))

        from portolan_cli.style import StyleInfo

        current_styles = [
            StyleInfo(
                key="styles/default",
                href="./styles/default.json",
                title="Default",
                description="",
                path=tmp_path / "styles" / "default.json",
            )
        ]
        register_style_assets(tmp_path, current_styles)

        updated = json.loads((tmp_path / "collection.json").read_text())
        assert "styles/old" not in updated["assets"]
        assert updated["portolan:styles"] == ["styles/default"]
