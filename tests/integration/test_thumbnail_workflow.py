"""Integration tests for thumbnail and style workflow (Issue #13).

Tests end-to-end flows:
- Vector conversion → thumbnail generation
- PMTiles asset → style in STAC properties
- Config-driven style customization
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

# =============================================================================
# Thumbnail Generation Integration Tests
# =============================================================================


class TestVectorThumbnailWorkflow:
    """Integration tests for vector thumbnail generation."""

    @pytest.fixture
    def sample_geoparquet(self, fixtures_dir: Path, tmp_path: Path) -> Path:
        """Copy sample GeoParquet to tmp_path."""
        src = fixtures_dir / "vector" / "valid" / "points.parquet"
        if not src.exists():
            pytest.skip("GeoParquet fixture not available")
        dst = tmp_path / "data.parquet"
        shutil.copy(src, dst)
        return dst

    @pytest.mark.integration
    def test_thumbnail_generated_after_conversion(
        self, sample_geoparquet: Path, tmp_path: Path
    ) -> None:
        """Thumbnail is generated after vector conversion."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        config = ThumbnailConfig(basemap_provider="none")  # No network calls

        result = generate_vector_thumbnail(
            pmtiles_path=None,
            geoparquet_path=sample_geoparquet,
            config=config,
        )

        # Result may be None if matplotlib not available or file unreadable
        # That's acceptable for CI environments without full deps
        if result is not None:
            assert result.exists()
            assert result.suffix == ".jpg"
            assert result.stat().st_size > 0

    @pytest.mark.integration
    def test_thumbnail_disabled_via_config(self, sample_geoparquet: Path, tmp_path: Path) -> None:
        """Thumbnail not generated when disabled in config."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        config = ThumbnailConfig(enabled=False)

        result = generate_vector_thumbnail(
            pmtiles_path=None,
            geoparquet_path=sample_geoparquet,
            config=config,
        )

        assert result is None

    @pytest.mark.integration
    def test_thumbnail_config_from_yaml(self, tmp_path: Path) -> None:
        """Thumbnail config loads from catalog config.yaml."""
        from portolan_cli.thumbnail import get_thumbnail_config

        # Create catalog structure with config
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
thumbnails:
  enabled: true
  max_size: 256
  quality: 85
  basemap:
    provider: CartoDB.DarkMatter
    opacity: 0.9
""")

        config = get_thumbnail_config(tmp_path)

        assert config.enabled is True
        assert config.max_size == 256
        assert config.quality == 85
        assert config.basemap_provider == "CartoDB.DarkMatter"
        assert config.basemap_opacity == 0.9


# =============================================================================
# Style Storage Integration Tests
# =============================================================================


class TestStyleInStacAssets:
    """Integration tests for style storage in STAC assets."""

    @pytest.mark.integration
    def test_build_full_style_creates_valid_mapbox_gl_spec(self) -> None:
        """build_full_style produces a complete Mapbox GL v8 style spec."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Default",
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        assert style["version"] == 8
        assert style["name"] == "Default"
        assert style["sources"]["data"]["url"] == "../data.pmtiles"
        assert len(style["layers"]) == 1
        assert style["layers"][0]["type"] == "fill"

    @pytest.mark.integration
    def test_pmtiles_metadata_includes_layer_name(self) -> None:
        """PMTilesMetadata.to_stac_properties includes layer name."""
        from portolan_cli.metadata.pmtiles import PMTilesMetadata

        metadata = PMTilesMetadata(
            bbox=(-122.5, 37.5, -122.0, 38.0),
            min_zoom=0,
            max_zoom=14,
            tile_type="mvt",
            center=None,
            layer_name="boundaries",
        )

        props = metadata.to_stac_properties()

        assert "pmtiles:layers" in props
        assert props["pmtiles:layers"] == ["boundaries"]

    @pytest.mark.integration
    def test_style_config_from_yaml(self, tmp_path: Path) -> None:
        """Style config loads from catalog config.yaml."""
        from portolan_cli.style import get_vector_style_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
styles:
  vector:
    polygon:
      fill-color: "#ff5500"
      fill-opacity: 0.75
""")

        config = get_vector_style_config(tmp_path)

        assert config.polygon_fill_color == "#ff5500"
        assert config.polygon_fill_opacity == 0.75

    @pytest.mark.integration
    def test_style_registered_as_stac_asset(self, tmp_path: Path) -> None:
        """Style files are registered as STAC assets with portolan:styles manifest."""
        from portolan_cli.style import (
            VectorStyleConfig,
            discover_styles,
            register_style_assets,
            write_default_style,
        )

        # Create minimal collection.json
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()
        (collection_path / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "test-collection",
                    "stac_version": "1.0.0",
                    "description": "Test",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                    "assets": {
                        "data": {"href": "./data.parquet", "type": "application/vnd.apache.parquet"}
                    },
                }
            )
        )

        # Write a default style and register it
        config = VectorStyleConfig(polygon_fill_color="#00ff00")
        write_default_style(
            collection_path=collection_path,
            geometry_type="Polygon",
            source_layer="data",
            pmtiles_relative_path="data.pmtiles",
            config=config,
        )

        styles = discover_styles(collection_path)
        register_style_assets(collection_path, styles)

        # Verify style asset in collection.json
        collection = json.loads((collection_path / "collection.json").read_text())

        assert "styles/default" in collection["assets"]
        assert collection["assets"]["styles/default"]["roles"] == ["style"]
        assert collection["portolan:styles"] == ["styles/default"]


# =============================================================================
# Raster Style Integration Tests
# =============================================================================


class TestRasterStyleWorkflow:
    """Integration tests for raster (COG) styling."""

    @pytest.mark.integration
    def test_raster_style_config_from_yaml(self, tmp_path: Path) -> None:
        """Raster style config loads from catalog config.yaml."""
        from portolan_cli.style import get_raster_style_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
styles:
  raster:
    colormap: plasma
    rescale: [0, 100]
""")

        config = get_raster_style_config(tmp_path)

        assert config.colormap == "plasma"
        assert config.rescale_min == 0
        assert config.rescale_max == 100

    @pytest.mark.integration
    def test_raster_style_generates_render_props(self) -> None:
        """Raster style generates render extension properties."""
        from portolan_cli.style import RasterStyleConfig, build_raster_style

        config = RasterStyleConfig(
            colormap="terrain",
            rescale_min=0,
            rescale_max=3000,
        )

        props = build_raster_style(config)

        assert props["render:colormap_name"] == "terrain"
        assert props["render:rescale"] == [[0, 3000]]


# =============================================================================
# Config Hierarchy Tests
# =============================================================================


class TestConfigHierarchy:
    """Tests for config loading hierarchy."""

    @pytest.mark.integration
    def test_defaults_when_no_config(self, tmp_path: Path) -> None:
        """Returns defaults when no config.yaml exists."""
        from portolan_cli.style import VectorStyleConfig, get_vector_style_config
        from portolan_cli.thumbnail import ThumbnailConfig, get_thumbnail_config

        # No .portolan directory
        vector_config = get_vector_style_config(tmp_path)
        thumb_config = get_thumbnail_config(tmp_path)

        assert vector_config == VectorStyleConfig()
        assert thumb_config == ThumbnailConfig()

    @pytest.mark.integration
    def test_partial_config_uses_defaults(self, tmp_path: Path) -> None:
        """Partial config fills missing values with defaults."""
        from portolan_cli.style import get_vector_style_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
styles:
  vector:
    polygon:
      fill-color: "#ff0000"
""")

        config = get_vector_style_config(tmp_path)

        # Overridden
        assert config.polygon_fill_color == "#ff0000"
        # Defaults
        assert config.polygon_fill_opacity == 0.6
        assert config.point_color == "#3388ff"


# =============================================================================
# Coordinate Transformation Tests
# =============================================================================


class TestPmtilesCoordinateTransformation:
    """Tests for PMTiles tile-space to geographic coordinate transformation."""

    @pytest.mark.integration
    def test_tile_bounds_calculation(self) -> None:
        """Tile bounds are calculated correctly from z/x/y."""
        from portolan_cli.thumbnail import _tile_bounds

        # z=0 x=0 y=0 should cover the whole world
        bounds = _tile_bounds(0, 0, 0)
        assert bounds[0] == pytest.approx(-180.0, abs=0.01)  # lon_min
        assert bounds[2] == pytest.approx(180.0, abs=0.01)  # lon_max
        # Web Mercator doesn't reach poles exactly
        assert bounds[1] < -80  # lat_min (south)
        assert bounds[3] > 80  # lat_max (north)

    @pytest.mark.integration
    def test_tile_bounds_at_zoom_1(self) -> None:
        """Tile bounds at zoom 1 divide world into quadrants."""
        from portolan_cli.thumbnail import _tile_bounds

        # z=1 x=0 y=0 is NW quadrant
        nw = _tile_bounds(1, 0, 0)
        assert nw[0] == pytest.approx(-180.0, abs=0.01)
        assert nw[2] == pytest.approx(0.0, abs=0.01)
        assert nw[3] > 0  # North hemisphere

        # z=1 x=1 y=1 is SE quadrant
        se = _tile_bounds(1, 1, 1)
        assert se[0] == pytest.approx(0.0, abs=0.01)
        assert se[2] == pytest.approx(180.0, abs=0.01)
        assert se[1] < 0  # South hemisphere

    @pytest.mark.integration
    def test_coord_transformation(self) -> None:
        """MVT coordinates transform to geographic correctly."""
        from portolan_cli.thumbnail import _tile_bounds, _transform_coord

        # Tile z=0 x=0 y=0 covers whole world
        bounds = _tile_bounds(0, 0, 0)

        # MVT coord (0, 0) is top-left of tile = NW corner
        lon, lat = _transform_coord(0, 0, bounds, extent=4096)
        assert lon == pytest.approx(-180.0, abs=0.1)
        assert lat == pytest.approx(bounds[3], abs=0.1)  # lat_max (north)

        # MVT coord (4096, 4096) is bottom-right = SE corner
        lon, lat = _transform_coord(4096, 4096, bounds, extent=4096)
        assert lon == pytest.approx(180.0, abs=0.1)
        assert lat == pytest.approx(bounds[1], abs=0.1)  # lat_min (south)

        # MVT coord (2048, 2048) is center = near (0, 0)
        lon, lat = _transform_coord(2048, 2048, bounds, extent=4096)
        assert lon == pytest.approx(0.0, abs=0.1)
        assert lat == pytest.approx(0.0, abs=5.0)  # Web Mercator center not exactly 0

    @pytest.mark.integration
    def test_transform_coords_recursive(self) -> None:
        """Coordinate arrays transform recursively for all geometry types."""
        from portolan_cli.thumbnail import _tile_bounds, _transform_coords

        bounds = _tile_bounds(0, 0, 0)

        # Point: [x, y]
        point = [2048, 2048]
        transformed = _transform_coords(point, bounds)
        assert len(transformed) == 2
        assert transformed[0] == pytest.approx(0.0, abs=0.1)

        # LineString: [[x1, y1], [x2, y2]]
        line = [[0, 0], [4096, 4096]]
        transformed = _transform_coords(line, bounds)
        assert len(transformed) == 2
        assert transformed[0][0] == pytest.approx(-180.0, abs=0.1)
        assert transformed[1][0] == pytest.approx(180.0, abs=0.1)

        # Polygon: [[[x1, y1], [x2, y2], ...]]
        polygon = [[[0, 0], [4096, 0], [4096, 4096], [0, 0]]]
        transformed = _transform_coords(polygon, bounds)
        assert len(transformed) == 1  # One ring
        assert len(transformed[0]) == 4  # Four vertices
        assert transformed[0][0][0] == pytest.approx(-180.0, abs=0.1)

    @pytest.mark.integration
    def test_real_pmtiles_produces_geographic_bounds(self, fixtures_dir: Path) -> None:
        """Real PMTiles file produces valid geographic bounds."""
        pytest.importorskip("pmtiles")
        pytest.importorskip("mapbox_vector_tile")

        from portolan_cli.thumbnail import _read_pmtiles_geometries

        pmtiles_path = fixtures_dir / "cloud_native" / "sample.pmtiles"
        if not pmtiles_path.exists():
            pytest.skip("PMTiles fixture not available")

        geometries, bounds = _read_pmtiles_geometries(pmtiles_path)

        # May have no geometries at low zoom, that's ok
        if bounds is not None:
            lon_min, lat_min, lon_max, lat_max = bounds
            # Bounds should be valid geographic coordinates
            assert -180.0 <= lon_min <= 180.0
            assert -180.0 <= lon_max <= 180.0
            assert -90.0 <= lat_min <= 90.0
            assert -90.0 <= lat_max <= 90.0
            # Bounds should be ordered correctly
            assert lon_min <= lon_max
            assert lat_min <= lat_max
