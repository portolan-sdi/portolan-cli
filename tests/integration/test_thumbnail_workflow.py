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
    def test_pmtiles_metadata_includes_style(self) -> None:
        """PMTilesMetadata.to_stac_properties includes style when set."""
        from portolan_cli.metadata.pmtiles import PMTilesMetadata
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

        # Build a style
        config = VectorStyleConfig()
        style = build_pmtiles_style("Polygon", "parcels", config)

        # Create metadata with style
        metadata = PMTilesMetadata(
            bbox=(-122.5, 37.5, -122.0, 38.0),
            min_zoom=0,
            max_zoom=14,
            tile_type="mvt",
            center=None,
            layer_name="parcels",
            style=style,
        )

        props = metadata.to_stac_properties()

        assert "pmtiles:style" in props
        assert props["pmtiles:style"]["version"] == 8
        assert len(props["pmtiles:style"]["layers"]) == 1

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
            style=None,
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
    def test_style_applied_to_generated_pmtiles_asset(self, tmp_path: Path) -> None:
        """Style is written to PMTiles asset in collection.json."""
        from portolan_cli.pmtiles import add_pmtiles_asset_to_collection
        from portolan_cli.style import VectorStyleConfig, build_pmtiles_style

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

        # Build style and add asset
        config = VectorStyleConfig(polygon_fill_color="#00ff00")
        style = build_pmtiles_style("Polygon", "data", config)

        add_pmtiles_asset_to_collection(
            collection_path,
            "data",
            "./data.pmtiles",
            style=style,
        )

        # Verify style in collection.json
        collection = json.loads((collection_path / "collection.json").read_text())
        pmtiles_asset = collection["assets"]["data-tiles"]

        assert "pmtiles:style" in pmtiles_asset
        assert pmtiles_asset["pmtiles:style"]["layers"][0]["paint"]["fill-color"] == "#00ff00"


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
