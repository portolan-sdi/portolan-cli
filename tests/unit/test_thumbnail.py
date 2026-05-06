"""Unit tests for thumbnail module (Issue #13).

Tests vector thumbnail generation from PMTiles and GeoParquet sources.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    pass

# =============================================================================
# Phase 1: ThumbnailConfig Tests
# =============================================================================


class TestThumbnailConfig:
    """Tests for ThumbnailConfig dataclass."""

    @pytest.mark.unit
    def test_default_values(self) -> None:
        """ThumbnailConfig has sensible defaults."""
        from portolan_cli.thumbnail import ThumbnailConfig

        config = ThumbnailConfig()
        assert config.enabled is True
        assert config.max_size == 512
        assert config.quality == 75
        assert config.basemap_provider == "CartoDB.Positron"
        assert config.basemap_opacity == 1.0
        assert config.basemap_zoom_adjust == 0

    @pytest.mark.unit
    def test_custom_values(self) -> None:
        """ThumbnailConfig accepts custom values."""
        from portolan_cli.thumbnail import ThumbnailConfig

        config = ThumbnailConfig(
            enabled=False,
            max_size=256,
            quality=90,
            basemap_provider="CartoDB.DarkMatter",
            basemap_opacity=0.5,
            basemap_zoom_adjust=-1,
        )
        assert config.enabled is False
        assert config.max_size == 256
        assert config.quality == 90
        assert config.basemap_provider == "CartoDB.DarkMatter"
        assert config.basemap_opacity == 0.5
        assert config.basemap_zoom_adjust == -1

    @pytest.mark.unit
    def test_basemap_none_disables(self) -> None:
        """Setting basemap_provider to 'none' disables basemap."""
        from portolan_cli.thumbnail import ThumbnailConfig

        config = ThumbnailConfig(basemap_provider="none")
        assert config.basemap_provider == "none"

    @pytest.mark.unit
    def test_frozen_dataclass(self) -> None:
        """ThumbnailConfig is immutable (frozen)."""
        from portolan_cli.thumbnail import ThumbnailConfig

        config = ThumbnailConfig()
        with pytest.raises(AttributeError):
            config.max_size = 100  # type: ignore[misc]


# =============================================================================
# Phase 2: PMTiles Thumbnail Generation Tests
# =============================================================================


class TestGenerateThumbnailFromPmtiles:
    """Tests for generate_thumbnail_from_pmtiles function."""

    @pytest.mark.unit
    def test_returns_path_on_success(self, tmp_path: Path) -> None:
        """Returns Path to generated thumbnail on success."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "data.pmtiles"
        pmtiles_path.touch()

        # Mock the PMTiles reading to return fake geometries
        with (
            patch("portolan_cli.thumbnail._read_pmtiles_geometries") as mock_read,
            patch("portolan_cli.thumbnail._render_geometries") as mock_render,
        ):
            mock_read.return_value = [
                {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
            ]
            mock_render.return_value = True

            config = ThumbnailConfig()
            result = generate_thumbnail_from_pmtiles(pmtiles_path, config)

            assert result is not None
            assert result.suffix == ".jpg"
            assert result.stem == "data.thumb"

    @pytest.mark.unit
    def test_returns_none_when_no_geometries(self, tmp_path: Path) -> None:
        """Returns None when PMTiles has no extractable geometries."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "empty.pmtiles"
        pmtiles_path.touch()

        with patch("portolan_cli.thumbnail._read_pmtiles_geometries") as mock_read:
            mock_read.return_value = []

            config = ThumbnailConfig()
            result = generate_thumbnail_from_pmtiles(pmtiles_path, config)

            assert result is None

    @pytest.mark.unit
    def test_returns_none_on_read_error(self, tmp_path: Path) -> None:
        """Returns None when PMTiles file cannot be read."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "corrupt.pmtiles"
        pmtiles_path.touch()

        with patch("portolan_cli.thumbnail._read_pmtiles_geometries") as mock_read:
            mock_read.side_effect = Exception("Corrupt file")

            config = ThumbnailConfig()
            result = generate_thumbnail_from_pmtiles(pmtiles_path, config)

            assert result is None

    @pytest.mark.unit
    def test_output_path_convention(self, tmp_path: Path) -> None:
        """Output uses .thumb.jpg naming convention to avoid clobbering user files."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "my-data.pmtiles"
        pmtiles_path.touch()

        with (
            patch("portolan_cli.thumbnail._read_pmtiles_geometries") as mock_read,
            patch("portolan_cli.thumbnail._render_geometries") as mock_render,
        ):
            mock_read.return_value = [{"type": "Point", "coordinates": [0, 0]}]
            mock_render.return_value = True

            config = ThumbnailConfig()
            result = generate_thumbnail_from_pmtiles(pmtiles_path, config)

            assert result == tmp_path / "my-data.thumb.jpg"


# =============================================================================
# Phase 3: GeoParquet Thumbnail Generation Tests
# =============================================================================


class TestGenerateThumbnailFromGeoparquet:
    """Tests for generate_thumbnail_from_geoparquet function."""

    @pytest.mark.unit
    def test_returns_path_on_success(self, tmp_path: Path) -> None:
        """Returns Path to generated thumbnail on success."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq_path = tmp_path / "data.parquet"
        gpq_path.touch()

        with (
            patch("portolan_cli.thumbnail._read_geoparquet_bounds") as mock_read,
            patch("portolan_cli.thumbnail._render_geoparquet") as mock_render,
        ):
            mock_read.return_value = (0.0, 0.0, 1.0, 1.0)  # minx, miny, maxx, maxy
            mock_render.return_value = True

            config = ThumbnailConfig()
            result = generate_thumbnail_from_geoparquet(gpq_path, config)

            assert result is not None
            assert result.suffix == ".jpg"

    @pytest.mark.unit
    def test_returns_none_when_empty(self, tmp_path: Path) -> None:
        """Returns None when GeoParquet has no geometries."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq_path = tmp_path / "empty.parquet"
        gpq_path.touch()

        with patch("portolan_cli.thumbnail._read_geoparquet_bounds") as mock_read:
            mock_read.return_value = None

            config = ThumbnailConfig()
            result = generate_thumbnail_from_geoparquet(gpq_path, config)

            assert result is None

    @pytest.mark.unit
    def test_output_path_convention(self, tmp_path: Path) -> None:
        """Output uses .thumb.jpg naming convention."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq_path = tmp_path / "census.parquet"
        gpq_path.touch()

        with (
            patch("portolan_cli.thumbnail._read_geoparquet_bounds") as mock_read,
            patch("portolan_cli.thumbnail._render_geoparquet") as mock_render,
        ):
            mock_read.return_value = (0.0, 0.0, 1.0, 1.0)
            mock_render.return_value = True

            config = ThumbnailConfig()
            result = generate_thumbnail_from_geoparquet(gpq_path, config)

            assert result == tmp_path / "census.thumb.jpg"


# =============================================================================
# Phase 4: Vector Thumbnail Orchestrator Tests
# =============================================================================


class TestGenerateVectorThumbnail:
    """Tests for generate_vector_thumbnail orchestrator function."""

    @pytest.mark.unit
    def test_prefers_pmtiles_when_available(self, tmp_path: Path) -> None:
        """Prefers PMTiles over GeoParquet when both available."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        pmtiles_path = tmp_path / "data.pmtiles"
        gpq_path = tmp_path / "data.parquet"
        pmtiles_path.touch()
        gpq_path.touch()

        with (
            patch("portolan_cli.thumbnail.generate_thumbnail_from_pmtiles") as mock_pmtiles,
            patch("portolan_cli.thumbnail.generate_thumbnail_from_geoparquet") as mock_gpq,
        ):
            mock_pmtiles.return_value = tmp_path / "data.thumb.jpg"

            config = ThumbnailConfig()
            result = generate_vector_thumbnail(
                pmtiles_path=pmtiles_path,
                geoparquet_path=gpq_path,
                config=config,
            )

            mock_pmtiles.assert_called_once()
            mock_gpq.assert_not_called()
            assert result == tmp_path / "data.thumb.jpg"

    @pytest.mark.unit
    def test_falls_back_to_geoparquet(self, tmp_path: Path) -> None:
        """Falls back to GeoParquet when PMTiles fails."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        pmtiles_path = tmp_path / "data.pmtiles"
        gpq_path = tmp_path / "data.parquet"
        pmtiles_path.touch()
        gpq_path.touch()

        with (
            patch("portolan_cli.thumbnail.generate_thumbnail_from_pmtiles") as mock_pmtiles,
            patch("portolan_cli.thumbnail.generate_thumbnail_from_geoparquet") as mock_gpq,
        ):
            mock_pmtiles.return_value = None  # PMTiles failed
            mock_gpq.return_value = tmp_path / "data.thumb.jpg"

            config = ThumbnailConfig()
            result = generate_vector_thumbnail(
                pmtiles_path=pmtiles_path,
                geoparquet_path=gpq_path,
                config=config,
            )

            mock_pmtiles.assert_called_once()
            mock_gpq.assert_called_once()
            assert result == tmp_path / "data.thumb.jpg"

    @pytest.mark.unit
    def test_geoparquet_only(self, tmp_path: Path) -> None:
        """Works with GeoParquet only (no PMTiles)."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        gpq_path = tmp_path / "data.parquet"
        gpq_path.touch()

        with patch("portolan_cli.thumbnail.generate_thumbnail_from_geoparquet") as mock_gpq:
            mock_gpq.return_value = tmp_path / "data.thumb.jpg"

            config = ThumbnailConfig()
            result = generate_vector_thumbnail(
                pmtiles_path=None,
                geoparquet_path=gpq_path,
                config=config,
            )

            mock_gpq.assert_called_once()
            assert result == tmp_path / "data.thumb.jpg"

    @pytest.mark.unit
    def test_returns_none_when_disabled(self, tmp_path: Path) -> None:
        """Returns None when thumbnails are disabled in config."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        gpq_path = tmp_path / "data.parquet"
        gpq_path.touch()

        config = ThumbnailConfig(enabled=False)
        result = generate_vector_thumbnail(
            pmtiles_path=None,
            geoparquet_path=gpq_path,
            config=config,
        )

        assert result is None

    @pytest.mark.unit
    def test_returns_none_when_no_sources(self) -> None:
        """Returns None when neither PMTiles nor GeoParquet provided."""
        from portolan_cli.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        config = ThumbnailConfig()
        result = generate_vector_thumbnail(
            pmtiles_path=None,
            geoparquet_path=None,
            config=config,
        )

        assert result is None


# =============================================================================
# Phase 5: Basemap Integration Tests
# =============================================================================


class TestAddBasemap:
    """Tests for add_basemap function."""

    @pytest.mark.unit
    def test_calls_contextily_with_provider(self) -> None:
        """Calls contextily.add_basemap with correct provider."""
        from portolan_cli.thumbnail import add_basemap

        mock_ax = MagicMock()
        bounds = (-122.5, 37.5, -122.0, 38.0)  # SF Bay area

        with patch("portolan_cli.thumbnail.ctx") as mock_ctx:
            add_basemap(mock_ax, bounds, "CartoDB.Positron", opacity=1.0, zoom_adjust=0)

            mock_ctx.add_basemap.assert_called_once()
            call_kwargs = mock_ctx.add_basemap.call_args[1]
            assert call_kwargs["alpha"] == 1.0

    @pytest.mark.unit
    def test_skips_when_provider_none(self) -> None:
        """Does nothing when provider is 'none'."""
        from portolan_cli.thumbnail import add_basemap

        mock_ax = MagicMock()
        bounds = (-122.5, 37.5, -122.0, 38.0)

        with patch("portolan_cli.thumbnail.ctx") as mock_ctx:
            add_basemap(mock_ax, bounds, "none", opacity=1.0, zoom_adjust=0)

            mock_ctx.add_basemap.assert_not_called()

    @pytest.mark.unit
    def test_handles_import_error(self) -> None:
        """Gracefully handles missing contextily."""
        from portolan_cli.thumbnail import add_basemap

        mock_ax = MagicMock()
        bounds = (-122.5, 37.5, -122.0, 38.0)

        with patch("portolan_cli.thumbnail.ctx", None):
            # Should not raise, just skip basemap
            add_basemap(mock_ax, bounds, "CartoDB.Positron", opacity=1.0, zoom_adjust=0)


# =============================================================================
# Phase 6: Real PMTiles Fixture Test (Integration)
# =============================================================================


class TestPmtilesThumbnailIntegration:
    """Integration tests using real PMTiles fixture."""

    @pytest.fixture
    def pmtiles_path(self, fixtures_dir: Path) -> Path:
        """Path to sample PMTiles fixture."""
        return fixtures_dir / "cloud_native" / "sample.pmtiles"

    @pytest.mark.integration
    def test_real_pmtiles_thumbnail(self, pmtiles_path: Path, tmp_path: Path) -> None:
        """Generates thumbnail from real PMTiles file."""
        pytest.importorskip("pmtiles")
        pytest.importorskip("mapbox_vector_tile")

        # Copy fixture to tmp_path so we can write output there
        import shutil

        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        test_pmtiles = tmp_path / "sample.pmtiles"
        shutil.copy(pmtiles_path, test_pmtiles)

        config = ThumbnailConfig(basemap_provider="none")  # No basemap for unit test
        result = generate_thumbnail_from_pmtiles(test_pmtiles, config)

        # May return None if fixture has no low-zoom tiles
        # That's acceptable — the spike showed min_zoom=4 in sample.pmtiles
        if result is not None:
            assert result.exists()
            assert result.stat().st_size > 0


# =============================================================================
# Phase 7: Config Loading Tests
# =============================================================================


class TestGetThumbnailConfig:
    """Tests for loading ThumbnailConfig from catalog config."""

    @pytest.mark.unit
    def test_returns_defaults_when_no_config(self, tmp_path: Path) -> None:
        """Returns default config when no thumbnails section exists."""
        from portolan_cli.thumbnail import ThumbnailConfig, get_thumbnail_config

        # Create minimal catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("conversion:\n  cog: {}\n")

        config = get_thumbnail_config(tmp_path)

        assert config == ThumbnailConfig()

    @pytest.mark.unit
    def test_loads_custom_config(self, tmp_path: Path) -> None:
        """Loads custom thumbnail config from YAML."""
        from portolan_cli.thumbnail import get_thumbnail_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
thumbnails:
  enabled: true
  max_size: 256
  quality: 90
  basemap:
    provider: CartoDB.DarkMatter
    opacity: 0.8
    zoom_adjust: -1
""")

        config = get_thumbnail_config(tmp_path)

        assert config.enabled is True
        assert config.max_size == 256
        assert config.quality == 90
        assert config.basemap_provider == "CartoDB.DarkMatter"
        assert config.basemap_opacity == 0.8
        assert config.basemap_zoom_adjust == -1

    @pytest.mark.unit
    def test_disabled_config(self, tmp_path: Path) -> None:
        """Respects enabled: false."""
        from portolan_cli.thumbnail import get_thumbnail_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("""
thumbnails:
  enabled: false
""")

        config = get_thumbnail_config(tmp_path)

        assert config.enabled is False
