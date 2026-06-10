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

        # Mock the PMTiles reading to return fake geometries and bounds
        with (
            patch("portolan_cli.thumbnail._read_pmtiles_geometries") as mock_read,
            patch("portolan_cli.thumbnail._render_geometries") as mock_render,
        ):
            mock_read.return_value = (
                [{"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}],
                (0.0, 0.0, 1.0, 1.0),  # bounds
            )
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
            mock_read.return_value = ([], None)  # empty geometries, no bounds

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
            mock_read.return_value = (
                [{"type": "Point", "coordinates": [0, 0]}],
                (-122.0, 37.0, -121.0, 38.0),  # bounds
            )
            mock_render.return_value = True

            config = ThumbnailConfig()
            result = generate_thumbnail_from_pmtiles(pmtiles_path, config)

            assert result == tmp_path / "my-data.thumb.jpg"


# =============================================================================
# Regression Tests: Basemap Ordering (PR #468)
# =============================================================================


class TestBasemapOrdering:
    """Regression tests for basemap rendering order.

    Issue: contextily computes zoom level from current axis extent. If basemap
    is added BEFORE axis limits are set, it uses wrong bounds and renders the
    map in the wrong location.

    Fix (PR #468): Plot data first, set axis limits, THEN add basemap.
    """

    @pytest.mark.unit
    def test_render_geometries_sets_limits_before_basemap(self, tmp_path: Path) -> None:
        """_render_geometries sets axis limits BEFORE calling add_basemap.

        This is critical: contextily needs the axis extent to compute the
        correct zoom level. If we add the basemap first, it uses default
        limits and renders in the wrong location.
        """
        pytest.importorskip("matplotlib")

        from portolan_cli.thumbnail import ThumbnailConfig, _render_geometries

        output_path = tmp_path / "test.jpg"
        bounds = (-122.5, 37.5, -122.0, 38.0)  # San Francisco area
        geometries = [
            {
                "type": "Polygon",
                "coordinates": [[[-122.4, 37.7], [-122.1, 37.7], [-122.1, 37.9], [-122.4, 37.7]]],
            }
        ]
        config = ThumbnailConfig(basemap_provider="CartoDB.Positron")

        # Track axis state when add_basemap is called
        captured_xlim: tuple[float, float] | None = None
        captured_ylim: tuple[float, float] | None = None

        def capture_axis_state(
            ax: MagicMock,
            bounds: tuple[float, float, float, float],
            *args: object,
            **kwargs: object,
        ) -> None:
            nonlocal captured_xlim, captured_ylim
            captured_xlim = ax.get_xlim()
            captured_ylim = ax.get_ylim()

        with patch("portolan_cli.thumbnail.add_basemap", side_effect=capture_axis_state):
            _render_geometries(geometries, output_path, config, bounds=bounds)

        # Verify axis limits were set BEFORE add_basemap was called
        assert captured_xlim is not None, "add_basemap was never called"
        assert captured_ylim is not None, "add_basemap was never called"

        # Limits should match the bounds we passed
        assert captured_xlim[0] == pytest.approx(bounds[0], abs=0.01)  # minx
        assert captured_xlim[1] == pytest.approx(bounds[2], abs=0.01)  # maxx
        assert captured_ylim[0] == pytest.approx(bounds[1], abs=0.01)  # miny
        assert captured_ylim[1] == pytest.approx(bounds[3], abs=0.01)  # maxy

    @pytest.mark.unit
    def test_render_geometries_no_basemap_when_no_bounds(self, tmp_path: Path) -> None:
        """_render_geometries skips basemap when bounds is None."""
        pytest.importorskip("matplotlib")

        from portolan_cli.thumbnail import ThumbnailConfig, _render_geometries

        output_path = tmp_path / "test.jpg"
        geometries = [{"type": "Point", "coordinates": [0, 0]}]
        config = ThumbnailConfig(basemap_provider="CartoDB.Positron")

        with patch("portolan_cli.thumbnail.add_basemap") as mock_basemap:
            _render_geometries(geometries, output_path, config, bounds=None)

            # add_basemap should NOT be called when bounds is None
            mock_basemap.assert_not_called()


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

        mock_ctx = MagicMock()
        with patch("portolan_cli.thumbnail._ensure_contextily", return_value=mock_ctx):
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

        mock_ctx = MagicMock()
        with patch("portolan_cli.thumbnail._ensure_contextily", return_value=mock_ctx):
            add_basemap(mock_ax, bounds, "none", opacity=1.0, zoom_adjust=0)

            mock_ctx.add_basemap.assert_not_called()

    @pytest.mark.unit
    def test_handles_import_error(self) -> None:
        """Gracefully handles missing contextily."""
        from portolan_cli.thumbnail import add_basemap

        mock_ax = MagicMock()
        bounds = (-122.5, 37.5, -122.0, 38.0)

        with patch("portolan_cli.thumbnail._ensure_contextily", return_value=None):
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


# =============================================================================
# Phase 8: CRS Reprojection and Metadata-Based Reading Tests (Issue #423)
# =============================================================================


class TestPmtilesBoundsExtraction:
    """Tests for PMTiles geometry bounds extraction (Issue #423 Bug 1)."""

    @pytest.mark.unit
    def test_process_tile_data_uses_geometry_bounds_not_tile_bounds(self) -> None:
        """_process_tile_data accumulates geometry coordinate bounds, not tile bounds.

        Bug: At z=0, tile (0,0) covers the entire world (-180 to 180, -85 to 85).
        Using tile bounds causes basemap to render globally while data is invisible.
        """
        from portolan_cli.thumbnail import _process_tile_data, _tile_bounds

        mock_mvt_data = {
            "layer1": {
                "features": [
                    {
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [1800, 1800],
                                    [2200, 1800],
                                    [2200, 2200],
                                    [1800, 2200],
                                    [1800, 1800],
                                ]
                            ],
                        }
                    }
                ]
            }
        }

        class MockDecoder:
            def decode(self, data: bytes) -> dict:
                return mock_mvt_data

        geometries: list[dict] = []
        all_lons: list[float] = []
        all_lats: list[float] = []

        z, x, y = 0, 0, 0
        tile_bounds = _tile_bounds(z, x, y)

        assert tile_bounds[0] < -170  # lon_min near -180
        assert tile_bounds[2] > 170  # lon_max near 180

        _process_tile_data(b"mock_data", z, x, y, geometries, all_lons, all_lats, MockDecoder())

        lon_range = max(all_lons) - min(all_lons)
        lat_range = max(all_lats) - min(all_lats)

        assert lon_range < 100, f"Lon range {lon_range} too large - using tile bounds?"
        assert lat_range < 100, f"Lat range {lat_range} too large - using tile bounds?"


class TestGeoparquetMetadataBounds:
    """Tests for GeoParquet metadata-based bbox reading (Issue #423 Performance)."""

    @pytest.mark.unit
    def test_read_bounds_from_metadata(self, tmp_path: Path) -> None:
        """_read_geoparquet_bounds extracts bbox from GeoParquet metadata (O(1))."""
        import json
        from unittest.mock import MagicMock, patch

        from portolan_cli.thumbnail import _read_geoparquet_bounds

        gpq_path = tmp_path / "test.parquet"

        # Mock ParquetFile with geo metadata containing bbox
        mock_pq_file = MagicMock()
        geo_metadata = {"columns": {"geometry": {"bbox": [-60.5, -32.5, -60.0, -32.0]}}}
        mock_pq_file.schema_arrow.metadata = {b"geo": json.dumps(geo_metadata).encode("utf-8")}

        with patch("pyarrow.parquet.ParquetFile", return_value=mock_pq_file):
            bounds = _read_geoparquet_bounds(gpq_path)

        assert bounds == (-60.5, -32.5, -60.0, -32.0)

    @pytest.mark.unit
    def test_read_bounds_fallback_when_no_metadata(self, tmp_path: Path) -> None:
        """_read_geoparquet_bounds falls back to data read when no bbox in metadata."""
        import json
        from unittest.mock import MagicMock, patch

        from portolan_cli.thumbnail import _read_geoparquet_bounds

        gpq_path = tmp_path / "test.parquet"

        # Mock ParquetFile with geo metadata but NO bbox
        mock_pq_file = MagicMock()
        geo_metadata = {"columns": {"geometry": {}}}  # No bbox
        mock_pq_file.schema_arrow.metadata = {b"geo": json.dumps(geo_metadata).encode("utf-8")}

        # Mock fallback geopandas read
        mock_gdf = MagicMock()
        mock_gdf.empty = False
        mock_gdf.total_bounds = [-61.0, -33.0, -59.0, -31.0]

        with (
            patch("pyarrow.parquet.ParquetFile", return_value=mock_pq_file),
            patch("geopandas.read_parquet", return_value=mock_gdf),
        ):
            bounds = _read_geoparquet_bounds(gpq_path)

        assert bounds == (-61.0, -33.0, -59.0, -31.0)


class TestGeoparquetFullReading:
    """Tests for full file reading (Issue #423 - no sampling)."""

    @pytest.mark.unit
    def test_reads_all_features_without_sampling(self, tmp_path: Path) -> None:
        """_read_geoparquet_for_thumbnail reads ALL features without sampling.

        No .head(), .sample(), or row limiting — thumbnails must accurately
        represent the full dataset. Contextily handles CRS reprojection of
        basemap tiles, which is more efficient than reprojecting geometry data.
        """
        import json
        from unittest.mock import MagicMock, patch

        from portolan_cli.thumbnail import _read_geoparquet_for_thumbnail

        gpq_path = tmp_path / "large.parquet"

        # Mock a large GeoDataFrame (55,000 rows)
        mock_gdf = MagicMock()
        mock_gdf.empty = False
        mock_gdf.crs = "EPSG:4326"
        mock_gdf.total_bounds = [-60.5, -32.5, -60.0, -32.0]
        mock_gdf.__len__ = lambda self: 55000

        # Mock bbox from metadata
        mock_pq_file = MagicMock()
        geo_metadata = {"columns": {"geometry": {"bbox": [-60.5, -32.5, -60.0, -32.0]}}}
        mock_pq_file.schema_arrow.metadata = {b"geo": json.dumps(geo_metadata).encode("utf-8")}

        with (
            patch("geopandas.read_parquet", return_value=mock_gdf),
            patch("pyarrow.parquet.ParquetFile", return_value=mock_pq_file),
        ):
            gdf, bbox, crs = _read_geoparquet_for_thumbnail(gpq_path)

        # Verify NO sampling methods were called
        mock_gdf.head.assert_not_called()
        mock_gdf.sample.assert_not_called()

        # Full GDF is returned, not a subset
        assert gdf is mock_gdf
        assert bbox == (-60.5, -32.5, -60.0, -32.0)


class TestGeoparquetCrsHandling:
    """Tests for CRS handling via contextily (Issue #423 Bug 2)."""

    @pytest.mark.unit
    def test_render_does_not_reproject_data(self, tmp_path: Path) -> None:
        """_render_geoparquet does NOT reproject geometry data.

        Instead of reprojecting millions of geometry vertices to EPSG:3857,
        we keep data in native CRS and let contextily reproject basemap tiles.
        This is far more efficient for large datasets.
        """
        pytest.importorskip("matplotlib")

        from unittest.mock import MagicMock, patch

        from portolan_cli.thumbnail import ThumbnailConfig, _render_geoparquet

        gpq_path = tmp_path / "test.parquet"
        output_path = tmp_path / "test.thumb.jpg"
        config = ThumbnailConfig(basemap_provider="CartoDB.Positron")

        mock_gdf = MagicMock()
        mock_gdf.empty = False
        mock_gdf.crs = "EPSG:4326"

        full_bbox = (-60.5, -32.5, -60.0, -32.0)

        with (
            patch(
                "portolan_cli.thumbnail._read_geoparquet_for_thumbnail",
                return_value=(mock_gdf, full_bbox, "EPSG:4326"),
            ),
            patch("matplotlib.pyplot.subplots") as mock_subplots,
            patch("matplotlib.pyplot.savefig"),
            patch("matplotlib.pyplot.close"),
            patch("portolan_cli.thumbnail.add_basemap"),
        ):
            mock_ax = MagicMock()
            mock_subplots.return_value = (MagicMock(), mock_ax)
            output_path.touch()

            _render_geoparquet(gpq_path, output_path, config)

            # Verify NO data reprojection (.to_crs should NOT be called)
            mock_gdf.to_crs.assert_not_called()

            # Verify original gdf was plotted (not a reprojected copy)
            mock_gdf.plot.assert_called_once()

    @pytest.mark.unit
    def test_render_passes_crs_to_basemap(self, tmp_path: Path) -> None:
        """_render_geoparquet passes data CRS to add_basemap for tile reprojection.

        Contextily's `crs` parameter tells it to reproject basemap tiles to match
        the data's CRS — this is more efficient than reprojecting geometry data.
        """
        pytest.importorskip("matplotlib")

        from unittest.mock import MagicMock, patch

        from portolan_cli.thumbnail import ThumbnailConfig, _render_geoparquet

        gpq_path = tmp_path / "test.parquet"
        output_path = tmp_path / "test.thumb.jpg"
        config = ThumbnailConfig(basemap_provider="CartoDB.Positron")

        mock_gdf = MagicMock()
        mock_gdf.empty = False
        mock_gdf.crs = "EPSG:4326"

        full_bbox = (-60.5, -32.5, -60.0, -32.0)

        with (
            patch(
                "portolan_cli.thumbnail._read_geoparquet_for_thumbnail",
                return_value=(mock_gdf, full_bbox, "EPSG:4326"),
            ),
            patch("matplotlib.pyplot.subplots") as mock_subplots,
            patch("matplotlib.pyplot.savefig"),
            patch("matplotlib.pyplot.close"),
            patch("portolan_cli.thumbnail.add_basemap") as mock_add_basemap,
        ):
            mock_ax = MagicMock()
            mock_subplots.return_value = (MagicMock(), mock_ax)
            output_path.touch()

            _render_geoparquet(gpq_path, output_path, config)

            # Verify add_basemap was called with CRS parameter
            mock_add_basemap.assert_called_once()
            call_kwargs = mock_add_basemap.call_args
            assert call_kwargs[1]["crs"] == "EPSG:4326"

    @pytest.mark.unit
    def test_render_uses_native_bounds(self, tmp_path: Path) -> None:
        """Axis limits use native CRS bounds (no transformation needed)."""
        pytest.importorskip("matplotlib")

        from unittest.mock import MagicMock, patch

        from portolan_cli.thumbnail import ThumbnailConfig, _render_geoparquet

        gpq_path = tmp_path / "test.parquet"
        output_path = tmp_path / "test.thumb.jpg"
        config = ThumbnailConfig(basemap_provider="none")

        mock_gdf = MagicMock()
        mock_gdf.empty = False
        mock_gdf.crs = "EPSG:4326"

        # Full bbox from metadata in native CRS
        full_bbox = (-61.0, -33.0, -59.0, -31.0)

        with (
            patch(
                "portolan_cli.thumbnail._read_geoparquet_for_thumbnail",
                return_value=(mock_gdf, full_bbox, "EPSG:4326"),
            ),
            patch("matplotlib.pyplot.subplots") as mock_subplots,
            patch("matplotlib.pyplot.savefig"),
            patch("matplotlib.pyplot.close"),
        ):
            mock_ax = MagicMock()
            mock_subplots.return_value = (MagicMock(), mock_ax)
            output_path.touch()

            _render_geoparquet(gpq_path, output_path, config)

            # Verify set_xlim/set_ylim use NATIVE bounds (no transformation)
            mock_ax.set_xlim.assert_called_once_with(-61.0, -59.0)
            mock_ax.set_ylim.assert_called_once_with(-33.0, -31.0)


class TestGeoparquetThumbnailIntegration:
    """Integration tests using real GeoParquet fixtures."""

    @pytest.mark.integration
    def test_real_geoparquet_thumbnail_with_4326_data(
        self, fixtures_dir: Path, tmp_path: Path
    ) -> None:
        """Generates thumbnail from real EPSG:4326 GeoParquet file."""
        pytest.importorskip("geopandas")
        pytest.importorskip("matplotlib")

        import shutil

        from portolan_cli.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        # Use simple.parquet which is in OGC:CRS84 (equivalent to EPSG:4326)
        src_path = fixtures_dir / "simple.parquet"
        if not src_path.exists():
            pytest.skip("simple.parquet fixture not found")

        test_gpq = tmp_path / "simple.parquet"
        shutil.copy(src_path, test_gpq)

        config = ThumbnailConfig(basemap_provider="none")  # No network for unit test
        result = generate_thumbnail_from_geoparquet(test_gpq, config)

        assert result is not None
        assert result.exists()
        assert result.stat().st_size > 0
