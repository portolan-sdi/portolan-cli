"""Unit tests for thumbnail module (Issue #13).

Tests vector thumbnail generation from PMTiles and GeoParquet sources.
"""

from __future__ import annotations

import io
import json
import threading
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

# =============================================================================
# Phase 1: ThumbnailConfig Tests
# =============================================================================


class TestThumbnailConfig:
    """Tests for ThumbnailConfig dataclass."""

    @pytest.mark.unit
    def test_default_values(self) -> None:
        """ThumbnailConfig has sensible defaults."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig

        config = ThumbnailConfig()
        assert config.enabled is True
        assert config.max_size == 512
        assert config.quality == 75
        assert config.basemap_provider == "Esri.WorldGrayCanvas"
        assert config.basemap_opacity == 1.0
        assert config.basemap_zoom_adjust == 0
        assert config.basemap_api_key is None

    @pytest.mark.unit
    def test_custom_values(self) -> None:
        """ThumbnailConfig accepts custom values."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig

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
        from portolan_cli.viz.thumbnail import ThumbnailConfig

        config = ThumbnailConfig(basemap_provider="none")
        assert config.basemap_provider == "none"

    @pytest.mark.unit
    def test_frozen_dataclass(self) -> None:
        """ThumbnailConfig is immutable (frozen)."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig

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
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "data.pmtiles"
        pmtiles_path.touch()

        # Mock the PMTiles reading to return fake geometries and bounds
        with (
            patch("portolan_cli.viz.thumbnail._read_pmtiles_geometries") as mock_read,
            patch("portolan_cli.viz.thumbnail._render_geometries") as mock_render,
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
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "empty.pmtiles"
        pmtiles_path.touch()

        with patch("portolan_cli.viz.thumbnail._read_pmtiles_geometries") as mock_read:
            mock_read.return_value = ([], None)  # empty geometries, no bounds

            config = ThumbnailConfig()
            result = generate_thumbnail_from_pmtiles(pmtiles_path, config)

            assert result is None

    @pytest.mark.unit
    def test_returns_none_on_read_error(self, tmp_path: Path) -> None:
        """Returns None when PMTiles file cannot be read."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "corrupt.pmtiles"
        pmtiles_path.touch()

        with patch("portolan_cli.viz.thumbnail._read_pmtiles_geometries") as mock_read:
            mock_read.side_effect = Exception("Corrupt file")

            config = ThumbnailConfig()
            result = generate_thumbnail_from_pmtiles(pmtiles_path, config)

            assert result is None

    @pytest.mark.unit
    def test_output_path_convention(self, tmp_path: Path) -> None:
        """Output uses .thumb.jpg naming convention to avoid clobbering user files."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

        pmtiles_path = tmp_path / "my-data.pmtiles"
        pmtiles_path.touch()

        with (
            patch("portolan_cli.viz.thumbnail._read_pmtiles_geometries") as mock_read,
            patch("portolan_cli.viz.thumbnail._render_geometries") as mock_render,
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
    """The frame is set before the basemap reads it.

    The basemap zoom and tile range come from the axes limits. If the basemap
    ran before the limits were set it would fetch tiles for the default 0..1
    extent (PR #468).
    """

    @pytest.mark.unit
    def test_render_geometries_sets_limits_before_basemap(self, tmp_path: Path) -> None:
        from pyproj import Transformer

        from portolan_cli.viz.thumbnail import ThumbnailConfig, _render_geometries

        output_path = tmp_path / "test.jpg"
        geometries = [
            {
                "type": "Polygon",
                "coordinates": [[[-122.4, 37.7], [-122.1, 37.7], [-122.1, 37.9], [-122.4, 37.7]]],
            }
        ]
        bounds = (-122.5, 37.5, -122.0, 38.0)  # SF Bay area
        captured: list[tuple[tuple[float, float], tuple[float, float]]] = []

        def capture_axis_state(ax: Any, *args: object, **kwargs: object) -> None:
            captured.append((ax.get_xlim(), ax.get_ylim()))

        with patch("portolan_cli.viz.thumbnail.add_basemap", side_effect=capture_axis_state):
            _render_geometries(geometries, output_path, ThumbnailConfig(), bounds=bounds)

        ((xlim, ylim),) = captured
        to_mercator = Transformer.from_crs("OGC:CRS84", "EPSG:3857", always_xy=True)
        minx, miny, maxx, maxy = to_mercator.transform_bounds(*bounds)
        # The framed limits enclose the projected data bounds, not 0..1.
        assert xlim[0] < minx < maxx < xlim[1]
        assert ylim[0] < miny < maxy < ylim[1]

    @pytest.mark.unit
    def test_render_geometries_frames_the_geometries_without_bounds(
        self, tmp_path: Path, saved_figures: list[Any]
    ) -> None:
        """With no bounds, the frame comes from the geometries themselves."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, _render_geometries

        geometries = [
            {"type": "Point", "coordinates": [10.0, 10.0]},
            {"type": "Point", "coordinates": [20.0, 15.0]},
        ]
        config = ThumbnailConfig(basemap_provider="none")

        assert _render_geometries(geometries, tmp_path / "test.jpg", config, bounds=None)

        (figure,) = saved_figures
        xlim = figure.axes[0].get_xlim()
        # 10 and 20 degrees east in EPSG:3857 metres, inside the framed limits.
        assert xlim[0] < 1_113_194.9 < 2_226_389.8 < xlim[1]


# =============================================================================
# Phase 3: GeoParquet Thumbnail Generation Tests
# =============================================================================


class TestGenerateThumbnailFromGeoparquet:
    """Tests for generate_thumbnail_from_geoparquet function."""

    @pytest.mark.unit
    def test_returns_path_on_success(self, tmp_path: Path) -> None:
        """Returns Path to generated thumbnail on success."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq_path = tmp_path / "data.parquet"
        gpq_path.touch()

        with (
            patch("portolan_cli.viz.thumbnail._render_geoparquet") as mock_render,
        ):
            mock_render.return_value = True

            config = ThumbnailConfig()
            result = generate_thumbnail_from_geoparquet(gpq_path, config)

            assert result is not None
            assert result.suffix == ".jpg"

    @pytest.mark.unit
    def test_returns_none_when_empty(self, tmp_path: Path) -> None:
        """Returns None when GeoParquet has no geometries."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq_path = tmp_path / "empty.parquet"
        gpq_path.touch()

        with patch(
            "portolan_cli.viz.thumbnail._read_geoparquet_for_thumbnail",
            return_value=(None, None, None),
        ):
            result = generate_thumbnail_from_geoparquet(gpq_path, ThumbnailConfig())

        assert result is None
        assert not (tmp_path / "empty.thumb.jpg").exists()

    @pytest.mark.unit
    def test_output_path_convention(self, tmp_path: Path) -> None:
        """Output uses .thumb.jpg naming convention."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq_path = tmp_path / "census.parquet"
        gpq_path.touch()

        with (
            patch("portolan_cli.viz.thumbnail._render_geoparquet") as mock_render,
        ):
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
    def test_prefers_geoparquet_when_available(self, tmp_path: Path) -> None:
        """Prefers the exact GeoParquet geometry over PMTiles (issue #786)."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        pmtiles_path = tmp_path / "data.pmtiles"
        gpq_path = tmp_path / "data.parquet"
        pmtiles_path.touch()
        gpq_path.touch()

        with (
            patch("portolan_cli.viz.thumbnail.generate_thumbnail_from_pmtiles") as mock_pmtiles,
            patch("portolan_cli.viz.thumbnail.generate_thumbnail_from_geoparquet") as mock_gpq,
        ):
            mock_gpq.return_value = tmp_path / "data.thumb.jpg"

            config = ThumbnailConfig()
            result = generate_vector_thumbnail(
                pmtiles_path=pmtiles_path,
                geoparquet_path=gpq_path,
                config=config,
            )

            mock_gpq.assert_called_once()
            mock_pmtiles.assert_not_called()
            assert result == tmp_path / "data.thumb.jpg"

    @pytest.mark.unit
    def test_falls_back_to_pmtiles(self, tmp_path: Path) -> None:
        """Falls back to PMTiles when the GeoParquet render fails."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        pmtiles_path = tmp_path / "data.pmtiles"
        gpq_path = tmp_path / "data.parquet"
        pmtiles_path.touch()
        gpq_path.touch()

        with (
            patch("portolan_cli.viz.thumbnail.generate_thumbnail_from_pmtiles") as mock_pmtiles,
            patch("portolan_cli.viz.thumbnail.generate_thumbnail_from_geoparquet") as mock_gpq,
        ):
            mock_gpq.return_value = None  # GeoParquet render failed
            mock_pmtiles.return_value = tmp_path / "data.thumb.jpg"

            config = ThumbnailConfig()
            result = generate_vector_thumbnail(
                pmtiles_path=pmtiles_path,
                geoparquet_path=gpq_path,
                config=config,
            )

            mock_gpq.assert_called_once()
            mock_pmtiles.assert_called_once()
            assert result == tmp_path / "data.thumb.jpg"

    @pytest.mark.unit
    def test_missing_parquet_uses_pmtiles(self, tmp_path: Path) -> None:
        """A geoparquet path that does not exist on disk goes straight to PMTiles."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        pmtiles_path = tmp_path / "data.pmtiles"
        pmtiles_path.touch()
        gone = tmp_path / "gone.parquet"  # never created

        with (
            patch("portolan_cli.viz.thumbnail.generate_thumbnail_from_pmtiles") as mock_pmtiles,
            patch("portolan_cli.viz.thumbnail.generate_thumbnail_from_geoparquet") as mock_gpq,
        ):
            mock_pmtiles.return_value = tmp_path / "data.thumb.jpg"

            config = ThumbnailConfig()
            result = generate_vector_thumbnail(
                pmtiles_path=pmtiles_path,
                geoparquet_path=gone,
                config=config,
            )

            mock_gpq.assert_not_called()
            mock_pmtiles.assert_called_once()
            assert result == tmp_path / "data.thumb.jpg"

    @pytest.mark.unit
    def test_geoparquet_only(self, tmp_path: Path) -> None:
        """Works with GeoParquet only (no PMTiles)."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_vector_thumbnail

        gpq_path = tmp_path / "data.parquet"
        gpq_path.touch()

        with patch("portolan_cli.viz.thumbnail.generate_thumbnail_from_geoparquet") as mock_gpq:
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
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_vector_thumbnail

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
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_vector_thumbnail

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

    @staticmethod
    def _axes() -> Any:
        from portolan_cli.viz.thumbnail import _new_figure

        _figure, ax = _new_figure((0.0, 6_000_000.0, 200_000.0, 6_200_000.0), 512)
        return ax

    @pytest.mark.unit
    def test_draws_the_tiles_under_the_frame(self, stub_tiles: list[str]) -> None:
        from portolan_cli.viz.thumbnail import add_basemap

        ax = self._axes()
        add_basemap(ax, "Esri.WorldGrayCanvas", opacity=0.5, zoom_adjust=0)

        (image,) = ax.images
        assert image.get_alpha() == 0.5
        assert image.get_zorder() == 0
        assert ax.get_xlim() == (0.0, 200_000.0)
        assert stub_tiles

    @pytest.mark.unit
    def test_skips_when_provider_none(self, stub_tiles: list[str]) -> None:
        from portolan_cli.viz.thumbnail import add_basemap

        ax = self._axes()
        add_basemap(ax, "none", opacity=1.0, zoom_adjust=0)

        assert list(ax.images) == []
        assert stub_tiles == []

    @pytest.mark.unit
    def test_handles_missing_contextily(self) -> None:
        """The autouse fixture makes contextily unavailable here."""
        from portolan_cli.viz.thumbnail import add_basemap

        ax = self._axes()
        add_basemap(ax, "Esri.WorldGrayCanvas", opacity=1.0, zoom_adjust=0)

        assert list(ax.images) == []

    @pytest.mark.unit
    def test_unknown_provider_warns(
        self, stub_tiles: list[str], caplog: pytest.LogCaptureFixture
    ) -> None:
        from portolan_cli.viz.thumbnail import add_basemap

        ax = self._axes()
        with caplog.at_level("WARNING", logger="portolan_cli.viz.thumbnail"):
            add_basemap(ax, "No.SuchProvider")

        assert list(ax.images) == []
        assert "Unknown basemap provider 'No.SuchProvider'" in caplog.text


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

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_pmtiles

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
        from portolan_cli.viz.thumbnail import ThumbnailConfig, get_thumbnail_config

        # Create minimal catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("conversion:\n  cog: {}\n")

        config = get_thumbnail_config(tmp_path)

        assert config == ThumbnailConfig()

    @pytest.mark.unit
    def test_loads_custom_config(self, tmp_path: Path) -> None:
        """Loads custom thumbnail config from YAML."""
        from portolan_cli.viz.thumbnail import get_thumbnail_config

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
        from portolan_cli.viz.thumbnail import get_thumbnail_config

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
        from portolan_cli.viz.thumbnail import _process_tile_data, _tile_bounds

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

    @staticmethod
    def _write_parquet_with_geo(path: Path, geo_metadata: dict) -> Path:
        """Write a Parquet file that declares ``geo_metadata`` in its footer."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table({"geometry": pa.array([b""], pa.binary())})
        schema = table.schema.with_metadata({b"geo": json.dumps(geo_metadata).encode("utf-8")})
        pq.write_table(table.cast(schema), str(path))
        return path

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "bbox",
        [
            pytest.param([-60.5, -32.5, -60.0, -32.0], id="2d"),
            # Distinct z values, so a slice of the first four cannot pass by coincidence.
            # A 6-element 3D bbox reduces to 2D rather than keeping minz (issue #854).
            pytest.param([-60.5, -32.5, 120.0, -60.0, -32.0, 480.0], id="3d"),
        ],
    )
    def test_read_bounds_from_metadata(self, tmp_path: Path, bbox: list[float]) -> None:
        """_read_geoparquet_bounds extracts the XY bbox from GeoParquet metadata (O(1))."""
        from portolan_cli.viz.thumbnail import _read_geoparquet_bounds

        gpq_path = self._write_parquet_with_geo(
            tmp_path / "test.parquet",
            {"columns": {"geometry": {"bbox": bbox}}},
        )

        assert _read_geoparquet_bounds(gpq_path) == (-60.5, -32.5, -60.0, -32.0)

    @pytest.mark.unit
    def test_read_bounds_fallback_when_no_metadata(self, tmp_path: Path) -> None:
        """_read_geoparquet_bounds falls back to data read when no bbox in metadata."""
        from unittest.mock import patch

        from portolan_cli.viz.thumbnail import _read_geoparquet_bounds

        gpq_path = self._write_parquet_with_geo(
            tmp_path / "test.parquet",
            {"columns": {"geometry": {}}},  # No bbox
        )

        # Mock fallback geopandas read
        mock_gdf = MagicMock()
        mock_gdf.empty = False
        mock_gdf.total_bounds = [-61.0, -33.0, -59.0, -31.0]

        with patch("geopandas.read_parquet", return_value=mock_gdf):
            bounds = _read_geoparquet_bounds(gpq_path)

        assert bounds == (-61.0, -33.0, -59.0, -31.0)


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

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

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


# =============================================================================
# Phase 5: Data-aware framing (#518 Track 1)
# =============================================================================


class TestFrameBounds:
    """Tests for _frame_bounds aspect-cap + padding (Issue #518)."""

    @pytest.mark.unit
    def test_square_bounds_only_get_margin(self) -> None:
        """A square extent is not aspect-adjusted; only the margin expands it."""
        from portolan_cli.viz.thumbnail import _frame_bounds

        framed = _frame_bounds((0.0, 0.0, 10.0, 10.0), max_aspect=2.5, margin=0.05)
        # 5% of a 10-unit span = 0.5 on each side, no aspect change.
        assert framed[0] == pytest.approx(-0.5)
        assert framed[1] == pytest.approx(-0.5)
        assert framed[2] == pytest.approx(10.5)
        assert framed[3] == pytest.approx(10.5)

    @pytest.mark.unit
    def test_tall_sliver_capped_by_widening(self) -> None:
        """A 1:100 (w:h) sliver is widened so aspect is capped at max_aspect."""
        from portolan_cli.viz.thumbnail import _frame_bounds

        framed = _frame_bounds((0.0, 0.0, 1.0, 100.0), max_aspect=2.5, margin=0.0)
        width = framed[2] - framed[0]
        height = framed[3] - framed[1]
        # Capped: height/width == max_aspect, achieved by growing WIDTH.
        assert height / width == pytest.approx(2.5, rel=1e-6)
        assert height == pytest.approx(100.0)  # height untouched
        # Geometry stays centered on its original center (x=0.5).
        assert (framed[0] + framed[2]) / 2 == pytest.approx(0.5)

    @pytest.mark.unit
    def test_wide_sliver_capped_by_heightening(self) -> None:
        """A 100:1 (w:h) sliver is heightened so aspect is capped at max_aspect."""
        from portolan_cli.viz.thumbnail import _frame_bounds

        framed = _frame_bounds((0.0, 0.0, 100.0, 1.0), max_aspect=2.5, margin=0.0)
        width = framed[2] - framed[0]
        height = framed[3] - framed[1]
        assert width / height == pytest.approx(2.5, rel=1e-6)
        assert width == pytest.approx(100.0)
        assert (framed[1] + framed[3]) / 2 == pytest.approx(0.5)

    @pytest.mark.unit
    def test_degenerate_point_gets_finite_box(self) -> None:
        """A zero-area extent (single point) is expanded to a finite box."""
        from portolan_cli.viz.thumbnail import _frame_bounds

        framed = _frame_bounds((5.0, 5.0, 5.0, 5.0))
        assert framed[2] > framed[0]
        assert framed[3] > framed[1]


class TestComputeRenderParams:
    """Tests for data-aware _compute_render_params (Issue #518)."""

    @pytest.mark.unit
    def test_sparse_points_larger_than_dense(self) -> None:
        """Few points render with larger markers than a dense point cloud."""
        from portolan_cli.viz.thumbnail import _compute_render_params

        sparse = _compute_render_params("point", 5)
        dense = _compute_render_params("point", 50_000)
        assert sparse.marker_size > dense.marker_size

    @pytest.mark.unit
    def test_dense_polygons_more_transparent(self) -> None:
        """Dense polygon layers get lower fill opacity so they don't blob solid."""
        from portolan_cli.viz.thumbnail import _compute_render_params

        sparse = _compute_render_params("polygon", 5)
        dense = _compute_render_params("polygon", 50_000)
        assert dense.fill_opacity < sparse.fill_opacity

    @pytest.mark.unit
    def test_opacity_never_washed_out(self) -> None:
        """Fill opacity stays >= 0.5 for every geometry type and density.

        This is the anti-washout guarantee: the pale WFS default (0.2) must
        never reach the canvas via these params.
        """
        from portolan_cli.viz.thumbnail import _compute_render_params

        for geom in ("point", "line", "polygon"):
            for count in (1, 100, 1_000, 100_000):
                assert _compute_render_params(geom, count).fill_opacity >= 0.5

    @pytest.mark.unit
    def test_polygon_has_visible_edge(self) -> None:
        """Polygons always get a non-hairline stroke width."""
        from portolan_cli.viz.thumbnail import _compute_render_params

        assert _compute_render_params("polygon", 100).stroke_width > 0

    @pytest.mark.unit
    @pytest.mark.parametrize("geom", ["point", "line", "polygon"])
    def test_off_axis_dimensions_are_floored_not_zeroed(self, geom: str) -> None:
        """Both marker_size and stroke_width stay > 0 for every geometry type.

        A single RenderParams is applied to every feature in a layer, so a
        mixed-geometry layer would render its minority type invisibly if the
        off-axis dimension were zero (points need marker_size, lines/edges need
        stroke_width). Regression for that: every type must keep both positive.
        """
        from portolan_cli.viz.thumbnail import _compute_render_params

        params = _compute_render_params(geom, 100)
        assert params.marker_size > 0, f"{geom}: points would be invisible"
        assert params.stroke_width > 0, f"{geom}: lines/edges would be invisible"


class TestThumbnailIgnoresPaleStylePaint:
    """The matplotlib floor must NOT honor pale WFS paint (Issue #518).

    Regression for the washed-out bug: a present style used to override the
    punchy thumbnail defaults with its own opacity/edge (e.g. fill-opacity 0.2,
    hairline outline, #dadada). These tests fail against that old behavior.
    """

    @staticmethod
    def _pale_style_file(tmp_path: Path) -> Path:
        style = {
            "version": 8,
            "sources": {},
            "layers": [
                {
                    "id": "fill",
                    "type": "fill",
                    "paint": {
                        "fill-color": "#dadada",
                        "fill-opacity": 0.2,
                        "fill-outline-color": "#cccccc",
                    },
                }
            ],
        }
        path = tmp_path / "style.json"
        path.write_text(json.dumps(style))
        return path

    @pytest.mark.integration
    def test_geoparquet_uses_punchy_paint_not_style(self, tmp_path: Path) -> None:
        """_render_geoparquet ignores a pale fill-only style and draws the preset.

        The pale style fill is #dadada. The preset fill #3388ff is blue.
        """
        gpd = pytest.importorskip("geopandas")
        import numpy as np
        from PIL import Image
        from shapely.geometry import box

        from portolan_cli.viz.thumbnail import ThumbnailConfig, _render_geoparquet

        gpq_path = tmp_path / "test.parquet"
        gpd.GeoDataFrame(geometry=[box(10, 10, 90, 90)], crs="OGC:CRS84").to_parquet(gpq_path)
        output_path = tmp_path / "test.thumb.jpg"
        config = ThumbnailConfig(basemap_provider="none")

        assert _render_geoparquet(
            gpq_path, output_path, config, style_path=self._pale_style_file(tmp_path)
        )

        arr = np.asarray(Image.open(output_path).convert("RGB")).reshape(-1, 3)
        fill = arr[(arr < 240).any(axis=1)]
        assert len(fill) > 0, "no geometry rendered"
        mean = fill.mean(axis=0)
        assert mean[2] > mean[0] + 20, f"fill is not punchy blue (mean RGB={mean})"

    @pytest.mark.integration
    def test_pmtiles_path_uses_punchy_fill_not_pale_style(self, tmp_path: Path) -> None:
        """_render_geometries (PMTiles twin) ignores a pale fill-only style.

        Parity with the GeoParquet test above: the historically regression-prone
        PMTiles path must also render the punchy preset (#3388ff), not the pale
        WFS fill (#dadada). Pre-fix the old code read style.fill_color into the
        default fill, so the polygon would render gray — this asserts blue.
        """
        pytest.importorskip("matplotlib")
        np = pytest.importorskip("numpy")
        pil = pytest.importorskip("PIL.Image")

        from portolan_cli.viz.thumbnail import ThumbnailConfig, _render_geometries

        style_path = self._pale_style_file(tmp_path)
        output_path = tmp_path / "pmtiles.thumb.jpg"
        config = ThumbnailConfig(basemap_provider="none")  # no network
        geometries = [
            {
                "type": "Polygon",
                "coordinates": [[[10, 10], [90, 10], [90, 90], [10, 90], [10, 10]]],
                "properties": {},
            }
        ]

        assert _render_geometries(
            geometries, output_path, config, bounds=(10, 10, 90, 90), style_path=style_path
        )

        arr = np.asarray(pil.open(output_path).convert("RGB")).reshape(-1, 3)
        fill = arr[(arr < 240).any(axis=1)]  # non-white pixels = the polygon
        assert len(fill) > 0, "no geometry rendered"
        mean = fill.mean(axis=0)
        # Punchy #3388ff is blue-dominant; pale #dadada is neutral gray (R≈G≈B).
        assert mean[2] > mean[0] + 20, f"fill is not punchy blue (mean RGB={mean})"


class TestThumbnailRendersVisibleGeometry:
    """End-to-end render of a representative spread — assert non-blank output.

    Pixel variance / a non-white minimum proves geometry actually drew, which
    is the concrete regression for the washed-out bug (#514/#518).
    """

    @staticmethod
    def _write_gdf(geometries: list, tmp_path: Path, name: str) -> Path:
        import geopandas as gpd

        gdf = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")
        path = tmp_path / f"{name}.parquet"
        gdf.to_parquet(path)
        return path

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "kind", ["sparse_points", "dense_lines", "many_polygons", "giant_polygon"]
    )
    def test_render_is_not_blank(self, kind: str, tmp_path: Path) -> None:
        """Each representative layer renders visible (non-white) geometry."""
        pytest.importorskip("geopandas")
        pytest.importorskip("matplotlib")
        np = pytest.importorskip("numpy")
        pil = pytest.importorskip("PIL.Image")
        from shapely.geometry import LineString, Point, Polygon

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        if kind == "sparse_points":
            geoms = [Point(x, x) for x in range(-50, 50, 5)]
        elif kind == "dense_lines":
            geoms = [LineString([(i, 0), (i + 1, 10)]) for i in range(200)]
        elif kind == "many_polygons":
            geoms = [
                Polygon([(i, j), (i + 0.8, j), (i + 0.8, j + 0.8), (i, j + 0.8)])
                for i in range(20)
                for j in range(20)
            ]
        else:  # giant_polygon: one hemisphere-spanning sliver (the provincia case)
            geoms = [Polygon([(-65, -90), (-63, -90), (-63, -20), (-65, -20)])]

        gpq_path = self._write_gdf(geoms, tmp_path, kind)
        config = ThumbnailConfig(basemap_provider="none")  # no network
        result = generate_thumbnail_from_geoparquet(gpq_path, config)

        assert result is not None and result.exists()
        assert result.stat().st_size > 0
        arr = np.asarray(pil.open(result).convert("L"))
        # Not a blank white tile: real ink is present.
        assert arr.min() < 200, f"{kind}: thumbnail appears blank (min={arr.min()})"
        assert arr.std() > 1.0, f"{kind}: thumbnail has no variance (std={arr.std()})"

    @pytest.mark.integration
    def test_mixed_geometry_minority_points_stay_visible(self, tmp_path: Path) -> None:
        """Points in a polygon-dominant layer still render (visibility floor, #518).

        One RenderParams is applied to the whole frame, keyed off the dominant
        geometry type. With marker_size zeroed for polygons (the pre-fix
        behavior), the minority points drew at size 0 and vanished. Polygons sit
        bottom-left, points top-right; we assert the points' corner has ink.
        """
        pytest.importorskip("geopandas")
        pytest.importorskip("matplotlib")
        np = pytest.importorskip("numpy")
        pil = pytest.importorskip("PIL.Image")
        from shapely.geometry import Point, Polygon

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        # 50 polygons (dominant) bottom-left, 16 points top-right (minority).
        polys = [
            Polygon([(i, j), (i + 0.8, j), (i + 0.8, j + 0.8), (i, j + 0.8)])
            for i in range(10)
            for j in range(5)
        ]
        points = [Point(88 + dx, 88 + dy) for dx in range(4) for dy in range(4)]
        gpq_path = self._write_gdf([*polys, *points], tmp_path, "mixed")

        config = ThumbnailConfig(basemap_provider="none")  # no network
        result = generate_thumbnail_from_geoparquet(gpq_path, config)

        assert result is not None and result.exists()
        arr = np.asarray(pil.open(result).convert("L"))
        h, w = arr.shape
        # Top-right block holds only the points (polygons are bottom-left).
        corner = arr[: int(0.15 * h), int(0.85 * w) :]
        assert corner.min() < 200, "minority points rendered invisibly in mixed layer"

    @pytest.mark.integration
    def test_renders_geographic_crs_with_projected_coords(self, tmp_path: Path) -> None:
        """A layer declaring a geographic CRS but holding projected-magnitude
        coords still renders (every collection gets a thumbnail, #518).

        geopandas otherwise tries a latitude-corrected aspect on the mislabeled
        coords and raises "aspect must be finite and positive", yielding no
        thumbnail. We force an equal aspect to keep the floor robust.
        """
        gpd = pytest.importorskip("geopandas")
        pytest.importorskip("matplotlib")
        from shapely.geometry import Polygon

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        # CRS84 (geographic) declared, but coordinates are ~1.6e6 (projected).
        geoms = [
            Polygon(
                [
                    (1_611_000 + i, 1_910_000),
                    (1_611_500 + i, 1_910_000),
                    (1_611_500 + i, 1_910_500),
                    (1_611_000 + i, 1_910_500),
                ]
            )
            for i in range(0, 1000, 100)
        ]
        gdf = gpd.GeoDataFrame(geometry=geoms, crs="OGC:CRS84")
        gpq_path = tmp_path / "mislabeled.parquet"
        gdf.to_parquet(gpq_path)

        config = ThumbnailConfig(basemap_provider="none")
        result = generate_thumbnail_from_geoparquet(gpq_path, config)

        assert result is not None and result.exists()
        assert result.stat().st_size > 0


class TestGeoparquetSampling:
    """A bounded render, since thumbnails are now generated on every add (#683)."""

    @staticmethod
    def _write_geoparquet(path: Path, rows: int, row_group_size: int) -> None:
        """A real GeoParquet whose points march west to east across the extent.

        Spatially ordered on purpose: geoparquet-io writes spatially sorted
        output, so a leading-N sample would draw only the western corner. The
        ordering is what makes the stride assertion meaningful.
        """
        import geopandas as gpd
        import shapely

        gdf = gpd.GeoDataFrame(
            {"idx": list(range(rows))},
            geometry=[shapely.Point(x / rows * 10.0, 0.5) for x in range(rows)],
            crs="EPSG:4326",
        )
        gdf.to_parquet(path, row_group_size=row_group_size)

    @pytest.mark.unit
    def test_below_the_cap_reads_every_feature(self, tmp_path: Path) -> None:
        """The guarantee from #423 still holds for ordinary files."""
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "small.parquet"
        self._write_geoparquet(gpq, rows=500, row_group_size=100)

        gdf, _bbox, _crs = _read_geoparquet_for_thumbnail(gpq, max_features=10_000)

        assert len(gdf) == 500

    @pytest.mark.unit
    def test_above_the_cap_samples(self, tmp_path: Path) -> None:
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "big.parquet"
        self._write_geoparquet(gpq, rows=1000, row_group_size=100)

        gdf, _bbox, _crs = _read_geoparquet_for_thumbnail(gpq, max_features=200)

        assert 0 < len(gdf) < 1000, "A file over the cap must not be read whole"

    @pytest.mark.unit
    def test_the_sample_spans_the_full_extent(self, tmp_path: Path) -> None:
        """A leading-N sample would cover the western sliver only."""
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "big.parquet"
        self._write_geoparquet(gpq, rows=1000, row_group_size=100)

        gdf, bbox, _crs = _read_geoparquet_for_thumbnail(gpq, max_features=200)

        # Points run x = 0 .. 9.99. A leading 200 would stop near x = 2.
        assert gdf.total_bounds[2] > 8.0, "Sample must reach the eastern edge"
        # And the frame still comes from file metadata, not from the sample.
        assert bbox is not None
        assert bbox[2] > 9.9

    @pytest.mark.unit
    def test_sampling_preserves_the_declared_crs(self, tmp_path: Path) -> None:
        """The rebuilt frame must not silently lose its CRS."""
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "big.parquet"
        self._write_geoparquet(gpq, rows=1000, row_group_size=100)

        gdf, _bbox, source_crs = _read_geoparquet_for_thumbnail(gpq, max_features=200)

        assert gdf.crs is not None
        assert gdf.crs.to_epsg() == 4326
        assert source_crs is not None

    @pytest.mark.unit
    def test_single_row_group_still_caps_the_feature_count(self, tmp_path: Path) -> None:
        """geoparquet-io writes one row group for many files; the cap must hold."""
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "one_group.parquet"
        self._write_geoparquet(gpq, rows=1000, row_group_size=100_000)

        gdf, _bbox, _crs = _read_geoparquet_for_thumbnail(gpq, max_features=200)

        assert len(gdf) == 200
        assert gdf.total_bounds[2] > 8.0, "Even one group must be sampled across"

    @pytest.mark.unit
    def test_max_features_none_reads_everything(self, tmp_path: Path) -> None:
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "big.parquet"
        self._write_geoparquet(gpq, rows=1000, row_group_size=100)

        gdf, _bbox, _crs = _read_geoparquet_for_thumbnail(gpq, max_features=None)

        assert len(gdf) == 1000


@pytest.mark.unit
def test_the_render_leaves_the_global_backend_alone() -> None:
    """The render draws on its own Agg canvas, so no pyplot state is involved.

    TkAgg, the Windows default, can abort the process from a thread. Before the
    fix the render switched the process-wide backend to Agg through pyplot.
    """
    import matplotlib as mpl
    from matplotlib.backends.backend_agg import FigureCanvasAgg

    from portolan_cli.viz.thumbnail import _new_figure

    previous = mpl.get_backend()
    mpl.use("svg")
    try:
        figure, _ax = _new_figure((0.0, 0.0, 2.0, 1.0), 512)
        assert mpl.get_backend().lower() == "svg"
        assert isinstance(figure.canvas, FigureCanvasAgg)
        assert tuple(figure.get_size_inches() * figure.dpi) == pytest.approx((512, 256))
    finally:
        mpl.use(previous)


@pytest.mark.unit
def test_thumbnails_quality_reaches_the_geoparquet_render(tmp_path: Path) -> None:
    """thumbnails.quality set the COG thumbnail only. The GeoParquet one kept 75."""
    import shutil
    from pathlib import Path as _Path

    pytest.importorskip("geopandas")
    from PIL import Image

    from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

    fixture = _Path(__file__).parents[1] / "fixtures" / "realdata" / "open-buildings.parquet"
    sizes = {}
    for quality in (5, 95):
        source = tmp_path / f"q{quality}" / "buildings.parquet"
        source.parent.mkdir()
        shutil.copy(fixture, source)
        config = ThumbnailConfig(quality=quality, basemap_provider="none")

        thumbnail = generate_thumbnail_from_geoparquet(source, config)

        assert thumbnail is not None
        with Image.open(thumbnail) as image:
            assert image.format == "JPEG"
        sizes[quality] = thumbnail.stat().st_size

    assert sizes[5] < sizes[95] / 2


# =============================================================================
# Cheap basemap, one draw, bulk geometry (Issues #993 and #1007)
# =============================================================================

#: The stub tile colour. No thumbnail preset uses it, so a pixel of this colour
#: in the output came from the basemap.
_STUB_TILE_RGBA = (201, 74, 203, 255)

_REALDATA = Path(__file__).parents[1] / "fixtures" / "realdata"


@contextmanager
def _tracing() -> Iterator[None]:
    """Turn the trace counters on for one block."""
    from portolan_cli import trace

    trace.reset()
    trace.enable()
    try:
        yield
    finally:
        trace.reset()


def _counter(name: str) -> int:
    from portolan_cli import trace

    return trace.counters().get(name, 0)


@pytest.fixture
def stub_tiles(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Serve every basemap tile as one fixed PNG and record each tile URL.

    The real contextily runs: it picks the tiles, fetches them through its
    ``_fetch_tile``, and merges them. Only the HTTP request is replaced. The
    autouse ``no_basemap_tiles`` fixture turns the basemap off. This fixture
    turns it on again with the stub.
    """
    ctx = pytest.importorskip("contextily")
    import contextily.tile
    import numpy as np
    from PIL import Image

    from portolan_cli.viz import thumbnail

    buffer = io.BytesIO()
    Image.new("RGBA", (256, 256), _STUB_TILE_RGBA).save(buffer, "PNG")
    png = buffer.getvalue()
    urls: list[str] = []
    lock = threading.Lock()

    def fetch(tile_url: str, wait: int, max_retries: int, headers: dict[str, str]) -> Any:
        with lock:
            urls.append(tile_url)
        return np.asarray(Image.open(io.BytesIO(png)).convert("RGBA"))

    monkeypatch.setattr(contextily.tile, "_fetch_tile", fetch)
    monkeypatch.setattr(thumbnail, "_ensure_contextily", lambda: ctx)
    monkeypatch.setattr(thumbnail, "_tile_memo", OrderedDict())
    monkeypatch.setattr(thumbnail, "_warned_keyless", set())
    return urls


@pytest.fixture
def saved_figures(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record each matplotlib Figure that a render saves."""
    from matplotlib.figure import Figure

    figures: list[Any] = []
    original = Figure.savefig

    def spy(self: Any, *args: Any, **kwargs: Any) -> Any:
        figures.append(self)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Figure, "savefig", spy)
    return figures


def _copy_fixture(name: str, directory: Path) -> Path:
    import shutil

    directory.mkdir(parents=True, exist_ok=True)
    target = directory / name
    shutil.copy(_REALDATA / name, target)
    return target


def _expected_frame(gpq_path: Path) -> tuple[float, float, float, float]:
    """The framed EPSG:3857 extent of a CRS84 GeoParquet, from its footer bbox."""
    import pyarrow.parquet as pq
    from pyproj import Transformer

    from portolan_cli.viz.thumbnail import _frame_bounds

    geo = json.loads(pq.ParquetFile(gpq_path).metadata.metadata[b"geo"])
    minx, miny, maxx, maxy = geo["columns"][geo["primary_column"]]["bbox"]
    to_mercator = Transformer.from_crs("OGC:CRS84", "EPSG:3857", always_xy=True)
    return _frame_bounds(to_mercator.transform_bounds(minx, miny, maxx, maxy, densify_pts=21))


def _stub_pixels(image_path: Path) -> int:
    """Count the output pixels that hold the stub tile colour."""
    import numpy as np
    from PIL import Image

    rgb = np.asarray(Image.open(image_path).convert("RGB")).astype(int)
    target = np.array(_STUB_TILE_RGBA[:3])
    return int((np.abs(rgb - target).sum(axis=2) < 30).sum())


class TestCheapBasemap:
    """The basemap stays on by default and costs one draw and no warp (#993)."""

    @pytest.mark.integration
    @pytest.mark.parametrize("name", ["fieldmaps-boundaries.parquet", "open-buildings.parquet"])
    def test_one_draw_and_the_frame_on_realdata(
        self,
        name: str,
        tmp_path: Path,
        stub_tiles: list[str],
        saved_figures: list[Any],
    ) -> None:
        """One draw, the tiles of one fetch, and an image that fills the frame.

        Before the fix, savefig with bbox_inches="tight" and contextily's
        attribution each forced a draw (3 draws), and the axes filled about
        396 of the 512 px because the figure kept the default subplot margins.
        """
        from PIL import Image

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq = _copy_fixture(name, tmp_path)
        with _tracing():
            result = generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig())
            draws = _counter("thumbnail.draws")
            fetched = _counter("tiles.fetched")

        assert result is not None
        assert draws == 1
        assert fetched == len(stub_tiles)
        assert fetched > 0
        assert _stub_pixels(result) > 0, "the stub basemap is not in the image"

        (figure,) = saved_figures
        (ax,) = figure.axes
        frame = _expected_frame(gpq)
        assert ax.get_xlim() == pytest.approx((frame[0], frame[2]))
        assert ax.get_ylim() == pytest.approx((frame[1], frame[3]))
        assert tuple(ax.get_position().bounds) == (0.0, 0.0, 1.0, 1.0)

        width, height = frame[2] - frame[0], frame[3] - frame[1]
        with Image.open(result) as image:
            if width >= height:
                assert image.size == (512, round(512 * height / width))
            else:
                assert image.size == (round(512 * width / height), 512)

    @pytest.mark.integration
    def test_contextily_add_basemap_and_its_warp_never_run(
        self, tmp_path: Path, stub_tiles: list[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The render draws in EPSG:3857, so no tile is reprojected.

        Before the fix the data stayed in its CRS and contextily warped the
        tiles into it, which took 17 s of a 36 s 100-collection add.
        """
        import contextily
        import contextily.plotting
        import contextily.tile

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        def forbidden(*_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("contextily warped or drew the basemap itself")

        for module in (contextily, contextily.plotting):
            monkeypatch.setattr(module, "add_basemap", forbidden)
        for module in (contextily.plotting, contextily.tile):
            monkeypatch.setattr(module, "warp_tiles", forbidden)

        gpq = _copy_fixture("open-buildings.parquet", tmp_path)
        result = generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig())

        assert result is not None
        assert _stub_pixels(result) > 0

    @pytest.mark.integration
    def test_tiles_are_fetched_in_parallel_without_the_disk_cache(
        self, tmp_path: Path, stub_tiles: list[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """bounds2img gets several connections and threads, not processes.

        contextily switches to worker processes when the joblib disk cache is
        on and n_connections is above 1, so the render turns the cache off.
        """
        import contextily

        from portolan_cli.viz import thumbnail

        calls: list[dict[str, Any]] = []
        original = contextily.bounds2img

        def spy(*args: Any, **kwargs: Any) -> Any:
            calls.append(kwargs)
            return original(*args, **kwargs)

        monkeypatch.setattr(contextily, "bounds2img", spy)
        gpq = _copy_fixture("open-buildings.parquet", tmp_path)
        assert thumbnail.generate_thumbnail_from_geoparquet(gpq, thumbnail.ThumbnailConfig())

        (kwargs,) = calls
        assert kwargs["n_connections"] == 2
        assert kwargs["use_cache"] is False

    @pytest.mark.integration
    def test_a_second_render_of_the_same_extent_fetches_no_tile(
        self, tmp_path: Path, stub_tiles: list[str]
    ) -> None:
        """Many collections over one extent fetch its tiles once per run."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        first = _copy_fixture("open-buildings.parquet", tmp_path / "a")
        second = _copy_fixture("open-buildings.parquet", tmp_path / "b")
        with _tracing():
            assert generate_thumbnail_from_geoparquet(first, ThumbnailConfig())
            after_first = _counter("tiles.fetched")
            assert generate_thumbnail_from_geoparquet(second, ThumbnailConfig())
            after_second = _counter("tiles.fetched")

        assert after_first == len(stub_tiles)
        assert after_first > 0
        assert after_second == after_first

    @pytest.mark.integration
    def test_the_zoom_fetches_no_more_than_the_image_needs(
        self, tmp_path: Path, stub_tiles: list[str]
    ) -> None:
        """The merged tiles cover the frame at 1x to 2x the output resolution.

        One zoom level lower would give less than one tile pixel per output
        pixel, so the basemap would blur. One level higher would fetch about
        four times the tiles.
        """
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq = _copy_fixture("open-buildings.parquet", tmp_path)
        assert generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig())

        (zoom,) = {int(url.split("/")[-3]) for url in stub_tiles}
        frame = _expected_frame(gpq)
        world = 2 * 20037508.342789244
        span = max(frame[2] - frame[0], frame[3] - frame[1])
        source_px = 256 * 2**zoom * span / world
        assert 512 <= source_px < 1024

    @pytest.mark.integration
    def test_a_tiny_extent_clamps_to_the_provider_max_zoom(
        self, tmp_path: Path, stub_tiles: list[str]
    ) -> None:
        """A 1 m polygon would need zoom 20 or more. Esri serves up to 16.

        contextily raises for a zoom above the provider maximum, so an
        unclamped zoom would leave the thumbnail with no basemap.
        """
        gpd = pytest.importorskip("geopandas")
        from shapely.geometry import box

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq = tmp_path / "tiny.parquet"
        gpd.GeoDataFrame(geometry=[box(2.0, 41.0, 2.00001, 41.00001)], crs="OGC:CRS84").to_parquet(
            gpq
        )

        result = generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig())

        assert result is not None
        assert {int(url.split("/")[-3]) for url in stub_tiles} == {16}
        assert _stub_pixels(result) > 0

    @pytest.mark.integration
    def test_the_attribution_is_drawn_without_contextily(
        self, tmp_path: Path, stub_tiles: list[str], saved_figures: list[Any]
    ) -> None:
        """The provider attribution stays on the image (Esri requires it)."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq = _copy_fixture("open-buildings.parquet", tmp_path)
        assert generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig())

        (figure,) = saved_figures
        texts = [text.get_text() for text in figure.axes[0].texts]
        assert texts == ["Tiles (C) Esri -- Esri, DeLorme, NAVTEQ"]

    @pytest.mark.integration
    def test_pmtiles_path_draws_once_in_web_mercator(
        self, tmp_path: Path, stub_tiles: list[str], saved_figures: list[Any]
    ) -> None:
        """The PMTiles twin uses the same render: one draw, frame in EPSG:3857."""
        from pyproj import Transformer

        from portolan_cli.viz.thumbnail import ThumbnailConfig, _frame_bounds, _render_geometries

        bounds = (-122.5, 37.5, -122.0, 38.0)
        geometries = [
            {
                "type": "Polygon",
                "coordinates": [[[-122.5, 37.5], [-122.0, 37.5], [-122.0, 38.0], [-122.5, 37.5]]],
                "properties": {},
            },
            {"type": "Point", "coordinates": [-122.2, 37.8], "properties": {}},
        ]
        output = tmp_path / "data.thumb.jpg"
        with _tracing():
            assert _render_geometries(geometries, output, ThumbnailConfig(), bounds=bounds)
            draws = _counter("thumbnail.draws")
            fetched = _counter("tiles.fetched")

        assert draws == 1
        assert fetched == len(stub_tiles)
        assert fetched > 0
        to_mercator = Transformer.from_crs("OGC:CRS84", "EPSG:3857", always_xy=True)
        frame = _frame_bounds(to_mercator.transform_bounds(*bounds, densify_pts=21))
        (figure,) = saved_figures
        assert figure.axes[0].get_xlim() == pytest.approx((frame[0], frame[2]))
        assert figure.axes[0].get_ylim() == pytest.approx((frame[1], frame[3]))

    @pytest.mark.integration
    def test_polar_data_keeps_its_basemap(self, tmp_path: Path, stub_tiles: list[str]) -> None:
        """Latitude 90 maps to infinity in EPSG:3857, so the render clips it."""
        gpd = pytest.importorskip("geopandas")
        from shapely.geometry import Polygon

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gdf = gpd.GeoDataFrame(
            geometry=[Polygon([(-65, -90), (-63, -90), (-63, -20), (-65, -20)])], crs="OGC:CRS84"
        )
        gpq = tmp_path / "polar.parquet"
        gdf.to_parquet(gpq)

        result = generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig())

        assert result is not None
        assert _stub_pixels(result) > 0

    @pytest.mark.integration
    def test_one_point_gets_a_one_degree_box(
        self, tmp_path: Path, saved_figures: list[Any]
    ) -> None:
        """A single point is framed in degrees, then projected.

        Framed after the projection, its nominal box would be 1 m wide.
        """
        from portolan_cli.viz.thumbnail import ThumbnailConfig, _render_geometries

        geometries = [{"type": "Point", "coordinates": [0.0, 0.0]}]
        config = ThumbnailConfig(basemap_provider="none")

        assert _render_geometries(geometries, tmp_path / "p.jpg", config)

        (figure,) = saved_figures
        left, right = figure.axes[0].get_xlim()
        # One degree of longitude is 111,319.5 m, plus a 5% margin each side.
        assert right - left == pytest.approx(111_319.49 * 1.1, rel=1e-3)

    @pytest.mark.integration
    def test_mislabeled_projected_coords_render_without_a_basemap(
        self, tmp_path: Path, stub_tiles: list[str]
    ) -> None:
        """Coordinates that cannot be lon/lat get no basemap, but still a thumbnail."""
        gpd = pytest.importorskip("geopandas")
        from shapely.geometry import box

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gdf = gpd.GeoDataFrame(
            geometry=[box(1_611_000 + i, 1_910_000, 1_611_500 + i, 1_910_500) for i in range(5)],
            crs="OGC:CRS84",
        )
        gpq = tmp_path / "mislabeled.parquet"
        gdf.to_parquet(gpq)

        with _tracing():
            result = generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig())
            draws = _counter("thumbnail.draws")

        assert result is not None
        assert draws == 1
        assert stub_tiles == []


class TestBasemapApiKey:
    """CARTO serves a watermark tile to a request without a key."""

    @pytest.mark.unit
    def test_config_reads_the_key(self, tmp_path: Path) -> None:
        from portolan_cli.viz.thumbnail import get_thumbnail_config

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "thumbnails:\n  basemap:\n    api_key: abc123\n"
        )

        assert get_thumbnail_config(tmp_path).basemap_api_key == "abc123"

    @pytest.mark.unit
    def test_the_environment_sets_the_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from portolan_cli.viz.thumbnail import get_thumbnail_config

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("thumbnails:\n  enabled: true\n")
        monkeypatch.setenv("PORTOLAN_THUMBNAILS_BASEMAP_API_KEY", "fromenv")

        assert get_thumbnail_config(tmp_path).basemap_api_key == "fromenv"

    @pytest.mark.integration
    def test_the_key_goes_on_each_tile_url(self, tmp_path: Path, stub_tiles: list[str]) -> None:
        """CARTO reads the key from a ``key`` query parameter."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq = _copy_fixture("open-buildings.parquet", tmp_path)
        config = ThumbnailConfig(basemap_provider="CartoDB.Positron", basemap_api_key="abc123")
        assert generate_thumbnail_from_geoparquet(gpq, config)

        assert stub_tiles
        assert all(url.endswith(".png?key=abc123") for url in stub_tiles), stub_tiles

    @pytest.mark.integration
    def test_keyless_carto_warns_once_and_draws_no_watermark(
        self, tmp_path: Path, stub_tiles: list[str], caplog: pytest.LogCaptureFixture
    ) -> None:
        """CARTO without a key gives one warning per run and no basemap.

        CARTO answers a keyless request with a tile that reads "API KEY
        REQUIRED". Before the fix every thumbnail carried that watermark.
        """
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        config = ThumbnailConfig(basemap_provider="CartoDB.Positron")
        first = _copy_fixture("open-buildings.parquet", tmp_path / "a")
        second = _copy_fixture("open-buildings.parquet", tmp_path / "b")
        with caplog.at_level("WARNING", logger="portolan_cli.viz.thumbnail"):
            assert generate_thumbnail_from_geoparquet(first, config)
            assert generate_thumbnail_from_geoparquet(second, config)

        warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1, warnings
        assert "thumbnails.basemap.api_key" in warnings[0]
        assert "PORTOLAN_THUMBNAILS_BASEMAP_API_KEY" in warnings[0]
        assert stub_tiles == []

    @pytest.mark.integration
    def test_the_key_fills_a_token_placeholder(self, tmp_path: Path, stub_tiles: list[str]) -> None:
        """A provider with a token field in its URL gets the key there."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq = _copy_fixture("open-buildings.parquet", tmp_path)
        config = ThumbnailConfig(
            basemap_provider="Thunderforest.OpenCycleMap", basemap_api_key="abc123"
        )
        assert generate_thumbnail_from_geoparquet(gpq, config)

        assert stub_tiles
        assert all(url.endswith("?apikey=abc123") for url in stub_tiles), stub_tiles


class TestBulkGeometry:
    """One path per colour, not one matplotlib object per feature (#993 part 4)."""

    @pytest.mark.integration
    def test_polygons_draw_as_one_path(self, tmp_path: Path, saved_figures: list[Any]) -> None:
        """Before the fix geopandas built one PathPatch per polygon (2,000 here)."""
        gpd = pytest.importorskip("geopandas")
        from shapely.geometry import box

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        boxes = [
            box(i * 0.01, j * 0.01, i * 0.01 + 0.008, j * 0.01 + 0.008)
            for i in range(50)
            for j in range(40)
        ]
        gpq = tmp_path / "boxes.parquet"
        gpd.GeoDataFrame(geometry=boxes, crs="OGC:CRS84").to_parquet(gpq)

        assert generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig(basemap_provider="none"))

        (figure,) = saved_figures
        ax = figure.axes[0]
        assert len(ax.patches) == 1
        assert list(ax.collections) == []
        assert list(ax.lines) == []

    @pytest.mark.integration
    def test_pmtiles_points_and_lines_draw_in_bulk(
        self, tmp_path: Path, saved_figures: list[Any]
    ) -> None:
        """Before the fix each PMTiles point and line was its own ax.plot line."""
        from portolan_cli.viz.thumbnail import ThumbnailConfig, _render_geometries

        points: list[dict[str, Any]] = [
            {"type": "Point", "coordinates": [i * 0.1, 1.0], "properties": {}} for i in range(30)
        ]
        lines: list[dict[str, Any]] = [
            {"type": "LineString", "coordinates": [[i * 0.1, 0], [i * 0.1, 0.5]], "properties": {}}
            for i in range(30)
        ]
        output = tmp_path / "pm.thumb.jpg"
        config = ThumbnailConfig(basemap_provider="none")

        assert _render_geometries(points + lines, output, config, bounds=(0.0, 0.0, 2.9, 1.0))

        (figure,) = saved_figures
        ax = figure.axes[0]
        assert list(ax.lines) == []
        assert len(ax.collections) == 1  # every point in one scatter
        assert len(ax.collections[0].get_offsets()) == 30
        assert len(ax.patches) == 1  # every line in one path

    @pytest.mark.integration
    def test_a_hole_stays_empty(self, tmp_path: Path) -> None:
        """The interior ring of a polygon is not filled, whatever its winding."""
        gpd = pytest.importorskip("geopandas")
        import numpy as np
        from PIL import Image
        from shapely.geometry import Polygon

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        outer = [(0, 0), (10, 0), (10, 10), (0, 10)]
        inner = [(3, 3), (7, 3), (7, 7), (3, 7)]  # the same winding as the shell, on purpose
        gpq = tmp_path / "donut.parquet"
        gpd.GeoDataFrame(geometry=[Polygon(outer, [inner])], crs="OGC:CRS84").to_parquet(gpq)

        result = generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig(basemap_provider="none"))

        assert result is not None
        grey = np.asarray(Image.open(result).convert("L"))
        h, w = grey.shape
        assert grey[h // 2, w // 2] > 245, "the hole is filled"
        assert grey[int(h * 0.2), w // 2] < 200, "the ring is not filled"


class TestGeometryOnlyReads:
    """The thumbnail reads the geometry column and nothing else (#993)."""

    @staticmethod
    def _wide_parquet(path: Path, rows: int) -> None:
        gpd = pytest.importorskip("geopandas")
        import shapely

        gdf = gpd.GeoDataFrame(
            {
                "name": [f"feature-{i}" for i in range(rows)],
                "kind": ["a" if i % 2 else "b" for i in range(rows)],
            },
            geometry=[shapely.Point(i / rows, 0.5) for i in range(rows)],
            crs="EPSG:4326",
        )
        gdf.to_parquet(path, row_group_size=max(1, rows // 4))

    @pytest.mark.unit
    @pytest.mark.parametrize("max_features", [10_000, 50], ids=["full", "sampled"])
    def test_only_the_geometry_column_is_read(self, tmp_path: Path, max_features: int) -> None:
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "wide.parquet"
        self._wide_parquet(gpq, rows=400)

        gdf, _bbox, _crs = _read_geoparquet_for_thumbnail(gpq, max_features=max_features)

        assert list(gdf.columns) == ["geometry"]

    @pytest.mark.unit
    def test_a_categorical_style_adds_its_one_field(self, tmp_path: Path) -> None:
        from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

        gpq = tmp_path / "wide.parquet"
        self._wide_parquet(gpq, rows=400)

        gdf, _bbox, _crs = _read_geoparquet_for_thumbnail(
            gpq, max_features=10_000, color_field="KIND"
        )

        assert list(gdf.columns) == ["kind", "geometry"]

    @pytest.mark.unit
    def test_no_duckdb_connection_reads_the_footer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The footer comes from the one open ParquetFile, not a DuckDB query."""
        import duckdb

        from portolan_cli.viz.thumbnail import ThumbnailConfig, generate_thumbnail_from_geoparquet

        gpq = tmp_path / "wide.parquet"
        self._wide_parquet(gpq, rows=400)
        connects: list[object] = []
        original = duckdb.connect

        def spy(*args: Any, **kwargs: Any) -> Any:
            connects.append(args)
            return original(*args, **kwargs)

        monkeypatch.setattr(duckdb, "connect", spy)
        assert generate_thumbnail_from_geoparquet(gpq, ThumbnailConfig(basemap_provider="none"))

        assert connects == []
