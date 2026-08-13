"""Benchmarks for the thumbnail feature cap (Issue #683).

Thumbnails joined the default ``add`` pipeline, so an unbounded read became
every user's problem rather than an opt-in cost. ``thumbnails.max_features``
caps a render by sampling row groups spread across the file. These benchmarks
track what the cap buys and confirm the sample still spans the whole extent,
which is the property that makes a cheaper read acceptable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.viz.thumbnail import _read_geoparquet_for_thumbnail

#: Rows in the synthetic layer. Large enough that the cap bites at 100_000,
#: small enough to build in a second.
ROW_COUNT = 400_000
MAX_FEATURES = 100_000


@pytest.fixture(scope="module")
def large_geoparquet(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A spatially sorted point layer spanning 0-100 degrees of longitude."""
    import geopandas as gpd
    import numpy as np
    from shapely.geometry import Point

    xs = np.linspace(0.0, 100.0, ROW_COUNT)
    frame = gpd.GeoDataFrame(
        {"idx": np.arange(ROW_COUNT)},
        geometry=[Point(x, x / 100.0) for x in xs],
        crs="EPSG:4326",
    )
    path = tmp_path_factory.mktemp("thumbnail-bench") / "points.parquet"
    frame.to_parquet(path, row_group_size=10_000)
    return path


class TestThumbnailSamplingBenchmarks:
    """The cap's cost and its correctness, measured rather than asserted in prose."""

    @pytest.mark.benchmark(group="thumbnail-read")
    @pytest.mark.slow
    def test_full_read_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        large_geoparquet: Path,
    ) -> None:
        """Baseline: every feature read, which is what happened before the cap."""
        result = benchmark(_read_geoparquet_for_thumbnail, large_geoparquet, None)
        gdf, _, _ = result
        assert len(gdf) == ROW_COUNT

    @pytest.mark.benchmark(group="thumbnail-read")
    @pytest.mark.slow
    def test_sampled_read_performance(
        self,
        benchmark,  # type: ignore[no-untyped-def]
        large_geoparquet: Path,
    ) -> None:
        """The capped read, and proof the sample still reaches the far edge."""
        result = benchmark(_read_geoparquet_for_thumbnail, large_geoparquet, MAX_FEATURES)
        gdf, full_bbox, _ = result

        assert len(gdf) < ROW_COUNT, "The cap must reduce the rows drawn"
        # A leading sample would stop near longitude 25. Spread picks reach 100.
        assert gdf.total_bounds[2] > 90.0, "The sample must reach the eastern edge"
        assert full_bbox is not None
        assert full_bbox[2] > 99.0, "The frame comes from metadata, not the sample"
