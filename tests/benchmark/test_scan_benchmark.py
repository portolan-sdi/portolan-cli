"""CodSpeed benchmarks for ``scan`` on a tree of about 1,900 files.

CodSpeed counts CPU instructions in simulation mode, so a run on a shared
runner gives the same number each time. ``tests/perf/test_scan_scaling.py``
holds the linear-work property of #314. The old SC-001 and SC-002 tests
asserted wall-clock limits on shared runners and are gone.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import rasterio
from rasterio.transform import from_bounds

from portolan_cli.scan.core import scan_directory

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


# =============================================================================
# Minimal Valid File Content (Issue #464)
# =============================================================================
# Fake file content (b"dummy content") causes rasterio/pyarrow to throw
# exceptions during format validation. Exception handling is slower than
# the happy path, causing flaky timing on CI. These minimal valid bytes
# ensure format detection succeeds without triggering error paths.


def _create_minimal_tiff() -> bytes:
    """Create minimal valid TIFF (1x1 pixel, ~370 bytes).

    Not a COG, but valid enough for rasterio to open without error.
    Will be classified as CONVERTIBLE (non-COG TIFF) quickly.
    """
    buf = io.BytesIO()
    profile = {
        "driver": "GTiff",
        "dtype": "uint8",
        "width": 1,
        "height": 1,
        "count": 1,
        "crs": "EPSG:4326",
        "transform": from_bounds(-180, -90, 180, 90, 1, 1),
    }
    with rasterio.open(buf, "w", **profile) as dst:
        dst.write(np.array([[[0]]], dtype=np.uint8))
    return buf.getvalue()


def _create_minimal_parquet() -> bytes:
    """Create minimal valid Parquet (1 row, 1 column, ~490 bytes).

    Plain Parquet without geo metadata. Will be classified as
    CLOUD_NATIVE (plain Parquet) quickly by is_geoparquet() check.
    """
    buf = io.BytesIO()
    table = pa.table({"id": pa.array([1], type=pa.int32())})
    pq.write_table(table, buf)
    return buf.getvalue()


# Cache the bytes at module level (created once per test session)
_MINIMAL_TIFF: bytes | None = None
_MINIMAL_PARQUET: bytes | None = None


def _get_minimal_tiff() -> bytes:
    """Get cached minimal TIFF bytes."""
    global _MINIMAL_TIFF
    if _MINIMAL_TIFF is None:
        _MINIMAL_TIFF = _create_minimal_tiff()
    return _MINIMAL_TIFF


def _get_minimal_parquet() -> bytes:
    """Get cached minimal Parquet bytes."""
    global _MINIMAL_PARQUET
    if _MINIMAL_PARQUET is None:
        _MINIMAL_PARQUET = _create_minimal_parquet()
    return _MINIMAL_PARQUET


# =============================================================================
# Benchmark Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def benchmark_dir_1k(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a directory with ~1000 files for benchmarking.

    Structure: 100 directories with 10 files each = 1000 files.
    Mix of formats: .parquet, .geojson, .shp (with sidecars), .tif
    """
    base = tmp_path_factory.mktemp("benchmark_1k")
    _create_benchmark_structure(base, dirs=100, files_per_dir=10)
    return base


@pytest.fixture(scope="session")
def benchmark_dir_deep(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a deeply nested directory structure.

    Structure: 10 levels deep, 10 files at each level = 100 files.
    Tests depth traversal performance.
    """
    base = tmp_path_factory.mktemp("benchmark_deep")
    _create_deep_structure(base, depth=10, files_per_level=10)
    return base


def _create_benchmark_structure(base: Path, dirs: int, files_per_dir: int) -> None:
    """Create benchmark directory structure with mixed formats.

    Note: Every 4th file adds 3 sidecar files (.shp, .dbf, .shx), so actual
    file count = dirs × files_per_dir + dirs × (files_per_dir // 4) × 3.
    For default 1K target (100 dirs × 10 files): ~1900 files total.

    Uses minimal valid file content for formats requiring content inspection
    (.tif, .parquet) to avoid slow exception handling paths. See issue #464.
    """
    extensions = [".parquet", ".geojson", ".tif", ".gpkg"]

    # Get valid bytes for content-inspected formats (cached)
    tiff_bytes = _get_minimal_tiff()
    parquet_bytes = _get_minimal_parquet()

    for i in range(dirs):
        subdir = base / f"dir_{i:04d}"
        subdir.mkdir(parents=True, exist_ok=True)

        for j in range(files_per_dir):
            ext = extensions[j % len(extensions)]
            filename = f"file_{j:04d}{ext}"

            # Use valid bytes for formats that trigger content inspection
            if ext == ".tif":
                content = tiff_bytes
            elif ext == ".parquet":
                content = parquet_bytes
            else:
                content = b"dummy content"

            (subdir / filename).write_bytes(content)

            # Add shapefile sidecars every 4th file
            if j % 4 == 0:
                stem = f"file_{j:04d}"
                (subdir / f"{stem}.shp").write_bytes(b"shp content")
                (subdir / f"{stem}.dbf").write_bytes(b"dbf content")
                (subdir / f"{stem}.shx").write_bytes(b"shx content")


def _create_deep_structure(base: Path, depth: int, files_per_level: int) -> None:
    """Create deeply nested directory structure."""
    current = base
    parquet_bytes = _get_minimal_parquet()

    for level in range(depth):
        current = current / f"level_{level}"
        current.mkdir(parents=True, exist_ok=True)

        for j in range(files_per_level):
            filename = f"file_{j:04d}.parquet"
            (current / filename).write_bytes(parquet_bytes)


# =============================================================================
# Benchmarks
# =============================================================================

pytestmark = pytest.mark.benchmark


def test_scan_1k_files(benchmark_dir_1k: Path, benchmark: Callable[..., Any]) -> None:
    result = benchmark(scan_directory, benchmark_dir_1k)

    assert result.directories_scanned == 101  # the root and its 100 directories


def test_scan_deep_nesting(benchmark_dir_deep: Path, benchmark: Callable[..., Any]) -> None:
    result = benchmark(scan_directory, benchmark_dir_deep)

    assert result.directories_scanned == 11  # the root and 10 nested levels
