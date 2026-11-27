"""CodSpeed benchmarks for ``scan`` on a tree of about 1,900 files.
tests/perf/test_scan_scaling.py holds the linear-work property.
"""

from __future__ import annotations

import functools
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


# Minimal valid file content. Invalid bytes make rasterio and pyarrow raise,
# and the exception path is slow and noisy.


def _create_minimal_tiff() -> bytes:
    """Create a minimal valid 1x1 TIFF that is not a COG."""
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
    """Create a minimal plain Parquet file with no geo metadata."""
    buf = io.BytesIO()
    table = pa.table({"id": pa.array([1], type=pa.int32())})
    pq.write_table(table, buf)
    return buf.getvalue()


@functools.cache
def _get_minimal_tiff() -> bytes:
    """Get cached minimal TIFF bytes."""
    return _create_minimal_tiff()


@functools.cache
def _get_minimal_parquet() -> bytes:
    """Get cached minimal Parquet bytes."""
    return _create_minimal_parquet()


# Benchmark fixtures


@pytest.fixture(scope="session")
def benchmark_dir_1k(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create 100 directories of 10 mixed-format files each."""
    base = tmp_path_factory.mktemp("benchmark_1k")
    _create_benchmark_structure(base, dirs=100, files_per_dir=10)
    return base


@pytest.fixture(scope="session")
def benchmark_dir_deep(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a tree 10 levels deep with 10 files at each level."""
    base = tmp_path_factory.mktemp("benchmark_deep")
    _create_deep_structure(base, depth=10, files_per_level=10)
    return base


def _create_benchmark_structure(base: Path, dirs: int, files_per_dir: int) -> None:
    """Create the benchmark tree; every 4th file adds 3 shapefile sidecars.
    Formats that need content inspection get valid bytes, so detection takes no slow error path.
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
        current /= f"level_{level}"
        current.mkdir(parents=True, exist_ok=True)

        for j in range(files_per_level):
            filename = f"file_{j:04d}.parquet"
            (current / filename).write_bytes(parquet_bytes)


# Benchmarks

pytestmark = pytest.mark.benchmark


def test_scan_1k_files(benchmark_dir_1k: Path, benchmark: Callable[..., Any]) -> None:
    result = benchmark(scan_directory, benchmark_dir_1k)

    assert result.directories_scanned == 101  # the root and its 100 directories


def test_scan_deep_nesting(benchmark_dir_deep: Path, benchmark: Callable[..., Any]) -> None:
    result = benchmark(scan_directory, benchmark_dir_deep)

    assert result.directories_scanned == 11  # the root and 10 nested levels
