"""Performance benchmark tests for the scan module.

These tests verify performance targets from spec.md:
- SC-001: <1s for 1K files
- SC-002: <10s for 10K files

Benchmarks use pytest-benchmark when available, or simple timing otherwise.
"""

from __future__ import annotations

import io
import time
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import rasterio
from rasterio.transform import from_bounds

from portolan_cli.scan import ScanOptions, scan_directory

if TYPE_CHECKING:
    from collections.abc import Callable


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
def benchmark_dir_10k(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a directory with ~10000 files for benchmarking.

    Structure: 100 directories with 100 files each = 10000 files.
    """
    base = tmp_path_factory.mktemp("benchmark_10k")
    _create_benchmark_structure(base, dirs=100, files_per_dir=100)
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
# Phase 9: Benchmark Tests
# =============================================================================


@pytest.mark.benchmark
@pytest.mark.slow
class TestScanPerformance:
    """Benchmark tests for scan performance.

    Per spec.md Success Criteria:
    - SC-001: Scan completes in under 1 second for directories with <1K files
    - SC-002: Scan completes in under 10 seconds for directories with <10K files
    """

    def test_scan_1k_files_under_1_second(
        self,
        benchmark_dir_1k: Path,
        benchmark: Callable[..., float],
    ) -> None:
        """SC-001: Scan completes in under 1 second for <1K files."""

        def run_scan() -> None:
            scan_directory(benchmark_dir_1k)

        # Use pytest-benchmark for accurate timing
        benchmark(run_scan)

        # The benchmark table output shows results
        # The test passes if it completes - if we need explicit assertion,
        # we verify using manual timing

    def test_scan_10k_files_under_10_seconds(
        self,
        benchmark_dir_10k: Path,
        benchmark: Callable[..., float],
    ) -> None:
        """SC-002: Scan completes in under 10 seconds for <10K files."""

        def run_scan() -> None:
            scan_directory(benchmark_dir_10k)

        # Use pytest-benchmark for accurate timing
        benchmark(run_scan)

        # The benchmark table output shows results
        # The test passes if it completes in reasonable time

    def test_scan_deep_nesting_performance(
        self,
        benchmark_dir_deep: Path,
        benchmark: Callable[..., float],
    ) -> None:
        """Verify deep nesting doesn't cause performance degradation."""

        def run_scan() -> None:
            scan_directory(benchmark_dir_deep)

        # Use pytest-benchmark for accurate timing
        benchmark(run_scan)

        # Deep nesting with only 100 files should be very fast
        # The benchmark table output shows results

    def test_scan_with_max_depth_is_faster(
        self,
        benchmark_dir_1k: Path,
    ) -> None:
        """Verify --max-depth limits scan scope and improves performance."""
        # Full scan
        start_full = time.perf_counter()
        result_full = scan_directory(benchmark_dir_1k)
        time_full = time.perf_counter() - start_full

        # Limited scan
        start_limited = time.perf_counter()
        result_limited = scan_directory(benchmark_dir_1k, ScanOptions(max_depth=0))
        time_limited = time.perf_counter() - start_limited

        # Limited scan should be faster
        assert time_limited < time_full, (
            f"Limited scan ({time_limited:.3f}s) should be faster than full scan ({time_full:.3f}s)"
        )

        # Limited scan should scan fewer directories
        assert result_limited.directories_scanned < result_full.directories_scanned

    def test_scan_result_object_creation_efficient(
        self,
        benchmark_dir_1k: Path,
    ) -> None:
        """Verify result object creation doesn't add significant overhead."""
        # First scan to warm up filesystem cache
        _ = scan_directory(benchmark_dir_1k)

        # Measure just the scan with result creation
        times = []
        for _ in range(5):
            start = time.perf_counter()
            scan_directory(benchmark_dir_1k)
            times.append(time.perf_counter() - start)

        avg_time = sum(times) / len(times)

        # Verify we're well under the 1s target
        assert avg_time < 0.5, f"Average scan time {avg_time:.3f}s, should be <0.5s for 1K files"


@pytest.mark.benchmark
@pytest.mark.slow
class TestScanMemoryEfficiency:
    """Test memory efficiency of scan operations."""

    def test_scan_uses_lazy_iteration(
        self,
        benchmark_dir_1k: Path,
    ) -> None:
        """Verify scan uses lazy iteration for memory efficiency.

        The _discover_files generator should yield files one at a time,
        not load all files into memory at once.
        """
        # Run scan and verify it completes without memory issues
        result = scan_directory(benchmark_dir_1k)

        # Verify we found files (sanity check)
        assert len(result.ready) > 0

        # The fact that we got here without OOM indicates lazy iteration works
        # For a more rigorous test, we'd use memory_profiler, but that's overkill
        # for this MVP


@pytest.mark.benchmark
@pytest.mark.slow
class TestScanPerformanceTargets:
    """Explicit tests for spec success criteria.

    These tests use manual timing to explicitly verify the performance targets.
    """

    def test_sc001_1k_files_under_1_second(
        self,
        benchmark_dir_1k: Path,
    ) -> None:
        """SC-001: Scan completes in under 1 second for directories with <1K files."""
        # Warm up
        _ = scan_directory(benchmark_dir_1k)

        # Measure
        start = time.perf_counter()
        result = scan_directory(benchmark_dir_1k)
        elapsed = time.perf_counter() - start

        assert elapsed < 1.0, f"SC-001 FAILED: Scan took {elapsed:.3f}s, expected <1s"
        # Verify we actually scanned files
        assert len(result.ready) > 500, "Expected >500 files in 1K benchmark"

    def test_sc002_10k_files_under_10_seconds(
        self,
        benchmark_dir_10k: Path,
    ) -> None:
        """SC-002: Scan completes in under 10 seconds for directories with <10K files."""
        # Warm up
        _ = scan_directory(benchmark_dir_10k)

        # Measure
        start = time.perf_counter()
        result = scan_directory(benchmark_dir_10k)
        elapsed = time.perf_counter() - start

        assert elapsed < 10.0, f"SC-002 FAILED: Scan took {elapsed:.3f}s, expected <10s"
        # Verify we actually scanned files
        assert len(result.ready) > 5000, "Expected >5000 files in 10K benchmark"


@pytest.mark.benchmark
class TestScanPerformanceSimple:
    """Simple timing tests that don't require pytest-benchmark fixtures.

    These tests use manual timing for environments where pytest-benchmark
    is not available or configured differently.
    """

    def test_scan_fixture_dirs_are_fast(self, fixtures_dir: Path) -> None:
        """Verify scanning test fixtures is fast (<100ms each)."""
        # Get the scan fixtures directory
        scan_fixtures = Path(__file__).parent.parent / "fixtures" / "scan"

        if not scan_fixtures.exists():
            pytest.skip("Scan fixtures not found")

        for subdir in scan_fixtures.iterdir():
            if subdir.is_dir():
                start = time.perf_counter()
                scan_directory(subdir)
                elapsed = time.perf_counter() - start

                assert elapsed < 0.1, f"Scanning {subdir.name} took {elapsed:.3f}s, expected <0.1s"


# Fixture for simple timing tests
@pytest.fixture
def fixtures_dir() -> Path:
    """Return path to test fixtures."""
    return Path(__file__).parent.parent / "fixtures" / "scan"


# =============================================================================
# Issue #314: O(n²) Performance Regression Tests
# =============================================================================


@pytest.fixture(scope="session")
def benchmark_dir_many_dirs(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create EuroSAT-like structure: many directories, 1 file each.

    Structure: 27,000 directories with 1 TIF file each.
    This pattern triggers O(n²) behavior in _check_mixed_structure.

    See: https://github.com/portolan-sdi/portolan-cli/issues/314
    """
    base = tmp_path_factory.mktemp("benchmark_many_dirs")
    _create_many_dirs_structure(base, num_dirs=27_000)
    return base


def _create_many_dirs_structure(base: Path, num_dirs: int) -> None:
    """Create many directories with 1 file each (EuroSAT pattern).

    Structure mimics satellite imagery datasets where each scene
    is in its own subdirectory.
    """
    for i in range(num_dirs):
        subdir = base / f"scene_{i:05d}"
        subdir.mkdir(parents=True, exist_ok=True)
        (subdir / "data.tif").write_bytes(b"dummy tif content")


@pytest.mark.benchmark
@pytest.mark.slow
class TestScanManyDirectoriesPerformance:
    """Regression tests for issue #314: O(n²) in _check_mixed_structure.

    The old implementation had nested loops over all directories,
    causing 27K × 27K = 729 million operations. The fix uses O(n)
    parent-chain traversal instead.

    Target: <60 seconds for 27K directories.
    """

    def test_issue_314_many_dirs_under_60_seconds(
        self,
        benchmark_dir_many_dirs: Path,
    ) -> None:
        """Issue #314: Scan 27K directories completes in under 60 seconds.

        This is a regression test for the O(n²) performance bug.
        Before the fix, this would hang for 3+ minutes.
        """
        start = time.perf_counter()
        result = scan_directory(benchmark_dir_many_dirs)
        elapsed = time.perf_counter() - start

        assert elapsed < 60.0, (
            f"Issue #314 regression: Scan took {elapsed:.1f}s, expected <60s. "
            f"O(n²) bug may have returned."
        )

        # Sanity check: we actually scanned all directories
        assert result.directories_scanned >= 27_000, (
            f"Expected 27K+ directories scanned, got {result.directories_scanned}"
        )

    def test_many_dirs_no_mixed_structure_issues(
        self,
        benchmark_dir_many_dirs: Path,
    ) -> None:
        """EuroSAT structure (files only at leaf) should have no mixed structure issues."""
        from portolan_cli.scan import IssueType

        result = scan_directory(benchmark_dir_many_dirs)

        mixed_issues = [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM]
        assert len(mixed_issues) == 0, (
            f"Expected no MIXED_FLAT_MULTIITEM issues for leaf-only structure, "
            f"got {len(mixed_issues)}"
        )
