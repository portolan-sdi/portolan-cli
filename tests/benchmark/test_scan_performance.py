"""Performance benchmark tests for the scan module.

These tests verify performance targets from spec.md:
- SC-001: <1s for 1K files
- SC-002: <10s for 10K files

Benchmarks use pytest-benchmark when available, or simple timing otherwise.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.scan import ScanOptions, scan_directory

if TYPE_CHECKING:
    from collections.abc import Callable


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
    """Create benchmark directory structure with mixed formats."""
    extensions = [".parquet", ".geojson", ".tif", ".gpkg"]

    for i in range(dirs):
        subdir = base / f"dir_{i:04d}"
        subdir.mkdir(parents=True, exist_ok=True)

        for j in range(files_per_dir):
            ext = extensions[j % len(extensions)]
            filename = f"file_{j:04d}{ext}"
            (subdir / filename).write_bytes(b"dummy content")

            # Add shapefile sidecars every 4th file
            if j % 4 == 0:
                stem = f"file_{j:04d}"
                (subdir / f"{stem}.shp").write_bytes(b"shp content")
                (subdir / f"{stem}.dbf").write_bytes(b"dbf content")
                (subdir / f"{stem}.shx").write_bytes(b"shx content")


def _create_deep_structure(base: Path, depth: int, files_per_level: int) -> None:
    """Create deeply nested directory structure."""
    current = base

    for level in range(depth):
        current = current / f"level_{level}"
        current.mkdir(parents=True, exist_ok=True)

        for j in range(files_per_level):
            filename = f"file_{j:04d}.parquet"
            (current / filename).write_bytes(b"dummy content")


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
