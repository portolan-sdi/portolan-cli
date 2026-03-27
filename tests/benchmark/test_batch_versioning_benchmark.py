"""Benchmark tests for batch versioning (Issue #281).

Verifies that the batch approach achieves O(n) versioning performance
instead of O(n²).

These tests use real GeoJSON files to measure actual performance.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def benchmark_catalog(tmp_path: Path) -> Path:
    """Create a catalog for benchmarking."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")

    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "benchmark-catalog",
        "description": "Catalog for benchmarking",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


def create_geojson_files(directory: Path, count: int) -> list[Path]:
    """Create N minimal GeoJSON files for benchmarking."""
    files = []
    for i in range(count):
        item_dir = directory / f"item-{i:04d}"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson = item_dir / f"data-{i:04d}.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [-122.4 + (i * 0.001), 37.8 + (i * 0.001)],
                            },
                            "properties": {"id": i, "name": f"Feature {i}"},
                        }
                    ],
                }
            )
        )
        files.append(geojson)

    return files


class TestBatchVersioningPerformance:
    """Performance tests for batch versioning."""

    @pytest.mark.benchmark
    def test_batch_writes_versions_once_per_collection(
        self,
        benchmark_catalog: Path,
    ) -> None:
        """Verify batch approach writes versions.json once per collection.

        This is the key metric: regardless of file count, we should only
        write versions.json once per collection.
        """
        from portolan_cli.dataset import add_files
        from portolan_cli.versions import write_versions

        collection_dir = benchmark_catalog / "test-collection"
        collection_dir.mkdir()

        # Create 20 files (enough to see the pattern)
        files = create_geojson_files(collection_dir, 20)

        write_count = 0
        original_write = write_versions

        def counting_write(*args, **kwargs):
            nonlocal write_count
            write_count += 1
            return original_write(*args, **kwargs)

        with patch("portolan_cli.dataset.write_versions", side_effect=counting_write):
            add_files(
                paths=files,
                catalog_root=benchmark_catalog,
                collection_id="test-collection",
            )

        # Should write versions.json exactly once (batch mode)
        assert write_count == 1, (
            f"Expected 1 write_versions call (batched), got {write_count}. "
            f"With {len(files)} files, old O(n²) approach would have {len(files)} writes."
        )

    @pytest.mark.benchmark
    def test_scaling_is_linear_not_quadratic(
        self,
        benchmark_catalog: Path,
    ) -> None:
        """Verify that adding 2x files takes ~2x time, not 4x.

        This tests the actual performance characteristic of O(n) vs O(n²).
        We use small file counts to keep the test fast.
        """
        from portolan_cli.dataset import add_files

        # Test with 10 files
        collection_10 = benchmark_catalog / "collection-10"
        collection_10.mkdir()
        files_10 = create_geojson_files(collection_10, 10)

        start = time.perf_counter()
        add_files(
            paths=files_10,
            catalog_root=benchmark_catalog,
            collection_id="collection-10",
        )
        time_10 = time.perf_counter() - start

        # Test with 20 files (2x)
        collection_20 = benchmark_catalog / "collection-20"
        collection_20.mkdir()
        files_20 = create_geojson_files(collection_20, 20)

        start = time.perf_counter()
        add_files(
            paths=files_20,
            catalog_root=benchmark_catalog,
            collection_id="collection-20",
        )
        time_20 = time.perf_counter() - start

        # O(n) scaling: 2x files should take ~2x time (with margin for variance)
        # O(n²) scaling: 2x files would take ~4x time
        ratio = time_20 / time_10 if time_10 > 0 else float("inf")

        # Allow up to 3x for variance (should be ~2x for O(n), would be ~4x for O(n²))
        assert ratio < 3.5, (
            f"Scaling ratio is {ratio:.2f}x for 2x files. "
            f"Expected ~2x (O(n)), got {ratio:.2f}x. "
            f"Times: 10 files = {time_10:.3f}s, 20 files = {time_20:.3f}s"
        )

        # Log the actual performance for tracking
        print(f"\n  Performance: 10 files = {time_10:.3f}s, 20 files = {time_20:.3f}s")
        print(f"  Scaling ratio: {ratio:.2f}x (ideal = 2.0x)")

    @pytest.mark.benchmark
    def test_add_directory_uses_batch_versioning(
        self,
        benchmark_catalog: Path,
    ) -> None:
        """Verify add_directory also uses batch versioning."""
        from portolan_cli.dataset import add_directory
        from portolan_cli.versions import write_versions

        collection_dir = benchmark_catalog / "dir-collection"
        collection_dir.mkdir()

        # Create files directly in collection dir (add_directory scans for them)
        for i in range(10):
            geojson = collection_dir / f"data-{i:03d}.geojson"
            geojson.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {"type": "Point", "coordinates": [i, i]},
                                "properties": {},
                            }
                        ],
                    }
                )
            )

        write_count = 0
        original_write = write_versions

        def counting_write(*args, **kwargs):
            nonlocal write_count
            write_count += 1
            return original_write(*args, **kwargs)

        with patch("portolan_cli.dataset.write_versions", side_effect=counting_write):
            add_directory(
                path=collection_dir,
                catalog_root=benchmark_catalog,
                collection_id="dir-collection",
                recursive=False,
            )

        # Should write versions.json exactly once
        assert write_count == 1, (
            f"Expected 1 write_versions call from add_directory, got {write_count}"
        )
