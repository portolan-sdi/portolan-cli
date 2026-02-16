"""Benchmark tests for catalog operations.

These establish performance baselines. Add real benchmarks as features mature.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from portolan_cli.catalog import Catalog


@pytest.mark.benchmark
def test_catalog_init_performance(benchmark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """Benchmark Catalog.init() to establish baseline performance.

    This measures the time to create a new catalog, which involves:
    - Creating the .portolan directory
    - Writing the catalog.json with STAC metadata

    As the catalog grows more complex, this benchmark will help
    detect performance regressions.
    """

    def init_catalog() -> Catalog:
        # Clean up between iterations so each run starts fresh
        # v2 structure: .portolan/ AND catalog.json at root
        portolan_dir = tmp_path / ".portolan"
        if portolan_dir.exists():
            shutil.rmtree(portolan_dir)
        catalog_file = tmp_path / "catalog.json"
        if catalog_file.exists():
            catalog_file.unlink()
        return Catalog.init(tmp_path)

    result = benchmark(init_catalog)
    assert result is not None
