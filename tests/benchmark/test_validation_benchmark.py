"""Benchmark tests for validation operations.

Establishes performance baselines for catalog validation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.validation import check


@pytest.fixture
def valid_catalog(tmp_path: Path) -> Path:
    """Create a valid Portolan catalog for benchmarking.

    Uses v2 file structure: catalog.json at root, .portolan/ for management files.
    """
    # v2 structure: .portolan directory with config and state
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.json").write_text("{}")
    (portolan_dir / "state.json").write_text("{}")

    # v2 structure: catalog.json at ROOT level
    catalog_file = tmp_path / "catalog.json"
    catalog_file.write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "benchmark-catalog",
                "description": "A catalog for benchmarking",
                "links": [],
            }
        )
    )
    return tmp_path


@pytest.mark.benchmark
def test_check_valid_catalog_performance(benchmark, valid_catalog: Path) -> None:  # type: ignore[no-untyped-def]
    """Benchmark check() on a valid catalog.

    This measures the time to run all validation rules against a valid catalog.
    As we add more rules, this benchmark will help detect performance regressions.
    """
    result = benchmark(check, valid_catalog)
    assert result.passed is True


@pytest.mark.benchmark
def test_check_invalid_catalog_performance(benchmark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """Benchmark check() on an invalid catalog (no .portolan dir).

    This measures early-exit performance when the catalog doesn't exist.
    Should be very fast since it fails on the first rule.
    """
    result = benchmark(check, tmp_path)
    assert result.passed is False
