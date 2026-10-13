"""Regression tests for issue #465: O(n²) add on collection-level vector assets.

`_scan_item_assets` scans the whole collection directory for every file added.
For collection-level single-file vector assets `item_dir` IS the
collection dir, so adding N files re-checksums every sibling → O(n²) work. This
made the 1000-file stress tests exceed the 300s nightly timeout.

Test 1 bounds the redundant scan; Test 2 guards against the fix over-skipping
and dropping legitimate non-geo companion assets.

Test 1 adds copies of a conformant GeoParquet (with a bbox covering column).
A copy of the bare `simple.parquet` fixture lacks that column, so `add`
rewrites every copy through geoparquet-io (issue #805). The rewrite has nothing
to do with the scan bound and made the test take 8 s locally and 41 s on CI.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

import portolan_cli.preparation as prep_mod
from portolan_cli.add import _batch_sibling_names, add_files

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.unit]


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Minimal initialized catalog."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog for issue #465",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))
    return tmp_path


def _collection_assets(collection_dir: Path) -> dict[str, dict]:
    data = json.loads((collection_dir / "collection.json").read_text())
    return data.get("assets", {})


@pytest.fixture(scope="module")
def conformant_parquet_bytes(tmp_path_factory: pytest.TempPathFactory, fixtures_dir: Path) -> bytes:
    """`simple.parquet` with the bbox covering column that `add` asks for.

    The test fails rather than silently paying for 20 rewrites when the
    conformance rule changes and this file stops meeting it.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    from portolan_cli.conversion_config import VectorSettings
    from portolan_cli.convert import apply_vector_settings
    from portolan_cli.preparation import _needs_spatial_rewrite

    path = tmp_path_factory.mktemp("conformant") / "simple.parquet"
    settings = VectorSettings()
    apply_vector_settings(gpio.read(str(fixtures_dir / "simple.parquet")), settings).write(
        str(path)
    )
    assert not _needs_spatial_rewrite(path, settings)
    return path.read_bytes()


def test_collection_level_scan_is_bounded_not_quadratic(
    initialized_catalog: Path, conformant_parquet_bytes: bytes
) -> None:
    """Adding N collection-level vectors must NOT re-checksum every sibling.

    Pre-fix, each file's scan re-checksums all N siblings, so `compute_checksum`
    is called N*N times (400 for N=20, measured). Post-fix each file checksums
    only itself (+ its own sidecars), so the count is O(N) (20, measured). The
    bound 3*N (60) sits far below the quadratic value and above the linear count.
    """
    n = 20
    collection_dir = initialized_catalog / "many"
    collection_dir.mkdir()
    for i in range(n):
        (collection_dir / f"f{i:04d}.parquet").write_bytes(conformant_parquet_bytes)

    # `_scan_item_assets` lives in preparation.py and checksums through that
    # module's import. Counting `add.compute_checksum` saw 0 calls with or
    # without the fix, so the bound below could never fail.
    real_compute_checksum = prep_mod.compute_checksum
    call_count = 0

    def counting_checksum(path: Path) -> str:
        nonlocal call_count
        call_count += 1
        return real_compute_checksum(path)

    with patch.object(prep_mod, "compute_checksum", side_effect=counting_checksum):
        added, _skipped, failures = add_files(
            paths=[collection_dir],
            catalog_root=initialized_catalog,
            collection_id="many",
        )

    assert not failures, f"unexpected failures: {failures}"

    # The counter must see the scan: each file checksums at least itself.
    assert call_count >= n, f"compute_checksum counted {call_count} calls for {n} files"
    # Bound: linear, not quadratic. Without the fix the scan makes n*n = 400.
    assert call_count <= 3 * n, (
        f"compute_checksum called {call_count} times for {n} files "
        f"(expected O(n) <= {3 * n}; quadratic would be ~{n * n}). "
        "The collection-level scan is re-checksumming siblings (issue #465)."
    )

    # Correctness: the bound must not be met by dropping assets. All N must be
    # tracked in collection.json and in the single versions.json version entry.
    assets = _collection_assets(collection_dir)
    assert len(assets) == n, f"expected {n} assets, got {len(assets)}: {sorted(assets)}"

    versions_data = json.loads((collection_dir / "versions.json").read_text())
    assert len(versions_data["versions"]) == 1
    version_assets = versions_data["versions"][0]["assets"]
    assert len(version_assets) == n, (
        f"expected {n} assets in versions.json, got {len(version_assets)}"
    )


def test_loose_non_geo_companions_are_not_dropped(
    initialized_catalog: Path, fixtures_dir: Path
) -> None:
    """Non-geo companions (not in GEOSPATIAL_EXTENSIONS) must stay tracked.

    Files like `.txt`/`.png` are filtered out of `files_to_process` and are never
    deferred, so at collection level they are tracked ONLY by the sibling scan.
    The O(n²) fix must skip only *other items'* geo files, never these companions. Guards against an over-aggressive skip rule.
    """
    collection_dir = initialized_catalog / "with-companions"
    collection_dir.mkdir()
    (collection_dir / "data.parquet").write_bytes((fixtures_dir / "simple.parquet").read_bytes())
    (collection_dir / "notes.txt").write_text("field notes for this dataset\n")
    (collection_dir / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)

    added, _skipped, failures = add_files(
        paths=[collection_dir],
        catalog_root=initialized_catalog,
        collection_id="with-companions",
    )
    assert not failures, f"unexpected failures: {failures}"

    hrefs = {a.get("href", "") for a in _collection_assets(collection_dir).values()}
    assert any(h.endswith("data.parquet") for h in hrefs), f"geo asset missing: {hrefs}"
    assert any(h.endswith("notes.txt") for h in hrefs), (
        f"non-geo companion notes.txt was dropped: {hrefs}"
    )
    assert any(h.endswith("preview.png") for h in hrefs), (
        f"non-geo companion preview.png was dropped: {hrefs}"
    )


def test_batch_sibling_names_includes_multilayer_outputs(fixtures_dir: Path) -> None:
    """Multi-layer sources contribute their per-layer `{stem}_{layer}.parquet`
    output names so sibling layer items skip each other in a flat collection
    scan (issue #465). Without these, each layer's scan re-checksums the other
    layers' outputs — O(n²) in the layer count.
    """
    gpkg = fixtures_dir / "multilayer" / "multilayer.gpkg"
    names = _batch_sibling_names([gpkg])

    # Source + its single-file output form (always added).
    assert "multilayer.gpkg" in names
    assert "multilayer.parquet" in names
    # Per-layer outputs, matching convert_multilayer_file's `{stem}_{layer}.parquet`.
    assert {
        "multilayer_lines.parquet",
        "multilayer_points.parquet",
        "multilayer_polygons.parquet",
    } <= names
