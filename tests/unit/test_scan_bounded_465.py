"""O(n²) add on collection-level vector assets."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
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
    assets: dict[str, dict] = data.get("assets", {})
    return assets


@pytest.fixture(scope="module")
def conformant_parquet_bytes(tmp_path_factory: pytest.TempPathFactory, fixtures_dir: Path) -> bytes:
    """`simple.parquet` with the bbox covering column that `add` asks for."""
    import geoparquet_io as gpio

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
    n = 20
    collection_dir = initialized_catalog / "many"
    collection_dir.mkdir()
    for i in range(n):
        (collection_dir / f"f{i:04d}.parquet").write_bytes(conformant_parquet_bytes)

    # Count the files the scan measures.
    real_measure = prep_mod._measure
    call_count = 0

    def counting_measure(*args: Any, **kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        return real_measure(*args, **kwargs)

    with patch.object(prep_mod, "_measure", side_effect=counting_measure):
        _added, _skipped, failures = add_files(
            paths=[collection_dir],
            catalog_root=initialized_catalog,
            collection_id="many",
        )

    assert not failures, f"unexpected failures: {failures}"

    # The counter must see the scan: each file checksums at least itself.
    assert call_count >= n, f"_measure counted {call_count} calls for {n} files"
    # Bound: linear, not quadratic. Without the fix the scan makes n*n = 400.
    assert call_count <= 3 * n, (
        f"_measure called {call_count} times for {n} files "
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
    collection_dir = initialized_catalog / "with-companions"
    collection_dir.mkdir()
    (collection_dir / "data.parquet").write_bytes((fixtures_dir / "simple.parquet").read_bytes())
    (collection_dir / "notes.txt").write_text("field notes for this dataset\n")
    (collection_dir / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)

    _added, _skipped, failures = add_files(
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
