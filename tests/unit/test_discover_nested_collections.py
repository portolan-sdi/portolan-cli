"""Tests for recursive collection discovery in nested catalog structures."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.sync.push import discover_collections

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def catalog_with_config(tmp_path: Path) -> Path:
    """Create a valid catalog root with .portolan/config.yaml."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")
    return tmp_path


@pytest.mark.unit
def test_nested_structure_discovered(catalog_with_config: Path) -> None:
    # Create nested structure
    nested_collection = catalog_with_config / "sub-catalog" / "collection"
    nested_collection.mkdir(parents=True)
    (nested_collection / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    assert result == ["sub-catalog/collection"]


@pytest.mark.unit
def test_mixed_flat_and_nested_structure(catalog_with_config: Path) -> None:
    # Create flat collection
    flat = catalog_with_config / "flat-collection"
    flat.mkdir()
    (flat / "versions.json").write_text("{}")

    # Create nested collection
    nested = catalog_with_config / "sub-catalog" / "nested-collection"
    nested.mkdir(parents=True)
    (nested / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    # Should be sorted alphabetically
    assert result == ["flat-collection", "sub-catalog/nested-collection"]


@pytest.mark.unit
def test_deeply_nested_structure(catalog_with_config: Path) -> None:
    deep = catalog_with_config / "level1" / "level2" / "level3" / "deep-collection"
    deep.mkdir(parents=True)
    (deep / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    assert result == ["level1/level2/level3/deep-collection"]


@pytest.mark.unit
def test_portolan_exclusion_in_nested_directory(catalog_with_config: Path) -> None:
    # Create .portolan internal structure (should be excluded)
    internal = catalog_with_config / ".portolan" / "internal"
    internal.mkdir(parents=True)
    (internal / "versions.json").write_text("{}")

    # Create nested .portolan (should be excluded)
    nested_portolan = catalog_with_config / "sub-catalog" / ".portolan" / "cache"
    nested_portolan.mkdir(parents=True)
    (nested_portolan / "versions.json").write_text("{}")

    # Create real collection (should be included)
    real = catalog_with_config / "sub-catalog" / "real-collection"
    real.mkdir(parents=True)
    (real / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    assert result == ["sub-catalog/real-collection"]


@pytest.mark.unit
def test_multiple_nested_subcatalogs(catalog_with_config: Path) -> None:
    # Subcatalog A with two collections
    for name in ["collection-1", "collection-2"]:
        coll = catalog_with_config / "subcatalog-a" / name
        coll.mkdir(parents=True)
        (coll / "versions.json").write_text("{}")

    # Subcatalog B with one collection
    coll = catalog_with_config / "subcatalog-b" / "collection-3"
    coll.mkdir(parents=True)
    (coll / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    assert result == [
        "subcatalog-a/collection-1",
        "subcatalog-a/collection-2",
        "subcatalog-b/collection-3",
    ]


@pytest.mark.unit
def test_subcatalog_without_collections_ignored(catalog_with_config: Path) -> None:
    # Create subcatalog without versions.json
    empty = catalog_with_config / "empty-subcatalog" / "some-dir"
    empty.mkdir(parents=True)
    (empty / "data.txt").write_text("not a collection")

    # Create real collection
    real = catalog_with_config / "real-collection"
    real.mkdir()
    (real / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    assert result == ["real-collection"]


@pytest.mark.unit
def test_root_level_versions_json_ignored(catalog_with_config: Path) -> None:
    # Create versions.json at root (should be ignored)
    (catalog_with_config / "versions.json").write_text("{}")

    # Create real collection in subdirectory
    real = catalog_with_config / "real-collection"
    real.mkdir()
    (real / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    assert result == ["real-collection"]


@pytest.mark.unit
def test_symlink_cycle_detection(catalog_with_config: Path) -> None:
    # Create real collection
    real = catalog_with_config / "real-collection"
    real.mkdir()
    (real / "versions.json").write_text("{}")

    # Create symlink that points back to catalog root (creates cycle)
    loop = catalog_with_config / "loop"
    loop.symlink_to(catalog_with_config)

    # Should not hang, should detect the cycle and deduplicate
    result = discover_collections(catalog_with_config)

    # The real collection should appear exactly once
    assert result == ["real-collection"]


@pytest.mark.unit
def test_empty_nested_directory_structure(catalog_with_config: Path) -> None:
    # Create empty nested structure
    empty = catalog_with_config / "empty-sub" / "another-empty"
    empty.mkdir(parents=True)

    # Create real collection
    real = catalog_with_config / "real-collection"
    real.mkdir()
    (real / "versions.json").write_text("{}")

    result = discover_collections(catalog_with_config)

    assert result == ["real-collection"]
