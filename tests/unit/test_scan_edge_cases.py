"""Tests for scan edge case detection (nested catalogs per ADR-0032)."""

from pathlib import Path

from portolan_cli.scan_detect import detect_stac_catalogs


def test_error_on_catalog_and_collection_in_same_directory(tmp_path: Path):
    """Test that directories with both catalog.json and collection.json are detected as errors."""
    # Create a directory with both files
    mixed_dir = tmp_path / "mixed"
    mixed_dir.mkdir()
    (mixed_dir / "catalog.json").write_text('{"type": "Catalog"}')
    (mixed_dir / "collection.json").write_text('{"type": "Collection"}')

    # Detect STAC catalogs
    results = detect_stac_catalogs(tmp_path)

    # Should find both files
    catalog_files = [r for r in results if r.format_type == "stac_catalog"]
    collection_files = [r for r in results if r.format_type == "stac_collection"]

    assert len(catalog_files) == 1
    assert len(collection_files) == 1

    # Both should be in the same directory
    assert catalog_files[0].path.parent == collection_files[0].path.parent


def test_nested_catalogs_are_detected(tmp_path: Path):
    """Test that nested catalog.json files are detected."""
    # Create nested structure
    root_catalog = tmp_path / "catalog.json"
    root_catalog.write_text('{"type": "Catalog"}')

    sub_catalog = tmp_path / "environment" / "catalog.json"
    sub_catalog.parent.mkdir()
    sub_catalog.write_text('{"type": "Catalog"}')

    deep_catalog = tmp_path / "environment" / "air-quality" / "catalog.json"
    deep_catalog.parent.mkdir()
    deep_catalog.write_text('{"type": "Catalog"}')

    # Detect catalogs
    results = detect_stac_catalogs(tmp_path)
    catalog_files = [r for r in results if r.format_type == "stac_catalog"]

    assert len(catalog_files) == 3


def test_nested_collections_are_detected(tmp_path: Path):
    """Test that collections at any depth are detected."""
    # Create nested structure with collections
    root_catalog = tmp_path / "catalog.json"
    root_catalog.write_text('{"type": "Catalog"}')

    # Collection at root level
    root_collection = tmp_path / "demographics" / "collection.json"
    root_collection.parent.mkdir()
    root_collection.write_text('{"type": "Collection"}')

    # Nested catalog
    sub_catalog = tmp_path / "environment" / "catalog.json"
    sub_catalog.parent.mkdir()
    sub_catalog.write_text('{"type": "Catalog"}')

    # Collection under nested catalog
    nested_collection = tmp_path / "environment" / "air-quality" / "collection.json"
    nested_collection.parent.mkdir()
    nested_collection.write_text('{"type": "Collection"}')

    # Detect all
    results = detect_stac_catalogs(tmp_path)
    catalog_files = [r for r in results if r.format_type == "stac_catalog"]
    collection_files = [r for r in results if r.format_type == "stac_collection"]

    assert len(catalog_files) == 2
    assert len(collection_files) == 2
