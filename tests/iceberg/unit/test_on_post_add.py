"""Tests for IcebergBackend.on_post_add() hook.

Moved from portolan-cli's test_remote_upload_on_add.py. The on_post_add hook
combines STAC extension metadata update and remote STAC metadata upload.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pystac
import pytest

from portolan_cli.backends.iceberg.stac_generator import (
    STAC_ICEBERG_EXTENSION,
    STAC_TABLE_EXTENSION,
)


@pytest.fixture
def catalog_with_stac(tmp_path: Path) -> tuple[Path, Path, pystac.Collection]:
    """Create a catalog with STAC files for on_post_add testing.

    Returns (catalog_root, item_dir, collection).
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog"}')

    collection_dir = catalog_root / "boundaries"
    collection_dir.mkdir()
    (collection_dir / "collection.json").write_text('{"type": "Collection"}')

    item_dir = collection_dir / "item1"
    item_dir.mkdir()
    (item_dir / "data.parquet").write_bytes(b"fake parquet")
    (item_dir / "item1.json").write_text('{"type": "Feature"}')

    collection = pystac.Collection(
        id="boundaries",
        description="test",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent(intervals=[[None, None]]),
        ),
    )

    return catalog_root, item_dir, collection


def _make_context(
    catalog_root: Path,
    item_dir: Path,
    collection: pystac.Collection,
    remote: str | None = "gs://test-bucket/catalog",
) -> dict:
    """Build a PostAddContext dict for testing."""
    return {
        "catalog_root": catalog_root,
        "collection_id": "boundaries",
        "collection_dir": catalog_root / "boundaries",
        "collection": collection,
        "item_id": "item1",
        "item_dir": item_dir,
        "asset_files": {"data.parquet": (item_dir / "data.parquet", "abc")},
        "remote": remote,
    }


@pytest.mark.integration
def test_on_post_add_updates_stac_extensions(iceberg_backend, parquet_file, catalog_with_stac):
    """on_post_add should update collection extra_fields with STAC extension metadata."""
    catalog_root, item_dir, collection = catalog_with_stac

    # Publish so get_stac_metadata has data
    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={
            "columns": ["id", "name", "value"],
            "types": {"id": "int64", "name": "string", "value": "float64"},
            "hash": "abc",
        },
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with patch("portolan_cli.backends.iceberg.backend.upload_file", create=True):
        iceberg_backend.on_post_add(context)

    # Collection should have table:columns in extra_fields
    assert "table:columns" in collection.extra_fields
    # Extensions should be set via pystac attribute, not extra_fields
    assert STAC_TABLE_EXTENSION in collection.stac_extensions
    assert STAC_ICEBERG_EXTENSION in collection.stac_extensions


@pytest.mark.integration
def test_on_post_add_uploads_stac_metadata_when_remote_set(
    iceberg_backend, parquet_file, catalog_with_stac
):
    """on_post_add should upload STAC metadata when remote is configured."""
    catalog_root, item_dir, collection = catalog_with_stac

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote="gs://test-bucket/catalog")

    with patch("portolan_cli.upload.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        iceberg_backend.on_post_add(context)

    assert mock_upload.call_count >= 1
    destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
    assert any("item1.json" in d for d in destinations)


@pytest.mark.integration
def test_on_post_add_no_upload_when_remote_is_none(
    iceberg_backend, parquet_file, catalog_with_stac
):
    """on_post_add should not upload when remote is None."""
    catalog_root, item_dir, collection = catalog_with_stac

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with patch("portolan_cli.upload.upload_file") as mock_upload:
        iceberg_backend.on_post_add(context)

    mock_upload.assert_not_called()


@pytest.mark.integration
def test_on_post_add_uploads_correct_remote_paths(iceberg_backend, parquet_file, catalog_with_stac):
    """Remote paths should follow the correct pattern for STAC metadata."""
    catalog_root, item_dir, collection = catalog_with_stac

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote="gs://test-bucket/catalog")

    with patch("portolan_cli.upload.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        iceberg_backend.on_post_add(context)

    destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
    assert "gs://test-bucket/catalog/boundaries/item1/item1.json" in destinations
    assert "gs://test-bucket/catalog/boundaries/collection.json" in destinations
    assert "gs://test-bucket/catalog/catalog.json" in destinations
    # Data files should NOT be uploaded
    assert "gs://test-bucket/catalog/boundaries/item1/data.parquet" not in destinations


@pytest.mark.integration
def test_on_post_add_strips_trailing_slash(iceberg_backend, parquet_file, catalog_with_stac):
    """Trailing slash in remote URL should be stripped to avoid double-slash."""
    catalog_root, item_dir, collection = catalog_with_stac

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote="gs://test-bucket/catalog/")

    with patch("portolan_cli.upload.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        iceberg_backend.on_post_add(context)

    destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
    assert all("//" not in d.split("://")[1] for d in destinations)


@pytest.mark.integration
def test_on_post_add_no_data_files_uploaded(iceberg_backend, parquet_file, catalog_with_stac):
    """on_post_add should never upload data files -- Iceberg manages them."""
    catalog_root, item_dir, collection = catalog_with_stac

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote="gs://test-bucket/catalog")

    with patch("portolan_cli.upload.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        iceberg_backend.on_post_add(context)

    destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
    assert not any("data.parquet" in d for d in destinations)


# --- Extension and asset merge behavior ---


@pytest.mark.integration
def test_on_post_add_merges_stac_extensions(iceberg_backend, parquet_file, catalog_with_stac):
    """on_post_add should merge portolake extensions into collection.stac_extensions."""
    catalog_root, item_dir, collection = catalog_with_stac
    collection.stac_extensions = [STAC_TABLE_EXTENSION]

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with patch("portolan_cli.backends.iceberg.backend.upload_file", create=True):
        iceberg_backend.on_post_add(context)

    assert STAC_TABLE_EXTENSION in collection.stac_extensions
    assert STAC_ICEBERG_EXTENSION in collection.stac_extensions


@pytest.mark.integration
def test_on_post_add_preserves_existing_extensions(
    iceberg_backend, parquet_file, catalog_with_stac
):
    """on_post_add should preserve third-party extensions already on the collection."""
    catalog_root, item_dir, collection = catalog_with_stac
    projection_ext = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
    collection.stac_extensions = [projection_ext]

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with patch("portolan_cli.backends.iceberg.backend.upload_file", create=True):
        iceberg_backend.on_post_add(context)

    assert projection_ext in collection.stac_extensions
    assert STAC_TABLE_EXTENSION in collection.stac_extensions
    assert STAC_ICEBERG_EXTENSION in collection.stac_extensions


@pytest.mark.integration
def test_on_post_add_no_duplicate_extensions(iceberg_backend, parquet_file, catalog_with_stac):
    """on_post_add should not create duplicate extension entries."""
    catalog_root, item_dir, collection = catalog_with_stac
    collection.stac_extensions = [STAC_TABLE_EXTENSION, STAC_ICEBERG_EXTENSION]

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with patch("portolan_cli.backends.iceberg.backend.upload_file", create=True):
        iceberg_backend.on_post_add(context)

    assert len(collection.stac_extensions) == len(set(collection.stac_extensions))


@pytest.mark.integration
def test_on_post_add_sets_iceberg_data_asset(iceberg_backend, parquet_file, catalog_with_stac):
    """on_post_add should set a data asset via pystac API with correct media type."""
    catalog_root, item_dir, collection = catalog_with_stac

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with patch("portolan_cli.backends.iceberg.backend.upload_file", create=True):
        iceberg_backend.on_post_add(context)

    assert "data" in collection.assets
    assert collection.assets["data"].media_type == "application/x-iceberg"
    assert collection.assets["data"].roles == ["data"]


@pytest.mark.integration
def test_on_post_add_preserves_existing_assets(iceberg_backend, parquet_file, catalog_with_stac):
    """on_post_add should preserve non-data assets already on the collection."""
    catalog_root, item_dir, collection = catalog_with_stac
    collection.assets["thumbnail"] = pystac.Asset(
        href="https://example.com/thumb.png",
        media_type="image/png",
        roles=["thumbnail"],
    )

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with patch("portolan_cli.backends.iceberg.backend.upload_file", create=True):
        iceberg_backend.on_post_add(context)

    assert "thumbnail" in collection.assets
    assert "data" in collection.assets
