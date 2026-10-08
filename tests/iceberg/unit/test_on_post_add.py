"""Tests for IcebergBackend.on_post_add() hook.

Moved from portolan-cli's test_remote_upload_on_add.py. The on_post_add hook
combines STAC extension metadata update and remote STAC metadata upload.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pystac
import pytest

from portolan_cli.backends.iceberg.stac_generator import (
    STAC_ICEBERG_EXTENSION,
    STAC_TABLE_EXTENSION,
)

if TYPE_CHECKING:
    from pathlib import Path


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
        "asset_files": {"data.parquet": (item_dir / "data.parquet", "abc", 1024)},
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
def test_on_post_add_logs_warning_when_stac_update_fails(
    iceberg_backend, parquet_file, catalog_with_stac, caplog
):
    """STAC enrichment failures must surface as a warning with traceback, not be swallowed."""
    catalog_root, item_dir, collection = catalog_with_stac

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    context = _make_context(catalog_root, item_dir, collection, remote=None)

    with (
        patch(
            "portolan_cli.backends.iceberg.stac_generator.generate_collection_metadata",
            side_effect=RuntimeError("boom"),
        ),
        caplog.at_level(logging.WARNING, logger="portolan_cli.backends.iceberg.backend"),
    ):
        # Best-effort enrichment: the add must not fail if STAC update fails.
        iceberg_backend.on_post_add(context)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("boundaries" in r.getMessage() for r in warnings)
    # exc_info must be attached so the traceback reaches the logs.
    assert any(r.exc_info is not None for r in warnings)


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

    with patch("portolan_cli.sync.upload.upload_file") as mock_upload:
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

    with patch("portolan_cli.sync.upload.upload_file") as mock_upload:
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

    with patch("portolan_cli.sync.upload.upload_file") as mock_upload:
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

    with patch("portolan_cli.sync.upload.upload_file") as mock_upload:
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

    with patch("portolan_cli.sync.upload.upload_file") as mock_upload:
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


def _publish_and_run_post_add(iceberg_backend, parquet_file, catalog_root, item_dir, collection):
    """Publish one collection and run the post-add hook over it.

    Every assertion below needs the same three steps, so they live here rather
    than in each test (the duplicate-code gate reads tests/ too).
    """
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


@pytest.mark.integration
def test_on_post_add_leaves_the_data_asset_alone(iceberg_backend, parquet_file, catalog_with_stac):
    """An asset already keyed "data" survives (issue #883).

    The backend used to assign assets["data"] the warehouse directory, typed
    application/x-iceberg. A generated catalog keys its GeoParquet asset by
    filename, so there the assignment added a second asset carrying the same
    "data" role. A collection that does use the "data" key, such as a
    hand-authored one, lost it.
    """
    catalog_root, item_dir, collection = catalog_with_stac
    collection.assets["data"] = pystac.Asset(
        href="./roads.parquet",
        media_type="application/vnd.apache.parquet",
        roles=["data"],
    )

    _publish_and_run_post_add(iceberg_backend, parquet_file, catalog_root, item_dir, collection)

    assert collection.assets["data"].href == "./roads.parquet"
    assert collection.assets["data"].media_type == "application/vnd.apache.parquet"
    assert collection.assets["data"].roles == ["data"]


@pytest.mark.integration
def test_on_post_add_writes_no_iceberg_asset_for_a_local_warehouse(
    iceberg_backend, parquet_file, catalog_with_stac
):
    """A local warehouse gets no asset, because no reader can fetch the file.

    The extension says to add the asset only when the metadata.json is a
    document a reader can fetch. The default SQLite catalog writes a file://
    location, which also carries the absolute path of the machine that ran the
    command.
    """
    catalog_root, item_dir, collection = catalog_with_stac

    _publish_and_run_post_add(iceberg_backend, parquet_file, catalog_root, item_dir, collection)

    assert "iceberg" not in collection.assets
    assert "iceberg:metadata_location" not in collection.extra_fields
    assert collection.extra_fields["iceberg:catalog_type"] == "sql"
    assert collection.extra_fields["iceberg:table_id"] == "portolake.boundaries"


@pytest.mark.integration
def test_on_post_add_adds_the_iceberg_metadata_asset_when_fetchable(
    iceberg_backend, parquet_file, catalog_with_stac, monkeypatch
):
    """An https metadata.json becomes its own asset, with the metadata role.

    A reader opens the table from this file with no catalog service, which is
    what the convention asks for. The role is "metadata" alone, because the
    GeoParquet file keeps the data role.
    """
    from portolan_cli.backends.iceberg import stac_generator

    published = "https://data.example.org/warehouse/boundaries/metadata/v3.metadata.json"
    real = stac_generator.generate_collection_metadata

    def _published(table):
        meta = real(table)
        meta["iceberg:metadata_location"] = published
        return meta

    monkeypatch.setattr(stac_generator, "generate_collection_metadata", _published)

    catalog_root, item_dir, collection = catalog_with_stac
    _publish_and_run_post_add(iceberg_backend, parquet_file, catalog_root, item_dir, collection)

    asset = collection.assets["iceberg"]
    assert asset.href == published
    assert asset.media_type == "application/vnd.apache.iceberg+json"
    assert asset.roles == ["metadata"]
    assert collection.extra_fields["iceberg:metadata_location"] == published


@pytest.mark.integration
def test_on_post_add_preserves_existing_assets(iceberg_backend, parquet_file, catalog_with_stac):
    """on_post_add should leave every asset already on the collection alone."""
    catalog_root, item_dir, collection = catalog_with_stac
    collection.assets["thumbnail"] = pystac.Asset(
        href="https://example.com/thumb.png",
        media_type="image/png",
        roles=["thumbnail"],
    )
    collection.assets["data"] = pystac.Asset(
        href="./roads.parquet",
        media_type="application/vnd.apache.parquet",
        roles=["data"],
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
    assert collection.assets["data"].href == "./roads.parquet"


@pytest.mark.integration
def test_on_post_add_keeps_the_structural_links(iceberg_backend, parquet_file, catalog_with_stac):
    """The hook re-saves the collection, and must not re-root it (issue #940).

    PySTAC points ``root`` at the object it saves, so a plain save makes the
    collection its own root and drops ``parent``. ``portolan check`` then
    reports PTL-LNK-001 and PTL-LNK-006 on every collection the backend writes.
    """
    import json

    catalog_root, item_dir, collection = catalog_with_stac

    _publish_and_run_post_add(iceberg_backend, parquet_file, catalog_root, item_dir, collection)

    written = json.loads(
        (catalog_root / "boundaries" / "collection.json").read_text(encoding="utf-8")
    )
    rels = {link["rel"]: link["href"] for link in written.get("links", [])}

    assert "parent" in rels, f"no parent link: {sorted(rels)}"
    assert rels["root"] != "./collection.json", "root points at the collection itself"
