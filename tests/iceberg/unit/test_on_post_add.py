"""``IcebergBackend.on_post_add``: the Iceberg fields on collection.json and the remote upload.

The hook runs after ``add`` published the version. It merges the
``table:*`` and ``iceberg:*`` fields into the collection document on disk,
adds the extension URLs and the Iceberg metadata asset, and uploads the
STAC files when a remote is configured. The table is on the fake lake.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pystac
import pytest

from portolan_cli.backends.iceberg.stac_generator import (
    STAC_ICEBERG_EXTENSION,
    STAC_TABLE_EXTENSION,
)
from tests.iceberg.fake_lake import Lake

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def _catalog_with_stac(lake: Lake) -> tuple[Path, pystac.Collection]:
    """Write a catalog.json, a collection.json with hook fields, and an item."""
    root = lake.root
    (root / "catalog.json").write_text('{"type": "Catalog"}', encoding="utf-8")
    collection_dir = root / "roads"
    collection_dir.mkdir(exist_ok=True)
    (collection_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": "roads",
                "stac_extensions": ["https://example.org/ext/v1.0.0/schema.json"],
                "example:enriched": True,
                "links": [{"rel": "root", "href": "../catalog.json"}],
                "assets": {"data": {"href": "./roads.parquet", "roles": ["data"]}},
            }
        ),
        encoding="utf-8",
    )
    item_dir = collection_dir / "item1"
    item_dir.mkdir(exist_ok=True)
    (item_dir / "item1.json").write_text('{"type": "Feature"}', encoding="utf-8")
    collection = pystac.Collection(
        id="roads",
        description="test",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent(intervals=[[None, None]]),
        ),
    )
    return item_dir, collection


def _context(lake: Lake, item_dir: Path, collection: pystac.Collection, remote: str | None) -> Any:
    return {
        "catalog_root": lake.root,
        "collection_id": "roads",
        "collection_dir": lake.root / "roads",
        "collection": collection,
        "item_id": "item1",
        "item_dir": item_dir,
        "asset_files": {"roads.parquet": (lake.root / "roads" / "roads.parquet", "abc", 1024)},
        "remote": remote,
        "items": [{"item_id": "item1", "item_dir": item_dir, "asset_files": {}}],
    }


def _run(
    lake: Lake, remote: str | None = None
) -> tuple[dict[str, Any], pystac.Collection, MagicMock]:
    item_dir, collection = _catalog_with_stac(lake)
    lake.publish()
    with patch("portolan_cli.sync.upload.upload_file") as upload:
        upload.return_value = MagicMock(success=True)
        lake.backend.on_post_add(_context(lake, item_dir, collection, remote))
    document = json.loads((lake.root / "roads" / "collection.json").read_text(encoding="utf-8"))
    return document, collection, upload


# --- the fields ---


def test_the_fields_describe_a_v3_table_in_a_rest_catalog(lake: Lake) -> None:
    document, _, _ = _run(lake)

    assert document["iceberg:catalog_type"] == "rest"
    assert document["iceberg:catalog_uri"] == "https://catalog.example.org"
    assert document["iceberg:authorization_type"] == "none"
    assert document["iceberg:table_id"] == "portolake.roads"
    assert document["iceberg:format_version"] == 3
    assert document["iceberg:current_snapshot_id"] == str(lake.metadata()["current-snapshot-id"])
    assert document["table:columns"] == [
        {"name": "id", "type": "int64"},
        {"name": "name", "type": "string"},
        {"name": "geometry", "type": "geometry"},
    ]
    assert document["table:primary_geometry"] == "geometry"


def test_the_row_count_is_queried_not_read_from_the_summary(lake: Lake) -> None:
    document, _, _ = _run(lake)

    assert document["table:row_count"] == 3
    assert any(s.startswith('SELECT count(*) FROM lake."portolake"."roads"') for s in lake.sql)


def test_the_metadata_asset_points_at_the_https_location(lake: Lake) -> None:
    document, collection, _ = _run(lake)

    location = document["iceberg:metadata_location"]
    assert location.startswith("https://s3.example.org/warehouse/portolake/roads/metadata/")
    assert document["assets"]["iceberg"] == {
        "href": location,
        "type": "application/vnd.apache.iceberg+json",
        "roles": ["metadata"],
        "description": "Apache Iceberg table metadata, read with DuckDB iceberg_scan()",
    }
    assert collection.assets["iceberg"].href == location


def test_an_unfetchable_location_gets_the_fields_but_no_asset(tmp_path: Path) -> None:
    """An s3:// location is not a document a reader can fetch (PTL-AST-002)."""
    lake = Lake(tmp_path, use_ssl=False)

    document, _, _ = _run(lake)

    assert document["iceberg:metadata_location"].startswith("s3://warehouse/")
    assert "iceberg" not in document["assets"]


def test_the_extensions_are_added_once_and_the_others_kept(lake: Lake) -> None:
    document, collection, _ = _run(lake)
    lake.backend.on_post_add(_context(lake, lake.root / "roads" / "item1", collection, None))
    again = json.loads((lake.root / "roads" / "collection.json").read_text(encoding="utf-8"))

    assert document["stac_extensions"] == [
        "https://example.org/ext/v1.0.0/schema.json",
        STAC_TABLE_EXTENSION,
        STAC_ICEBERG_EXTENSION,
    ]
    assert again["stac_extensions"] == document["stac_extensions"]
    assert collection.stac_extensions.count(STAC_TABLE_EXTENSION) == 1


def test_the_hook_fields_the_data_asset_and_the_links_survive(lake: Lake) -> None:
    """The merge edits the file on disk. A pystac save would drop the hook fields (#947)."""
    document, _, _ = _run(lake)

    assert document["example:enriched"] is True
    assert document["assets"]["data"] == {"href": "./roads.parquet", "roles": ["data"]}
    assert document["links"] == [{"rel": "root", "href": "../catalog.json"}]


def test_a_failure_in_the_fields_is_logged_and_does_not_raise(
    lake: Lake, caplog: pytest.LogCaptureFixture
) -> None:
    item_dir, collection = _catalog_with_stac(lake)
    (lake.root / "roads" / "collection.json").unlink()
    lake.publish()

    with caplog.at_level(logging.WARNING):
        lake.backend.on_post_add(_context(lake, item_dir, collection, None))

    assert "Could not update STAC extensions for collection roads" in caplog.text


# --- the upload ---


def test_the_stac_files_go_to_the_remote_and_the_data_does_not(lake: Lake) -> None:
    _, _, upload = _run(lake, remote="gs://bucket/catalog/")

    destinations = [call.kwargs["destination"] for call in upload.call_args_list]
    assert destinations == [
        "gs://bucket/catalog/roads/item1/item1.json",
        "gs://bucket/catalog/roads/collection.json",
        "gs://bucket/catalog/catalog.json",
    ]
    assert not any(
        str(call.kwargs["source"]).endswith(".parquet") for call in upload.call_args_list
    )


def test_no_remote_means_no_upload(lake: Lake) -> None:
    _, _, upload = _run(lake, remote=None)

    upload.assert_not_called()
