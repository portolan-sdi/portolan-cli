"""``IcebergBackend.refresh``: rewrite the Iceberg fields after the catalog moved the metadata.

A hosted catalog runs maintenance on its tables. The maintenance writes a new
metadata file, so ``iceberg:metadata_location`` on ``collection.json`` names an
older file. ``refresh`` reads the table again and merges the current fields.
The table is on the fake lake.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from tests.iceberg.fake_lake import Lake

pytestmark = pytest.mark.unit


def _collection(lake: Lake) -> Path:
    """Publish one version, then write the fields as ``add`` does."""
    path = lake.root / "roads" / "collection.json"
    lake.publish()
    path.write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": "roads",
                "example:enriched": True,
                "assets": {"data": {"href": "./roads.parquet", "roles": ["data"]}},
            }
        ),
        encoding="utf-8",
    )
    lake.backend.write_stac_fields("roads", path.parent)
    return path


def _maintenance(lake: Lake) -> None:
    """Rewrite the metadata file of the table, as the maintenance of a catalog does."""
    lake.catalog.tables[("portolake", "roads")]["location"] += 1


def _read(path: Path) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return document


def test_refresh_writes_the_new_metadata_location(lake: Lake) -> None:
    path = _collection(lake)
    before = _read(path)["iceberg:metadata_location"]
    _maintenance(lake)

    result = lake.backend.refresh("roads")

    after = _read(path)
    assert before.endswith("/metadata/v4.metadata.json")
    assert after["iceberg:metadata_location"] == before.replace("v4.", "v5.")
    assert after["assets"]["iceberg"]["href"] == after["iceberg:metadata_location"]
    assert result.previous_location == before
    assert result.metadata_location == after["iceberg:metadata_location"]
    assert result.changed is True


def test_refresh_keeps_the_other_fields_and_assets(lake: Lake) -> None:
    path = _collection(lake)
    _maintenance(lake)

    lake.backend.refresh("roads")

    after = _read(path)
    assert after["example:enriched"] is True
    assert after["assets"]["data"] == {"href": "./roads.parquet", "roles": ["data"]}
    assert after["iceberg:current_snapshot_id"] == str(lake.metadata()["current-snapshot-id"])


def test_refresh_without_maintenance_changes_nothing(lake: Lake) -> None:
    path = _collection(lake)
    before = path.read_text(encoding="utf-8")

    result = lake.backend.refresh("roads")

    assert result.changed is False
    assert result.previous_location == result.metadata_location
    assert _read(path) == json.loads(before)


def test_refresh_without_collection_json_raises(lake: Lake) -> None:
    lake.publish()

    with pytest.raises(FileNotFoundError, match=r"collection.json of collection 'roads'"):
        lake.backend.refresh("roads")


def test_refresh_without_a_table_raises(lake: Lake) -> None:
    path = lake.root / "roads" / "collection.json"
    path.parent.mkdir()
    path.write_text('{"type": "Collection", "id": "roads"}', encoding="utf-8")

    with pytest.raises(FileNotFoundError, match=r"No Iceberg table portolake.roads"):
        lake.backend.refresh("roads")


def test_refresh_refuses_an_unsafe_collection_name(lake: Lake) -> None:
    with pytest.raises(ValueError, match="Invalid collection name"):
        lake.backend.refresh("../roads")


def test_refresh_uploads_the_collection_to_the_remote(lake: Lake) -> None:
    path = _collection(lake)
    _maintenance(lake)

    with patch("portolan_cli.sync.upload.upload_file") as upload:
        upload.return_value = MagicMock(success=True)
        lake.backend.refresh("roads", remote="gs://bucket/catalog/")

    upload.assert_called_once_with(
        source=path, destination="gs://bucket/catalog/roads/collection.json"
    )


def test_refresh_without_a_remote_uploads_nothing(lake: Lake) -> None:
    _collection(lake)
    _maintenance(lake)

    with patch("portolan_cli.sync.upload.upload_file") as upload:
        lake.backend.refresh("roads")

    upload.assert_not_called()
