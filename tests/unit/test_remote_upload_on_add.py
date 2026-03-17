"""Tests for remote upload on add (iceberg backend + remote configured).

Phase 2 of PLAN-portolake-remote-mode: when backend=iceberg and remote is set,
add_dataset() should upload converted files and STAC metadata to the remote
location after local processing.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def catalog_with_stac(tmp_path: Path) -> tuple[Path, Path]:
    """Create a catalog with STAC files for upload testing.

    Returns (catalog_root, item_dir).
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

    return catalog_root, item_dir


@pytest.mark.unit
def test_upload_called_when_remote_configured(catalog_with_stac):
    """_upload_to_remote_if_configured should upload data files to remote."""
    from portolan_cli.dataset import _upload_to_remote_if_configured

    catalog_root, item_dir = catalog_with_stac

    with patch("portolan_cli.dataset.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        _upload_to_remote_if_configured(
            catalog_root=catalog_root,
            collection_id="boundaries",
            item_id="item1",
            item_dir=item_dir,
            asset_files={"data.parquet": (item_dir / "data.parquet", "abc")},
            remote="gs://test-bucket/catalog",
        )

        assert mock_upload.call_count >= 1
        destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
        assert any("data.parquet" in d for d in destinations)


@pytest.mark.unit
def test_upload_includes_stac_item_json(catalog_with_stac):
    """Upload should include the STAC item JSON."""
    from portolan_cli.dataset import _upload_to_remote_if_configured

    catalog_root, item_dir = catalog_with_stac

    with patch("portolan_cli.dataset.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        _upload_to_remote_if_configured(
            catalog_root=catalog_root,
            collection_id="boundaries",
            item_id="item1",
            item_dir=item_dir,
            asset_files={"data.parquet": (item_dir / "data.parquet", "abc")},
            remote="gs://test-bucket/catalog",
        )

        destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
        assert any("item1/item1.json" in d for d in destinations)


@pytest.mark.unit
def test_upload_includes_stac_collection_json(catalog_with_stac):
    """Upload should include the STAC collection JSON."""
    from portolan_cli.dataset import _upload_to_remote_if_configured

    catalog_root, item_dir = catalog_with_stac

    with patch("portolan_cli.dataset.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        _upload_to_remote_if_configured(
            catalog_root=catalog_root,
            collection_id="boundaries",
            item_id="item1",
            item_dir=item_dir,
            asset_files={"data.parquet": (item_dir / "data.parquet", "abc")},
            remote="gs://test-bucket/catalog",
        )

        destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
        assert any("boundaries/collection.json" in d for d in destinations)


@pytest.mark.unit
def test_upload_includes_catalog_json(catalog_with_stac):
    """Upload should include the root catalog.json."""
    from portolan_cli.dataset import _upload_to_remote_if_configured

    catalog_root, item_dir = catalog_with_stac

    with patch("portolan_cli.dataset.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        _upload_to_remote_if_configured(
            catalog_root=catalog_root,
            collection_id="boundaries",
            item_id="item1",
            item_dir=item_dir,
            asset_files={"data.parquet": (item_dir / "data.parquet", "abc")},
            remote="gs://test-bucket/catalog",
        )

        destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
        assert any(d.endswith("catalog.json") for d in destinations)


@pytest.mark.unit
def test_no_upload_when_remote_is_none():
    """No upload should happen when remote is None."""
    from portolan_cli.dataset import _upload_to_remote_if_configured

    with patch("portolan_cli.dataset.upload_file") as mock_upload:
        _upload_to_remote_if_configured(
            catalog_root=Path("/fake"),
            collection_id="boundaries",
            item_id="item1",
            item_dir=Path("/fake/boundaries/item1"),
            asset_files={"data.parquet": (Path("/fake/data.parquet"), "abc")},
            remote=None,
        )
        mock_upload.assert_not_called()


@pytest.mark.unit
def test_upload_constructs_correct_remote_paths(catalog_with_stac):
    """Remote paths should follow {remote}/{collection}/{item}/{filename} pattern."""
    from portolan_cli.dataset import _upload_to_remote_if_configured

    catalog_root, item_dir = catalog_with_stac

    with patch("portolan_cli.dataset.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        _upload_to_remote_if_configured(
            catalog_root=catalog_root,
            collection_id="boundaries",
            item_id="item1",
            item_dir=item_dir,
            asset_files={"data.parquet": (item_dir / "data.parquet", "abc")},
            remote="gs://test-bucket/catalog",
        )

        destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
        assert "gs://test-bucket/catalog/boundaries/item1/data.parquet" in destinations


@pytest.mark.unit
def test_upload_strips_trailing_slash_from_remote(catalog_with_stac):
    """Remote URL trailing slash should be stripped to avoid double-slash."""
    from portolan_cli.dataset import _upload_to_remote_if_configured

    catalog_root, item_dir = catalog_with_stac

    with patch("portolan_cli.dataset.upload_file") as mock_upload:
        mock_upload.return_value = MagicMock(success=True)
        _upload_to_remote_if_configured(
            catalog_root=catalog_root,
            collection_id="boundaries",
            item_id="item1",
            item_dir=item_dir,
            asset_files={"data.parquet": (item_dir / "data.parquet", "abc")},
            remote="gs://test-bucket/catalog/",
        )

        destinations = [call.kwargs["destination"] for call in mock_upload.call_args_list]
        # No double slashes
        assert all("//" not in d.split("://")[1] for d in destinations)
