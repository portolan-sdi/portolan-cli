"""Unit tests for dataset._finalize_with_backend.

Covers the versioning + on_post_add dispatch that occurs after a batch of
items has been written to the filesystem.  All I/O is mocked so no real
Iceberg installation is required.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.dataset import PreparedDataset, _finalize_with_backend
from portolan_cli.formats import FormatType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_item(
    tmp_path: Path,
    item_id: str = "census",
    collection_id: str = "boundaries",
    filename: str = "census.parquet",
    is_collection_level: bool = False,
) -> PreparedDataset:
    """Build a minimal PreparedDataset suitable for _finalize_with_backend."""
    item_dir = tmp_path / collection_id / item_id
    item_dir.mkdir(parents=True, exist_ok=True)
    asset_path = item_dir / filename
    asset_path.write_bytes(b"fake data")
    item_json = item_dir / "item.json"
    item_json.write_text("{}")

    return PreparedDataset(
        item_id=item_id,
        collection_id=collection_id,
        format_type=FormatType.VECTOR,
        bbox=[-10.0, -10.0, 10.0, 10.0],
        asset_files={filename: (asset_path, "abc123")},
        item_json_path=item_json,
        is_collection_level_asset=is_collection_level,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFinalizeWithBackend:
    """Tests for _finalize_with_backend helper in dataset.py."""

    @pytest.mark.unit
    def test_calls_publish_version(self, tmp_path: Path) -> None:
        """_finalize_with_backend publishes a version snapshot via publish_version."""
        item = _make_item(tmp_path)
        catalog_root = tmp_path

        with (
            patch("portolan_cli.version_ops.publish_version") as mock_publish,
            patch("portolan_cli.backends.get_backend") as mock_get_backend,
            patch("portolan_cli.config.get_setting", return_value=None),
        ):
            mock_backend = MagicMock(spec=[])  # no on_post_add
            mock_get_backend.return_value = mock_backend

            _finalize_with_backend(
                catalog_root=catalog_root,
                collection_id="boundaries",
                collection_dir=tmp_path / "boundaries",
                collection=MagicMock(),
                items=[item],
                active_backend="iceberg",
            )

        mock_publish.assert_called_once()
        call_args = mock_publish.call_args
        assert call_args.args[0] == "boundaries"
        assert "census/census.parquet" in call_args.kwargs["assets"]
        assert call_args.kwargs["catalog_root"] == catalog_root

    @pytest.mark.unit
    def test_asset_key_uses_item_id_prefix(self, tmp_path: Path) -> None:
        """Non-collection-level assets are keyed as 'item_id/filename'."""
        item = _make_item(tmp_path, item_id="my-item", filename="data.parquet")

        with (
            patch("portolan_cli.version_ops.publish_version") as mock_publish,
            patch("portolan_cli.backends.get_backend") as mock_get_backend,
            patch("portolan_cli.config.get_setting", return_value=None),
        ):
            mock_backend = MagicMock(spec=[])
            mock_get_backend.return_value = mock_backend

            _finalize_with_backend(
                catalog_root=tmp_path,
                collection_id="boundaries",
                collection_dir=tmp_path / "boundaries",
                collection=MagicMock(),
                items=[item],
                active_backend="iceberg",
            )

        assets = mock_publish.call_args.kwargs["assets"]
        assert "my-item/data.parquet" in assets

    @pytest.mark.unit
    def test_collection_level_asset_key_has_no_item_prefix(self, tmp_path: Path) -> None:
        """Collection-level assets are keyed by filename only (no item_id prefix)."""
        item = _make_item(tmp_path, filename="metadata.parquet", is_collection_level=True)

        with (
            patch("portolan_cli.version_ops.publish_version") as mock_publish,
            patch("portolan_cli.backends.get_backend") as mock_get_backend,
            patch("portolan_cli.config.get_setting", return_value=None),
        ):
            mock_backend = MagicMock(spec=[])
            mock_get_backend.return_value = mock_backend

            _finalize_with_backend(
                catalog_root=tmp_path,
                collection_id="boundaries",
                collection_dir=tmp_path / "boundaries",
                collection=MagicMock(),
                items=[item],
                active_backend="iceberg",
            )

        assets = mock_publish.call_args.kwargs["assets"]
        # Should be "metadata.parquet" not "census/metadata.parquet"
        assert "metadata.parquet" in assets

    @pytest.mark.unit
    def test_calls_on_post_add_when_present(self, tmp_path: Path) -> None:
        """_finalize_with_backend calls backend.on_post_add when method exists."""
        item = _make_item(tmp_path)

        with (
            patch("portolan_cli.version_ops.publish_version"),
            patch("portolan_cli.backends.get_backend") as mock_get_backend,
            patch("portolan_cli.config.get_setting", return_value="gs://bucket/catalog"),
        ):
            mock_backend = MagicMock()  # has on_post_add by default
            mock_get_backend.return_value = mock_backend
            collection_mock = MagicMock()

            _finalize_with_backend(
                catalog_root=tmp_path,
                collection_id="boundaries",
                collection_dir=tmp_path / "boundaries",
                collection=collection_mock,
                items=[item],
                active_backend="iceberg",
            )

        mock_backend.on_post_add.assert_called_once()
        ctx = mock_backend.on_post_add.call_args.args[0]
        assert ctx["catalog_root"] == tmp_path
        assert ctx["collection_id"] == "boundaries"
        assert ctx["item_id"] == "census"
        assert ctx["remote"] == "gs://bucket/catalog"

    @pytest.mark.unit
    def test_skips_on_post_add_when_absent(self, tmp_path: Path) -> None:
        """_finalize_with_backend skips on_post_add when method is not on backend."""
        item = _make_item(tmp_path)

        with (
            patch("portolan_cli.version_ops.publish_version"),
            patch("portolan_cli.backends.get_backend") as mock_get_backend,
            patch("portolan_cli.config.get_setting", return_value=None),
        ):
            # spec=[] means the mock has no attributes → hasattr(backend, 'on_post_add') = False
            mock_backend = MagicMock(spec=[])
            mock_get_backend.return_value = mock_backend

            # Should not raise
            _finalize_with_backend(
                catalog_root=tmp_path,
                collection_id="boundaries",
                collection_dir=tmp_path / "boundaries",
                collection=MagicMock(),
                items=[item],
                active_backend="iceberg",
            )

    @pytest.mark.unit
    def test_on_post_add_receives_all_items(self, tmp_path: Path) -> None:
        """_finalize_with_backend passes all items in context to on_post_add."""
        items = [
            _make_item(tmp_path, item_id="item1", filename="a.parquet"),
            _make_item(tmp_path, item_id="item2", filename="b.parquet"),
        ]

        with (
            patch("portolan_cli.version_ops.publish_version"),
            patch("portolan_cli.backends.get_backend") as mock_get_backend,
            patch("portolan_cli.config.get_setting", return_value=None),
        ):
            mock_backend = MagicMock()
            mock_get_backend.return_value = mock_backend

            _finalize_with_backend(
                catalog_root=tmp_path,
                collection_id="boundaries",
                collection_dir=tmp_path / "boundaries",
                collection=MagicMock(),
                items=items,
                active_backend="iceberg",
            )

        ctx = mock_backend.on_post_add.call_args.args[0]
        assert len(ctx["items"]) == 2
        item_ids = {i["item_id"] for i in ctx["items"]}
        assert item_ids == {"item1", "item2"}

    @pytest.mark.unit
    def test_multiple_assets_per_item_all_published(self, tmp_path: Path) -> None:
        """All asset files in each item are included in the published assets dict."""
        item_dir = tmp_path / "boundaries" / "census"
        item_dir.mkdir(parents=True)
        item_json = item_dir / "item.json"
        item_json.write_text("{}")
        file_a = item_dir / "data.parquet"
        file_b = item_dir / "metadata.json"
        file_a.write_bytes(b"parquet")
        file_b.write_bytes(b"meta")

        item = PreparedDataset(
            item_id="census",
            collection_id="boundaries",
            format_type=FormatType.VECTOR,
            bbox=[0, 0, 1, 1],
            asset_files={
                "data.parquet": (file_a, "hash1"),
                "metadata.json": (file_b, "hash2"),
            },
            item_json_path=item_json,
        )

        with (
            patch("portolan_cli.version_ops.publish_version") as mock_publish,
            patch("portolan_cli.backends.get_backend") as mock_get_backend,
            patch("portolan_cli.config.get_setting", return_value=None),
        ):
            mock_backend = MagicMock(spec=[])
            mock_get_backend.return_value = mock_backend

            _finalize_with_backend(
                catalog_root=tmp_path,
                collection_id="boundaries",
                collection_dir=tmp_path / "boundaries",
                collection=MagicMock(),
                items=[item],
                active_backend="iceberg",
            )

        assets = mock_publish.call_args.kwargs["assets"]
        assert "census/data.parquet" in assets
        assert "census/metadata.json" in assets
