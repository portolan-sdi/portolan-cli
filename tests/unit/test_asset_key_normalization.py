"""Tests for STAC asset key normalization and titles.

When _scan_item_assets builds STAC asset entries, well-known roles
(thumbnail, metadata, documentation) should:
- Use the role name as the asset key (e.g., "thumbnail" not "preview")
  so STAC consumers can find the asset by role without inspecting paths.
- Carry a default title matching the convention used by Element 84
  Earth Search (every asset has a title).

Falls back to stem on collision so a user with two thumbnails or two
metadata files still gets stable keys.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from portolan_cli.dataset import _scan_item_assets


@pytest.fixture
def item_dir_with_data(tmp_path: Path, valid_points_parquet: Path) -> tuple[Path, Path]:
    """Item directory with a parquet primary data file."""
    item_dir = tmp_path / "collection" / "item-id"
    item_dir.mkdir(parents=True)
    data_file = item_dir / "data.parquet"
    shutil.copy(valid_points_parquet, data_file)
    return item_dir, data_file


class TestAssetKeyNormalization:
    """Well-known roles get stable, role-keyed asset keys."""

    @pytest.mark.unit
    def test_thumbnail_uses_role_name_as_key(self, item_dir_with_data: tuple[Path, Path]) -> None:
        """A preview.png with role thumbnail is keyed as "thumbnail"."""
        item_dir, data_file = item_dir_with_data
        # File named preview.png (not "thumbnail.png") to prove key is
        # derived from role, not from filename.
        (item_dir / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert "thumbnail" in assets
        assert assets["thumbnail"].roles == ["thumbnail"]
        assert assets["thumbnail"].href == "preview.png"

    @pytest.mark.unit
    def test_metadata_uses_role_name_as_key(self, item_dir_with_data: tuple[Path, Path]) -> None:
        """A sidecar XML is keyed as "metadata"."""
        item_dir, data_file = item_dir_with_data
        (item_dir / "iso19139.xml").write_text("<gmd:MD_Metadata/>")

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert "metadata" in assets
        assert assets["metadata"].roles == ["metadata"]

    @pytest.mark.unit
    def test_documentation_uses_role_name_as_key(
        self, item_dir_with_data: tuple[Path, Path]
    ) -> None:
        """A README.md is keyed as "documentation"."""
        item_dir, data_file = item_dir_with_data
        (item_dir / "README.md").write_text("# Hello")

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert "documentation" in assets
        assert assets["documentation"].roles == ["documentation"]

    @pytest.mark.unit
    def test_collision_falls_back_to_stem(self, item_dir_with_data: tuple[Path, Path]) -> None:
        """Two thumbnails: first wins the role key, second uses its stem."""
        item_dir, data_file = item_dir_with_data
        (item_dir / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
        (item_dir / "overview.jpg").write_bytes(b"\xff\xd8\xff" + b"\x00" * 100)

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        # Exactly one of them claims the role key, the other falls back.
        thumbnail_assets = {k: v for k, v in assets.items() if v.roles == ["thumbnail"]}
        assert len(thumbnail_assets) == 2
        assert "thumbnail" in thumbnail_assets
        # Second thumbnail keyed by stem (preview or overview, whichever lost the race)
        other_keys = set(thumbnail_assets) - {"thumbnail"}
        assert other_keys.issubset({"preview", "overview"})
        assert len(other_keys) == 1

    @pytest.mark.unit
    def test_primary_data_still_keyed_data(self, item_dir_with_data: tuple[Path, Path]) -> None:
        """Primary file remains keyed as "data" (unchanged behavior)."""
        item_dir, data_file = item_dir_with_data

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert "data" in assets
        assert assets["data"].roles == ["data"]


class TestAssetTitles:
    """Well-known roles carry a default title."""

    @pytest.mark.unit
    def test_thumbnail_has_default_title(self, item_dir_with_data: tuple[Path, Path]) -> None:
        item_dir, data_file = item_dir_with_data
        (item_dir / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert assets["thumbnail"].title == "Thumbnail"

    @pytest.mark.unit
    def test_metadata_has_default_title(self, item_dir_with_data: tuple[Path, Path]) -> None:
        item_dir, data_file = item_dir_with_data
        (item_dir / "iso.xml").write_text("<x/>")

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert assets["metadata"].title == "Metadata"

    @pytest.mark.unit
    def test_documentation_has_default_title(self, item_dir_with_data: tuple[Path, Path]) -> None:
        item_dir, data_file = item_dir_with_data
        (item_dir / "README.md").write_text("# Hi")

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert assets["documentation"].title == "Documentation"

    @pytest.mark.unit
    def test_data_has_default_title(self, item_dir_with_data: tuple[Path, Path]) -> None:
        item_dir, data_file = item_dir_with_data

        assets, _, _ = _scan_item_assets(
            item_dir=item_dir,
            item_id="item-id",
            primary_file=data_file,
            collection_dir=item_dir.parent,
        )

        assert assets["data"].title == "Data"
