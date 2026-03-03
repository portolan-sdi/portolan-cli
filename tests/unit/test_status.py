"""Tests for the status command.

Tests the status module (library layer) and CLI command that shows
tracking states: untracked, tracked, modified, deleted.

Per issue #133: status tracks ALL files in item directories (not just geo files).
Per ADR-0023: item files live in collection/{item_id}/ subdirectories.
Per ADR-0023: versions.json is at collection/versions.json (not .portolan/).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.status import (
    IGNORED_FILES,
    FileStatus,
    StatusResult,
    get_catalog_status,
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def make_catalog(tmp_path: Path, collection_links: list[str] | None = None) -> None:
    """Write a minimal catalog.json to tmp_path."""
    links = [{"rel": "child", "href": f"./{c}/collection.json"} for c in (collection_links or [])]
    (tmp_path / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "test-catalog",
                "stac_version": "1.0.0",
                "description": "Test catalog",
                "links": links,
            }
        )
    )


def make_collection(col_dir: Path) -> None:
    """Write a minimal collection.json inside col_dir (creates dir if needed)."""
    col_dir.mkdir(parents=True, exist_ok=True)
    (col_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": col_dir.name,
                "stac_version": "1.0.0",
                "description": f"Collection {col_dir.name}",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [],
            }
        )
    )


def make_versions_json(
    col_dir: Path,
    assets: dict[str, dict],  # key: relative path from col_dir, value: asset metadata
) -> None:
    """Write a versions.json at collection root (per ADR-0023)."""
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:30:00Z",
                "breaking": False,
                "assets": assets,
                "changes": list(assets.keys()),
            }
        ],
    }
    (col_dir / "versions.json").write_text(json.dumps(versions_data))


def asset_entry(
    *,
    size_bytes: int = 17,
    sha256: str = "abc123",
    mtime: float | None = None,
    href: str = "",
) -> dict:
    """Build an asset metadata dict for versions.json."""
    entry: dict = {
        "sha256": sha256,
        "size_bytes": size_bytes,
        "href": href,
    }
    if mtime is not None:
        entry["mtime"] = mtime
    return entry


# ─────────────────────────────────────────────────────────────────────────────
# TestStatusLibrary - core get_catalog_status() behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestStatusLibrary:
    """Tests for the status library functions."""

    @pytest.mark.unit
    def test_empty_catalog_returns_empty_status(self, tmp_path: Path) -> None:
        """A catalog with no collections returns an empty status result."""
        make_catalog(tmp_path)

        result = get_catalog_status(tmp_path)

        assert result.untracked == []
        assert result.modified == []
        assert result.deleted == []
        assert result.is_clean()

    @pytest.mark.unit
    def test_untracked_file_detected_in_item_dir(self, tmp_path: Path) -> None:
        """Files in item subdirectories but not in versions.json are untracked."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        # Create item subdir with a file - no versions.json yet
        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        (item_dir / "data.parquet").write_bytes(b"fake parquet data")

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].collection_id == "demographics"
        assert result.untracked[0].item_id == "census-2020"
        assert result.untracked[0].filename == "data.parquet"
        assert not result.is_clean()

    @pytest.mark.unit
    def test_untracked_non_geo_file_detected(self, tmp_path: Path) -> None:
        """Non-geospatial files (e.g., .txt, .json) in item dirs are also tracked."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        (item_dir / "notes.txt").write_text("some notes")
        (item_dir / "schema.json").write_text("{}")

        result = get_catalog_status(tmp_path)

        filenames = [f.filename for f in result.untracked]
        assert "notes.txt" in filenames
        assert "schema.json" in filenames
        assert len(result.untracked) == 2

    @pytest.mark.unit
    def test_ignored_files_excluded(self, tmp_path: Path) -> None:
        """Files in IGNORED_FILES set are never reported as untracked."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        # Write all ignored files alongside a real file
        for ignored in IGNORED_FILES:
            (item_dir / ignored).write_bytes(b"")
        (item_dir / "data.parquet").write_bytes(b"real data")

        result = get_catalog_status(tmp_path)

        filenames = [f.filename for f in result.untracked]
        for ignored in IGNORED_FILES:
            assert ignored not in filenames, f"{ignored} should be filtered out"
        assert "data.parquet" in filenames

    @pytest.mark.unit
    def test_tracked_file_not_in_results(self, tmp_path: Path) -> None:
        """Files that are tracked and unchanged don't appear in status."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        data_file = item_dir / "data.parquet"
        data_file.write_bytes(b"fake parquet data")

        file_stat = data_file.stat()
        make_versions_json(
            col_dir,
            {
                "census-2020/data.parquet": asset_entry(
                    size_bytes=file_stat.st_size,
                    sha256="abc123",
                    mtime=file_stat.st_mtime,
                    href="demographics/census-2020/data.parquet",
                )
            },
        )

        result = get_catalog_status(tmp_path)

        assert result.untracked == []
        assert result.modified == []
        assert result.deleted == []
        assert result.is_clean()

    @pytest.mark.unit
    def test_modified_file_detected(self, tmp_path: Path) -> None:
        """Files with changed mtime/size are detected as modified."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        data_file = item_dir / "data.parquet"
        data_file.write_bytes(b"fake parquet data")

        # versions.json records different size/mtime -> file is modified
        make_versions_json(
            col_dir,
            {
                "census-2020/data.parquet": asset_entry(
                    size_bytes=9999,
                    sha256="different_hash",
                    mtime=0.0,
                    href="demographics/census-2020/data.parquet",
                )
            },
        )

        result = get_catalog_status(tmp_path)

        assert len(result.modified) == 1
        assert result.modified[0].collection_id == "demographics"
        assert result.modified[0].item_id == "census-2020"
        assert result.modified[0].filename == "data.parquet"
        assert not result.is_clean()

    @pytest.mark.unit
    def test_deleted_file_detected(self, tmp_path: Path) -> None:
        """Files in versions.json but missing from disk are deleted."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        # versions.json references a file that doesn't exist on disk
        make_versions_json(
            col_dir,
            {
                "census-2020/deleted_file.parquet": asset_entry(
                    href="demographics/census-2020/deleted_file.parquet"
                )
            },
        )

        result = get_catalog_status(tmp_path)

        assert len(result.deleted) == 1
        assert result.deleted[0].collection_id == "demographics"
        assert result.deleted[0].item_id == "census-2020"
        assert result.deleted[0].filename == "deleted_file.parquet"
        assert not result.is_clean()

    @pytest.mark.unit
    def test_multiple_items_in_collection(self, tmp_path: Path) -> None:
        """Files in multiple item subdirectories are all scanned."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        # Two item directories
        for item_id in ["census-2020", "census-2021"]:
            item_dir = col_dir / item_id
            item_dir.mkdir()
            (item_dir / "data.parquet").write_bytes(b"data")

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 2
        item_ids = {f.item_id for f in result.untracked}
        assert item_ids == {"census-2020", "census-2021"}

    @pytest.mark.unit
    def test_multiple_collections(self, tmp_path: Path) -> None:
        """Status aggregates across multiple collections."""
        make_catalog(tmp_path, ["demographics", "imagery"])

        # Collection 1: demographics with untracked file in item dir
        col1 = tmp_path / "demographics"
        make_collection(col1)
        item1 = col1 / "census-2020"
        item1.mkdir()
        (item1 / "data.parquet").write_bytes(b"data")

        # Collection 2: imagery with deleted file
        col2 = tmp_path / "imagery"
        make_collection(col2)
        make_versions_json(
            col2,
            {"satellite-2024/image.tif": asset_entry(href="imagery/satellite-2024/image.tif")},
        )

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].collection_id == "demographics"
        assert len(result.deleted) == 1
        assert result.deleted[0].collection_id == "imagery"

    @pytest.mark.unit
    def test_collection_json_itself_excluded(self, tmp_path: Path) -> None:
        """collection.json at the collection root is not reported as an asset."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        # No item directories -> nothing to report
        result = get_catalog_status(tmp_path)
        assert result.is_clean()

    @pytest.mark.unit
    def test_versions_json_at_collection_root_not_item_portolan(self, tmp_path: Path) -> None:
        """versions.json is read from collection root, not .portolan/."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        data_file = item_dir / "data.parquet"
        data_file.write_bytes(b"data")
        file_stat = data_file.stat()

        # Put versions.json at WRONG location (.portolan/) - should be ignored
        portolan_dir = col_dir / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-15T10:30:00Z",
                            "breaking": False,
                            "assets": {
                                "census-2020/data.parquet": asset_entry(
                                    size_bytes=file_stat.st_size,
                                    mtime=file_stat.st_mtime,
                                    href="demographics/census-2020/data.parquet",
                                )
                            },
                            "changes": ["census-2020/data.parquet"],
                        }
                    ],
                }
            )
        )

        # File should still show as untracked (wrong location not read)
        result = get_catalog_status(tmp_path)
        assert len(result.untracked) == 1

        # Now put versions.json at CORRECT location (collection root)
        make_versions_json(
            col_dir,
            {
                "census-2020/data.parquet": asset_entry(
                    size_bytes=file_stat.st_size,
                    mtime=file_stat.st_mtime,
                    href="demographics/census-2020/data.parquet",
                )
            },
        )

        result2 = get_catalog_status(tmp_path)
        assert result2.is_clean()

    @pytest.mark.unit
    def test_hidden_directories_skipped(self, tmp_path: Path) -> None:
        """Hidden directories (e.g. .portolan, .git) are not treated as collections."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        # Create a hidden directory alongside - it should not be treated as item dir
        hidden = col_dir / ".portolan"
        hidden.mkdir()
        (hidden / "config.yaml").write_text("key: value")

        result = get_catalog_status(tmp_path)
        assert result.is_clean()

    @pytest.mark.unit
    def test_no_catalog_raises_error(self, tmp_path: Path) -> None:
        """get_catalog_status raises error when no catalog.json exists."""
        with pytest.raises(FileNotFoundError, match="catalog.json not found"):
            get_catalog_status(tmp_path)

    @pytest.mark.unit
    def test_corrupt_versions_json_treated_as_untracked(self, tmp_path: Path) -> None:
        """If versions.json is corrupt, all files in item dirs are treated as untracked."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        (item_dir / "data.parquet").write_bytes(b"data")

        # Write invalid JSON
        (col_dir / "versions.json").write_text("{ invalid json }")

        result = get_catalog_status(tmp_path)
        assert len(result.untracked) == 1

    @pytest.mark.unit
    def test_multiple_files_per_item(self, tmp_path: Path) -> None:
        """Multiple files in one item directory are all tracked."""
        make_catalog(tmp_path, ["imagery"])
        col_dir = tmp_path / "imagery"
        make_collection(col_dir)

        item_dir = col_dir / "satellite-2024"
        item_dir.mkdir()
        (item_dir / "image.tif").write_bytes(b"tif data")
        (item_dir / "thumbnail.png").write_bytes(b"png data")
        (item_dir / "metadata.json").write_text("{}")

        result = get_catalog_status(tmp_path)
        assert len(result.untracked) == 3
        filenames = {f.filename for f in result.untracked}
        assert filenames == {"image.tif", "thumbnail.png", "metadata.json"}

    @pytest.mark.unit
    def test_item_json_excluded_from_results(self, tmp_path: Path) -> None:
        """item.json within an item directory is not reported as an asset to track."""
        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)

        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        # item.json is STAC metadata, not a user asset
        (item_dir / "item.json").write_text(json.dumps({"type": "Feature"}))
        (item_dir / "data.parquet").write_bytes(b"data")

        result = get_catalog_status(tmp_path)

        filenames = [f.filename for f in result.untracked]
        # item.json is a STAC metadata file - it should NOT appear as untracked asset
        assert "item.json" not in filenames
        assert "data.parquet" in filenames


# ─────────────────────────────────────────────────────────────────────────────
# TestStatusCLI - CLI command behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestStatusCLI:
    """Tests for the status CLI command."""

    @pytest.mark.unit
    def test_status_clean_catalog(self, tmp_path: Path) -> None:
        """Status command shows clean message when nothing to report."""
        import os

        make_catalog(tmp_path)

        runner = CliRunner()
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Nothing to commit, working tree clean" in result.output

    @pytest.mark.unit
    def test_status_shows_untracked(self, tmp_path: Path) -> None:
        """Status command shows untracked files with # prefix."""
        import os

        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)
        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        (item_dir / "data.parquet").write_bytes(b"data")

        runner = CliRunner()
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Untracked:" in result.output
        assert "demographics/census-2020/data.parquet" in result.output

    @pytest.mark.unit
    def test_status_shows_non_geo_untracked(self, tmp_path: Path) -> None:
        """Status command shows non-geospatial files as untracked too."""
        import os

        make_catalog(tmp_path, ["demographics"])
        col_dir = tmp_path / "demographics"
        make_collection(col_dir)
        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        (item_dir / "notes.txt").write_text("some notes")

        runner = CliRunner()
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "demographics/census-2020/notes.txt" in result.output

    @pytest.mark.unit
    def test_status_shows_modified(self, tmp_path: Path) -> None:
        """Status command shows modified files."""
        import os

        make_catalog(tmp_path, ["imagery"])
        col_dir = tmp_path / "imagery"
        make_collection(col_dir)
        item_dir = col_dir / "satellite-2024"
        item_dir.mkdir()
        data_file = item_dir / "image.tif"
        data_file.write_bytes(b"modified tif data")

        make_versions_json(
            col_dir,
            {
                "satellite-2024/image.tif": asset_entry(
                    size_bytes=9999,
                    sha256="old_hash",
                    mtime=0.0,
                    href="imagery/satellite-2024/image.tif",
                )
            },
        )

        runner = CliRunner()
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Modified:" in result.output
        assert "imagery/satellite-2024/image.tif" in result.output

    @pytest.mark.unit
    def test_status_no_catalog_error(self, tmp_path: Path) -> None:
        """Status command exits with error when no catalog found."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["status"], catch_exceptions=False)

        assert result.exit_code == 1
        assert "catalog.json not found" in result.output or "No catalog found" in result.output


# ─────────────────────────────────────────────────────────────────────────────
# TestFileStatus - dataclass behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestFileStatus:
    """Tests for the FileStatus dataclass."""

    @pytest.mark.unit
    def test_file_status_path_property(self) -> None:
        """FileStatus.path returns collection_id/item_id/filename."""
        fs = FileStatus(
            collection_id="demographics",
            item_id="census-2020",
            filename="data.parquet",
        )
        assert fs.path == "demographics/census-2020/data.parquet"

    @pytest.mark.unit
    def test_file_status_equality(self) -> None:
        """FileStatus instances with same values are equal."""
        fs1 = FileStatus(
            collection_id="demographics", item_id="census-2020", filename="data.parquet"
        )
        fs2 = FileStatus(
            collection_id="demographics", item_id="census-2020", filename="data.parquet"
        )
        assert fs1 == fs2

    @pytest.mark.unit
    def test_file_status_path_includes_item_id(self) -> None:
        """path includes all three components."""
        fs = FileStatus(collection_id="col", item_id="item", filename="file.txt")
        parts = fs.path.split("/")
        assert parts == ["col", "item", "file.txt"]


# ─────────────────────────────────────────────────────────────────────────────
# TestStatusResult - dataclass behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestStatusResult:
    """Tests for the StatusResult dataclass."""

    @pytest.mark.unit
    def test_is_clean_all_empty(self) -> None:
        """is_clean returns True when all lists are empty."""
        result = StatusResult(untracked=[], modified=[], deleted=[])
        assert result.is_clean()

    @pytest.mark.unit
    def test_is_clean_with_untracked(self) -> None:
        """is_clean returns False when untracked files exist."""
        result = StatusResult(
            untracked=[FileStatus("col", "item", "file.parquet")],
            modified=[],
            deleted=[],
        )
        assert not result.is_clean()

    @pytest.mark.unit
    def test_is_clean_with_modified(self) -> None:
        """is_clean returns False when modified files exist."""
        result = StatusResult(
            untracked=[],
            modified=[FileStatus("col", "item", "file.parquet")],
            deleted=[],
        )
        assert not result.is_clean()

    @pytest.mark.unit
    def test_is_clean_with_deleted(self) -> None:
        """is_clean returns False when deleted files exist."""
        result = StatusResult(
            untracked=[],
            modified=[],
            deleted=[FileStatus("col", "item", "file.parquet")],
        )
        assert not result.is_clean()


# ─────────────────────────────────────────────────────────────────────────────
# TestIgnoredFiles - IGNORED_FILES constant behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestIgnoredFiles:
    """Tests for IGNORED_FILES default set."""

    @pytest.mark.unit
    def test_ignored_files_is_frozenset(self) -> None:
        """IGNORED_FILES should be a frozenset."""
        assert isinstance(IGNORED_FILES, frozenset)

    @pytest.mark.unit
    def test_ds_store_ignored(self) -> None:
        """.DS_Store should be in IGNORED_FILES."""
        assert ".DS_Store" in IGNORED_FILES

    @pytest.mark.unit
    def test_thumbs_db_ignored(self) -> None:
        """Thumbs.db should be in IGNORED_FILES."""
        assert "Thumbs.db" in IGNORED_FILES

    @pytest.mark.unit
    def test_gitkeep_ignored(self) -> None:
        """.gitkeep should be in IGNORED_FILES."""
        assert ".gitkeep" in IGNORED_FILES
