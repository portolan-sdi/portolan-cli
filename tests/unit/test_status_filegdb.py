"""Tests for FileGDB directory handling in status command.

Issue #174: FileGDB directories should be treated as single assets,
not recursed into with internal files (.gdbtable, .gdbtablx, .spx, etc.)
reported as individual untracked files.

This is the status counterpart to scan_detect.py's FileGDB handling.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.status import get_catalog_status

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def make_catalog(tmp_path: Path) -> None:
    """Write a minimal managed catalog to tmp_path (per ADR-0023 and ADR-0029)."""
    # Create .portolan sentinel files (per ADR-0029)
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")

    # Create catalog.json
    catalog = {
        "type": "Catalog",
        "id": "test-catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog, indent=2))


def make_filegdb(gdb_path: Path, num_tables: int = 2) -> None:
    """Create a minimal FileGDB directory structure.

    Args:
        gdb_path: Path where the .gdb directory should be created.
        num_tables: Number of .gdbtable files to create.
    """
    gdb_path.mkdir(parents=True, exist_ok=True)
    # Create the 'gdb' marker file
    (gdb_path / "gdb").write_bytes(b"\x00")
    # Create internal .gdbtable files
    for i in range(num_tables):
        (gdb_path / f"a0000000{i + 1}.gdbtable").write_bytes(b"\x00")
        (gdb_path / f"a0000000{i + 1}.gdbtablx").write_bytes(b"\x00")
        (gdb_path / f"a0000000{i + 1}.spx").write_bytes(b"\x00")


def make_collection(tmp_path: Path, collection_id: str, item_id: str) -> tuple[Path, Path]:
    """Create a collection with an item directory.

    Returns:
        Tuple of (collection_dir, item_dir).
    """
    col_dir = tmp_path / collection_id
    col_dir.mkdir(parents=True, exist_ok=True)

    # Create collection.json (initialized collection)
    collection = {
        "type": "Collection",
        "id": collection_id,
        "stac_version": "1.0.0",
        "description": f"Test collection {collection_id}",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
    }
    (col_dir / "collection.json").write_text(json.dumps(collection, indent=2))

    item_dir = col_dir / item_id
    item_dir.mkdir(parents=True, exist_ok=True)

    return col_dir, item_dir


# ─────────────────────────────────────────────────────────────────────────────
# Unit Tests for FileGDB handling in status
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.unit
class TestStatusFileGDBAsContainer:
    """Tests that status treats FileGDB directories as single container assets."""

    def test_filegdb_reported_as_single_untracked_asset(self, tmp_path: Path) -> None:
        """FileGDB directory should appear as one untracked file, not internal files."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        # Create a FileGDB inside the item directory
        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)

        result = get_catalog_status(tmp_path)

        # Should have exactly one untracked entry for the .gdb directory
        assert len(result.untracked) == 1
        untracked = result.untracked[0]
        assert untracked.collection_id == "ocha"
        assert untracked.item_id == "latest"
        assert untracked.filename == "boundaries.gdb"

    def test_filegdb_internal_files_not_listed(self, tmp_path: Path) -> None:
        """Internal FileGDB files (.gdbtable, etc.) should NOT appear in status."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        gdb_path = item_dir / "data.gdb"
        make_filegdb(gdb_path, num_tables=3)

        result = get_catalog_status(tmp_path)

        # No internal file extensions should be in the results
        internal_extensions = {".gdbtable", ".gdbtablx", ".spx", ".gdbindexes"}
        for f in result.untracked:
            assert not any(f.filename.endswith(ext) for ext in internal_extensions), (
                f"Internal FileGDB file should not appear in status: {f.filename}"
            )

    def test_filegdb_with_sibling_files(self, tmp_path: Path) -> None:
        """FileGDB alongside regular files should all be reported correctly."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "boundaries", "v1")

        # Create a FileGDB
        gdb_path = item_dir / "admin.gdb"
        make_filegdb(gdb_path)

        # Create sibling files
        (item_dir / "metadata.json").write_text('{"source": "OCHA"}')
        (item_dir / "readme.txt").write_text("README content")

        result = get_catalog_status(tmp_path)

        # Should have 3 untracked: admin.gdb, metadata.json, readme.txt
        assert len(result.untracked) == 3
        filenames = {f.filename for f in result.untracked}
        assert filenames == {"admin.gdb", "metadata.json", "readme.txt"}

    def test_multiple_filegdb_directories(self, tmp_path: Path) -> None:
        """Multiple FileGDB directories should each appear as single assets."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "census", "2020")

        # Create multiple FileGDB directories
        for name in ["tracts.gdb", "blocks.gdb", "counties.gdb"]:
            gdb_path = item_dir / name
            make_filegdb(gdb_path)

        result = get_catalog_status(tmp_path)

        # Should have exactly 3 untracked entries, one per .gdb
        assert len(result.untracked) == 3
        filenames = {f.filename for f in result.untracked}
        assert filenames == {"tracts.gdb", "blocks.gdb", "counties.gdb"}

    def test_empty_gdb_directory_treated_as_regular_dir(self, tmp_path: Path) -> None:
        """Empty .gdb directory (no .gdbtable files) is NOT a real FileGDB.

        Such directories should be recursed into normally since they're not
        valid FileGDB containers. The internal files should appear in status.
        """
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "data", "v1")

        # Create empty .gdb directory (NOT a valid FileGDB)
        fake_gdb = item_dir / "fake.gdb"
        fake_gdb.mkdir()
        # Add some files inside (not FileGDB structure)
        (fake_gdb / "notes.txt").write_text("Not a real FileGDB")
        (fake_gdb / "data.csv").write_text("a,b,c\n1,2,3")

        result = get_catalog_status(tmp_path)

        # Files inside the fake .gdb should be found (it's not a real FileGDB)
        # Should contain the nested files using path notation
        assert any("notes.txt" in f.filename for f in result.untracked)
        assert any("data.csv" in f.filename for f in result.untracked)

    def test_filegdb_in_nested_hive_partition(self, tmp_path: Path) -> None:
        """FileGDB inside hive-partitioned structure should be handled correctly."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "partitioned", "v1")

        # Create hive partition structure
        partition_dir = item_dir / "year=2024" / "region=US"
        partition_dir.mkdir(parents=True)

        # Create FileGDB inside partition
        gdb_path = partition_dir / "data.gdb"
        make_filegdb(gdb_path)

        # Create a regular file alongside
        (partition_dir / "summary.json").write_text("{}")

        result = get_catalog_status(tmp_path)

        # Should have 2 untracked entries
        assert len(result.untracked) == 2
        filenames = {f.filename for f in result.untracked}
        # The FileGDB should appear with its relative path
        assert "year=2024/region=US/data.gdb" in filenames
        assert "year=2024/region=US/summary.json" in filenames


@pytest.mark.unit
class TestFileGDBLockFiles:
    """Tests for handling FileGDB lock files."""

    def test_filegdb_with_lock_files_reported_once(self, tmp_path: Path) -> None:
        """FileGDB with lock files should still appear as single asset."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "active", "current")

        gdb_path = item_dir / "editing.gdb"
        make_filegdb(gdb_path)
        # Add lock files (from active ArcGIS editing session)
        (gdb_path / "a00000001.lck").write_bytes(b"\x00")
        (gdb_path / "lockfile").write_bytes(b"\x00")

        result = get_catalog_status(tmp_path)

        # Should have exactly one entry for the .gdb
        assert len(result.untracked) == 1
        assert result.untracked[0].filename == "editing.gdb"


@pytest.mark.unit
class TestFileGDBCaseSensitivity:
    """Tests for case-insensitive .gdb extension handling."""

    def test_uppercase_gdb_extension(self, tmp_path: Path) -> None:
        """Uppercase .GDB extension should be treated as FileGDB."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "data", "v1")

        gdb_path = item_dir / "DATA.GDB"
        make_filegdb(gdb_path)

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].filename == "DATA.GDB"

    def test_mixed_case_gdb_extension(self, tmp_path: Path) -> None:
        """Mixed case .GdB extension should be treated as FileGDB."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "data", "v1")

        gdb_path = item_dir / "Mixed.GdB"
        make_filegdb(gdb_path)

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].filename == "Mixed.GdB"


@pytest.mark.unit
class TestFileGDBPathDisplay:
    """Tests for correct path display in status output."""

    def test_filegdb_path_format(self, tmp_path: Path) -> None:
        """FileGDB should be displayed with collection/item/filename.gdb path."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha-global-admin-boundaries", "latest")

        gdb_path = item_dir / "global_admin_boundaries_extended_latest.gdb"
        make_filegdb(gdb_path)

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        fs = result.untracked[0]
        assert fs.collection_id == "ocha-global-admin-boundaries"
        assert fs.item_id == "latest"
        assert fs.filename == "global_admin_boundaries_extended_latest.gdb"
        # Check the path property
        expected_path = (
            "ocha-global-admin-boundaries/latest/global_admin_boundaries_extended_latest.gdb"
        )
        assert fs.path == expected_path
