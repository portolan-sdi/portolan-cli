"""Tests for FileGDB directory handling in status command.

Issue #174: FileGDB directories should be treated as single assets,
not recursed into with internal files (.gdbtable, .gdbtablx, .spx, etc.)
reported as individual untracked files.

This is the status counterpart to scan_detect.py's FileGDB handling.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from portolan_cli.dataset import compute_dir_checksum
from portolan_cli.status import get_catalog_status
from portolan_cli.versions import Asset, VersionsFile, add_version, write_versions

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


def track_filegdb_in_versions(
    col_dir: Path,
    item_id: str,
    gdb_path: Path,
    collection_id: str,
) -> None:
    """Write a versions.json that records gdb_path as a tracked asset.

    Uses compute_dir_checksum to compute the current fingerprint so that
    subsequent calls to get_catalog_status will find the asset in-sync.

    Args:
        col_dir: Collection directory (where versions.json lives).
        item_id: Item ID (subdirectory name).
        gdb_path: Absolute path to the .gdb directory on disk.
        collection_id: Collection ID string.
    """
    versions_path = col_dir / "versions.json"
    checksum = compute_dir_checksum(gdb_path)
    stat = gdb_path.stat()
    asset_key = f"{item_id}/{gdb_path.name}"
    href = f"{collection_id}/{item_id}/{gdb_path.name}"
    assets: dict[str, Asset] = {
        asset_key: Asset(
            sha256=checksum,
            size_bytes=0,  # Directory: size not meaningful
            href=href,
            mtime=stat.st_mtime,
        )
    }
    empty = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
    versioned = add_version(empty, version="1.0.0", assets=assets, breaking=False)
    write_versions(versions_path, versioned)


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

    def test_gdb_directory_without_internal_structure_treated_as_regular_dir(
        self, tmp_path: Path
    ) -> None:
        """.gdb directory that lacks FileGDB internal structure is not a real FileGDB.

        is_filegdb() requires either .gdbtable files or a 'gdb' marker file.
        A .gdb directory containing only arbitrary files (no internal structure)
        should be recursed into normally. The files inside should appear in status.

        Note: this is NOT an "empty" .gdb directory — it contains files, just not
        the FileGDB-specific structure that is_filegdb() requires.
        """
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "data", "v1")

        # Create .gdb directory WITHOUT valid FileGDB internal structure
        fake_gdb = item_dir / "fake.gdb"
        fake_gdb.mkdir()
        # Add files that are NOT FileGDB internal files (no .gdbtable, no 'gdb' marker)
        (fake_gdb / "notes.txt").write_text("Not a real FileGDB")
        (fake_gdb / "data.csv").write_text("a,b,c\n1,2,3")

        result = get_catalog_status(tmp_path)

        # Files inside the invalid .gdb should be found (it's not a real FileGDB)
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


@pytest.mark.unit
class TestFileGDBTrackedModified:
    """Tests that a tracked FileGDB appearing modified is detected correctly.

    These tests verify Bug 1 fix: is_current() must not crash when called with
    a directory path, and Bug 2 fix: tracked FileGDB dirs must appear in
    versions.json so the modified/deleted detection paths are reachable.
    """

    def test_tracked_filegdb_unchanged_is_clean(self, tmp_path: Path) -> None:
        """A tracked FileGDB with unchanged contents should not appear in any list."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)

        # Record current fingerprint in versions.json
        track_filegdb_in_versions(col_dir, "latest", gdb_path, "ocha")

        result = get_catalog_status(tmp_path)

        assert result.is_clean(), (
            f"Expected clean status for unchanged tracked FileGDB, "
            f"got untracked={result.untracked}, modified={result.modified}, "
            f"deleted={result.deleted}"
        )

    def test_tracked_filegdb_with_added_internal_file_shows_as_modified(
        self, tmp_path: Path
    ) -> None:
        """A tracked FileGDB that has a new file added inside it shows as modified."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)

        # Record the fingerprint BEFORE adding a new internal file
        track_filegdb_in_versions(col_dir, "latest", gdb_path, "ocha")

        # Add a new file inside the FileGDB (simulates ArcGIS adding a new layer)
        # Sleep briefly to ensure mtime changes (FAT32 has 2s resolution, but
        # Linux tmpfs is fine with even 1ms. Use a direct stat manipulation.)
        time.sleep(0.01)
        (gdb_path / "a00000003.gdbtable").write_bytes(b"\xff\xfe")

        result = get_catalog_status(tmp_path)

        assert len(result.modified) == 1, f"Expected 1 modified entry, got: {result.modified}"
        assert result.modified[0].filename == "boundaries.gdb"
        assert result.modified[0].collection_id == "ocha"
        assert result.modified[0].item_id == "latest"
        assert len(result.untracked) == 0
        assert len(result.deleted) == 0

    def test_is_current_does_not_crash_on_filegdb_directory(self, tmp_path: Path) -> None:
        """is_current() must not raise ValueError when called with a FileGDB directory.

        This is a regression test for Bug 1: compute_checksum() raises ValueError
        for non-regular-file paths. The fix routes directory paths through
        compute_dir_checksum() instead.
        """
        from portolan_cli.dataset import is_current

        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)

        versions_path = col_dir / "versions.json"
        track_filegdb_in_versions(col_dir, "latest", gdb_path, "ocha")

        # This must not raise ValueError ("Not a regular file")
        result = is_current(gdb_path, versions_path, asset_key="latest/boundaries.gdb")
        assert result is True  # Unchanged — fingerprint matches


@pytest.mark.unit
class TestFileGDBTrackedDeleted:
    """Tests that a deleted (tracked but missing) FileGDB is detected correctly.

    This verifies Bug 3 fix: FileGDB dirs written to versions.json by _scan_item_assets
    must appear in the deleted list when they are removed from disk.
    """

    def test_tracked_filegdb_removed_from_disk_shows_as_deleted(self, tmp_path: Path) -> None:
        """A FileGDB tracked in versions.json but deleted from disk shows as deleted."""
        import shutil

        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)

        # Track it first
        track_filegdb_in_versions(col_dir, "latest", gdb_path, "ocha")

        # Now remove the FileGDB from disk
        shutil.rmtree(gdb_path)

        result = get_catalog_status(tmp_path)

        assert len(result.deleted) == 1, f"Expected 1 deleted entry, got: {result.deleted}"
        assert result.deleted[0].filename == "boundaries.gdb"
        assert result.deleted[0].collection_id == "ocha"
        assert result.deleted[0].item_id == "latest"
        assert len(result.untracked) == 0
        assert len(result.modified) == 0

    def test_tracked_filegdb_and_regular_file_both_deleted(self, tmp_path: Path) -> None:
        """Both a tracked FileGDB and a regular file deleted from disk both appear."""
        import shutil

        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)
        txt_path = item_dir / "readme.txt"
        txt_path.write_text("README")

        # Track the FileGDB manually, and track the txt file too via a second version
        checksum_gdb = compute_dir_checksum(gdb_path)
        stat_gdb = gdb_path.stat()

        import hashlib

        checksum_txt = hashlib.sha256(txt_path.read_bytes()).hexdigest()
        stat_txt = txt_path.stat()

        from portolan_cli.versions import Asset, VersionsFile, add_version, write_versions

        assets: dict[str, Asset] = {
            "latest/boundaries.gdb": Asset(
                sha256=checksum_gdb,
                size_bytes=0,
                href="ocha/latest/boundaries.gdb",
                mtime=stat_gdb.st_mtime,
            ),
            "latest/readme.txt": Asset(
                sha256=checksum_txt,
                size_bytes=stat_txt.st_size,
                href="ocha/latest/readme.txt",
                mtime=stat_txt.st_mtime,
            ),
        }
        empty = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        versioned = add_version(empty, version="1.0.0", assets=assets, breaking=False)
        write_versions(col_dir / "versions.json", versioned)

        # Delete both from disk
        shutil.rmtree(gdb_path)
        txt_path.unlink()

        result = get_catalog_status(tmp_path)

        deleted_filenames = {f.filename for f in result.deleted}
        assert "boundaries.gdb" in deleted_filenames
        assert "readme.txt" in deleted_filenames
        assert len(result.deleted) == 2
        assert len(result.untracked) == 0
        assert len(result.modified) == 0


@pytest.mark.unit
class TestGdbZipFile:
    """.gdb.zip archives are regular files, not directory containers.

    A .gdb.zip should be tracked like any other file (via its checksum),
    NOT recursed into or treated as a FileGDB directory.
    """

    def test_gdb_zip_reported_as_single_untracked_file(self, tmp_path: Path) -> None:
        """.gdb.zip appears as a single untracked file, not a container."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "data", "v1")

        # Create a .gdb.zip file (a regular file, not a directory)
        zip_path = item_dir / "boundaries.gdb.zip"
        zip_path.write_bytes(b"PK\x03\x04")  # Minimal ZIP magic bytes

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].filename == "boundaries.gdb.zip"
        assert result.untracked[0].collection_id == "data"
        assert result.untracked[0].item_id == "v1"

    def test_gdb_zip_and_gdb_dir_coexist(self, tmp_path: Path) -> None:
        """.gdb.zip and .gdb directory can coexist and both appear correctly."""
        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "data", "v1")

        # Create a .gdb directory (FileGDB)
        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)

        # Create a .gdb.zip file (archived FileGDB)
        zip_path = item_dir / "boundaries.gdb.zip"
        zip_path.write_bytes(b"PK\x03\x04")

        result = get_catalog_status(tmp_path)

        filenames = {f.filename for f in result.untracked}
        assert "boundaries.gdb" in filenames
        assert "boundaries.gdb.zip" in filenames
        assert len(result.untracked) == 2


@pytest.mark.integration
class TestFileGDBRoundTrip:
    """Round-trip integration tests: _scan_item_assets writes FileGDB to versions.json,
    then get_catalog_status reads it back and reports the correct state.

    These tests exercise the complete pipeline without calling add_dataset
    (which requires geo data processing). Instead they directly call
    _scan_item_assets and _update_versions to simulate what add_dataset does.
    """

    def test_scan_item_assets_includes_filegdb(self, tmp_path: Path) -> None:
        """_scan_item_assets must include a FileGDB directory as an asset."""
        from portolan_cli.dataset import _scan_item_assets  # type: ignore[attr-defined]

        item_dir = tmp_path / "item"
        item_dir.mkdir()

        # Create a FileGDB inside the item directory
        gdb_path = item_dir / "data.gdb"
        make_filegdb(gdb_path)

        # Create a dummy primary file (required by _scan_item_assets signature)
        primary = item_dir / "dummy.parquet"
        primary.write_bytes(b"\x00")

        # collection_dir is the parent for this test setup
        stac_assets, asset_files, asset_paths = _scan_item_assets(
            item_dir, "item", primary, collection_dir=tmp_path
        )

        # The FileGDB should appear in all three return values
        assert "data.gdb" in asset_files, (
            f"Expected 'data.gdb' in asset_files, got: {list(asset_files.keys())}"
        )
        assert any("data.gdb" in p for p in asset_paths)

    def test_filegdb_tracked_via_scan_then_status_reports_clean(self, tmp_path: Path) -> None:
        """Full round-trip: FileGDB tracked by _scan_item_assets shows as clean in status."""
        from portolan_cli.dataset import _scan_item_assets  # type: ignore[attr-defined]

        make_catalog(tmp_path)
        col_dir, item_dir = make_collection(tmp_path, "ocha", "latest")

        # Create a FileGDB inside the item directory
        gdb_path = item_dir / "boundaries.gdb"
        make_filegdb(gdb_path)

        # Create a dummy primary file
        primary = item_dir / "dummy.parquet"
        primary.write_bytes(b"\x00")

        # Simulate what add_dataset does: scan assets and write versions.json
        _stac_assets, asset_files, _asset_paths = _scan_item_assets(
            item_dir, "latest", primary, collection_dir=col_dir
        )

        # Build versions.json using the same asset structure as _update_versions()
        collection_id = "ocha"
        item_id = "latest"
        assets: dict[str, Asset] = {}
        for filename, (file_path, file_checksum) in asset_files.items():
            href = f"{collection_id}/{item_id}/{filename}"
            asset_key = f"{item_id}/{filename}"
            stat = file_path.stat()
            size_bytes = stat.st_size if file_path.is_file() else 0
            assets[asset_key] = Asset(
                sha256=file_checksum,
                size_bytes=size_bytes,
                href=href,
                mtime=stat.st_mtime,
            )

        versions_path = col_dir / "versions.json"
        empty = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        versioned = add_version(empty, version="1.0.0", assets=assets, breaking=False)
        write_versions(versions_path, versioned)

        # Now check status: should be clean (FileGDB fingerprint matches)
        result = get_catalog_status(tmp_path)

        assert result.is_clean(), (
            f"Expected clean after add round-trip, got: "
            f"untracked={[f.path for f in result.untracked]}, "
            f"modified={[f.path for f in result.modified]}, "
            f"deleted={[f.path for f in result.deleted]}"
        )
