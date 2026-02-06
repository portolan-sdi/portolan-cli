"""Unit tests for versions.json read/write/update operations.

Tests the versions module which manages the versions.json file that serves as
the single source of truth for dataset versioning (ADR-0005).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.versions import (
    Asset,
    Version,
    VersionsFile,
    add_version,
    read_versions,
    write_versions,
)

if TYPE_CHECKING:
    pass


class TestVersionsFileModel:
    """Tests for the VersionsFile data model."""

    @pytest.mark.unit
    def test_create_empty_versions_file(self) -> None:
        """VersionsFile can be created with minimal required fields."""
        vf = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        assert vf.spec_version == "1.0.0"
        assert vf.current_version is None
        assert vf.versions == []

    @pytest.mark.unit
    def test_create_versions_file_with_version(self) -> None:
        """VersionsFile can include version entries."""
        asset = Asset(
            sha256="abc123",
            size_bytes=1024,
            href="data.parquet",
        )
        version = Version(
            version="1.0.0",
            created=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            breaking=False,
            assets={"data.parquet": asset},
            changes=["data.parquet"],
        )
        vf = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[version],
        )
        assert vf.current_version == "1.0.0"
        assert len(vf.versions) == 1
        assert vf.versions[0].assets["data.parquet"].sha256 == "abc123"


class TestReadVersions:
    """Tests for reading versions.json from disk."""

    @pytest.mark.unit
    def test_read_valid_versions_file(self, tmp_path: Path) -> None:
        """read_versions parses a valid versions.json file."""
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-15T10:30:00Z",
                    "breaking": False,
                    "assets": {
                        "data.parquet": {
                            "sha256": "abc123",
                            "size_bytes": 1024,
                            "href": "data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                }
            ],
        }
        versions_path = tmp_path / "versions.json"
        versions_path.write_text(json.dumps(versions_data))

        vf = read_versions(versions_path)

        assert vf.spec_version == "1.0.0"
        assert vf.current_version == "1.0.0"
        assert len(vf.versions) == 1
        assert vf.versions[0].version == "1.0.0"
        assert vf.versions[0].assets["data.parquet"].sha256 == "abc123"

    @pytest.mark.unit
    def test_read_empty_versions_file(self, tmp_path: Path) -> None:
        """read_versions handles a versions file with no versions."""
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": None,
            "versions": [],
        }
        versions_path = tmp_path / "versions.json"
        versions_path.write_text(json.dumps(versions_data))

        vf = read_versions(versions_path)

        assert vf.current_version is None
        assert vf.versions == []

    @pytest.mark.unit
    def test_read_nonexistent_file_raises(self, tmp_path: Path) -> None:
        """read_versions raises FileNotFoundError for missing files."""
        versions_path = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            read_versions(versions_path)

    @pytest.mark.unit
    def test_read_invalid_json_raises(self, tmp_path: Path) -> None:
        """read_versions raises ValueError for malformed JSON."""
        versions_path = tmp_path / "versions.json"
        versions_path.write_text("{not valid json")

        with pytest.raises(ValueError, match="Invalid JSON"):
            read_versions(versions_path)

    @pytest.mark.unit
    def test_read_invalid_schema_raises(self, tmp_path: Path) -> None:
        """read_versions raises ValueError for JSON that doesn't match schema."""
        versions_path = tmp_path / "versions.json"
        versions_path.write_text(json.dumps({"wrong": "schema"}))

        with pytest.raises(ValueError, match="Invalid versions.json schema"):
            read_versions(versions_path)


class TestWriteVersions:
    """Tests for writing versions.json to disk."""

    @pytest.mark.unit
    def test_write_empty_versions_file(self, tmp_path: Path) -> None:
        """write_versions creates a valid JSON file for empty versions."""
        vf = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        versions_path = tmp_path / "versions.json"

        write_versions(versions_path, vf)

        assert versions_path.exists()
        data = json.loads(versions_path.read_text())
        assert data["spec_version"] == "1.0.0"
        assert data["current_version"] is None
        assert data["versions"] == []

    @pytest.mark.unit
    def test_write_versions_file_with_data(self, tmp_path: Path) -> None:
        """write_versions preserves all version data."""
        asset = Asset(sha256="def456", size_bytes=2048, href="raster.tif")
        version = Version(
            version="2.0.0",
            created=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            breaking=True,
            assets={"raster.tif": asset},
            changes=["raster.tif"],
        )
        vf = VersionsFile(
            spec_version="1.0.0",
            current_version="2.0.0",
            versions=[version],
        )
        versions_path = tmp_path / "versions.json"

        write_versions(versions_path, vf)

        data = json.loads(versions_path.read_text())
        assert data["current_version"] == "2.0.0"
        assert len(data["versions"]) == 1
        assert data["versions"][0]["breaking"] is True
        assert data["versions"][0]["assets"]["raster.tif"]["sha256"] == "def456"

    @pytest.mark.unit
    def test_write_roundtrip(self, tmp_path: Path) -> None:
        """write then read produces identical data."""
        asset = Asset(sha256="roundtrip", size_bytes=512, href="test.parquet")
        version = Version(
            version="1.0.0",
            created=datetime(2024, 3, 15, 8, 0, 0, tzinfo=timezone.utc),
            breaking=False,
            assets={"test.parquet": asset},
            changes=["test.parquet"],
        )
        vf = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[version],
        )
        versions_path = tmp_path / "versions.json"

        write_versions(versions_path, vf)
        loaded = read_versions(versions_path)

        assert loaded.spec_version == vf.spec_version
        assert loaded.current_version == vf.current_version
        assert len(loaded.versions) == len(vf.versions)
        assert loaded.versions[0].version == vf.versions[0].version
        assert loaded.versions[0].assets["test.parquet"].sha256 == "roundtrip"

    @pytest.mark.unit
    def test_write_creates_parent_directories(self, tmp_path: Path) -> None:
        """write_versions creates parent directories if they don't exist."""
        vf = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        versions_path = tmp_path / "nested" / "dirs" / "versions.json"

        write_versions(versions_path, vf)

        assert versions_path.exists()


class TestAddVersion:
    """Tests for adding new versions to VersionsFile."""

    @pytest.mark.unit
    def test_add_first_version(self) -> None:
        """add_version creates first version in empty VersionsFile."""
        vf = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        assets = {"data.parquet": Asset(sha256="first", size_bytes=1000, href="data.parquet")}

        updated = add_version(
            vf,
            version="1.0.0",
            assets=assets,
            breaking=False,
        )

        assert updated.current_version == "1.0.0"
        assert len(updated.versions) == 1
        assert updated.versions[0].version == "1.0.0"
        assert updated.versions[0].changes == ["data.parquet"]

    @pytest.mark.unit
    def test_add_subsequent_version(self) -> None:
        """add_version appends to existing versions."""
        existing_asset = Asset(sha256="old", size_bytes=500, href="old.parquet")
        existing_version = Version(
            version="1.0.0",
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            breaking=False,
            assets={"old.parquet": existing_asset},
            changes=["old.parquet"],
        )
        vf = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[existing_version],
        )
        new_assets = {"new.parquet": Asset(sha256="new", size_bytes=2000, href="new.parquet")}

        updated = add_version(vf, version="2.0.0", assets=new_assets, breaking=True)

        assert updated.current_version == "2.0.0"
        assert len(updated.versions) == 2
        assert updated.versions[1].version == "2.0.0"
        assert updated.versions[1].breaking is True

    @pytest.mark.unit
    def test_add_version_sets_created_timestamp(self) -> None:
        """add_version sets created timestamp to current UTC time."""
        vf = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        assets = {"f.parquet": Asset(sha256="x", size_bytes=1, href="f.parquet")}

        before = datetime.now(timezone.utc)
        updated = add_version(vf, version="1.0.0", assets=assets, breaking=False)
        after = datetime.now(timezone.utc)

        created = updated.versions[0].created
        assert before <= created <= after

    @pytest.mark.unit
    def test_add_version_computes_changes(self) -> None:
        """add_version correctly identifies changed files."""
        # Start with one asset
        old_asset = Asset(sha256="same", size_bytes=100, href="unchanged.parquet")
        existing_version = Version(
            version="1.0.0",
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            breaking=False,
            assets={"unchanged.parquet": old_asset},
            changes=["unchanged.parquet"],
        )
        vf = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[existing_version],
        )

        # Add version with same file (unchanged) + new file
        new_assets = {
            "unchanged.parquet": Asset(sha256="same", size_bytes=100, href="unchanged.parquet"),
            "new.parquet": Asset(sha256="brand_new", size_bytes=200, href="new.parquet"),
        }
        updated = add_version(vf, version="1.1.0", assets=new_assets, breaking=False)

        # Only new.parquet should be in changes (sha256 differs or file is new)
        assert "new.parquet" in updated.versions[1].changes
        assert "unchanged.parquet" not in updated.versions[1].changes

    @pytest.mark.unit
    def test_add_version_detects_modified_file(self) -> None:
        """add_version detects when existing file has new checksum."""
        old_asset = Asset(sha256="old_hash", size_bytes=100, href="data.parquet")
        existing_version = Version(
            version="1.0.0",
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            breaking=False,
            assets={"data.parquet": old_asset},
            changes=["data.parquet"],
        )
        vf = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[existing_version],
        )

        # Same filename, different checksum
        modified_assets = {
            "data.parquet": Asset(sha256="new_hash", size_bytes=150, href="data.parquet")
        }
        updated = add_version(vf, version="1.0.1", assets=modified_assets, breaking=False)

        assert "data.parquet" in updated.versions[1].changes

    @pytest.mark.unit
    def test_add_version_immutable(self) -> None:
        """add_version returns new VersionsFile, doesn't mutate original."""
        vf = VersionsFile(spec_version="1.0.0", current_version=None, versions=[])
        assets = {"f.parquet": Asset(sha256="x", size_bytes=1, href="f.parquet")}

        updated = add_version(vf, version="1.0.0", assets=assets, breaking=False)

        assert vf.current_version is None
        assert len(vf.versions) == 0
        assert updated.current_version == "1.0.0"
        assert len(updated.versions) == 1
