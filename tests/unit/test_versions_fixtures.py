"""Tests for versions.json test fixtures.

These tests verify that the fixtures are valid and can be parsed by the versions module.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.versions import read_versions

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "metadata" / "versions"


class TestValidVersionsFixtures:
    """Tests for valid versions.json fixtures."""

    @pytest.mark.unit
    def test_versions_v1_parses(self) -> None:
        """versions_v1.json should parse successfully."""
        fixture_path = FIXTURES_DIR / "valid" / "versions_v1.json"
        versions_file = read_versions(fixture_path)

        assert versions_file.spec_version == "1.0.0"
        assert versions_file.current_version == "1.0.0"
        assert len(versions_file.versions) == 1

        # Verify the single version
        v1 = versions_file.versions[0]
        assert v1.version == "1.0.0"
        assert not v1.breaking
        assert "parcels.parquet" in v1.assets
        assert v1.assets["parcels.parquet"].source_mtime == 1705312200.0

    @pytest.mark.unit
    def test_versions_v3_parses(self) -> None:
        """versions_v3.json should parse successfully with version history."""
        fixture_path = FIXTURES_DIR / "valid" / "versions_v3.json"
        versions_file = read_versions(fixture_path)

        assert versions_file.spec_version == "1.0.0"
        assert versions_file.current_version == "1.2.0"
        assert len(versions_file.versions) == 3

        # Verify version progression
        assert versions_file.versions[0].version == "1.0.0"
        assert versions_file.versions[1].version == "1.1.0"
        assert versions_file.versions[2].version == "1.2.0"

        # v1.1.0 added buildings.parquet
        assert "buildings.parquet" in versions_file.versions[1].assets
        assert "buildings.parquet" in versions_file.versions[1].changes

        # v1.2.0 only changed buildings
        assert versions_file.versions[2].changes == ["buildings.parquet"]

    @pytest.mark.unit
    def test_versions_breaking_parses(self) -> None:
        """versions_breaking.json should parse with breaking=true."""
        fixture_path = FIXTURES_DIR / "valid" / "versions_breaking.json"
        versions_file = read_versions(fixture_path)

        assert versions_file.current_version == "2.0.0"
        assert len(versions_file.versions) == 2

        # First version is not breaking
        assert not versions_file.versions[0].breaking

        # Second version is breaking
        assert versions_file.versions[1].breaking
        assert versions_file.versions[1].version == "2.0.0"

    @pytest.mark.unit
    def test_versions_partial_sync_parses(self) -> None:
        """versions_partial_sync.json should parse with mixed sync state."""
        fixture_path = FIXTURES_DIR / "valid" / "versions_partial_sync.json"
        versions_file = read_versions(fixture_path)

        assert versions_file.current_version == "1.1.0"
        assert len(versions_file.versions) == 2

        # Get latest version
        latest = versions_file.versions[-1]
        assert latest.version == "1.1.0"

        # Only roads changed in v1.1.0
        assert latest.changes == ["roads.parquet"]

        # All three assets still present
        assert "roads.parquet" in latest.assets
        assert "rivers.parquet" in latest.assets
        assert "lakes.parquet" in latest.assets

        # Rivers and lakes have same checksum as v1.0.0
        v1 = versions_file.versions[0]
        assert latest.assets["rivers.parquet"].sha256 == v1.assets["rivers.parquet"].sha256
        assert latest.assets["lakes.parquet"].sha256 == v1.assets["lakes.parquet"].sha256


class TestInvalidVersionsFixtures:
    """Tests for invalid versions.json fixtures.

    These fixtures are intentionally malformed to test error handling.
    """

    @pytest.mark.unit
    def test_bad_checksum_is_valid_json(self) -> None:
        """versions_bad_checksum.json should be valid JSON but semantically invalid.

        Note: The versions module doesn't validate checksum length, so this
        will parse. The semantic validation is for downstream consumers.
        """
        fixture_path = FIXTURES_DIR / "invalid" / "versions_bad_checksum.json"

        # Should be valid JSON
        with open(fixture_path) as f:
            data = json.load(f)

        assert "spec_version" in data
        # The bad checksum is only 6 chars, not 64
        assert len(data["versions"][0]["assets"]["data.parquet"]["sha256"]) == 6

    @pytest.mark.unit
    def test_missing_current_is_valid_json(self) -> None:
        """versions_missing_current.json should be valid JSON.

        current_version points to '2.0.0' but only '1.0.0' exists.
        """
        fixture_path = FIXTURES_DIR / "invalid" / "versions_missing_current.json"

        # Should be valid JSON
        with open(fixture_path) as f:
            data = json.load(f)

        assert data["current_version"] == "2.0.0"
        assert all(v["version"] != "2.0.0" for v in data["versions"])

    @pytest.mark.unit
    def test_duplicate_is_valid_json(self) -> None:
        """versions_duplicate.json should be valid JSON with duplicate versions.

        Two entries with version '1.0.0'.
        """
        fixture_path = FIXTURES_DIR / "invalid" / "versions_duplicate.json"

        # Should be valid JSON
        with open(fixture_path) as f:
            data = json.load(f)

        versions = [v["version"] for v in data["versions"]]
        assert versions.count("1.0.0") == 2


class TestFixtureIntegrity:
    """Tests to ensure fixtures maintain their documented properties."""

    @pytest.mark.unit
    def test_all_valid_fixtures_exist(self) -> None:
        """All documented valid fixtures should exist."""
        expected_files = [
            "versions_v1.json",
            "versions_v3.json",
            "versions_breaking.json",
            "versions_partial_sync.json",
        ]

        valid_dir = FIXTURES_DIR / "valid"
        for filename in expected_files:
            assert (valid_dir / filename).exists(), f"Missing fixture: {filename}"

    @pytest.mark.unit
    def test_all_invalid_fixtures_exist(self) -> None:
        """All documented invalid fixtures should exist."""
        expected_files = [
            "versions_bad_checksum.json",
            "versions_missing_current.json",
            "versions_duplicate.json",
        ]

        invalid_dir = FIXTURES_DIR / "invalid"
        for filename in expected_files:
            assert (invalid_dir / filename).exists(), f"Missing fixture: {filename}"

    @pytest.mark.unit
    def test_valid_fixtures_are_parseable(self) -> None:
        """All valid fixtures should be parseable by read_versions()."""
        valid_dir = FIXTURES_DIR / "valid"

        for fixture_path in valid_dir.glob("*.json"):
            # Should not raise
            versions_file = read_versions(fixture_path)
            assert versions_file.spec_version == "1.0.0"
            assert versions_file.current_version is not None

    @pytest.mark.unit
    def test_invalid_fixtures_are_valid_json(self) -> None:
        """Invalid fixtures should still be valid JSON for testing."""
        invalid_dir = FIXTURES_DIR / "invalid"

        for fixture_path in invalid_dir.glob("*.json"):
            # Should be valid JSON
            with open(fixture_path) as f:
                data = json.load(f)
            assert "spec_version" in data
