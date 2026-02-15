"""Unit tests for VersionModel and SchemaFingerprint dataclasses.

Tests cover:
- Version creation with semantic versioning
- Schema fingerprint for change detection
- Breaking change flag
- Asset versions with checksums
- JSON serialization (to_dict/from_dict)
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

# These will be implemented - tests first!
from portolan_cli.models.version import (
    AssetVersion,
    SchemaFingerprint,
    VersionModel,
)


class TestSchemaFingerprint:
    """Tests for SchemaFingerprint dataclass."""

    @pytest.mark.unit
    def test_create_geoparquet_fingerprint(self) -> None:
        """SchemaFingerprint can be created for GeoParquet."""
        fingerprint = SchemaFingerprint(
            type="geoparquet",
            fingerprint={
                "columns": {
                    "id": {"type": "int64", "nullable": False},
                    "geometry": {
                        "type": "binary",
                        "nullable": False,
                        "geometry_type": "Polygon",
                        "crs": "EPSG:4326",
                    },
                },
            },
        )

        assert fingerprint.type == "geoparquet"
        assert "columns" in fingerprint.fingerprint

    @pytest.mark.unit
    def test_create_cog_fingerprint(self) -> None:
        """SchemaFingerprint can be created for COG."""
        fingerprint = SchemaFingerprint(
            type="cog",
            fingerprint={
                "bands": [
                    {"name": "band_1", "data_type": "uint8", "nodata": 0},
                    {"name": "band_2", "data_type": "uint8", "nodata": 0},
                ],
                "crs": "EPSG:32610",
                "resolution": [10.0, 10.0],
            },
        )

        assert fingerprint.type == "cog"
        assert len(fingerprint.fingerprint["bands"]) == 2

    @pytest.mark.unit
    def test_fingerprint_to_dict(self) -> None:
        """SchemaFingerprint.to_dict() returns correct dict."""
        fingerprint = SchemaFingerprint(
            type="geoparquet",
            fingerprint={"columns": {"id": {"type": "int64"}}},
        )
        data = fingerprint.to_dict()

        assert data["type"] == "geoparquet"
        assert data["fingerprint"]["columns"]["id"]["type"] == "int64"

    @pytest.mark.unit
    def test_fingerprint_from_dict(self) -> None:
        """SchemaFingerprint.from_dict() creates SchemaFingerprint from dict."""
        data = {
            "type": "cog",
            "fingerprint": {
                "bands": [{"name": "b1", "data_type": "float32"}],
            },
        }
        fingerprint = SchemaFingerprint.from_dict(data)

        assert fingerprint.type == "cog"


class TestAssetVersion:
    """Tests for AssetVersion dataclass."""

    @pytest.mark.unit
    def test_create_asset_version(self) -> None:
        """AssetVersion can be created with all fields."""
        asset = AssetVersion(
            sha256="abc123def456...",
            size_bytes=1024000,
            href="v1.0.0/data.parquet",
        )

        assert asset.sha256 == "abc123def456..."
        assert asset.size_bytes == 1024000
        assert asset.href == "v1.0.0/data.parquet"

    @pytest.mark.unit
    def test_asset_version_to_dict(self) -> None:
        """AssetVersion.to_dict() returns correct dict."""
        asset = AssetVersion(
            sha256="abc123",
            size_bytes=2048,
            href="./data.parquet",
        )
        data = asset.to_dict()

        assert data["sha256"] == "abc123"
        assert data["size_bytes"] == 2048
        assert data["href"] == "./data.parquet"

    @pytest.mark.unit
    def test_asset_version_from_dict(self) -> None:
        """AssetVersion.from_dict() creates AssetVersion from dict."""
        data = {
            "sha256": "def456",
            "size_bytes": 4096,
            "href": "v2.0.0/image.tif",
        }
        asset = AssetVersion.from_dict(data)

        assert asset.sha256 == "def456"
        assert asset.size_bytes == 4096


class TestVersionModel:
    """Tests for VersionModel dataclass."""

    @pytest.mark.unit
    def test_create_version_with_required_fields(self) -> None:
        """VersionModel can be created with required fields."""
        now = datetime.now(timezone.utc)
        version = VersionModel(
            version="1.0.0",
            created=now,
            breaking=False,
            schema=SchemaFingerprint(type="geoparquet", fingerprint={}),
            assets={},
            changes=[],
        )

        assert version.version == "1.0.0"
        assert version.breaking is False

    @pytest.mark.unit
    def test_create_version_with_all_fields(self) -> None:
        """VersionModel can be created with all fields including message."""
        now = datetime.now(timezone.utc)
        version = VersionModel(
            version="2.0.0",
            created=now,
            breaking=True,
            message="Major schema overhaul - removed deprecated columns",
            schema=SchemaFingerprint(
                type="geoparquet",
                fingerprint={"columns": {"id": {"type": "int64"}}},
            ),
            assets={
                "data": AssetVersion(
                    sha256="abc123",
                    size_bytes=1024,
                    href="v2.0.0/data.parquet",
                ),
            },
            changes=["schema.json", "data.parquet"],
        )

        assert version.version == "2.0.0"
        assert version.breaking is True
        assert version.message == "Major schema overhaul - removed deprecated columns"
        assert len(version.changes) == 2


class TestVersionValidation:
    """Tests for VersionModel validation rules."""

    @pytest.mark.unit
    def test_version_must_be_semver(self) -> None:
        """version must be valid semantic version."""
        now = datetime.now(timezone.utc)

        # Valid semver versions
        valid_versions = ["1.0.0", "2.1.3", "0.0.1", "10.20.30"]
        for v in valid_versions:
            version = VersionModel(
                version=v,
                created=now,
                breaking=False,
                schema=SchemaFingerprint(type="geoparquet", fingerprint={}),
                assets={},
                changes=[],
            )
            assert version.version == v

    @pytest.mark.unit
    def test_invalid_semver_raises_error(self) -> None:
        """Invalid semantic version should raise ValueError."""
        now = datetime.now(timezone.utc)

        invalid_versions = ["1.0", "v1.0.0", "1", "1.0.0.0", "invalid"]
        for v in invalid_versions:
            with pytest.raises(ValueError, match="version"):
                VersionModel(
                    version=v,
                    created=now,
                    breaking=False,
                    schema=SchemaFingerprint(type="geoparquet", fingerprint={}),
                    assets={},
                    changes=[],
                )


class TestVersionSerialization:
    """Tests for VersionModel JSON serialization."""

    def _sample_version(self) -> VersionModel:
        """Create a sample version for serialization tests."""
        return VersionModel(
            version="1.2.3",
            created=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            breaking=False,
            message="Added new columns",
            schema=SchemaFingerprint(
                type="geoparquet",
                fingerprint={
                    "columns": {
                        "id": {"type": "int64"},
                        "name": {"type": "string"},
                    },
                },
            ),
            assets={
                "data": AssetVersion(
                    sha256="abc123def456",
                    size_bytes=1024000,
                    href="v1.2.3/data.parquet",
                ),
            },
            changes=["schema.json", "data.parquet"],
        )

    @pytest.mark.unit
    def test_to_dict_includes_required_fields(self) -> None:
        """to_dict() must include all required fields."""
        version = self._sample_version()
        data = version.to_dict()

        assert data["version"] == "1.2.3"
        assert data["created"] == "2024-01-15T12:00:00+00:00"
        assert data["breaking"] is False
        assert "schema" in data
        assert "assets" in data
        assert "changes" in data

    @pytest.mark.unit
    def test_to_dict_schema_structure(self) -> None:
        """to_dict() should have correct schema fingerprint structure."""
        version = self._sample_version()
        data = version.to_dict()

        assert data["schema"]["type"] == "geoparquet"
        assert "columns" in data["schema"]["fingerprint"]

    @pytest.mark.unit
    def test_to_dict_assets_structure(self) -> None:
        """to_dict() should have correct assets structure."""
        version = self._sample_version()
        data = version.to_dict()

        assert "data" in data["assets"]
        assert data["assets"]["data"]["sha256"] == "abc123def456"

    @pytest.mark.unit
    def test_from_dict_creates_version(self) -> None:
        """from_dict() should create VersionModel from dict."""
        data = {
            "version": "3.0.0",
            "created": "2024-06-01T00:00:00+00:00",
            "breaking": True,
            "message": "Breaking changes",
            "schema": {
                "type": "cog",
                "fingerprint": {"bands": []},
            },
            "assets": {},
            "changes": ["image.tif"],
        }
        version = VersionModel.from_dict(data)

        assert version.version == "3.0.0"
        assert version.breaking is True
        assert version.schema.type == "cog"

    @pytest.mark.unit
    def test_roundtrip_serialization(self) -> None:
        """to_dict -> from_dict should preserve all data."""
        original = self._sample_version()

        data = original.to_dict()
        restored = VersionModel.from_dict(data)

        assert restored.version == original.version
        assert restored.breaking == original.breaking
        assert restored.message == original.message
        assert len(restored.changes) == len(original.changes)


class TestVersionComparison:
    """Tests for version comparison and ordering."""

    @pytest.mark.unit
    def test_versions_can_be_compared(self) -> None:
        """Versions should support comparison operations."""
        now = datetime.now(timezone.utc)

        v1 = VersionModel(
            version="1.0.0",
            created=now,
            breaking=False,
            schema=SchemaFingerprint(type="geoparquet", fingerprint={}),
            assets={},
            changes=[],
        )
        v2 = VersionModel(
            version="2.0.0",
            created=now,
            breaking=True,
            schema=SchemaFingerprint(type="geoparquet", fingerprint={}),
            assets={},
            changes=[],
        )

        # Version comparison should work
        assert v1.version < v2.version
