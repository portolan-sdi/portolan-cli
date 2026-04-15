"""Tests for versioning helpers: semver logic and snapshot<->Version conversion."""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from portolan_cli.backends.iceberg.versioning import (
    build_assets,
    compute_next_version,
    snapshot_to_version,
    version_to_snapshot_properties,
)
from portolan_cli.versions import Asset, SchemaInfo

# --- compute_next_version ---


@pytest.mark.unit
def test_compute_next_version_first_is_1_0_0():
    """First version is always 1.0.0 regardless of breaking flag."""
    assert compute_next_version(None, breaking=False) == "1.0.0"
    assert compute_next_version(None, breaking=True) == "1.0.0"


@pytest.mark.unit
def test_compute_next_version_minor_bump():
    """Non-breaking change bumps minor version."""
    assert compute_next_version("1.0.0", breaking=False) == "1.1.0"
    assert compute_next_version("1.2.0", breaking=False) == "1.3.0"
    assert compute_next_version("2.5.3", breaking=False) == "2.6.0"


@pytest.mark.unit
def test_compute_next_version_major_bump_on_breaking():
    """Breaking change bumps major version, resets minor and patch."""
    assert compute_next_version("1.0.0", breaking=True) == "2.0.0"
    assert compute_next_version("1.2.3", breaking=True) == "2.0.0"
    assert compute_next_version("3.5.1", breaking=True) == "4.0.0"


# --- version_to_snapshot_properties ---


@pytest.mark.unit
def test_version_to_snapshot_properties_contains_all_keys():
    """All portolake.* keys should be present in output."""
    assets = {"data.parquet": Asset(sha256="abc123", size_bytes=1024, href="/data.parquet")}
    schema = SchemaInfo(type="geoparquet", fingerprint={"columns": ["id", "geom"]})
    props = version_to_snapshot_properties(
        version="1.0.0",
        breaking=False,
        message="Initial version",
        assets=assets,
        schema=schema,
        changes=["data.parquet"],
    )
    assert props["portolake.version"] == "1.0.0"
    assert props["portolake.breaking"] == "false"
    assert props["portolake.message"] == "Initial version"
    assert "portolake.assets" in props
    assert "portolake.schema" in props
    assert "portolake.changes" in props


@pytest.mark.unit
def test_version_to_snapshot_properties_all_values_are_strings():
    """All snapshot property values must be strings (Iceberg requirement)."""
    assets = {"f.parquet": Asset(sha256="x", size_bytes=10, href="/f")}
    props = version_to_snapshot_properties(
        version="1.0.0",
        breaking=True,
        message="test",
        assets=assets,
        schema=None,
        changes=["f.parquet"],
    )
    for key, value in props.items():
        assert isinstance(value, str), f"{key} value is {type(value)}, expected str"


@pytest.mark.unit
def test_version_to_snapshot_properties_roundtrip():
    """Properties should round-trip through JSON serialization."""
    assets = {
        "data.parquet": Asset(sha256="abc123", size_bytes=1024, href="/data.parquet"),
    }
    schema = SchemaInfo(type="geoparquet", fingerprint={"columns": ["id"]})
    props = version_to_snapshot_properties(
        version="2.1.0",
        breaking=True,
        message="Breaking schema change",
        assets=assets,
        schema=schema,
        changes=["data.parquet"],
    )
    # Deserialize and verify
    parsed_assets = json.loads(props["portolake.assets"])
    assert "data.parquet" in parsed_assets
    assert parsed_assets["data.parquet"]["sha256"] == "abc123"

    parsed_schema = json.loads(props["portolake.schema"])
    assert parsed_schema["type"] == "geoparquet"

    parsed_changes = json.loads(props["portolake.changes"])
    assert parsed_changes == ["data.parquet"]


# --- snapshot_to_version ---


@pytest.mark.unit
def test_snapshot_to_version_extracts_metadata():
    """snapshot_to_version should extract portolake.* properties into a Version."""
    assets = {"data.parquet": Asset(sha256="abc", size_bytes=512, href="/data")}
    schema = SchemaInfo(type="geoparquet", fingerprint={"columns": ["id"]})
    props = version_to_snapshot_properties(
        version="1.0.0",
        breaking=False,
        message="First version",
        assets=assets,
        schema=schema,
        changes=["data.parquet"],
    )

    # Mock a PyIceberg Snapshot
    mock_snapshot = MagicMock()
    mock_snapshot.timestamp_ms = int(datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1000)
    mock_summary = MagicMock()
    mock_summary.additional_properties = props
    mock_snapshot.summary = mock_summary

    version = snapshot_to_version(mock_snapshot)
    assert version.version == "1.0.0"
    assert version.breaking is False
    assert version.message == "First version"
    assert "data.parquet" in version.assets
    assert version.assets["data.parquet"].sha256 == "abc"
    assert version.schema is not None
    assert version.schema.type == "geoparquet"
    assert version.changes == ["data.parquet"]


@pytest.mark.unit
def test_snapshot_to_version_without_schema():
    """snapshot_to_version should handle missing schema gracefully."""
    props = version_to_snapshot_properties(
        version="1.1.0",
        breaking=False,
        message="No schema",
        assets={},
        schema=None,
        changes=[],
    )
    mock_snapshot = MagicMock()
    mock_snapshot.timestamp_ms = int(datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1000)
    mock_summary = MagicMock()
    mock_summary.additional_properties = props
    mock_snapshot.summary = mock_summary

    version = snapshot_to_version(mock_snapshot)
    assert version.version == "1.1.0"
    assert version.schema is None


# --- build_assets ---


@pytest.mark.unit
def test_build_assets_creates_asset_objects_with_relative_hrefs(tmp_path):
    """build_assets should create Asset objects with collection-relative hrefs."""
    test_file = tmp_path / "data.parquet"
    test_file.write_bytes(b"fake parquet content")

    assets, changes = build_assets({"data.parquet": str(test_file)}, collection="my-collection")
    assert "data.parquet" in assets
    assert assets["data.parquet"].size_bytes == 20
    assert assets["data.parquet"].sha256 != ""
    assert assets["data.parquet"].href == "my-collection/data.parquet"
    assert changes == ["data.parquet"]


@pytest.mark.unit
def test_build_assets_remote_asset_with_relative_hrefs():
    """build_assets should produce relative hrefs even for remote assets."""
    assets, changes = build_assets(
        {"remote.parquet": "s3://bucket/remote.parquet"}, collection="boundaries"
    )
    assert "remote.parquet" in assets
    assert assets["remote.parquet"].sha256 == ""
    assert assets["remote.parquet"].size_bytes == 0
    assert assets["remote.parquet"].href == "boundaries/remote.parquet"
    assert changes == ["remote.parquet"]


@pytest.mark.unit
def test_build_assets_nested_asset_key(tmp_path):
    """build_assets with item_id/filename key should produce correct href."""
    test_file = tmp_path / "data.parquet"
    test_file.write_bytes(b"content")

    assets, changes = build_assets({"item1/data.parquet": str(test_file)}, collection="boundaries")
    assert assets["item1/data.parquet"].href == "boundaries/item1/data.parquet"
