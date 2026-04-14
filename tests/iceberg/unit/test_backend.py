"""Tests for IcebergBackend implementation."""

import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.backends.protocol import VersioningBackend


def _write_parquet(path, rows=None):
    """Write a simple Parquet file for testing. Returns the path."""
    if rows is None:
        rows = {"id": [1], "val": ["x"]}
    table = pa.table(rows)
    pq.write_table(table, path)
    return path


# --- Constructor + Protocol ---


@pytest.mark.integration
def test_instantiates_with_no_args(iceberg_backend):
    """IcebergBackend should instantiate with no arguments."""
    assert iceberg_backend is not None


@pytest.mark.integration
def test_isinstance_versioning_backend(iceberg_backend):
    """IcebergBackend should satisfy the VersioningBackend protocol."""
    assert isinstance(iceberg_backend, VersioningBackend)


# --- Read Path ---


@pytest.mark.integration
def test_list_versions_empty_when_no_table(iceberg_backend):
    """list_versions returns empty list when collection doesn't exist."""
    result = iceberg_backend.list_versions("nonexistent-collection")
    assert result == []


@pytest.mark.integration
def test_get_current_version_raises_when_no_table(iceberg_backend):
    """get_current_version raises FileNotFoundError for missing collection."""
    with pytest.raises(FileNotFoundError):
        iceberg_backend.get_current_version("nonexistent-collection")


# --- Write Path (publish) ---


@pytest.mark.integration
def test_publish_creates_first_version_1_0_0(iceberg_backend, tmp_path):
    """First published version should always be 1.0.0."""
    asset = _write_parquet(tmp_path / "data.parquet")

    version = iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "abc123"},
        breaking=False,
        message="Initial version",
    )
    assert version.version == "1.0.0"
    assert version.breaking is False
    assert version.message == "Initial version"


@pytest.mark.integration
def test_publish_increments_minor_version(iceberg_backend, tmp_path):
    """Non-breaking change should increment minor version."""
    asset = _write_parquet(tmp_path / "data.parquet", {"id": [1], "val": ["v1"]})

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    _write_parquet(asset, {"id": [2], "val": ["v2"]})
    v2 = iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "h1"},
        breaking=False,
        message="v2",
    )
    assert v2.version == "1.1.0"


@pytest.mark.integration
def test_publish_increments_major_on_breaking(iceberg_backend, tmp_path):
    """Breaking change should increment major version."""
    asset = _write_parquet(tmp_path / "data.parquet", {"id": [1], "val": ["v1"]})

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    _write_parquet(asset, {"id": [2], "val": ["v2"]})
    v2 = iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={
            "columns": ["id", "geom"],
            "types": {"id": "int64", "geom": "geometry"},
            "hash": "h2",
        },
        breaking=True,
        message="Breaking schema change",
    )
    assert v2.version == "2.0.0"
    assert v2.breaking is True


@pytest.mark.integration
def test_publish_stores_assets_and_schema(iceberg_backend, tmp_path):
    """Published version should contain correct assets and schema."""
    asset = _write_parquet(tmp_path / "data.parquet", {"id": [1, 2], "name": ["a", "b"]})

    version = iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={
            "columns": ["id", "name"],
            "types": {"id": "int64", "name": "string"},
            "hash": "schema_hash",
        },
        breaking=False,
        message="With schema",
    )
    assert "data.parquet" in version.assets
    assert version.assets["data.parquet"].size_bytes > 0
    assert version.assets["data.parquet"].sha256 != ""
    assert version.schema is not None
    assert version.schema.type == "schema_hash"


@pytest.mark.integration
def test_publish_tracks_changes(iceberg_backend, tmp_path):
    """Published version should list changed assets."""
    f1 = _write_parquet(tmp_path / "a.parquet", {"id": [1], "x": ["a"]})
    f2 = _write_parquet(tmp_path / "b.parquet", {"id": [2], "x": ["b"]})

    version = iceberg_backend.publish(
        collection="test-collection",
        assets={"a.parquet": str(f1), "b.parquet": str(f2)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="Two assets",
    )
    assert set(version.changes) == {"a.parquet", "b.parquet"}


# --- Read After Write ---


@pytest.mark.integration
def test_get_current_version_after_publish(iceberg_backend, tmp_path):
    """get_current_version should return the latest published version."""
    asset = _write_parquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )
    current = iceberg_backend.get_current_version("test-collection")
    assert current.version == "1.0.0"


@pytest.mark.integration
def test_list_versions_ordered_oldest_first(iceberg_backend, tmp_path):
    """list_versions should return versions in chronological order."""
    asset = _write_parquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    _write_parquet(asset, {"id": [2], "val": ["v2"]})
    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v2",
    )

    versions = iceberg_backend.list_versions("test-collection")
    assert len(versions) == 2
    assert versions[0].version == "1.0.0"
    assert versions[1].version == "1.1.0"
    assert versions[0].created <= versions[1].created


# --- Rollback ---


@pytest.mark.integration
def test_rollback_restores_target_version(iceberg_backend, tmp_path):
    """Rollback should set current snapshot to the target version."""
    asset = _write_parquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    _write_parquet(asset, {"id": [2], "val": ["v2"]})
    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v2",
    )

    rolled = iceberg_backend.rollback("test-collection", "1.0.0")
    assert rolled.version == "1.0.0"

    current = iceberg_backend.get_current_version("test-collection")
    assert current.version == "1.0.0"


@pytest.mark.integration
def test_rollback_preserves_target_assets(iceberg_backend, tmp_path):
    """Rollback should restore the target version's asset metadata."""
    asset = _write_parquet(tmp_path / "data.parquet", {"id": [1], "val": ["v1"]})

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )
    v1 = iceberg_backend.get_current_version("test-collection")

    _write_parquet(asset, {"id": [2], "val": ["v2"]})
    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v2",
    )

    rolled = iceberg_backend.rollback("test-collection", "1.0.0")
    assert rolled.version == "1.0.0"
    assert rolled.assets["data.parquet"].sha256 == v1.assets["data.parquet"].sha256


@pytest.mark.integration
def test_rollback_nonexistent_version_raises_valueerror(iceberg_backend, tmp_path):
    """Rollback to nonexistent version should raise ValueError."""
    asset = _write_parquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    with pytest.raises(ValueError, match="99.99.99"):
        iceberg_backend.rollback("test-collection", "99.99.99")


# --- Prune ---


@pytest.mark.integration
def test_prune_dry_run_returns_prunable(iceberg_backend, tmp_path):
    """Prune with dry_run=True should return prunable versions without deleting."""
    asset = _write_parquet(tmp_path / "data.parquet")

    for i in range(4):
        _write_parquet(asset, {"id": [i], "val": [f"v{i}"]})
        iceberg_backend.publish(
            collection="test-collection",
            assets={"data.parquet": str(asset)},
            schema={"columns": [], "types": {}, "hash": "h"},
            breaking=False,
            message=f"v{i}",
        )

    prunable = iceberg_backend.prune("test-collection", keep=2, dry_run=True)
    assert len(prunable) == 2

    versions = iceberg_backend.list_versions("test-collection")
    assert len(versions) == 4


@pytest.mark.integration
def test_prune_removes_old_versions(iceberg_backend, tmp_path):
    """Prune should remove old versions."""
    asset = _write_parquet(tmp_path / "data.parquet")

    for i in range(4):
        _write_parquet(asset, {"id": [i], "val": [f"v{i}"]})
        iceberg_backend.publish(
            collection="test-collection",
            assets={"data.parquet": str(asset)},
            schema={"columns": [], "types": {}, "hash": "h"},
            breaking=False,
            message=f"v{i}",
        )

    pruned = iceberg_backend.prune("test-collection", keep=2, dry_run=False)
    assert len(pruned) == 2

    remaining = iceberg_backend.list_versions("test-collection")
    assert len(remaining) == 2


@pytest.mark.integration
def test_prune_keeps_n_most_recent(iceberg_backend, tmp_path):
    """Prune should keep the N most recent versions."""
    asset = _write_parquet(tmp_path / "data.parquet")

    for i in range(5):
        _write_parquet(asset, {"id": [i], "val": [f"v{i}"]})
        iceberg_backend.publish(
            collection="test-collection",
            assets={"data.parquet": str(asset)},
            schema={"columns": [], "types": {}, "hash": "h"},
            breaking=False,
            message=f"v{i}",
        )

    iceberg_backend.prune("test-collection", keep=3, dry_run=False)

    remaining = iceberg_backend.list_versions("test-collection")
    assert len(remaining) == 3
    assert remaining[-1].version == "1.4.0"
    assert remaining[-2].version == "1.3.0"
    assert remaining[-3].version == "1.2.0"


# --- Drift ---


@pytest.mark.integration
def test_check_drift_returns_report(iceberg_backend, tmp_path):
    """check_drift should return a DriftReport."""
    asset = _write_parquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    report = iceberg_backend.check_drift("test-collection")
    assert report["has_drift"] is False
    assert report["local_version"] == "1.0.0"
    assert report["remote_version"] == "1.0.0"
    assert isinstance(report["message"], str)


@pytest.mark.integration
def test_check_drift_no_collection(iceberg_backend):
    """check_drift should handle missing collections."""
    report = iceberg_backend.check_drift("nonexistent")
    assert report["has_drift"] is False
    assert report["local_version"] is None


# --- Edge Cases ---


@pytest.mark.integration
def test_empty_collection_name_rejected(iceberg_backend):
    """Empty collection name should raise ValueError."""
    with pytest.raises(ValueError, match="[Ee]mpty"):
        iceberg_backend.list_versions("")

    with pytest.raises(ValueError, match="[Ee]mpty"):
        iceberg_backend.list_versions("   ")


@pytest.mark.integration
def test_directory_traversal_rejected(iceberg_backend):
    """Directory traversal attempts should raise ValueError."""
    with pytest.raises(ValueError):
        iceberg_backend.list_versions("../../etc/passwd")

    with pytest.raises(ValueError):
        iceberg_backend.list_versions("..")


# --- catalog_root wiring ---


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="PyIceberg SQL catalog warehouse paths broken on Windows (apache/iceberg-python#1005)",
)
@pytest.mark.integration
def test_backend_with_catalog_root_creates_files_in_correct_location(tmp_path, monkeypatch):
    """IcebergBackend(catalog_root=path) should create iceberg.db under path/.portolan/."""
    from portolan_cli.backends.iceberg.backend import IcebergBackend

    # Isolate from ~/.pyiceberg.yaml (e.g., REST/BigLake config)
    monkeypatch.setattr("portolan_cli.backends.iceberg.config._get_external_config", lambda: None)
    backend = IcebergBackend(catalog_root=tmp_path)
    assert (tmp_path / ".portolan" / "iceberg.db").exists()

    asset = _write_parquet(tmp_path / "data.parquet")
    version = backend.publish(
        collection="test-col",
        assets={"data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="test",
    )
    assert version.version == "1.0.0"


# --- publish with removed parameter ---


@pytest.mark.integration
def test_publish_with_removed_excludes_assets(iceberg_backend, tmp_path):
    """publish(removed={"asset"}) should exclude that asset from the new version."""
    f1 = _write_parquet(tmp_path / "a.parquet", {"id": [1], "x": ["a"]})
    f2 = _write_parquet(tmp_path / "b.parquet", {"id": [2], "x": ["b"]})

    iceberg_backend.publish(
        collection="test-collection",
        assets={"a.parquet": str(f1), "b.parquet": str(f2)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1 with two assets",
    )

    # Publish v2 removing one asset (no new data assets)
    v2 = iceberg_backend.publish(
        collection="test-collection",
        assets={},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="remove b",
        removed={"b.parquet"},
    )
    assert "a.parquet" in v2.assets
    assert "b.parquet" not in v2.assets


@pytest.mark.integration
def test_publish_with_removed_and_new_assets(iceberg_backend, tmp_path):
    """publish can add new assets and remove old ones in the same call."""
    f1 = _write_parquet(tmp_path / "old.parquet", {"id": [1], "x": ["old"]})

    iceberg_backend.publish(
        collection="test-collection",
        assets={"old.parquet": str(f1)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    f2 = _write_parquet(tmp_path / "new.parquet", {"id": [2], "x": ["new"]})

    v2 = iceberg_backend.publish(
        collection="test-collection",
        assets={"new.parquet": str(f2)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="swap assets",
        removed={"old.parquet"},
    )
    assert "new.parquet" in v2.assets
    assert "old.parquet" not in v2.assets


@pytest.mark.integration
def test_get_current_version_returns_relative_hrefs(iceberg_backend, tmp_path):
    """get_current_version() should return Version with relative hrefs."""
    asset = _write_parquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(asset)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    version = iceberg_backend.get_current_version("boundaries")
    assert version.assets["item1/data.parquet"].href == "boundaries/item1/data.parquet"
    assert not version.assets["item1/data.parquet"].href.startswith("/")


@pytest.mark.integration
def test_publish_stores_relative_hrefs(iceberg_backend, tmp_path):
    """Published version assets should have catalog-root-relative hrefs."""
    asset = _write_parquet(tmp_path / "data.parquet", {"id": [1], "name": ["test"]})

    version = iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(asset)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "h"},
        breaking=False,
        message="test relative hrefs",
    )
    asset_obj = version.assets["item1/data.parquet"]
    assert asset_obj.href == "boundaries/item1/data.parquet"
    assert not asset_obj.href.startswith("/")


@pytest.mark.integration
def test_publish_relative_hrefs_survive_merge(iceberg_backend, tmp_path):
    """Relative hrefs should be preserved when merging with previous snapshot."""
    f1 = _write_parquet(tmp_path / "a.parquet", {"id": [1], "x": ["a"]})
    f2 = _write_parquet(tmp_path / "b.parquet", {"id": [2], "x": ["b"]})

    iceberg_backend.publish(
        collection="boundaries",
        assets={"a.parquet": str(f1)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    v2 = iceberg_backend.publish(
        collection="boundaries",
        assets={"b.parquet": str(f2)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v2 adds b",
    )
    assert v2.assets["a.parquet"].href == "boundaries/a.parquet"
    assert v2.assets["b.parquet"].href == "boundaries/b.parquet"


@pytest.mark.integration
def test_publish_removed_nonexistent_asset_is_noop(iceberg_backend, tmp_path):
    """Removing an asset that doesn't exist should not raise."""
    f1 = _write_parquet(tmp_path / "data.parquet")

    iceberg_backend.publish(
        collection="test-collection",
        assets={"data.parquet": str(f1)},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="v1",
    )

    v2 = iceberg_backend.publish(
        collection="test-collection",
        assets={},
        schema={"columns": [], "types": {}, "hash": "h"},
        breaking=False,
        message="remove ghost",
        removed={"ghost.parquet"},
    )
    assert "data.parquet" in v2.assets
    assert len(v2.assets) == 1
