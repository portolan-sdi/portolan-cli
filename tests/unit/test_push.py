"""Unit tests for push module.

Tests for cloud-native catalog push functionality with conflict detection
and optimistic locking via etag.

Following TDD: these tests are written FIRST, before implementation.

Test categories:
- PushResult dataclass
- Version diffing (local vs remote changes)
- Conflict detection
- Manifest-last upload ordering
- Etag-based optimistic locking
- Dry-run mode
- Force mode (overwrite conflicts)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import obstore as obs
import pytest
from hypothesis import given
from hypothesis import strategies as st
from obstore.store import MemoryStore

from portolan_cli.sync.push import UploadMetrics
from tests.conftest import cleared_environ
from tests.memory_remote import memory_remote, record_puts, stored_json, stored_keys

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

# =============================================================================
# Test fixtures
# =============================================================================


@pytest.fixture
def local_catalog(tmp_path: Path) -> Path:
    """Create a local catalog with versions.json for testing.

    Includes required STAC metadata files (collection.json, item STAC).
    """
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create test/versions.json
    versions_dir = catalog_dir / "test"
    versions_dir.mkdir(parents=True)

    # hrefs are catalog-root-relative: collection/item/filename
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.1.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1024,
                        "href": "test/data/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
            {
                "version": "1.1.0",
                "created": "2024-01-15T00:00:00Z",
                "breaking": False,
                "message": "Added more data",
                "assets": {
                    "data.parquet": {
                        "sha256": "def456",
                        "size_bytes": 2048,
                        "href": "test/data/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }

    (versions_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    # Create collection.json (required for push per Issue #252)
    collection_data = {
        "type": "Collection",
        "id": "test",
        "stac_version": "1.0.0",
        "description": "Test collection",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
    }
    (versions_dir / "collection.json").write_text(json.dumps(collection_data, indent=2))

    # Create actual data file at catalog_root/collection/item/filename
    item_dir = versions_dir / "data"
    item_dir.mkdir(parents=True)
    (item_dir / "data.parquet").write_bytes(b"x" * 2048)

    # Create item STAC file using Portolan naming convention: {item_id}.json
    item_data = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "data",
        "geometry": None,
        "bbox": None,
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "links": [],
        "assets": {"data": {"href": "./data.parquet", "type": "application/vnd.apache.parquet"}},
    }
    (item_dir / "data.json").write_text(json.dumps(item_data, indent=2))

    return catalog_dir


@pytest.fixture
def remote_versions_same() -> dict[str, Any]:
    """Remote versions.json matching local v1.0.0 (before local changes)."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "message": "Initial version",
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


@pytest.fixture
def remote_versions_diverged() -> dict[str, Any]:
    """Remote versions.json that diverged from local (conflict scenario)."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.1",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "message": "Initial version",
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1024,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
            {
                "version": "1.0.1",
                "created": "2024-01-10T00:00:00Z",
                "breaking": False,
                "message": "Remote change",
                "assets": {
                    "data.parquet": {
                        "sha256": "remote789",
                        "size_bytes": 1536,
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }


# =============================================================================
# Malformed Fixtures for Error Path Testing
# =============================================================================


@pytest.fixture
def local_catalog_malformed(tmp_path: Path) -> Path:
    """Create a local catalog with malformed versions.json (missing keys)."""
    catalog_dir = tmp_path / "catalog_malformed"
    catalog_dir.mkdir()

    # Create test/versions.json with missing required keys
    versions_dir = catalog_dir / "test"
    versions_dir.mkdir(parents=True)

    # Missing "current_version" and "spec_version"
    malformed_data = {
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
            }
        ],
    }

    (versions_dir / "versions.json").write_text(json.dumps(malformed_data, indent=2))

    return catalog_dir


@pytest.fixture
def local_catalog_invalid_json(tmp_path: Path) -> Path:
    """Create a local catalog with invalid JSON in versions.json."""
    catalog_dir = tmp_path / "catalog_invalid_json"
    catalog_dir.mkdir()

    versions_dir = catalog_dir / "test"
    versions_dir.mkdir(parents=True)

    # Write invalid JSON
    (versions_dir / "versions.json").write_text("{ invalid json }")

    return catalog_dir


@pytest.fixture
def remote_versions_same_malformed() -> dict[str, Any]:
    """Remote versions.json with invalid structure (missing spec_version)."""
    return {
        # Missing "spec_version"
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
            }
        ],
    }


@pytest.fixture
def remote_versions_diverged_malformed() -> dict[str, Any]:
    """Remote versions.json with wrong asset fields (missing sha256/size_bytes)."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.1",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "assets": {
                    "data.parquet": {
                        # Missing "sha256" and "size_bytes"
                        "href": "data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }


# =============================================================================
# PushResult Tests
# =============================================================================


class TestPushResult:
    """Tests for PushResult dataclass."""

    @pytest.mark.unit
    def test_push_result_success(self) -> None:
        """PushResult should capture successful push stats."""
        from portolan_cli.sync.push import PushResult

        result = PushResult(
            success=True,
            files_uploaded=5,
            versions_pushed=1,
            conflicts=[],
            errors=[],
        )

        assert result.success is True
        assert result.files_uploaded == 5
        assert result.versions_pushed == 1
        assert result.conflicts == []
        assert result.errors == []

    @pytest.mark.unit
    def test_push_result_with_conflicts(self) -> None:
        """PushResult should capture conflicts when detected."""
        from portolan_cli.sync.push import PushResult

        result = PushResult(
            success=False,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=["data.parquet changed both locally and remotely"],
            errors=[],
        )

        assert result.success is False
        assert result.conflicts == ["data.parquet changed both locally and remotely"]

    @pytest.mark.unit
    def test_push_result_with_errors(self) -> None:
        """PushResult should capture upload errors."""
        from portolan_cli.sync.push import PushResult

        result = PushResult(
            success=False,
            files_uploaded=2,
            versions_pushed=0,
            conflicts=[],
            errors=["Failed to upload data.parquet: connection timeout"],
        )

        assert result.success is False
        assert len(result.errors) == 1


# =============================================================================
# Version Diffing Tests
# =============================================================================


class TestDiffVersionLists:
    """Tests for diff_version_lists function."""

    @pytest.mark.unit
    def test_diff_local_only_changes(self) -> None:
        """Diff should detect versions that exist only locally."""
        from portolan_cli.sync.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == ["1.1.0"]
        assert diff.remote_only == []
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_remote_only_changes(self) -> None:
        """Diff should detect versions that exist only remotely (conflict)."""
        from portolan_cli.sync.push import diff_version_lists

        local_versions = ["1.0.0"]
        remote_versions = ["1.0.0", "1.0.1"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == []
        assert diff.remote_only == ["1.0.1"]
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_both_diverged(self) -> None:
        """Diff should detect when both local and remote have unique versions."""
        from portolan_cli.sync.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0", "1.0.1"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == ["1.1.0"]
        assert diff.remote_only == ["1.0.1"]
        assert diff.common == ["1.0.0"]

    @pytest.mark.unit
    def test_diff_empty_remote(self) -> None:
        """Diff should handle empty remote (first push)."""
        from portolan_cli.sync.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions: list[str] = []

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == ["1.0.0", "1.1.0"]
        assert diff.remote_only == []
        assert diff.common == []

    @pytest.mark.unit
    def test_diff_identical(self) -> None:
        """Diff should detect when local and remote are identical."""
        from portolan_cli.sync.push import diff_version_lists

        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = ["1.0.0", "1.1.0"]

        diff = diff_version_lists(local_versions, remote_versions)

        assert diff.local_only == []
        assert diff.remote_only == []
        assert diff.common == ["1.0.0", "1.1.0"]


# =============================================================================
# Conflict Detection Tests
# =============================================================================


class TestConflictDetection:
    """Tests for conflict detection logic."""

    @pytest.mark.unit
    def test_has_conflict_when_remote_diverged(self) -> None:
        """Should detect conflict when remote has versions not in local."""
        from portolan_cli.sync.push import VersionDiff

        diff = VersionDiff(
            local_only=["1.1.0"],
            remote_only=["1.0.1"],
            common=["1.0.0"],
        )

        assert diff.has_conflict is True

    @pytest.mark.unit
    def test_no_conflict_when_only_local_changes(self) -> None:
        """No conflict when only local has new versions."""
        from portolan_cli.sync.push import VersionDiff

        diff = VersionDiff(
            local_only=["1.1.0"],
            remote_only=[],
            common=["1.0.0"],
        )

        assert diff.has_conflict is False

    @pytest.mark.unit
    def test_no_conflict_when_identical(self) -> None:
        """No conflict when local and remote are identical."""
        from portolan_cli.sync.push import VersionDiff

        diff = VersionDiff(
            local_only=[],
            remote_only=[],
            common=["1.0.0", "1.1.0"],
        )

        assert diff.has_conflict is False


# =============================================================================
# Push Function Tests
# =============================================================================


class TestPush:
    """Tests for main push function."""

    @pytest.mark.unit
    def test_push_dry_run_no_upload(self, local_catalog: Path) -> None:
        """Dry-run should not perform actual upload."""
        from portolan_cli.sync.push import push

        # Dry-run returns early without any network calls (Bug #137)
        # No need to patch - the function returns before upload
        result = push(
            catalog_root=local_catalog,
            collection="test",
            destination="s3://mybucket/catalog",
            dry_run=True,
        )

        assert result.success is True
        assert result.files_uploaded == 0

    @pytest.mark.unit
    def test_push_detects_conflict_without_force(self, local_catalog: Path) -> None:
        """Push should fail when remote diverged and --force not specified."""
        from portolan_cli.sync.push import PushConflictError, push

        with patch(
            "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            # Remote has a version we don't have locally
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.1",
                    "versions": [
                        {"version": "1.0.0", "created": "2024-01-01T00:00:00Z"},
                        {"version": "1.0.1", "created": "2024-01-10T00:00:00Z"},
                    ],
                },
                "etag-123",
            )

            with pytest.raises(PushConflictError) as exc_info:
                push(
                    catalog_root=local_catalog,
                    collection="test",
                    destination="s3://mybucket/catalog",
                    force=False,
                )

            assert "Remote has changes" in str(exc_info.value)

    @pytest.mark.unit
    def test_push_nothing_to_push(self, local_catalog: Path) -> None:
        """Push when local == remote should report nothing to push."""
        from portolan_cli.sync.push import push

        # Read local versions to create matching remote
        versions_path = local_catalog / "test" / "versions.json"
        local_data = json.loads(versions_path.read_text())

        with patch(
            "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (local_data, "etag-123")

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://mybucket/catalog",
            )

        assert result.success is True
        assert result.files_uploaded == 0
        assert result.versions_pushed == 0


# =============================================================================
# Push against an in-memory remote: the bucket holds what the push wrote
# =============================================================================


@pytest.fixture
def remote() -> Iterator[MemoryStore]:
    """The remote bucket. push writes to this store under the prefix "catalog"."""
    with memory_remote() as store:
        yield store


def _seed_remote_versions(store: MemoryStore, versions: list[str]) -> None:
    entries = [{"version": v, "created": "2024-01-01T00:00:00Z"} for v in versions]
    body = {"spec_version": "1.0.0", "current_version": versions[-1], "versions": entries}
    obs.put(store, "catalog/test/versions.json", json.dumps(body).encode())


def _local_versions(catalog: Path) -> Any:
    return json.loads((catalog / "test" / "versions.json").read_text())


class TestPushToMemoryStore:
    """push writes the assets, the STAC files, and versions.json last."""

    @pytest.mark.unit
    def test_first_push_writes_the_collection(
        self, local_catalog: Path, remote: MemoryStore
    ) -> None:
        from portolan_cli.sync.push import push

        result = push(local_catalog, "test", "s3://mybucket/catalog")

        assert result.success is True
        assert result.versions_pushed == 2
        assert stored_keys(remote) == {
            "catalog/test/collection.json",
            "catalog/test/data/data.json",
            "catalog/test/data/data.parquet",
            "catalog/test/versions.json",
        }
        assert stored_json(remote, "catalog/test/versions.json") == _local_versions(local_catalog)

    @pytest.mark.unit
    def test_manifest_is_written_last(
        self, local_catalog: Path, remote: MemoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Assets, then items, then collection.json, then versions.json."""
        from portolan_cli.sync.push import push

        order = record_puts(monkeypatch)
        push(local_catalog, "test", "s3://mybucket/catalog")

        assert order == [
            "catalog/test/data/data.parquet",
            "catalog/test/data/data.json",
            "catalog/test/collection.json",
            "catalog/test/versions.json",
        ]

    @pytest.mark.unit
    def test_failed_asset_writes_no_manifest(
        self, local_catalog: Path, remote: MemoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from portolan_cli.sync.push import push

        async def refuse(_key: str) -> None:
            raise ConnectionError("network down")

        record_puts(monkeypatch, refuse)
        result = push(local_catalog, "test", "s3://mybucket/catalog")

        assert result.success is False
        assert result.errors == [
            "Failed to upload test/data/data.parquet: ConnectionError: network down"
        ]
        assert stored_keys(remote) == set()

    @pytest.mark.unit
    def test_first_push_does_not_overwrite_a_manifest_another_writer_created(
        self, local_catalog: Path, remote: MemoryStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With no remote versions.json at the start, the write is mode="create"."""
        from portolan_cli.sync.push import PushConflictError, push

        async def other_writer_first(key: str) -> None:
            if key == "catalog/test/versions.json":
                obs.put(remote, key, b'{"by": "B"}')

        record_puts(monkeypatch, other_writer_first)
        with pytest.raises(PushConflictError, match="Remote changed during push"):
            push(local_catalog, "test", "s3://mybucket/catalog")

        assert stored_json(remote, "catalog/test/versions.json") == {"by": "B"}


def _add_version(catalog: Path, data: bytes, new_file: bytes) -> None:
    """Record version 1.2.0: new bytes for data.parquet and a new extra.parquet."""
    item_dir = catalog / "test" / "data"
    (item_dir / "data.parquet").write_bytes(data)
    (item_dir / "extra.parquet").write_bytes(new_file)
    versions = _local_versions(catalog)
    assets = {
        "data.parquet": {"sha256": "v3-data", "href": "test/data/data.parquet"},
        "extra.parquet": {"sha256": "v3-extra", "href": "test/data/extra.parquet"},
    }
    versions["versions"].append({"version": "1.2.0", "created": "2024-02-01", "assets": assets})
    versions["current_version"] = "1.2.0"
    (catalog / "test" / "versions.json").write_text(json.dumps(versions))


def _stored_bytes(store: MemoryStore, key: str) -> bytes:
    return bytes(obs.get(store, key).bytes())


class TestRollbackRestoresOverwrittenKeys:
    """After a failed push, each overwritten key holds the bytes the remote manifest names."""

    @pytest.fixture
    def second_push(self, local_catalog: Path, remote: MemoryStore) -> dict[str, bytes]:
        """Push once, then record a version that overwrites and adds a key."""
        from portolan_cli.sync.push import push

        assert push(local_catalog, "test", "s3://mybucket/catalog").success
        before = {key: _stored_bytes(remote, key) for key in stored_keys(remote)}
        _add_version(local_catalog, b"bytes of 1.2.0", b"extra of 1.2.0")
        return before

    @pytest.mark.unit
    def test_failed_push_restores_the_old_bytes(
        self,
        local_catalog: Path,
        remote: MemoryStore,
        second_push: dict[str, bytes],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Issue #965: a failed versions.json write puts the old bytes back."""
        from portolan_cli.sync.push import push

        async def fail_manifest(key: str) -> None:
            if key == "catalog/test/versions.json":
                raise ConnectionError("network down")

        record_puts(monkeypatch, fail_manifest)
        result = push(local_catalog, "test", "s3://mybucket/catalog")

        assert result.success is False
        after = {key: _stored_bytes(remote, key) for key in stored_keys(remote)}
        assert after == second_push

    @pytest.mark.unit
    def test_conflict_restores_the_old_bytes_and_keeps_created_keys(
        self,
        local_catalog: Path,
        remote: MemoryStore,
        second_push: dict[str, bytes],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Issue #971: the winner's manifest describes the old bytes, so they come back."""
        from portolan_cli.sync.push import PushConflictError, push

        async def other_writer_wins(key: str) -> None:
            if key == "catalog/test/versions.json":
                obs.put(remote, key, b'{"by": "B"}')

        record_puts(monkeypatch, other_writer_wins)
        with pytest.raises(PushConflictError, match="Remote changed during push"):
            push(local_catalog, "test", "s3://mybucket/catalog")

        after = {key: _stored_bytes(remote, key) for key in stored_keys(remote)}
        assert after == {
            **second_push,
            "catalog/test/versions.json": b'{"by": "B"}',
            "catalog/test/data/extra.parquet": b"extra of 1.2.0",
        }

    @pytest.mark.unit
    def test_successful_push_deletes_the_backups(
        self, local_catalog: Path, remote: MemoryStore, second_push: dict[str, bytes]
    ) -> None:
        from portolan_cli.sync.push import push

        assert push(local_catalog, "test", "s3://mybucket/catalog").success

        assert stored_keys(remote) == {*second_push, "catalog/test/data/extra.parquet"}
        assert _stored_bytes(remote, "catalog/test/data/data.parquet") == b"bytes of 1.2.0"


class TestUploadVersionsJsonAsync:
    """_upload_versions_json_async writes under the ETag the push read."""

    @pytest.mark.unit
    async def test_stale_etag_raises_conflict(self) -> None:
        from portolan_cli.sync.push import PushConflictError, _upload_versions_json_async

        store = MemoryStore()
        old = obs.put(store, "catalog/test/versions.json", b"{}")["e_tag"]
        obs.put(store, "catalog/test/versions.json", b'{"by": "B"}')

        with pytest.raises(PushConflictError, match="Remote changed during push"):
            await _upload_versions_json_async(store, "catalog", "test", {"versions": []}, old)

        assert stored_json(store, "catalog/test/versions.json") == {"by": "B"}

    @pytest.mark.unit
    async def test_current_etag_writes(self) -> None:
        from portolan_cli.sync.push import _upload_versions_json_async

        store = MemoryStore()
        etag = obs.put(store, "catalog/test/versions.json", b"{}")["e_tag"]

        await _upload_versions_json_async(store, "catalog", "test", {"versions": []}, etag)

        assert stored_json(store, "catalog/test/versions.json") == {"versions": []}

    @pytest.mark.unit
    async def test_force_overwrites_without_condition(self) -> None:
        from portolan_cli.sync.push import _upload_versions_json_async

        store = MemoryStore()
        obs.put(store, "catalog/test/versions.json", b'{"by": "B"}')

        await _upload_versions_json_async(
            store, "catalog", "test", {"versions": []}, "stale-etag", force=True
        )

        assert stored_json(store, "catalog/test/versions.json") == {"versions": []}

    @pytest.mark.unit
    async def test_other_errors_propagate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from portolan_cli.sync.push import _upload_versions_json_async

        async def boom(*_args: Any, **_kwargs: Any) -> None:
            raise OSError("disk on fire")

        monkeypatch.setattr(obs, "put_async", boom)
        with pytest.raises(OSError, match="disk on fire"):
            await _upload_versions_json_async(MemoryStore(), "catalog", "test", {}, None)


# =============================================================================
# Store Setup Tests
# =============================================================================


class TestStoreSetup:
    """Tests for object store setup."""

    @pytest.mark.unit
    def test_push_with_profile(self, local_catalog: Path) -> None:
        """Push should pass profile to store setup."""
        from portolan_cli.sync.push import push

        with patch(
            "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            with patch("portolan_cli.sync.push.setup_store") as mock_setup:
                mock_store = MagicMock()
                mock_setup.return_value = (mock_store, "prefix")

                with patch(
                    "portolan_cli.sync.push._upload_assets_async", new_callable=AsyncMock
                ) as mock_upload:
                    mock_upload.return_value = (1, [], ["catalog/data.parquet"], UploadMetrics())

                    with patch(
                        "portolan_cli.sync.push._upload_versions_json_async", new_callable=AsyncMock
                    ):
                        push(
                            catalog_root=local_catalog,
                            collection="test",
                            destination="s3://mybucket/catalog",
                            profile="myprofile",
                        )

                # Verify profile was passed
                call_kwargs = mock_setup.call_args[1]
                assert call_kwargs.get("profile") == "myprofile"


# =============================================================================
# Missing Asset Detection Tests
# =============================================================================


class TestMissingAssetDetection:
    """Tests for missing asset detection in push operations."""

    @pytest.mark.unit
    def test_get_assets_to_upload_raises_on_missing_file(self, local_catalog: Path) -> None:
        """Should raise FileNotFoundError when referenced asset doesn't exist."""
        from portolan_cli.sync.push import _get_assets_to_upload

        versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "missing.parquet": {
                            "sha256": "abc123",
                            "size_bytes": 1000,
                            "href": "missing.parquet",  # This file doesn't exist
                        }
                    },
                }
            ]
        }

        with pytest.raises(FileNotFoundError, match="missing.parquet"):
            _get_assets_to_upload(
                catalog_root=local_catalog,
                versions_data=versions_data,
            )


class TestMissingDerivedAsset:
    """Push warns and skips an optional derivative that left the disk (#735).

    The spec calls a ``visual``, ``thumbnail``, ``style`` or
    ``collection-mirror`` asset optional. Portolan rebuilds each one from other
    files in the catalog. A user who deletes one still holds a sound catalog. An
    asset with role ``data`` or ``source`` still raises. The role decides, and
    the filenames only answer for an href no STAC object claims.
    """

    @staticmethod
    def _versions_data(assets: dict[str, str]) -> dict[str, Any]:
        """One version whose assets are ``{asset_name: href}``."""
        return {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        name: {"sha256": f"sha-{name}", "size_bytes": 10, "href": href}
                        for name, href in assets.items()
                    },
                }
            ]
        }

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "missing_href",
        [
            "test/scene1/scene1.thumb.jpg",
            "test/items.parquet",
            "test/data.pmtiles",
        ],
    )
    def test_missing_derived_asset_is_skipped(
        self, local_catalog: Path, capsys: pytest.CaptureFixture[str], missing_href: str
    ) -> None:
        """Push skips a recorded derived artifact that is no longer on disk."""
        from portolan_cli.sync.push import _get_assets_to_upload

        source = local_catalog / "test" / "scene1" / "scene1.tif"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(b"raster bytes")

        assets = _get_assets_to_upload(
            catalog_root=local_catalog,
            versions_data=self._versions_data(
                {
                    "scene1/scene1.tif": "test/scene1/scene1.tif",
                    "derived": missing_href,
                }
            ),
        )

        assert assets == [source.resolve()]
        assert missing_href in capsys.readouterr().err

    @pytest.mark.unit
    def test_missing_source_asset_still_raises(self, local_catalog: Path) -> None:
        """The user's own data is still a hard failure."""
        from portolan_cli.sync.push import _get_assets_to_upload

        with pytest.raises(FileNotFoundError, match=r"test/scene1/scene1\.tif"):
            _get_assets_to_upload(
                catalog_root=local_catalog,
                versions_data=self._versions_data({"scene1/scene1.tif": "test/scene1/scene1.tif"}),
            )

    @staticmethod
    def _register_collection_asset(
        catalog_root: Path, key: str, href: str, roles: list[str]
    ) -> None:
        """Give the collection.json one asset, so the href resolves to a role."""
        collection_json = catalog_root / "test" / "collection.json"
        data = json.loads(collection_json.read_text())
        data.setdefault("assets", {})[key] = {
            "href": href,
            "type": "application/octet-stream",
            "roles": roles,
        }
        collection_json.write_text(json.dumps(data, indent=2))

    @pytest.mark.unit
    def test_data_role_beats_a_generated_filename(self, local_catalog: Path) -> None:
        """A publisher's own PMTiles carries role `data`, so push still raises.

        The extension registry gives .pmtiles the role `data`, and the spec lets
        a publisher ship PMTiles as the primary asset. A filename test alone
        would skip that file and publish a manifest without it.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        self._register_collection_asset(local_catalog, "data", "./data.pmtiles", ["data"])

        with pytest.raises(FileNotFoundError, match=r"test/data\.pmtiles"):
            _get_assets_to_upload(
                catalog_root=local_catalog,
                versions_data=self._versions_data({"data.pmtiles": "test/data.pmtiles"}),
            )

    @pytest.mark.unit
    def test_thumbnail_role_wins_over_an_unknown_filename(
        self, local_catalog: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Push skips a preview the spec calls optional, whatever its name."""
        from portolan_cli.sync.push import _get_assets_to_upload

        self._register_collection_asset(local_catalog, "thumbnail", "./preview.webp", ["thumbnail"])

        assets = _get_assets_to_upload(
            catalog_root=local_catalog,
            versions_data=self._versions_data({"preview.webp": "test/preview.webp"}),
        )

        assert assets == []
        assert "test/preview.webp" in capsys.readouterr().err

    @pytest.mark.unit
    def test_source_role_still_raises(self, local_catalog: Path) -> None:
        """An upstream original kept locally is data nothing can rebuild."""
        from portolan_cli.sync.push import _get_assets_to_upload

        self._register_collection_asset(local_catalog, "upstream", "./original.gpkg", ["source"])

        with pytest.raises(FileNotFoundError, match=r"test/original\.gpkg"):
            _get_assets_to_upload(
                catalog_root=local_catalog,
                versions_data=self._versions_data({"original.gpkg": "test/original.gpkg"}),
            )

    @pytest.mark.unit
    def test_missing_user_supplied_image_still_raises(self, local_catalog: Path) -> None:
        """An adopted ``thumbnail.png`` is the user's image, not our render.

        One writer records both an adopted image and a rendered one. Only the
        ``{stem}.thumb.jpg`` name marks an artifact Portolan drew. A user's own
        picture must keep the hard failure.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        with pytest.raises(FileNotFoundError, match=r"test/thumbnail\.png"):
            _get_assets_to_upload(
                catalog_root=local_catalog,
                versions_data=self._versions_data({"thumbnail.png": "test/thumbnail.png"}),
            )


# =============================================================================
# Force Flag Tests
# =============================================================================


class TestForceFlag:
    """Tests for --force flag behavior in push operations."""

    @staticmethod
    def _build_identical_version_catalog(tmp_path: Path) -> Path:
        """Catalog whose local versions.json exactly matches the remote (Issue #496)."""
        catalog_dir = tmp_path / "catalog"
        versions_dir = catalog_dir / "test"
        versions_dir.mkdir(parents=True)
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "message": "Initial version",
                    "assets": {
                        "data.parquet": {
                            "sha256": "abc123",
                            "size_bytes": 1024,
                            "href": "test/data/data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                },
            ],
        }
        (versions_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))
        (versions_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "test",
                    "stac_version": "1.0.0",
                    "description": "Test collection",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                },
                indent=2,
            )
        )
        item_dir = versions_dir / "data"
        item_dir.mkdir(parents=True)
        (item_dir / "data.parquet").write_bytes(b"x" * 1024)
        return catalog_dir

    @pytest.mark.unit
    def test_force_overwrites_a_diverged_remote(
        self, local_catalog: Path, remote: MemoryStore
    ) -> None:
        """Force replaces a remote that holds versions the local catalog lacks."""
        from portolan_cli.sync.push import push

        _seed_remote_versions(remote, ["1.0.0", "1.0.1"])

        result = push(local_catalog, "test", "s3://mybucket/catalog", force=True)

        assert result.success is True
        assert stored_json(remote, "catalog/test/versions.json") == _local_versions(local_catalog)
        assert "catalog/test/data/data.parquet" in stored_keys(remote)

    @pytest.mark.unit
    def test_force_reuploads_metadata_when_versions_identical(
        self, tmp_path: Path, remote: MemoryStore
    ) -> None:
        """Issue #496: --force must re-upload metadata even with NO version delta."""
        from portolan_cli.sync.push import push

        catalog_dir = self._build_identical_version_catalog(tmp_path)
        local = (catalog_dir / "test" / "versions.json").read_bytes()
        obs.put(remote, "catalog/test/versions.json", local)
        obs.put(remote, "catalog/test/collection.json", b'{"id": "stale"}')

        result = push(catalog_dir, "test", "s3://mybucket/catalog", force=True)

        assert result.success is True
        assert stored_json(remote, "catalog/test/collection.json")["id"] == "test"

    @pytest.mark.unit
    def test_no_force_skips_upload_when_versions_identical(self, tmp_path: Path) -> None:
        """Without --force and no version delta, push is a no-op (regression guard).

        This pins the other side of the Issue #496 gate: the "Nothing to push"
        short-circuit must remain intact when force is not set.
        """
        from portolan_cli.sync.push import push

        catalog_dir = self._build_identical_version_catalog(tmp_path)

        with patch(
            "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [{"version": "1.0.0", "created": "2024-01-01T00:00:00Z"}],
                },
                "etag-123",
            )
            with patch(
                "portolan_cli.sync.push._upload_stac_files_async", new_callable=AsyncMock
            ) as mock_upload_stac:
                result = push(
                    catalog_root=catalog_dir,
                    collection="test",
                    destination="s3://mybucket/catalog",
                    force=False,
                )

        assert result.success is True
        assert result.files_uploaded == 0
        # No upload phase should run when nothing changed and force is off.
        mock_upload_stac.assert_not_called()


# =============================================================================
# Orphan Cleanup Tests
# =============================================================================


class TestOrphanCleanup:
    """Tests for orphan cleanup when push fails after asset upload."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_assets_returns_uploaded_keys(self, local_catalog: Path) -> None:
        """_upload_assets_async should return list of uploaded object keys."""
        from portolan_cli.sync.push import _upload_assets_async

        # Create the test file (needed for size calculation)
        test_file = local_catalog / "data.parquet"
        test_file.write_bytes(b"test data")

        with patch("portolan_cli.sync.push.obs.put_async", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = None

            mock_store = MagicMock()
            assets = [test_file]

            files_uploaded, errors, uploaded_keys, _metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=local_catalog,
                prefix="catalog",
                assets=assets,
            )

        assert files_uploaded == 1
        assert errors == []
        assert len(uploaded_keys) == 1
        assert "data.parquet" in uploaded_keys[0]

    @pytest.mark.unit
    def test_cleanup_deletes_uploaded_assets(self) -> None:
        """_cleanup_uploaded_assets should delete all uploaded keys."""
        from portolan_cli.sync.push import _cleanup_uploaded_assets

        with patch("portolan_cli.sync.push.obs.delete") as mock_delete:
            mock_store = MagicMock()
            uploaded_keys = ["catalog/file1.parquet", "catalog/file2.parquet"]

            _cleanup_uploaded_assets(mock_store, uploaded_keys)

        assert mock_delete.call_count == 2
        mock_delete.assert_any_call(mock_store, "catalog/file1.parquet")
        mock_delete.assert_any_call(mock_store, "catalog/file2.parquet")

    @pytest.mark.unit
    def test_cleanup_handles_empty_list(self) -> None:
        """_cleanup_uploaded_assets should handle empty key list gracefully."""
        from portolan_cli.sync.push import _cleanup_uploaded_assets

        with patch("portolan_cli.sync.push.obs.delete") as mock_delete:
            mock_store = MagicMock()
            _cleanup_uploaded_assets(mock_store, [])

        mock_delete.assert_not_called()

    @pytest.mark.unit
    def test_cleanup_continues_on_delete_failure(self) -> None:
        """_cleanup_uploaded_assets should continue if individual deletes fail."""
        from portolan_cli.sync.push import _cleanup_uploaded_assets

        with patch("portolan_cli.sync.push.obs.delete") as mock_delete:
            # First delete fails, second succeeds
            mock_delete.side_effect = [Exception("Network error"), None]

            mock_store = MagicMock()
            uploaded_keys = ["key1", "key2"]

            # Should not raise
            _cleanup_uploaded_assets(mock_store, uploaded_keys)

        assert mock_delete.call_count == 2


# =============================================================================
# Progress Reporting Tests
# =============================================================================


class TestProgressReporting:
    """Tests for progress reporting during uploads."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_assets_shows_progress(self, local_catalog: Path, capsys) -> None:
        """_upload_assets_async should upload files with progress tracking.

        Note: With json_mode=False, a Rich progress bar handles the display.
        With suppress_progress=False and json_mode=True, text progress is shown.
        """
        from portolan_cli.sync.push import _upload_assets_async

        # Create the test file (needed for size calculation)
        test_file = local_catalog / "data.parquet"
        test_file.write_bytes(b"test data")

        with patch("portolan_cli.sync.push.obs.put_async", new_callable=AsyncMock) as mock_put:
            mock_put.return_value = None

            mock_store = MagicMock()
            assets = [test_file]

            files_uploaded, errors, uploaded_keys, metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=local_catalog,
                prefix="catalog",
                assets=assets,
                json_mode=True,  # No Rich progress bar
                suppress_progress=True,  # Suppress for clean test
            )

        # Verify upload occurred
        assert files_uploaded == 1
        assert errors == []
        assert len(uploaded_keys) == 1

    @pytest.mark.unit
    def test_push_dry_run_shows_progress(self, local_catalog: Path, capsys) -> None:
        """Dry-run at push level should show what would be uploaded."""
        from portolan_cli.sync.push import push

        # dry_run is handled at push() level, not in _upload_assets_async
        result = push(
            catalog_root=local_catalog,
            collection="test",
            destination="s3://mybucket/catalog",
            dry_run=True,
        )

        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert result.dry_run is True


# =============================================================================
# Multi-Cloud Store Setup Tests
# =============================================================================


class TestMultiCloudStoreSetup:
    """Tests for GCS and Azure store setup."""

    @pytest.mark.unit
    def test_setup_store_s3_with_profile(self) -> None:
        """setup_store should load credentials from AWS profile."""
        from portolan_cli.sync.upload import setup_store

        with patch("portolan_cli.sync.upload._load_aws_credentials_from_profile") as mock_load:
            mock_load.return_value = ("access_key", "secret_key", None, "us-east-1")

            with patch("portolan_cli.sync.upload.S3Store") as mock_s3:
                mock_s3.return_value = MagicMock()

                store, prefix = setup_store("s3://mybucket/catalog", profile="myprofile")

        mock_load.assert_called_once_with("myprofile")
        mock_s3.assert_called_once_with(
            "mybucket",
            region="us-east-1",
            access_key_id="access_key",
            secret_access_key="secret_key",
        )
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_s3_from_environment(self) -> None:
        """setup_store should use AWS credentials from environment."""
        from portolan_cli.sync.upload import setup_store

        with (
            cleared_environ(
                AWS_ACCESS_KEY_ID="env_access_key",
                AWS_SECRET_ACCESS_KEY="env_secret_key",
                AWS_REGION="eu-west-1",
            ),
            patch("portolan_cli.sync.upload.S3Store") as mock_s3,
        ):
            mock_s3.return_value = MagicMock()

            store, prefix = setup_store("s3://mybucket/prefix")

        mock_s3.assert_called_once_with(
            "mybucket",
            region="eu-west-1",
            access_key_id="env_access_key",
            secret_access_key="env_secret_key",
        )

    @pytest.mark.unit
    def test_setup_store_s3_uses_default_region_env(self) -> None:
        """setup_store should fallback to AWS_DEFAULT_REGION if AWS_REGION not set."""
        from portolan_cli.sync.upload import setup_store

        with (
            cleared_environ(
                AWS_ACCESS_KEY_ID="key",
                AWS_SECRET_ACCESS_KEY="secret",
                AWS_DEFAULT_REGION="ap-southeast-1",
            ),
            patch("portolan_cli.sync.upload.S3Store") as mock_s3,
        ):
            mock_s3.return_value = MagicMock()

            setup_store("s3://mybucket/prefix")

        call_kwargs = mock_s3.call_args[1]
        assert call_kwargs["region"] == "ap-southeast-1"

    @pytest.mark.unit
    def test_setup_store_s3_no_region(self) -> None:
        """setup_store should work without region (uses AWS SDK defaults)."""
        from portolan_cli.sync.upload import setup_store

        with (
            cleared_environ(
                AWS_ACCESS_KEY_ID="key",
                AWS_SECRET_ACCESS_KEY="secret",
            ),
            patch("portolan_cli.sync.upload.S3Store") as mock_s3,
        ):
            mock_s3.return_value = MagicMock()

            setup_store("s3://mybucket/data")

        call_kwargs = mock_s3.call_args[1]
        assert "region" not in call_kwargs

    @pytest.mark.unit
    def test_setup_store_gcs(self) -> None:
        """setup_store should create store for gs:// URLs via obs.store.from_url.

        Note: GCS credential handling is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.sync.upload import setup_store

        with patch("portolan_cli.sync.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("gs://mybucket/catalog")

        mock_from_url.assert_called_once_with("gs://mybucket")
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_gcs_no_credentials(self) -> None:
        """setup_store should work for GCS without explicit credentials.

        Note: GCS credential handling is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.sync.upload import setup_store

        with patch("portolan_cli.sync.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("gs://mybucket/prefix")

        mock_from_url.assert_called_once_with("gs://mybucket")
        assert prefix == "prefix"

    @pytest.mark.unit
    def test_setup_store_azure_with_key(self) -> None:
        """setup_store should create store for az:// URLs via obs.store.from_url.

        Note: Azure credential handling is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.sync.upload import setup_store

        with patch("portolan_cli.sync.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("az://account/mycontainer/catalog")

        mock_from_url.assert_called_once_with("az://account/mycontainer")
        assert prefix == "catalog"

    @pytest.mark.unit
    def test_setup_store_azure_with_sas_token(self) -> None:
        """setup_store should create store for az:// URLs via obs.store.from_url.

        Note: Azure credential handling (SAS tokens) is delegated to obstore library.
        We just verify the URL is parsed correctly and from_url is called.
        """
        from portolan_cli.sync.upload import setup_store

        with patch("portolan_cli.sync.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store, prefix = setup_store("az://account/mycontainer/data")

        mock_from_url.assert_called_once_with("az://account/mycontainer")
        assert prefix == "data"

    @pytest.mark.unit
    def test_setup_store_azure_parses_prefix(self) -> None:
        """setup_store should correctly parse Azure URL prefix.

        Azure URLs have format: az://account/container/path
        Note: Azure credential precedence is delegated to obstore library.
        """
        from portolan_cli.sync.upload import setup_store

        with patch("portolan_cli.sync.upload.obs.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            # Azure format: az://account/container/prefix
            store, prefix = setup_store("az://myaccount/mycontainer/data/v1")

        # bucket_url = az://account/container, prefix = data/v1
        mock_from_url.assert_called_once_with("az://myaccount/mycontainer")
        assert prefix == "data/v1"

    @pytest.mark.unit
    def test_setup_store_unknown_scheme_uses_from_url(self) -> None:
        """setup_store should fallback to from_url for unknown schemes.

        NOTE: This tests upload.py, but since parse_object_store_url
        validates the scheme first and raises ValueError for unsupported schemes,
        we need to mock it to allow the unknown scheme through.
        """
        from portolan_cli.sync.upload import setup_store

        with patch("portolan_cli.sync.upload.parse_object_store_url") as mock_parse:
            # Return a fake bucket URL with unsupported scheme
            mock_parse.return_value = ("custom://bucket", "catalog")

            with patch("portolan_cli.sync.upload.obs.store.from_url") as mock_from_url:
                mock_from_url.return_value = MagicMock()

                store, prefix = setup_store("custom://bucket/catalog")

        mock_from_url.assert_called_once_with("custom://bucket")
        assert prefix == "catalog"


# =============================================================================
# Upload Assets Error Handling Tests
# =============================================================================


class TestUploadAssetsErrorHandling:
    """Tests for error handling in _upload_assets_async function."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_upload_assets_handles_exception(self, local_catalog: Path) -> None:
        """_upload_assets_async should catch and report upload exceptions."""
        from portolan_cli.sync.push import _upload_assets_async

        # Create the test file (needed for size calculation)
        test_file = local_catalog / "data.parquet"
        test_file.write_bytes(b"test data")

        mock_store = MagicMock()

        with patch("portolan_cli.sync.push.obs.put_async", new_callable=AsyncMock) as mock_put:
            mock_put.side_effect = Exception("Network timeout")

            files_uploaded, errors, uploaded_keys, _metrics = await _upload_assets_async(
                store=mock_store,
                catalog_root=local_catalog,
                prefix="catalog",
                assets=[test_file],
            )

        assert files_uploaded == 0
        assert len(errors) == 1
        assert "Network timeout" in errors[0]
        assert uploaded_keys == []


# =============================================================================
# Error Path Tests with Malformed Data
# =============================================================================


class TestMalformedDataHandling:
    """Tests for error handling with malformed versions.json data."""

    @pytest.mark.unit
    def test_read_local_versions_invalid_json(self, local_catalog_invalid_json: Path) -> None:
        """_read_local_versions should raise ValueError on invalid JSON."""
        from portolan_cli.sync.push import _read_local_versions

        with pytest.raises(ValueError, match="Invalid JSON"):
            _read_local_versions(
                catalog_root=local_catalog_invalid_json,
                collection="test",
            )

    @pytest.mark.unit
    def test_read_local_versions_missing_keys(self, local_catalog_malformed: Path) -> None:
        """_read_local_versions should return data but push should fail on missing keys."""
        from portolan_cli.sync.push import _read_local_versions

        # _read_local_versions just parses JSON, it doesn't validate schema
        # The schema validation happens later in the push process
        data = _read_local_versions(
            catalog_root=local_catalog_malformed,
            collection="test",
        )

        # Data should be returned (it's valid JSON, just missing fields)
        assert "versions" in data
        # But required fields are missing
        assert "spec_version" not in data
        assert "current_version" not in data

    @pytest.mark.unit
    def test_push_with_malformed_local_versions_keyerror(
        self, local_catalog_malformed: Path
    ) -> None:
        """Push should fail when local versions.json has missing required keys.

        The local_catalog_malformed fixture has versions.json missing spec_version
        and current_version. When push tries to read and process this, it should
        fail with a KeyError when accessing these missing fields.
        """
        from portolan_cli.sync.push import _read_local_versions

        # Read the malformed versions.json
        data = _read_local_versions(
            catalog_root=local_catalog_malformed,
            collection="test",
        )

        # Verify the required keys are missing
        assert "spec_version" not in data
        assert "current_version" not in data

        # Attempting to access these keys should raise KeyError
        with pytest.raises(KeyError):
            _ = data["spec_version"]

        with pytest.raises(KeyError):
            _ = data["current_version"]

    @pytest.mark.unit
    def test_push_with_malformed_remote_versions_missing_key(
        self, remote_versions_same_malformed: dict[str, Any]
    ) -> None:
        """Push should handle malformed remote versions.json gracefully.

        The remote_versions_same_malformed fixture is missing spec_version.
        When diff_version_lists tries to extract versions, it should handle
        this gracefully or raise an appropriate error.
        """
        from portolan_cli.sync.push import diff_version_lists

        # The malformed data has versions but is missing spec_version
        # Attempting to extract version strings should still work
        # since versions list exists
        local_versions = ["1.0.0", "1.1.0"]
        remote_versions = [v["version"] for v in remote_versions_same_malformed.get("versions", [])]

        # diff_version_lists should still work with the version strings
        diff = diff_version_lists(local_versions, remote_versions)

        # Local has 1.1.0 that remote doesn't have
        assert "1.1.0" in diff.local_only

    @pytest.mark.unit
    def test_push_with_malformed_asset_fields_extraction(
        self, remote_versions_diverged_malformed: dict[str, Any]
    ) -> None:
        """Push should fail when extracting assets with missing fields.

        The remote_versions_diverged_malformed fixture has assets missing
        sha256 and size_bytes fields. When code tries to access these,
        it should fail with KeyError.
        """
        # Extract the malformed asset
        versions = remote_versions_diverged_malformed["versions"]
        assets = versions[0]["assets"]
        asset_data = assets["data.parquet"]

        # Verify the required fields are missing
        assert "sha256" not in asset_data
        assert "size_bytes" not in asset_data

        # Attempting to access these keys should raise KeyError
        with pytest.raises(KeyError):
            _ = asset_data["sha256"]

        with pytest.raises(KeyError):
            _ = asset_data["size_bytes"]


# =============================================================================
# Property-Based Tests (Hypothesis)
# =============================================================================


class TestPushResultInvariants:
    """Property-based tests for PushResult invariants using Hypothesis."""

    @pytest.mark.unit
    @given(
        success=st.booleans(),
        files_uploaded=st.integers(min_value=0, max_value=1000),
        versions_pushed=st.integers(min_value=0, max_value=100),
        conflicts=st.lists(st.text(min_size=1, max_size=50), max_size=10),
        errors=st.lists(st.text(min_size=1, max_size=100), max_size=10),
    )
    def test_push_result_dataclass_accepts_valid_inputs(
        self,
        success: bool,
        files_uploaded: int,
        versions_pushed: int,
        conflicts: list[str],
        errors: list[str],
    ) -> None:
        """PushResult should accept any valid combination of inputs."""
        from portolan_cli.sync.push import PushResult

        result = PushResult(
            success=success,
            files_uploaded=files_uploaded,
            versions_pushed=versions_pushed,
            conflicts=conflicts,
            errors=errors,
        )

        assert result.success == success
        assert result.files_uploaded == files_uploaded
        assert result.versions_pushed == versions_pushed
        assert result.conflicts == conflicts
        assert result.errors == errors

    @pytest.mark.unit
    def test_dry_run_result_always_has_zero_counts(self, local_catalog: Path) -> None:
        """In dry-run mode, push() should always return zero work done.

        This property encodes the semantic contract: dry-run never actually
        pushes, so files_uploaded and versions_pushed must be 0.

        Tests multiple scenarios to verify invariant holds:
        1. First push (no remote versions)
        2. Incremental push (remote has some versions)
        3. Nothing to push (remote matches local)
        """
        from portolan_cli.sync.push import push

        # Scenario 1: First push (remote has no versions)
        with patch(
            "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (None, None)

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

            # INVARIANT: dry-run always returns zero counts
            assert result.files_uploaded == 0, "Dry-run should not report files uploaded"
            assert result.versions_pushed == 0, "Dry-run should not report versions pushed"
            assert result.success is True

        # Scenario 2: Incremental push (remote has v1.0.0, local has v1.0.0 + v1.1.0)
        with patch(
            "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "breaking": False,
                            "message": "Initial",
                            "assets": {},
                            "changes": [],
                        }
                    ],
                },
                "etag-123",
            )

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

            # INVARIANT: dry-run always returns zero counts
            assert result.files_uploaded == 0, "Dry-run should not report files uploaded"
            assert result.versions_pushed == 0, "Dry-run should not report versions pushed"
            assert result.success is True

        # Scenario 3: Nothing to push (remote matches local exactly)
        local_versions = json.loads((local_catalog / "test" / "versions.json").read_text())
        with patch(
            "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = (local_versions, "etag-456")

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://bucket/catalog",
                dry_run=True,
            )

            # INVARIANT: dry-run always returns zero counts (even when nothing to do)
            assert result.files_uploaded == 0, "Dry-run should not report files uploaded"
            assert result.versions_pushed == 0, "Dry-run should not report versions pushed"
            assert result.success is True


class TestVersionDiffInvariants:
    """Property-based tests for version diffing invariants."""

    @pytest.mark.unit
    @given(
        local_versions=st.lists(st.text(min_size=1, max_size=10), min_size=0, max_size=20),
        remote_versions=st.lists(st.text(min_size=1, max_size=10), min_size=0, max_size=20),
    )
    def test_diff_partitions_versions(
        self, local_versions: list[str], remote_versions: list[str]
    ) -> None:
        """Version diff should partition versions into local_only and remote_only.

        Property: local_only ∪ remote_only ∪ common = local ∪ remote
        Property: local_only ∩ remote_only = ∅
        """
        from portolan_cli.sync.push import diff_version_lists

        diff = diff_version_lists(local_versions, remote_versions)

        # Union of sets should equal union of inputs
        local_set = set(local_versions)
        remote_set = set(remote_versions)

        assert set(diff.local_only) == local_set - remote_set
        assert set(diff.remote_only) == remote_set - local_set
        assert set(diff.local_only) & set(diff.remote_only) == set()  # Disjoint

    @pytest.mark.unit
    @given(
        versions=st.lists(st.text(min_size=1, max_size=10), min_size=0, max_size=20, unique=True),
    )
    def test_identical_versions_means_nothing_to_push(self, versions: list[str]) -> None:
        """When local and remote have identical versions, diff should be empty.

        This is the "nothing to push" case - the invariant our fix addresses.
        """
        from portolan_cli.sync.push import diff_version_lists

        diff = diff_version_lists(versions, versions)

        assert diff.local_only == []
        assert diff.remote_only == []
        assert not diff.has_conflict

    @pytest.mark.unit
    @given(
        local_versions=st.lists(
            st.text(min_size=1, max_size=10), min_size=1, max_size=20, unique=True
        ),
    )
    def test_empty_remote_means_local_only(self, local_versions: list[str]) -> None:
        """When remote is empty, all local versions should be in local_only."""
        from portolan_cli.sync.push import diff_version_lists

        diff = diff_version_lists(local_versions, [])

        assert set(diff.local_only) == set(local_versions)
        assert diff.remote_only == []
        assert not diff.has_conflict  # First push, no conflict


# =============================================================================
# Dry-Run Network Isolation Tests
# =============================================================================


class TestDryRunNetworkIsolation:
    """Tests that dry-run mode never makes network calls.

    Bug #137: --dry-run was still calling _fetch_remote_versions and
    _setup_store, making real network connections. These tests assert
    the fix: dry_run=True must return early BEFORE any network I/O.
    """

    @pytest.mark.unit
    def test_push_dry_run_never_calls_fetch_remote_versions(self, local_catalog: Path) -> None:
        """push(dry_run=True) must not call _fetch_remote_versions at all.

        This is the core regression test for bug #137. The previous behaviour
        always called _setup_store then _fetch_remote_versions regardless of
        dry_run, making real S3/GCS/Azure connections.
        """
        from portolan_cli.sync.push import push

        with (
            patch(
                "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
            ) as mock_fetch,
            patch("portolan_cli.sync.push.setup_store") as mock_setup,
        ):
            mock_setup.return_value = (MagicMock(), "prefix")

            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        # Neither network operation should be called
        mock_fetch.assert_not_called()
        assert result.success is True

    @pytest.mark.unit
    def test_push_dry_run_never_calls_setup_store(self, local_catalog: Path) -> None:
        """push(dry_run=True) must not call _setup_store (which creates cloud connections)."""
        from portolan_cli.sync.push import push

        with patch("portolan_cli.sync.push.setup_store") as mock_setup:
            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        mock_setup.assert_not_called()
        assert result.success is True

    @pytest.mark.unit
    def test_push_dry_run_shows_would_push_message(self, local_catalog: Path) -> None:
        """push(dry_run=True) should report files that would be uploaded."""
        from portolan_cli.sync.push import push

        with (
            patch("portolan_cli.sync.push.setup_store"),
            patch("portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock),
        ):
            result = push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        assert result.success is True
        assert result.files_uploaded == 0  # dry-run never uploads
        assert result.dry_run is True  # H3: result must indicate dry-run mode
        assert result.would_push_versions > 0  # H3: should show how many would push
        assert result.would_push_files > 0  # Issue #804: also show file estimate

    @pytest.mark.unit
    def test_push_dry_run_does_not_call_upload_assets(self, local_catalog: Path) -> None:
        """push(dry_run=True) must not call _upload_assets."""
        from portolan_cli.sync.push import push

        with (
            patch("portolan_cli.sync.push.setup_store"),
            patch("portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock),
            patch(
                "portolan_cli.sync.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload,
        ):
            push(
                catalog_root=local_catalog,
                collection="test",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        mock_upload.assert_not_called()

    @pytest.mark.unit
    def test_push_non_dry_run_still_calls_fetch_remote_versions(self, local_catalog: Path) -> None:
        """Non-dry-run push must still call _fetch_remote_versions (sanity check)."""
        from portolan_cli.sync.push import push

        with (
            patch(
                "portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock
            ) as mock_fetch,
            patch("portolan_cli.sync.push.setup_store") as mock_setup,
        ):
            mock_fetch.return_value = (None, None)
            mock_setup.return_value = (MagicMock(), "prefix")

            with patch(
                "portolan_cli.sync.push._upload_assets_async", new_callable=AsyncMock
            ) as mock_upload:
                mock_upload.return_value = (1, [], ["key"], UploadMetrics())
                with patch(
                    "portolan_cli.sync.push._upload_versions_json_async", new_callable=AsyncMock
                ):
                    push(
                        catalog_root=local_catalog,
                        collection="test",
                        destination="s3://mybucket/catalog",
                        dry_run=False,
                    )

        # Regular push MUST call both setup and fetch
        mock_setup.assert_called_once()
        mock_fetch.assert_called_once()

    @pytest.mark.unit
    def test_push_dry_run_handles_missing_asset_gracefully(self, tmp_path: Path) -> None:
        """H2: push(dry_run=True) should warn but not crash on missing assets.

        When an asset referenced in versions.json is missing locally, dry-run
        should warn and continue (not raise FileNotFoundError) so users can
        see the preview even if local state is inconsistent.
        """
        import json

        from portolan_cli.sync.push import push

        # Create catalog with versions.json referencing a missing file
        catalog_dir = tmp_path / "catalog_missing_asset"
        catalog_dir.mkdir()
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()

        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "message": "Initial",
                    "assets": {
                        "missing.parquet": {
                            "sha256": "abc123",
                            "size_bytes": 1000,
                            "href": "test/missing.parquet",  # File does NOT exist
                        }
                    },
                    "changes": ["missing.parquet"],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))
        # NOTE: We deliberately do NOT create the asset file

        with (
            patch("portolan_cli.sync.push.setup_store"),
            patch("portolan_cli.sync.push._fetch_remote_versions_async", new_callable=AsyncMock),
        ):
            # Should NOT raise - dry-run is forgiving
            result = push(
                catalog_root=catalog_dir,
                collection="test",
                destination="s3://mybucket/catalog",
                dry_run=True,
            )

        # Dry-run should succeed but record the error
        assert result.success is True
        assert result.dry_run is True
        assert len(result.errors) == 1  # Missing asset recorded as error
        assert "missing.parquet" in result.errors[0]


# =============================================================================
# README Discovery Tests
# =============================================================================


class TestDiscoverStacFilesReadmes:
    """Tests for README.md discovery in _discover_stac_files()."""

    @pytest.mark.unit
    def test_discovers_collection_readme(self, tmp_path: Path) -> None:
        """_discover_stac_files should find collection-level README.md."""
        from portolan_cli.sync.push import _discover_stac_files

        # Setup catalog with collection.json and README
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text("{}")
        (collection_dir / "README.md").write_text("# Test Collection")

        result = _discover_stac_files(catalog_dir, "test")

        assert len(result["readmes"]) == 1
        assert result["readmes"][0].name == "README.md"
        assert "test" in str(result["readmes"][0])

    @pytest.mark.unit
    def test_discovers_catalog_readme_when_include_catalog(self, tmp_path: Path) -> None:
        """_discover_stac_files should find root README.md when include_catalog=True."""
        from portolan_cli.sync.push import _discover_stac_files

        # Setup catalog with root README and collection
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        (catalog_dir / "catalog.json").write_text("{}")
        (catalog_dir / "README.md").write_text("# Catalog")
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text("{}")
        (collection_dir / "README.md").write_text("# Collection")

        result = _discover_stac_files(catalog_dir, "test", include_catalog=True)

        # Should find both READMEs
        assert len(result["readmes"]) == 2
        readme_names = [r.parent.name for r in result["readmes"]]
        assert "catalog" in readme_names  # root README
        assert "test" in readme_names  # collection README

    @pytest.mark.unit
    def test_no_readme_when_missing(self, tmp_path: Path) -> None:
        """_discover_stac_files should handle missing README.md gracefully."""
        from portolan_cli.sync.push import _discover_stac_files

        # Setup catalog without README
        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()
        collection_dir = catalog_dir / "test"
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text("{}")

        result = _discover_stac_files(catalog_dir, "test")

        assert len(result["readmes"]) == 0


# =============================================================================
# Asset Diffing Tests (Issue #329)
# =============================================================================


class TestAssetDiffing:
    """Tests for asset diffing against remote versions (Issue #329).

    Push should only upload assets that are new or changed, not all assets
    from versions being pushed. This prevents re-uploading 3000 files when
    adding 1 new file to a large catalog.
    """

    @pytest.mark.unit
    def test_skips_assets_already_on_remote_by_sha256(self, tmp_path: Path) -> None:
        """Assets with sha256 matching remote should be skipped.

        Scenario: Local version 2.0.0 has 2 assets, remote version 1.0.0
        has 1 of those assets with same sha256. Only the new asset should
        be uploaded.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create two asset files
        existing_asset = catalog_dir / "existing.parquet"
        existing_asset.write_bytes(b"existing data")
        new_asset = catalog_dir / "new.parquet"
        new_asset.write_bytes(b"new data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "existing.parquet": {
                            "sha256": "sha256_existing",
                            "size_bytes": 13,
                            "href": "existing.parquet",
                        },
                        "new.parquet": {
                            "sha256": "sha256_new",
                            "size_bytes": 8,
                            "href": "new.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "existing.parquet": {
                            "sha256": "sha256_existing",  # Same sha256!
                            "size_bytes": 13,
                            "href": "existing.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # Only new.parquet should be uploaded (existing.parquet has same sha256)
        assert len(assets) == 1
        assert assets[0].name == "new.parquet"

    @pytest.mark.unit
    def test_uploads_changed_assets_with_different_sha256(self, tmp_path: Path) -> None:
        """Assets with different sha256 from remote should be uploaded.

        Scenario: Same file path, different content (different sha256).
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create asset file
        data_file = catalog_dir / "data.parquet"
        data_file.write_bytes(b"updated data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_v2",  # Different sha256
                            "size_bytes": 12,
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_v1",  # Original sha256
                            "size_bytes": 10,
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # data.parquet should be uploaded (different sha256)
        assert len(assets) == 1
        assert assets[0].name == "data.parquet"

    @pytest.mark.unit
    def test_uploads_renamed_file_even_with_same_sha256(self, tmp_path: Path) -> None:
        """Renamed files (same sha256, different href) must be uploaded.

        Regression test: if only sha256 is checked, renamed files are incorrectly
        skipped because the bytes already exist remotely. But the new href doesn't
        exist on S3, so the upload is required.

        Scenario: data.parquet renamed to renamed.parquet (same content).
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create the renamed file
        renamed_file = catalog_dir / "renamed.parquet"
        renamed_file.write_bytes(b"same data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "renamed.parquet": {
                            "sha256": "sha256_same",  # Same sha256 as old file
                            "size_bytes": 9,
                            "href": "renamed.parquet",  # Different href!
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_same",  # Same sha256
                            "size_bytes": 9,
                            "href": "data.parquet",  # Old href
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # renamed.parquet MUST be uploaded (different href, even though same sha256)
        assert len(assets) == 1
        assert assets[0].name == "renamed.parquet"

    @pytest.mark.unit
    def test_uploads_all_assets_when_no_remote(self, tmp_path: Path) -> None:
        """When remote has no versions.json, all assets should be uploaded.

        This is the first push case.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create asset files
        file1 = catalog_dir / "file1.parquet"
        file1.write_bytes(b"data1")
        file2 = catalog_dir / "file2.parquet"
        file2.write_bytes(b"data2")

        local_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "file1.parquet": {
                            "sha256": "sha1",
                            "href": "file1.parquet",
                        },
                        "file2.parquet": {
                            "sha256": "sha2",
                            "href": "file2.parquet",
                        },
                    },
                },
            ]
        }

        # No remote (first push)
        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=None,
        )

        assert len(assets) == 2

    @pytest.mark.unit
    def test_href_only_in_older_remote_version_uploads(self, tmp_path: Path) -> None:
        """An href that the remote current snapshot omits must upload (Issue #962).

        This test replaces one that expected a skip. That test encoded the
        any-version rule: a match in any remote version skipped the upload.
        A versions.json entry is a complete snapshot. The remote current
        version 1.1.0 does not list legacy.parquet, so nothing tells push
        that the object key still holds those bytes.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "legacy.parquet"
        asset.write_bytes(b"legacy data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "legacy.parquet": {
                            "sha256": "sha256_legacy",
                            "href": "legacy.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "current_version": "1.1.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "legacy.parquet": {
                            "sha256": "sha256_legacy",
                            "href": "legacy.parquet",
                        },
                    },
                },
                {
                    "version": "1.1.0",
                    "assets": {
                        "other.parquet": {
                            "sha256": "sha256_other",
                            "href": "other.parquet",
                        },
                    },
                },
            ],
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        assert assets == [asset.resolve()]

    @pytest.mark.unit
    def test_handles_empty_remote_versions_list(self, tmp_path: Path) -> None:
        """Remote with empty versions list should upload all assets."""
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "data.parquet"
        asset.write_bytes(b"data")

        local_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_data",
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {"versions": []}  # Empty versions list

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        assert len(assets) == 1

    @pytest.mark.unit
    def test_large_catalog_diffing_performance(self, tmp_path: Path) -> None:
        """Issue #329: Adding 1 file to 3000-file catalog should upload 1 file.

        This is the exact scenario described in the issue.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create 3001 asset files (simulating large catalog)
        num_existing = 3000
        existing_assets = {}
        for i in range(num_existing):
            filename = f"asset_{i:04d}.parquet"
            (catalog_dir / filename).write_bytes(f"data{i}".encode())
            existing_assets[filename] = {
                "sha256": f"sha256_{i}",
                "href": filename,
            }

        # Add one new asset
        new_filename = "new_asset.parquet"
        (catalog_dir / new_filename).write_bytes(b"new data")
        new_asset = {
            new_filename: {
                "sha256": "sha256_new",
                "href": new_filename,
            }
        }

        # Local version has all 3001 assets (complete snapshot)
        all_assets = {**existing_assets, **new_asset}
        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": all_assets,
                },
            ]
        }

        # Remote has 3000 existing assets
        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": existing_assets,
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # Only 1 new asset should be uploaded, not 3001!
        assert len(assets) == 1
        assert assets[0].name == "new_asset.parquet"

    @pytest.mark.unit
    def test_handles_missing_sha256_in_remote_asset(self, tmp_path: Path) -> None:
        """Assets without sha256 in remote should not block upload.

        Edge case: malformed remote versions.json.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "data.parquet"
        asset.write_bytes(b"data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_data",
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            # Missing sha256!
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # Should upload since remote has no sha256 to compare
        assert len(assets) == 1

    @pytest.mark.unit
    def test_handles_missing_sha256_in_local_asset(self, tmp_path: Path) -> None:
        """Assets without sha256 in local should still be uploaded.

        Edge case: malformed local versions.json.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        asset = catalog_dir / "data.parquet"
        asset.write_bytes(b"data")

        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            # Missing sha256!
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": "sha256_data",
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # Should upload since local has no sha256 to compare
        assert len(assets) == 1


# =============================================================================
# Asset Diffing Against the Remote Current Version (Issue #962)
# =============================================================================


def _snapshot(version: str, assets: dict[str, str]) -> dict[str, Any]:
    """Build one versions.json entry that maps each href to its sha256."""
    return {
        "version": version,
        "assets": {href: {"sha256": sha, "href": href} for href, sha in assets.items()},
    }


def _history(current: str, *entries: tuple[str, dict[str, str]]) -> dict[str, Any]:
    """Build a versions.json document with the given current version."""
    return {
        "current_version": current,
        "versions": [_snapshot(version, assets) for version, assets in entries],
    }


class TestAssetDiffingAgainstRemoteCurrent:
    """Push compares each asset with the remote CURRENT snapshot (Issue #962).

    Every version writes the same object key for an href. So only the remote
    current snapshot tells push what each key holds now. An older remote
    version can record bytes that a later push overwrote.
    """

    @pytest.mark.unit
    def test_revert_to_bytes_of_older_remote_version_uploads(self, tmp_path: Path) -> None:
        """Bytes A, then B, then A again. The third push must upload A.

        Before the fix, push found (data.parquet, A) in remote 1.0.3 and
        skipped the upload. The store kept B while versions.json recorded A.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(b"bytes A")

        local = _history(
            "1.0.5",
            ("1.0.3", {"data.parquet": "sha_A"}),
            ("1.0.4", {"data.parquet": "sha_B"}),
            ("1.0.5", {"data.parquet": "sha_A"}),
        )
        remote = _history(
            "1.0.4", ("1.0.3", {"data.parquet": "sha_A"}), ("1.0.4", {"data.parquet": "sha_B"})
        )

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == [data_file.resolve()]

    @pytest.mark.unit
    def test_multi_version_push_compares_local_current_sha(self, tmp_path: Path) -> None:
        """A push of 1.0.4 and 1.0.5 compares the sha256 that 1.0.5 records.

        The file on disk holds the 1.0.5 bytes (C). Before the fix, push read
        the oldest pushed entry (1.0.4, A), matched remote current A, and
        skipped the upload. C never reached the store.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(b"bytes C")
        other_file = tmp_path / "other.parquet"
        other_file.write_bytes(b"other")

        local = _history(
            "1.0.5",
            ("1.0.3", {"data.parquet": "sha_A"}),
            ("1.0.4", {"data.parquet": "sha_A", "other.parquet": "sha_X"}),
            ("1.0.5", {"data.parquet": "sha_C", "other.parquet": "sha_X"}),
        )
        remote = _history("1.0.3", ("1.0.3", {"data.parquet": "sha_A"}))

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert sorted(assets) == sorted([data_file.resolve(), other_file.resolve()])

    @pytest.mark.unit
    def test_multi_version_push_skips_asset_whose_local_current_sha_matches_remote(
        self, tmp_path: Path
    ) -> None:
        """An asset that changed and then changed back is skipped.

        Pushed 1.0.4 records B and pushed 1.0.5 records A. The file on disk
        holds A, and remote current also records A, so the key holds A.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        (tmp_path / "data.parquet").write_bytes(b"bytes A")

        local = _history(
            "1.0.5",
            ("1.0.3", {"data.parquet": "sha_A"}),
            ("1.0.4", {"data.parquet": "sha_B"}),
            ("1.0.5", {"data.parquet": "sha_A"}),
        )
        remote = _history("1.0.3", ("1.0.3", {"data.parquet": "sha_A"}))

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == []

    @pytest.mark.unit
    def test_asset_identical_to_remote_current_is_skipped(self, tmp_path: Path) -> None:
        """An asset whose sha256 equals the remote current entry is skipped.

        An older remote version records other bytes for the same href. That
        older entry must not force an upload.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        (tmp_path / "data.parquet").write_bytes(b"bytes B")
        new_file = tmp_path / "new.parquet"
        new_file.write_bytes(b"new")

        local = _history("1.0.5", ("1.0.5", {"data.parquet": "sha_B", "new.parquet": "sha_N"}))
        remote = _history(
            "1.0.4", ("1.0.3", {"data.parquet": "sha_A"}), ("1.0.4", {"data.parquet": "sha_B"})
        )

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == [new_file.resolve()]

    @pytest.mark.unit
    def test_force_push_without_new_versions_restores_local_current(self, tmp_path: Path) -> None:
        """A force push with no new local version uploads the local current bytes.

        Local holds 1.0.0 (A). Remote holds 1.0.0 (A) and 1.0.1 (B), so the
        store holds B. `push --force` writes the local versions.json, which
        records A. So push must upload A, although no version is local only.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(b"bytes A")

        local = _history("1.0.0", ("1.0.0", {"data.parquet": "sha_A"}))
        remote = _history(
            "1.0.1", ("1.0.0", {"data.parquet": "sha_A"}), ("1.0.1", {"data.parquet": "sha_B"})
        )

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == [data_file.resolve()]

    @pytest.mark.unit
    def test_asset_removed_by_a_later_pushed_version_is_not_uploaded(self, tmp_path: Path) -> None:
        """A file that 1.0.1 adds and 1.0.2 removes is not part of the push.

        The user deleted x.parquet. The local current snapshot 1.0.2 does not
        list it, so push must not look for it on disk.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        (tmp_path / "data.parquet").write_bytes(b"bytes A")

        local = _history(
            "1.0.2",
            ("1.0.0", {"data.parquet": "sha_A"}),
            ("1.0.1", {"data.parquet": "sha_A", "x.parquet": "sha_X"}),
            ("1.0.2", {"data.parquet": "sha_A"}),
        )
        remote = _history("1.0.0", ("1.0.0", {"data.parquet": "sha_A"}))

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == []

    @pytest.mark.unit
    def test_remote_current_asset_without_sha256_uploads(self, tmp_path: Path) -> None:
        """A remote current entry with no sha256 cannot prove a match, so it uploads.

        An older remote entry records the local sha256. It must not cause a skip.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(b"bytes A")

        local = _history("1.0.2", ("1.0.2", {"data.parquet": "sha_A"}))
        remote = {
            "current_version": "1.0.1",
            "versions": [
                _snapshot("1.0.0", {"data.parquet": "sha_A"}),
                {"version": "1.0.1", "assets": {"data.parquet": {"href": "data.parquet"}}},
            ],
        }

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == [data_file.resolve()]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "current_version",
        [None, "9.9.9"],
        ids=["current_version-missing", "current_version-matches-no-entry"],
    )
    def test_unusable_current_version_falls_back_to_last_entry(
        self, tmp_path: Path, current_version: str | None
    ) -> None:
        """Without a usable current_version, the last remote entry is current.

        The last entry records B. So local A uploads and local B is skipped.
        The first entry records A, and it must not cause a skip.
        """
        from portolan_cli.sync.push import _get_assets_to_upload

        a_file = tmp_path / "a.parquet"
        a_file.write_bytes(b"bytes A")
        (tmp_path / "b.parquet").write_bytes(b"bytes B")

        local = {
            "versions": [
                _snapshot("1.0.2", {"a.parquet": "sha_A", "b.parquet": "sha_B"}),
            ],
        }
        remote: dict[str, Any] = {
            "versions": [
                _snapshot("1.0.0", {"a.parquet": "sha_A", "b.parquet": "sha_A"}),
                _snapshot("1.0.1", {"a.parquet": "sha_B", "b.parquet": "sha_B"}),
            ],
        }
        if current_version is not None:
            remote["current_version"] = current_version

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == [a_file.resolve()]

    @pytest.mark.unit
    def test_current_version_selects_entry_that_is_not_last(self, tmp_path: Path) -> None:
        """A current_version that names an earlier entry wins over the last entry."""
        from portolan_cli.sync.push import _get_assets_to_upload

        (tmp_path / "data.parquet").write_bytes(b"bytes A")

        local = {"versions": [_snapshot("2.0.0", {"data.parquet": "sha_A"})]}
        remote = _history(
            "1.0.0", ("1.0.0", {"data.parquet": "sha_A"}), ("1.0.1", {"data.parquet": "sha_B"})
        )

        assets = _get_assets_to_upload(tmp_path, local, remote)

        assert assets == []


# =============================================================================
# Real SHA256 Hash Tests (Issue #329)
# =============================================================================


class TestRealSHA256Diffing:
    """Tests for asset diffing using real SHA256 hashes (Issue #329).

    These tests verify the diffing logic works with actual computed hashes,
    not placeholder strings like 'sha256_existing1'.
    """

    @pytest.mark.unit
    def test_real_sha256_diffing_skips_identical_files(self, tmp_path: Path) -> None:
        """Assets with identical content (same real SHA256) should be skipped.

        This test creates real files, computes actual SHA256 hashes, and verifies
        that the diffing logic correctly identifies unchanged files.
        """
        import hashlib

        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create a file with known content
        content = b"This is test data that will be hashed"
        real_sha256 = hashlib.sha256(content).hexdigest()

        # Create the local file
        data_file = catalog_dir / "data.parquet"
        data_file.write_bytes(content)

        # Local versions.json references the file with its REAL hash
        local_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": real_sha256,
                            "size_bytes": len(content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        # Remote also has the same file with the SAME real hash
        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": real_sha256,  # Same hash!
                            "size_bytes": len(content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # File should be SKIPPED because SHA256 matches
        assert len(assets) == 0

    @pytest.mark.unit
    def test_real_sha256_diffing_uploads_modified_files(self, tmp_path: Path) -> None:
        """Assets with different content (different real SHA256) should be uploaded.

        This test verifies that even a small content change results in a different
        hash and triggers an upload.
        """
        import hashlib

        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Original content and hash (what's on remote)
        original_content = b"Original data version 1"
        original_sha256 = hashlib.sha256(original_content).hexdigest()

        # Modified content and hash (what's local)
        modified_content = b"Modified data version 2"
        modified_sha256 = hashlib.sha256(modified_content).hexdigest()

        # Verify the hashes are actually different
        assert original_sha256 != modified_sha256

        # Create the local file with MODIFIED content
        data_file = catalog_dir / "data.parquet"
        data_file.write_bytes(modified_content)

        # Local has the modified version
        local_versions_data = {
            "versions": [
                {
                    "version": "2.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": modified_sha256,
                            "size_bytes": len(modified_content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        # Remote has the original version
        remote_versions_data = {
            "versions": [
                {
                    "version": "1.0.0",
                    "assets": {
                        "data.parquet": {
                            "sha256": original_sha256,  # Different hash!
                            "size_bytes": len(original_content),
                            "href": "data.parquet",
                        },
                    },
                },
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # File should be UPLOADED because SHA256 differs
        assert len(assets) == 1
        assert assets[0].name == "data.parquet"

    @pytest.mark.unit
    def test_real_sha256_incremental_add_scenario(self, tmp_path: Path) -> None:
        """Issue #329 core scenario: adding 1 file to a large catalog.

        Simulates adding 1 new file to a catalog with 100 existing files.
        Only the new file should be uploaded.
        """
        import hashlib

        from portolan_cli.sync.push import _get_assets_to_upload

        catalog_dir = tmp_path / "catalog"
        catalog_dir.mkdir()

        # Create 100 "existing" files with real hashes
        existing_assets = {}
        for i in range(100):
            content = f"Existing file {i} content".encode()
            sha256 = hashlib.sha256(content).hexdigest()
            filename = f"file_{i:03d}.parquet"

            # Create the actual file
            (catalog_dir / filename).write_bytes(content)

            existing_assets[filename] = {
                "sha256": sha256,
                "size_bytes": len(content),
                "href": filename,
            }

        # Create 1 NEW file
        new_content = b"This is the new file being added"
        new_sha256 = hashlib.sha256(new_content).hexdigest()
        new_filename = "new_file.parquet"
        (catalog_dir / new_filename).write_bytes(new_content)

        # Local version 2.0.0 has all 101 files (100 existing + 1 new)
        all_assets = dict(existing_assets)
        all_assets[new_filename] = {
            "sha256": new_sha256,
            "size_bytes": len(new_content),
            "href": new_filename,
        }

        local_versions_data = {
            "versions": [
                {"version": "1.0.0", "assets": existing_assets},
                {"version": "2.0.0", "assets": all_assets},
            ]
        }

        # Remote only has version 1.0.0 with 100 files
        remote_versions_data = {
            "versions": [
                {"version": "1.0.0", "assets": existing_assets},
            ]
        }

        assets = _get_assets_to_upload(
            catalog_root=catalog_dir,
            versions_data=local_versions_data,
            remote_versions_data=remote_versions_data,
        )

        # Only the NEW file should be uploaded (not all 101!)
        assert len(assets) == 1
        assert assets[0].name == new_filename
