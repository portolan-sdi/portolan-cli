"""Unit tests for versioning backend protocol and plugin discovery.

Tests the VersioningBackend protocol and get_backend() discovery function
that allows external backends (like portolake) to integrate with portolan-cli.

"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from portolan_cli.backends import get_backend
from portolan_cli.backends.json_file import JsonFileBackend
from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint, VersioningBackend
from portolan_cli.hookspecs import hookimpl
from portolan_cli.plugins import manager

if TYPE_CHECKING:
    from collections.abc import Callable


class TestVersioningBackendProtocol:
    """Tests for the VersioningBackend protocol definition."""

    @pytest.mark.unit
    def test_json_file_backend_is_protocol_compliant(self) -> None:
        """JsonFileBackend passes isinstance check against VersioningBackend protocol.

        This uses @runtime_checkable to verify protocol compliance at runtime,
        which is more meaningful than hasattr checks on the Protocol class itself.
        """
        backend: VersioningBackend = JsonFileBackend()
        assert isinstance(backend, VersioningBackend)

    @pytest.mark.unit
    def test_protocol_is_runtime_checkable(self) -> None:
        """VersioningBackend has @runtime_checkable decorator for isinstance checks."""
        # Protocol must be runtime_checkable for isinstance to work
        assert hasattr(VersioningBackend, "__protocol_attrs__")

    @pytest.mark.unit
    def test_non_compliant_class_fails_isinstance(self) -> None:
        """Classes not implementing the protocol fail isinstance checks."""

        class NotABackend:
            pass

        obj = NotABackend()
        assert not isinstance(obj, VersioningBackend)

    @pytest.mark.unit
    def test_partial_implementation_fails_isinstance(self) -> None:
        """Partially implemented classes fail isinstance checks."""

        class PartialBackend:
            def get_current_version(self, collection: str) -> Any:
                pass

            # Missing other required methods

        obj = PartialBackend()
        # runtime_checkable only checks method names exist, not all of them
        # but at least this documents the expected behavior
        assert not isinstance(obj, VersioningBackend)


class TestTypedDicts:
    """Tests for TypedDict definitions used in the protocol."""

    @pytest.mark.unit
    def test_drift_report_has_required_keys(self) -> None:
        """DriftReport TypedDict defines expected keys."""
        # Create a valid DriftReport
        report: DriftReport = {
            "has_drift": False,
            "local_version": "1.0.0",
            "remote_version": "1.0.0",
            "message": "No drift detected",
        }
        assert report["has_drift"] is False
        assert report["local_version"] == "1.0.0"
        assert report["remote_version"] == "1.0.0"
        assert report["message"] == "No drift detected"

    @pytest.mark.unit
    def test_drift_report_allows_none_versions(self) -> None:
        """DriftReport allows None for version fields (no versions yet)."""
        report: DriftReport = {
            "has_drift": False,
            "local_version": None,
            "remote_version": None,
            "message": "No versions exist",
        }
        assert report["local_version"] is None
        assert report["remote_version"] is None

    @pytest.mark.unit
    def test_schema_fingerprint_has_required_keys(self) -> None:
        """SchemaFingerprint TypedDict defines expected keys."""
        fingerprint: SchemaFingerprint = {
            "columns": ["id", "geometry", "name"],
            "types": {"id": "int64", "geometry": "geometry", "name": "string"},
            "hash": "abc123",
        }
        assert fingerprint["columns"] == ["id", "geometry", "name"]
        assert fingerprint["types"]["geometry"] == "geometry"
        assert fingerprint["hash"] == "abc123"


class TestGetBackend:
    """Tests for the get_backend() discovery function."""

    @pytest.mark.unit
    def test_get_backend_file_returns_json_file_backend(self) -> None:
        """get_backend('file') returns a JsonFileBackend instance."""
        backend = get_backend("file")
        assert isinstance(backend, JsonFileBackend)

    @pytest.mark.unit
    def test_get_backend_default_is_file(self) -> None:
        """get_backend() without arguments defaults to 'file' backend."""
        backend = get_backend()
        assert isinstance(backend, JsonFileBackend)

    @pytest.mark.unit
    def test_get_backend_unknown_raises_value_error(self) -> None:
        """get_backend('unknown') raises ValueError for unregistered backends."""
        with pytest.raises(ValueError, match="Unknown backend: unknown"):
            get_backend("unknown")

    @pytest.mark.unit
    def test_get_backend_error_lists_available_backends(self) -> None:
        """ValueError message includes list of available backends."""
        with pytest.raises(ValueError) as exc_info:
            get_backend("nonexistent")
        # Should mention 'file' as available
        assert "file" in str(exc_info.value)

    @pytest.mark.unit
    def test_get_backend_creates_new_instance_each_call(self) -> None:
        """get_backend() creates a new instance on each call (not singleton)."""
        backend1 = get_backend("file")
        backend2 = get_backend("file")
        assert backend1 is not backend2

    @pytest.mark.unit
    def test_get_backend_empty_name_raises_value_error(self) -> None:
        """get_backend('') raises ValueError for empty string."""
        with pytest.raises(ValueError, match="Unknown backend:"):
            get_backend("")

    @pytest.mark.unit
    def test_get_backend_file_passes_catalog_root(self, tmp_path: Any) -> None:
        """get_backend('file', catalog_root=path) passes root to JsonFileBackend."""
        from pathlib import Path

        backend = get_backend("file", catalog_root=Path(tmp_path))
        assert isinstance(backend, JsonFileBackend)
        assert backend._catalog_root == Path(tmp_path)


class _BackendPlugin:
    """A plugin object whose `portolan_backends` hook returns fixed factories."""

    def __init__(self, factories: dict[str, Any]) -> None:
        self._factories = factories

    @hookimpl
    def portolan_backends(self) -> dict[str, Any]:
        return self._factories


class _RaisingPlugin:
    """A plugin whose `portolan_backends` hook raises."""

    @hookimpl
    def portolan_backends(self) -> dict[str, Any]:
        raise RuntimeError("backend registry is broken")


class TestGetBackendPlugins:
    """`get_backend` finds backends through the `portolan_backends` hook (#947)."""

    @pytest.mark.unit
    def test_plugin_backend_receives_catalog_root(
        self, register_plugin: Callable[..., None]
    ) -> None:
        """The factory is called with `catalog_root`."""
        from pathlib import Path

        mock_backend = MagicMock(spec=VersioningBackend)
        factory = MagicMock(return_value=mock_backend)
        register_plugin(_BackendPlugin({"custom": factory}), "test-plugin")
        root = Path("/some/catalog")

        backend = get_backend("custom", catalog_root=root)

        factory.assert_called_once_with(catalog_root=root)
        assert backend is mock_backend

    @pytest.mark.unit
    def test_unknown_name_lists_plugin_backends(self, register_plugin: Callable[..., None]) -> None:
        """The error names every backend the hooks registered."""
        register_plugin(_BackendPlugin({"custom": MagicMock()}), "test-plugin")

        with pytest.raises(
            ValueError, match=r"^Unknown backend: nope\. Available: custom, file, iceberg$"
        ):
            get_backend("nope")

    @pytest.mark.unit
    def test_hook_that_raises_is_skipped_with_a_warning(
        self, register_plugin: Callable[..., None]
    ) -> None:
        """One broken plugin does not stop the built-in backends."""
        register_plugin(_RaisingPlugin(), "broken-plugin")

        with pytest.warns(manager.PluginWarning, match="broken-plugin.*backend registry is broken"):
            backend = get_backend("file")

        assert isinstance(backend, JsonFileBackend)

    @pytest.mark.unit
    def test_builtin_file_backend_wins_a_name_collision(
        self, register_plugin: Callable[..., None]
    ) -> None:
        """A plugin cannot replace the built-in `file` backend."""
        register_plugin(_BackendPlugin({"file": MagicMock()}), "shadow-plugin")

        with pytest.warns(manager.PluginWarning, match="shadow-plugin.*'file'"):
            backend = get_backend("file")

        assert isinstance(backend, JsonFileBackend)


class TestGetBackendErrorHandling:
    """Tests for error handling around plugin backends."""

    @pytest.mark.unit
    def test_get_backend_handles_exception_on_instantiation(
        self, register_plugin: Callable[..., None]
    ) -> None:
        """A factory that raises becomes a ValueError that names the backend."""
        register_plugin(
            _BackendPlugin({"failing": MagicMock(side_effect=TypeError("numpy not found"))}),
            "test-plugin",
        )

        with pytest.raises(
            ValueError, match=r"^Failed to instantiate backend 'failing': numpy not found$"
        ):
            get_backend("failing")

    @pytest.mark.unit
    def test_get_backend_validates_protocol_compliance(
        self, register_plugin: Callable[..., None]
    ) -> None:
        """get_backend() validates that a plugin backend implements the protocol."""

        class NotABackend:
            def __init__(self, **_kwargs: Any) -> None:
                pass

        register_plugin(_BackendPlugin({"invalid": NotABackend}), "test-plugin")

        with pytest.raises(ValueError, match="does not implement VersioningBackend"):
            get_backend("invalid")


class TestJsonFileBackend:
    """Tests for the JsonFileBackend implementation."""

    @pytest.mark.unit
    def test_json_file_backend_instantiates(self) -> None:
        """JsonFileBackend can be instantiated without arguments."""
        backend = JsonFileBackend()
        assert backend is not None

    @pytest.mark.unit
    def test_json_file_backend_isinstance_versioning_backend(self) -> None:
        """JsonFileBackend is recognized as VersioningBackend via isinstance."""
        backend = JsonFileBackend()
        assert isinstance(backend, VersioningBackend)

    @pytest.mark.unit
    def test_json_file_backend_accepts_catalog_root(self, tmp_path: Any) -> None:
        """JsonFileBackend accepts catalog_root parameter."""
        from pathlib import Path

        backend = JsonFileBackend(catalog_root=Path(tmp_path))
        assert backend._catalog_root == Path(tmp_path)

    @pytest.mark.unit
    def test_empty_collection_name_rejected(self) -> None:
        """Empty collection name raises ValueError."""
        backend = JsonFileBackend()
        with pytest.raises(ValueError, match="Collection name cannot be empty"):
            backend._versions_path("")

    @pytest.mark.unit
    def test_whitespace_collection_name_rejected(self) -> None:
        """Whitespace-only collection name raises ValueError."""
        backend = JsonFileBackend()
        with pytest.raises(ValueError, match="Collection name cannot be empty"):
            backend._versions_path(" ")

    @pytest.mark.unit
    def test_nested_collection_id_keeps_its_full_path(self, tmp_path: Any) -> None:
        """A nested collection id resolves under every one of its segments (#723).

        ``add`` creates collections under subcatalogs (``infer_nested_collection_id``,
        ADR-0032), so ``climate/hittekaart`` is a real collection id. Flattening it
        through ``Path(collection).name`` wrote ``{root}/hittekaart/versions.json``
        and left the collection's own file untouched.
        """
        from pathlib import Path

        backend = JsonFileBackend(catalog_root=Path(tmp_path))

        result = backend._versions_path("climate/hittekaart")

        assert result == Path(tmp_path) / "climate" / "hittekaart" / "versions.json"

    @pytest.mark.unit
    def test_deeply_nested_collection_id_keeps_its_full_path(self, tmp_path: Any) -> None:
        """Nesting is not capped at two segments (``rivers/2020/q1``)."""
        from pathlib import Path

        backend = JsonFileBackend(catalog_root=Path(tmp_path))

        result = backend._versions_path("rivers/2020/q1")

        assert result == Path(tmp_path) / "rivers" / "2020" / "q1" / "versions.json"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "collection",
        ["../evil", "climate/../../evil", "climate/..", ".."],
    )
    def test_parent_traversal_rejected(self, tmp_path: Any, collection: str) -> None:
        """A collection id escaping the catalog root raises, nesting notwithstanding."""
        from pathlib import Path

        backend = JsonFileBackend(catalog_root=Path(tmp_path))

        with pytest.raises(ValueError, match="Invalid collection name"):
            backend._versions_path(collection)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "collection",
        [
            "/etc/passwd",
            "C:\\Windows\\system32",
            "C:/Windows/system32",
            "//server/share",
            "\\\\server\\share",
            "climate\\..\\..\\etc",
        ],
    )
    def test_absolute_collection_path_rejected(self, tmp_path: Any, collection: str) -> None:
        """An absolute collection id raises instead of escaping the catalog root.

        The guard must not depend on the host's path flavor. ``pathlib`` splits
        by platform, so on Windows ``/etc/passwd`` is rooted but not absolute
        (absolute needs a drive) and slipped through, while on POSIX every
        backslash form is one filename and slipped through the other way. A
        collection id is a slash-separated spec string, so both flavors judge it.
        """
        from pathlib import Path

        backend = JsonFileBackend(catalog_root=Path(tmp_path))

        with pytest.raises(ValueError, match="Invalid collection name"):
            backend._versions_path(collection)

    @pytest.mark.unit
    def test_dot_collection_rejected(self, tmp_path: Any) -> None:
        """``.`` resolves to the catalog root itself and is not a collection."""
        from pathlib import Path

        backend = JsonFileBackend(catalog_root=Path(tmp_path))

        with pytest.raises(ValueError, match="Invalid collection name"):
            backend._versions_path(".")

    @pytest.mark.unit
    def test_rollback_raises_not_implemented_with_clear_message(self) -> None:
        """JsonFileBackend.rollback raises NotImplementedError with explanation."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="enterprise plugin backends"):
            backend.rollback("test_collection", "1.0.0")

    @pytest.mark.unit
    def test_prune_raises_not_implemented_with_clear_message(self) -> None:
        """JsonFileBackend.prune raises NotImplementedError with explanation."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="enterprise plugin backends"):
            backend.prune("test_collection", keep=5, dry_run=True)


class TestJsonFileBackendIntegration:
    """Integration tests for JsonFileBackend with filesystem."""

    @pytest.mark.integration
    def test_get_current_version_returns_latest(self, tmp_path: Any) -> None:
        """get_current_version returns the most recent version."""
        import json
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        # Create versions.json with two versions
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.1.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                },
                {
                    "version": "1.1.0",
                    "created": "2024-01-02T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                },
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data))

        backend = JsonFileBackend(catalog_root=catalog_root)
        version = backend.get_current_version("test")

        assert version.version == "1.1.0"

    @pytest.mark.integration
    def test_get_current_version_raises_for_no_versions(self, tmp_path: Any) -> None:
        """get_current_version raises FileNotFoundError when no versions exist."""
        import json
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        versions_data = {
            "spec_version": "1.0.0",
            "current_version": None,
            "versions": [],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data))

        backend = JsonFileBackend(catalog_root=catalog_root)

        with pytest.raises(FileNotFoundError, match="No versions found"):
            backend.get_current_version("test")

    @pytest.mark.integration
    def test_list_versions_returns_empty_for_missing_file(self, tmp_path: Any) -> None:
        """list_versions returns empty list when versions.json doesn't exist."""
        from pathlib import Path

        backend = JsonFileBackend(catalog_root=Path(tmp_path))
        versions = backend.list_versions("nonexistent")

        assert versions == []

    @pytest.mark.integration
    def test_publish_creates_first_version(self, tmp_path: Any) -> None:
        """publish creates versions.json and first version."""
        import json
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        backend = JsonFileBackend(catalog_root=catalog_root)
        schema: SchemaFingerprint = {
            "columns": ["geometry", "name"],
            "types": {"geometry": "geometry", "name": "string"},
            "hash": "geoparquet",
        }

        version = backend.publish(
            collection="test",
            assets={},
            schema=schema,
            breaking=False,
            message="Initial import",
        )

        assert version.version == "1.0.0"
        assert version.message == "Initial import"
        assert version.schema is not None

        # Verify file was created
        versions_path = collection_dir / "versions.json"
        assert versions_path.exists()
        data = json.loads(versions_path.read_text())
        assert data["current_version"] == "1.0.0"
        assert data["versions"][0]["message"] == "Initial import"
        assert "schema" in data["versions"][0]

    @pytest.mark.integration
    def test_publish_increments_minor_version(self, tmp_path: Any) -> None:
        """publish increments minor version for non-breaking changes."""
        import json
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        # Create initial version
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data))

        backend = JsonFileBackend(catalog_root=catalog_root)
        schema: SchemaFingerprint = {
            "columns": [],
            "types": {},
            "hash": "geoparquet",
        }

        version = backend.publish(
            collection="test",
            assets={},
            schema=schema,
            breaking=False,
            message="Update",
        )

        assert version.version == "1.1.0"

    @pytest.mark.integration
    def test_publish_increments_major_version_for_breaking(self, tmp_path: Any) -> None:
        """publish increments major version for breaking changes."""
        import json
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        # Create initial version
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.2.3",
            "versions": [
                {
                    "version": "1.2.3",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data))

        backend = JsonFileBackend(catalog_root=catalog_root)
        schema: SchemaFingerprint = {
            "columns": [],
            "types": {},
            "hash": "geoparquet",
        }

        version = backend.publish(
            collection="test",
            assets={},
            schema=schema,
            breaking=True,
            message="Breaking change",
        )

        assert version.version == "2.0.0"

    @pytest.mark.integration
    def test_check_drift_returns_report(self, tmp_path: Any) -> None:
        """check_drift returns a DriftReport."""
        import json
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data))

        backend = JsonFileBackend(catalog_root=catalog_root)
        report = backend.check_drift("test")

        assert report["has_drift"] is False
        assert report["local_version"] == "1.0.0"

    @pytest.mark.integration
    def test_first_publish_with_breaking_true(self, tmp_path: Any) -> None:
        """First publish can have breaking=True (unusual but valid)."""
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        backend = JsonFileBackend(catalog_root=catalog_root)
        schema: SchemaFingerprint = {
            "columns": [],
            "types": {},
            "hash": "geoparquet",
        }

        version = backend.publish(
            collection="test",
            assets={},
            schema=schema,
            breaking=True,  # Breaking on first version
            message="Initial breaking release",
        )

        # First version is always 1.0.0 regardless of breaking flag
        assert version.version == "1.0.0"
        assert version.breaking is True

    @pytest.mark.integration
    def test_publish_with_removed_parameter(self, tmp_path: Any) -> None:
        """publish(removed=...) delegates removal to add_version."""
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        backend = JsonFileBackend(catalog_root=catalog_root)
        schema: SchemaFingerprint = {
            "columns": [],
            "types": {},
            "hash": "h",
        }

        # Create a file asset
        f = tmp_path / "data.parquet"
        f.write_bytes(b"data")

        # Publish v1 with an asset
        backend.publish(
            collection="test",
            assets={"data.parquet": str(f)},
            schema=schema,
            breaking=False,
            message="v1",
        )

        # Publish v2 removing the asset
        v2 = backend.publish(
            collection="test",
            assets={},
            schema=schema,
            breaking=False,
            message="remove data",
            removed={"data.parquet"},
        )
        assert v2.version == "1.1.0"

    @pytest.mark.integration
    def test_publish_produces_catalog_root_relative_hrefs(self, tmp_path: Any) -> None:
        """publish() stores hrefs as collection/asset_key (catalog-root-relative).

        pull.py resolves files via `catalog_root / asset.href`, so the href
        must be relative to catalog root, not an absolute filesystem path.
        """
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "agriculture"
        item_dir = collection_dir / "census"
        item_dir.mkdir(parents=True)

        # Create asset file inside the item directory
        asset_file = item_dir / "census.parquet"
        asset_file.write_bytes(b"fake parquet")

        backend = JsonFileBackend(catalog_root=catalog_root)
        schema: SchemaFingerprint = {
            "columns": [],
            "types": {},
            "hash": "h",
        }

        version = backend.publish(
            collection="agriculture",
            assets={"census/census.parquet": str(asset_file)},
            schema=schema,
            breaking=False,
            message="add census",
        )

        asset = version.assets["census/census.parquet"]
        assert asset.href == "agriculture/census/census.parquet"
        # Verify it resolves to the actual file
        assert (catalog_root / asset.href).exists()

    @pytest.mark.integration
    def test_publish_stores_mtime(self, tmp_path: Any) -> None:
        """publish() records file mtime for fast-path change detection."""
        from pathlib import Path

        catalog_root = Path(tmp_path)
        collection_dir = catalog_root / "test"
        collection_dir.mkdir(parents=True)

        asset_file = collection_dir / "data.parquet"
        asset_file.write_bytes(b"data")

        backend = JsonFileBackend(catalog_root=catalog_root)
        schema: SchemaFingerprint = {
            "columns": [],
            "types": {},
            "hash": "h",
        }

        version = backend.publish(
            collection="test",
            assets={"data.parquet": str(asset_file)},
            schema=schema,
            breaking=False,
            message="v1",
        )

        asset = version.assets["data.parquet"]
        assert asset.mtime is not None
        assert abs(asset.mtime - asset_file.stat().st_mtime) < 0.001


class _TryLastFilePlugin:
    """A plugin that asks pluggy to order it before the built-ins."""

    @hookimpl(trylast=True)
    def portolan_backends(self) -> dict[str, Any]:
        return {"file": MagicMock()}


class TestGetBackendMisbehavingPlugins:
    """A misbehaving `portolan_backends` hook cannot break or replace a built-in."""

    @pytest.mark.unit
    @pytest.mark.parametrize("result", [["example"], "example", 42])
    def test_non_mapping_result_is_skipped_with_a_warning(
        self, register_plugin: Callable[..., None], result: object
    ) -> None:
        """A result that is not a mapping produces a warning and no backend."""

        class _Plugin:
            @hookimpl
            def portolan_backends(self) -> object:
                return result

        register_plugin(_Plugin(), "odd-plugin")

        with pytest.warns(manager.PluginWarning, match="'odd-plugin'.*not a mapping"):
            backend = get_backend("file")

        assert isinstance(backend, JsonFileBackend)

    @pytest.mark.unit
    def test_trylast_plugin_cannot_replace_the_file_backend(
        self, register_plugin: Callable[..., None]
    ) -> None:
        """Built-in precedence does not depend on pluggy call order."""
        register_plugin(_TryLastFilePlugin(), "trylast-plugin")

        with pytest.warns(
            manager.PluginWarning, match="'trylast-plugin'.*'file'.*'builtin:backends'"
        ):
            backend = get_backend("file")

        assert isinstance(backend, JsonFileBackend)


class TestLegacyBackendsEntryPointGroup:
    """`portolan.backends` still works for one minor release, with a warning (#947).

    v0.8.0 and v1.0.0b0 found plugin backends in this group.
    """

    @pytest.fixture
    def legacy(self, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
        eps: list[Any] = []
        monkeypatch.setattr(
            manager, "entry_points", lambda *, group: [ep for ep in eps if ep.group == group]
        )
        monkeypatch.delenv(manager.ENV_VAR, raising=False)
        manager.reset_plugin_manager()
        return eps

    @staticmethod
    def _ep(name: str) -> Any:
        from importlib.metadata import EntryPoint

        return EntryPoint(
            name=name,
            value="portolan_cli.backends.json_file:JsonFileBackend",
            group="portolan.backends",
        )

    @pytest.mark.unit
    def test_legacy_backend_loads_with_a_deprecation_warning(
        self, legacy: list[Any], tmp_path: Any
    ) -> None:
        """A backend from the old group resolves and gets `catalog_root`."""
        legacy.append(self._ep("legacy"))

        with pytest.warns(manager.PluginWarning, match="'legacy'.*deprecated 'portolan.backends'"):
            backend = get_backend("legacy", catalog_root=tmp_path)

        assert isinstance(backend, JsonFileBackend)
        assert backend._catalog_root == tmp_path

    @pytest.mark.unit
    def test_legacy_backend_cannot_replace_the_file_backend(self, legacy: list[Any]) -> None:
        """A built-in name still wins over the old group."""
        legacy.append(self._ep("file"))

        with pytest.warns(manager.PluginWarning, match="'file'"):
            backend = get_backend("file")

        assert type(backend) is JsonFileBackend

    @pytest.mark.unit
    def test_legacy_backend_respects_portolan_plugins_none(
        self, legacy: list[Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`PORTOLAN_PLUGINS=none` hides the old group too."""
        legacy.append(self._ep("legacy"))
        monkeypatch.setenv(manager.ENV_VAR, "none")

        with pytest.raises(
            ValueError, match=r"^Unknown backend: legacy\. Available: file, iceberg$"
        ):
            get_backend("legacy")

    @pytest.mark.unit
    def test_hook_plugin_wins_over_the_legacy_group(self, legacy: list[Any]) -> None:
        """A name in the new hook wins over the same name in the old group."""
        legacy.append(self._ep("shared"))
        hook_backend = MagicMock(spec=VersioningBackend)
        manager.get_plugin_manager().register(
            _BackendPlugin({"shared": MagicMock(return_value=hook_backend)}), name="new"
        )

        with pytest.warns(manager.PluginWarning, match="'builtin:legacy_backends'.*'shared'"):
            backend = get_backend("shared")

        assert backend is hook_backend
