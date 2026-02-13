"""Unit tests for versioning backend protocol and plugin discovery.

Tests the VersioningBackend protocol and get_backend() discovery function
that allows external backends (like portolake) to integrate with portolan-cli.

See ADR-0015 (Two-Tier Versioning Architecture) and ADR-0003 (Plugin Architecture).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.backends import get_backend
from portolan_cli.backends.json_file import JsonFileBackend
from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint, VersioningBackend


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
    def test_get_backend_discovers_entry_point(self) -> None:
        """get_backend() discovers backends registered via entry points."""
        # Create a mock backend class that implements the protocol
        mock_backend = MagicMock(spec=VersioningBackend)
        mock_backend_class = MagicMock(return_value=mock_backend)
        mock_entry_point = MagicMock()
        mock_entry_point.name = "iceberg"
        mock_entry_point.load.return_value = mock_backend_class

        # Patch entry_points to return our mock
        with patch("portolan_cli.backends.entry_points") as mock_eps:
            mock_eps.return_value = [mock_entry_point]
            backend = get_backend("iceberg")

        # Verify the entry point was loaded and returns protocol-compliant object
        mock_entry_point.load.assert_called_once()
        mock_backend_class.assert_called_once()
        assert backend is mock_backend

    @pytest.mark.unit
    def test_get_backend_entry_point_name_must_match(self) -> None:
        """get_backend() only loads entry points with matching names."""
        # Create an entry point with a different name
        mock_entry_point = MagicMock()
        mock_entry_point.name = "other_backend"

        with patch("portolan_cli.backends.entry_points") as mock_eps:
            mock_eps.return_value = [mock_entry_point]
            with pytest.raises(ValueError, match="Unknown backend: iceberg"):
                get_backend("iceberg")


class TestGetBackendErrorHandling:
    """Tests for error handling in plugin discovery."""

    @pytest.mark.unit
    def test_get_backend_handles_import_error_on_load(self) -> None:
        """get_backend() handles ImportError when loading a broken plugin."""
        mock_entry_point = MagicMock()
        mock_entry_point.name = "broken"
        mock_entry_point.load.side_effect = ImportError("Module not found")

        with patch("portolan_cli.backends.entry_points") as mock_eps:
            mock_eps.return_value = [mock_entry_point]
            with pytest.raises(ValueError, match="Failed to load backend 'broken'"):
                get_backend("broken")

    @pytest.mark.unit
    def test_get_backend_handles_exception_on_instantiation(self) -> None:
        """get_backend() handles exceptions during plugin instantiation."""
        mock_backend_class = MagicMock(side_effect=TypeError("Missing argument"))
        mock_entry_point = MagicMock()
        mock_entry_point.name = "failing"
        mock_entry_point.load.return_value = mock_backend_class

        with patch("portolan_cli.backends.entry_points") as mock_eps:
            mock_eps.return_value = [mock_entry_point]
            with pytest.raises(ValueError, match="Failed to instantiate backend 'failing'"):
                get_backend("failing")

    @pytest.mark.unit
    def test_get_backend_validates_protocol_compliance(self) -> None:
        """get_backend() validates that loaded plugins implement the protocol."""

        # Create a class that doesn't implement VersioningBackend
        class NotABackend:
            pass

        mock_entry_point = MagicMock()
        mock_entry_point.name = "invalid"
        mock_entry_point.load.return_value = NotABackend

        with patch("portolan_cli.backends.entry_points") as mock_eps:
            mock_eps.return_value = [mock_entry_point]
            with pytest.raises(ValueError, match="does not implement VersioningBackend"):
                get_backend("invalid")

    @pytest.mark.unit
    def test_get_backend_error_message_is_actionable(self) -> None:
        """Error messages include plugin name for troubleshooting."""
        mock_entry_point = MagicMock()
        mock_entry_point.name = "problematic"
        mock_entry_point.load.side_effect = ImportError("numpy not found")

        with patch("portolan_cli.backends.entry_points") as mock_eps:
            mock_eps.return_value = [mock_entry_point]
            with pytest.raises(ValueError) as exc_info:
                get_backend("problematic")

            error_msg = str(exc_info.value)
            assert "problematic" in error_msg
            assert "numpy not found" in error_msg


class TestJsonFileBackend:
    """Tests for the JsonFileBackend stub implementation."""

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
    def test_get_current_version_raises_not_implemented(self) -> None:
        """JsonFileBackend.get_current_version raises NotImplementedError (stub)."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="Wire to versions.py"):
            backend.get_current_version("test_collection")

    @pytest.mark.unit
    def test_list_versions_raises_not_implemented(self) -> None:
        """JsonFileBackend.list_versions raises NotImplementedError (stub)."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="Wire to versions.py"):
            backend.list_versions("test_collection")

    @pytest.mark.unit
    def test_publish_raises_not_implemented(self) -> None:
        """JsonFileBackend.publish raises NotImplementedError (stub)."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="Wire to versions.py"):
            backend.publish(
                collection="test",
                assets={},
                schema={},
                breaking=False,
                message="test",
            )

    @pytest.mark.unit
    def test_rollback_raises_not_implemented(self) -> None:
        """JsonFileBackend.rollback raises NotImplementedError (stub)."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="Wire to versions.py"):
            backend.rollback("test_collection", "1.0.0")

    @pytest.mark.unit
    def test_prune_raises_not_implemented(self) -> None:
        """JsonFileBackend.prune raises NotImplementedError (stub)."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="Wire to versions.py"):
            backend.prune("test_collection", keep=5, dry_run=True)

    @pytest.mark.unit
    def test_check_drift_raises_not_implemented(self) -> None:
        """JsonFileBackend.check_drift raises NotImplementedError (stub)."""
        backend = JsonFileBackend()
        with pytest.raises(NotImplementedError, match="Wire to versions.py"):
            backend.check_drift("test_collection")
