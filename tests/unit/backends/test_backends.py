"""Unit tests for versioning backend protocol and plugin discovery.

Tests the VersioningBackend protocol and get_backend() discovery function
that allows external backends (like portolake) to integrate with portolan-cli.

See ADR-0015 (Two-Tier Versioning Architecture) and ADR-0003 (Plugin Architecture).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.backends import get_backend
from portolan_cli.backends.json_file import JsonFileBackend
from portolan_cli.backends.protocol import VersioningBackend


class TestVersioningBackendProtocol:
    """Tests for the VersioningBackend protocol definition."""

    @pytest.mark.unit
    def test_protocol_defines_get_current_version(self) -> None:
        """VersioningBackend protocol requires get_current_version method."""
        # Protocol should define this method
        assert hasattr(VersioningBackend, "get_current_version")

    @pytest.mark.unit
    def test_protocol_defines_list_versions(self) -> None:
        """VersioningBackend protocol requires list_versions method."""
        assert hasattr(VersioningBackend, "list_versions")

    @pytest.mark.unit
    def test_protocol_defines_publish(self) -> None:
        """VersioningBackend protocol requires publish method."""
        assert hasattr(VersioningBackend, "publish")

    @pytest.mark.unit
    def test_protocol_defines_rollback(self) -> None:
        """VersioningBackend protocol requires rollback method."""
        assert hasattr(VersioningBackend, "rollback")

    @pytest.mark.unit
    def test_protocol_defines_prune(self) -> None:
        """VersioningBackend protocol requires prune method."""
        assert hasattr(VersioningBackend, "prune")

    @pytest.mark.unit
    def test_protocol_defines_check_drift(self) -> None:
        """VersioningBackend protocol requires check_drift method."""
        assert hasattr(VersioningBackend, "check_drift")

    @pytest.mark.unit
    def test_json_file_backend_implements_protocol(self) -> None:
        """JsonFileBackend should be recognized as implementing VersioningBackend."""
        # This tests structural subtyping - JsonFileBackend should match the protocol
        backend = JsonFileBackend()
        # Check that we can assign it where VersioningBackend is expected
        # (this is a compile-time check enforced by mypy, but we verify at runtime too)
        assert hasattr(backend, "get_current_version")
        assert hasattr(backend, "list_versions")
        assert hasattr(backend, "publish")
        assert hasattr(backend, "rollback")
        assert hasattr(backend, "prune")
        assert hasattr(backend, "check_drift")


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
    def test_get_backend_discovers_entry_point(self) -> None:
        """get_backend() discovers backends registered via entry points."""
        # Create a mock entry point
        mock_backend_class = MagicMock(return_value=MagicMock(spec=VersioningBackend))
        mock_entry_point = MagicMock()
        mock_entry_point.name = "iceberg"
        mock_entry_point.load.return_value = mock_backend_class

        # Patch entry_points to return our mock
        with patch("portolan_cli.backends.entry_points") as mock_eps:
            mock_eps.return_value = [mock_entry_point]
            get_backend("iceberg")

        # Verify the entry point was loaded
        mock_entry_point.load.assert_called_once()
        mock_backend_class.assert_called_once()

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


class TestJsonFileBackend:
    """Tests for the JsonFileBackend stub implementation."""

    @pytest.mark.unit
    def test_json_file_backend_instantiates(self) -> None:
        """JsonFileBackend can be instantiated without arguments."""
        backend = JsonFileBackend()
        assert backend is not None

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
