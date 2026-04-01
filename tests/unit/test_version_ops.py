"""Unit tests for version_ops middle layer.

Tests the version_ops module that bridges CLI commands and versioning backends.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.version_ops import (
    _resolve_backend_name,
    get_current_version,
    list_versions,
    prune_versions,
    publish_version,
    rollback_version,
)


class TestResolveBackendName:
    """Tests for _resolve_backend_name precedence logic."""

    @pytest.mark.unit
    def test_cli_value_takes_highest_precedence(self) -> None:
        result = _resolve_backend_name("cli_backend", catalog_root=None)
        assert result == "cli_backend"

    @pytest.mark.unit
    def test_config_value_used_when_no_cli(self, tmp_path: Path) -> None:
        with patch("portolan_cli.config.get_setting", return_value="config_backend"):
            result = _resolve_backend_name(None, catalog_root=tmp_path)
        assert result == "config_backend"

    @pytest.mark.unit
    def test_defaults_to_file(self) -> None:
        with patch("portolan_cli.config.get_setting", return_value=None):
            result = _resolve_backend_name(None, catalog_root=None)
        assert result == "file"


class TestGetCurrentVersion:

    @pytest.mark.unit
    def test_delegates_to_backend(self) -> None:
        mock_version = MagicMock()
        mock_backend = MagicMock()
        mock_backend.get_current_version.return_value = mock_version

        with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
            result = get_current_version("my_collection", backend_name="file")

        mock_backend.get_current_version.assert_called_once_with("my_collection")
        assert result is mock_version

    @pytest.mark.unit
    def test_passes_catalog_root(self) -> None:
        mock_backend = MagicMock()
        root = Path("/some/path")

        with patch("portolan_cli.backends.get_backend", return_value=mock_backend) as mock_get:
            get_current_version("col", backend_name="file", catalog_root=root)

        mock_get.assert_called_once_with("file", catalog_root=root)


class TestListVersions:

    @pytest.mark.unit
    def test_delegates_to_backend(self) -> None:
        mock_versions = [MagicMock(), MagicMock()]
        mock_backend = MagicMock()
        mock_backend.list_versions.return_value = mock_versions

        with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
            result = list_versions("my_collection", backend_name="file")

        mock_backend.list_versions.assert_called_once_with("my_collection")
        assert result is mock_versions


class TestPublishVersion:

    @pytest.mark.unit
    def test_delegates_to_backend(self) -> None:
        mock_version = MagicMock()
        mock_backend = MagicMock()
        mock_backend.publish.return_value = mock_version

        schema = {"columns": [], "types": {}, "hash": "h"}

        with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
            result = publish_version(
                "col", assets={"a": "/path"}, schema=schema, message="test"
            )

        mock_backend.publish.assert_called_once_with(
            "col", {"a": "/path"}, schema, False, "test", removed=None
        )
        assert result is mock_version

    @pytest.mark.unit
    def test_passes_removed_parameter(self) -> None:
        mock_backend = MagicMock()

        schema = {"columns": [], "types": {}, "hash": "h"}

        with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
            publish_version(
                "col",
                assets={},
                schema=schema,
                message="remove",
                removed={"old.parquet"},
            )

        mock_backend.publish.assert_called_once_with(
            "col", {}, schema, False, "remove", removed={"old.parquet"}
        )


class TestRollbackVersion:

    @pytest.mark.unit
    def test_delegates_to_backend(self) -> None:
        mock_version = MagicMock()
        mock_backend = MagicMock()
        mock_backend.rollback.return_value = mock_version

        with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
            result = rollback_version("col", "1.0.0", backend_name="iceberg")

        mock_backend.rollback.assert_called_once_with("col", "1.0.0")
        assert result is mock_version


class TestPruneVersions:

    @pytest.mark.unit
    def test_delegates_to_backend(self) -> None:
        mock_pruned = [MagicMock()]
        mock_backend = MagicMock()
        mock_backend.prune.return_value = mock_pruned

        with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
            result = prune_versions("col", keep=5, dry_run=True, backend_name="iceberg")

        mock_backend.prune.assert_called_once_with("col", keep=5, dry_run=True)
        assert result is mock_pruned
