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
    publish_version,
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
            result = publish_version("col", assets={"a": "/path"}, schema=schema, message="test")

        mock_backend.publish.assert_called_once_with(
            "col", {"a": "/path"}, schema, False, "test", removed=None, version=None
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
            "col", {}, schema, False, "remove", removed={"old.parquet"}, version=None
        )


def _pull_result(
    *, downloaded: int, version: str, up_to_date: bool = False, success: bool = True
) -> object:
    from portolan_cli.sync.pull import PullResult

    return PullResult(
        success=success,
        files_downloaded=downloaded,
        files_skipped=0,
        local_version=None,
        remote_version=version,
        up_to_date=up_to_date,
    )


def _catalog_with_collections(root: Path, *ids: str) -> Path:
    for collection_id in ids:
        collection_dir = root / collection_id
        collection_dir.mkdir(parents=True)
        (collection_dir / "collection.json").write_text('{"type": "Collection"}')
    return root


class TestTryBackendPull:
    """Issue #977: pull is a typed protocol method, and a missing collection means all."""

    @pytest.mark.unit
    def test_no_collection_pulls_each_collection_of_the_catalog(self, tmp_path: Path) -> None:
        """On main the backend got collection=None and raised on an empty name."""
        from portolan_cli.version_ops import try_backend_pull

        root = _catalog_with_collections(tmp_path, "boundaries", "roads")
        backend = MagicMock()
        backend.pull.side_effect = [
            _pull_result(downloaded=2, version="1.0.0"),
            _pull_result(downloaded=3, version="2.0.0"),
        ]
        with (
            patch("portolan_cli.config.get_setting", return_value="iceberg"),
            patch("portolan_cli.backends.get_backend", return_value=backend),
        ):
            result = try_backend_pull(root, "s3://example-bucket/catalog", None, dry_run=False)

        assert [c.args for c in backend.pull.call_args_list] == [
            ("s3://example-bucket/catalog", root, "boundaries"),
            ("s3://example-bucket/catalog", root, "roads"),
        ]
        assert (result.handled, result.success, result.files_downloaded) == (True, True, 5)
        assert result.remote_version is None

    @pytest.mark.unit
    def test_an_empty_catalog_is_up_to_date(self, tmp_path: Path) -> None:
        from portolan_cli.version_ops import try_backend_pull

        backend = MagicMock()
        with (
            patch("portolan_cli.config.get_setting", return_value="iceberg"),
            patch("portolan_cli.backends.get_backend", return_value=backend),
        ):
            result = try_backend_pull(tmp_path, "s3://example-bucket/catalog", None, dry_run=False)

        backend.pull.assert_not_called()
        assert (result.handled, result.success, result.up_to_date) == (True, True, True)

    @pytest.mark.unit
    def test_one_failed_collection_fails_the_pull(self, tmp_path: Path) -> None:
        from portolan_cli.version_ops import try_backend_pull

        root = _catalog_with_collections(tmp_path, "a", "b")
        backend = MagicMock()
        backend.pull.side_effect = [
            _pull_result(downloaded=1, version="1.0.0"),
            _pull_result(downloaded=0, version="1.0.0", success=False),
        ]
        with (
            patch("portolan_cli.config.get_setting", return_value="iceberg"),
            patch("portolan_cli.backends.get_backend", return_value=backend),
        ):
            result = try_backend_pull(root, "s3://example-bucket/catalog", None, dry_run=False)

        assert (result.handled, result.success) == (True, False)


class TestJsonFileBackendHooks:
    """The file backend implements the four lifecycle hooks with neutral behavior."""

    @pytest.mark.unit
    def test_defaults(self, tmp_path: Path) -> None:
        from portolan_cli.backends.json_file import JsonFileBackend

        backend = JsonFileBackend(catalog_root=tmp_path)
        assert backend.supports_push() is True
        assert backend.pull("s3://b/c", tmp_path, "roads") is None
        assert backend.push_blocked_message(None) == (
            "Push is not supported with the 'file' backend."
        )
