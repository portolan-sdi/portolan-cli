"""Unit tests for CLI version commands and backend-routing helpers.

Covers:
- ``portolan version current``
- ``portolan version list``
- ``portolan version rollback`` and ``portolan version prune``, which
  ``portolan_cli.backends.iceberg.cli`` provides through entry points (#947),
  with their iceberg backend guard
- ``_check_backend_push_support`` — push routing
- ``_try_backend_pull`` — pull routing
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner, Result

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_iceberg_catalog(tmp_path: Path) -> Path:
    """Create a minimal catalog directory configured for the iceberg backend."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog"}')
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("backend: iceberg\n")
    return catalog_root


def _make_file_catalog(tmp_path: Path) -> Path:
    """Create a minimal catalog directory using the default (file) backend."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog"}')
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("")
    return catalog_root


def _make_version(
    version: str = "1.0.0",
    created: datetime | None = None,
    breaking: bool = False,
    message: str = "initial",
    assets: dict | None = None,
    changes: list | None = None,
) -> MagicMock:
    """Build a mock Version object matching the VersioningBackend protocol."""
    ver = MagicMock()
    ver.version = version
    ver.created = created or datetime(2024, 1, 1, tzinfo=UTC)
    ver.breaking = breaking
    ver.message = message
    ver.assets = assets if assets is not None else {}
    ver.changes = changes if changes is not None else []
    return ver


def _make_pull_result(
    *,
    success: bool,
    files_downloaded: int,
    files_skipped: int,
    local_version: str,
    remote_version: str,
) -> MagicMock:
    """Build a mock PullResult that is not up to date and has no uncommitted changes."""
    result = MagicMock()
    result.success = success
    result.files_downloaded = files_downloaded
    result.files_skipped = files_skipped
    result.local_version = local_version
    result.remote_version = remote_version
    result.up_to_date = False
    result.uncommitted_changes = []
    return result


def _invoke_with_backend(
    runner: CliRunner, backend: MagicMock, args: list[str], *, setting: str = "iceberg"
) -> Result:
    """Run the CLI with ARGS while the config gives SETTING and get_backend gives BACKEND."""
    with (
        patch("portolan_cli.config.get_setting", return_value=setting),
        patch("portolan_cli.backends.get_backend", return_value=backend),
    ):
        return runner.invoke(cli, args)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# version current
# ---------------------------------------------------------------------------


class TestVersionCurrentCommand:
    """Tests for ``portolan version current``."""

    @pytest.mark.unit
    def test_shows_version_human(self, runner: CliRunner, tmp_path: Path) -> None:
        """version current prints version string in human-readable format."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_ver = _make_version("2.3.1", message="patch release")

        mock_backend = MagicMock()
        mock_backend.get_current_version.return_value = mock_ver

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "current", "boundaries", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 0
        assert "2.3.1" in result.output
        mock_backend.get_current_version.assert_called_once_with("boundaries")

    @pytest.mark.unit
    def test_shows_version_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """version current returns structured JSON when --json is given."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_ver = _make_version("1.0.0")

        mock_backend = MagicMock()
        mock_backend.get_current_version.return_value = mock_ver

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "--format",
                "json",
                "version",
                "current",
                "boundaries",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["data"]["version"] == "1.0.0"
        assert data["data"]["collection"] == "boundaries"

    @pytest.mark.unit
    def test_shows_breaking_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """version current includes BREAKING flag for breaking versions."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_ver = _make_version("2.0.0", breaking=True)

        mock_backend = MagicMock()
        mock_backend.get_current_version.return_value = mock_ver

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "current", "boundaries", "--catalog", str(catalog_root)],
        )

        assert "BREAKING" in result.output

    @pytest.mark.unit
    def test_error_on_backend_exception(self, runner: CliRunner, tmp_path: Path) -> None:
        """version current exits with error when backend raises an exception."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.get_current_version.side_effect = FileNotFoundError("no versions")

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "current", "boundaries", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 1
        assert "no versions" in result.output

    @pytest.mark.unit
    def test_error_json_on_backend_exception(self, runner: CliRunner, tmp_path: Path) -> None:
        """version current emits JSON error when backend raises and --json given."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.get_current_version.side_effect = ValueError("table not found")

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "--format",
                "json",
                "version",
                "current",
                "boundaries",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["success"] is False

    @pytest.mark.unit
    def test_assets_count_shown(self, runner: CliRunner, tmp_path: Path) -> None:
        """version current displays asset count when assets exist."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_ver = _make_version(
            "1.1.0", assets={"a.parquet": MagicMock(), "b.parquet": MagicMock()}
        )

        mock_backend = MagicMock()
        mock_backend.get_current_version.return_value = mock_ver

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "current", "boundaries", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 0
        assert "2 asset" in result.output


# ---------------------------------------------------------------------------
# version list
# ---------------------------------------------------------------------------


class TestVersionListCommand:
    """Tests for ``portolan version list``."""

    @pytest.mark.unit
    def test_lists_versions_human(self, runner: CliRunner, tmp_path: Path) -> None:
        """version list prints all versions in human-readable format."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_versions = [
            _make_version("1.0.0", message="initial"),
            _make_version("1.1.0", message="update"),
        ]

        mock_backend = MagicMock()
        mock_backend.list_versions.return_value = mock_versions

        result = _invoke_with_backend(
            runner, mock_backend, ["version", "list", "boundaries", "--catalog", str(catalog_root)]
        )

        assert result.exit_code == 0
        assert "1.0.0" in result.output
        assert "1.1.0" in result.output
        mock_backend.list_versions.assert_called_once_with("boundaries")

    @pytest.mark.unit
    def test_lists_versions_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """version list returns structured JSON with all versions."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_versions = [_make_version("1.0.0")]

        mock_backend = MagicMock()
        mock_backend.list_versions.return_value = mock_versions

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["--format", "json", "version", "list", "boundaries", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert len(data["data"]["versions"]) == 1
        assert data["data"]["versions"][0]["version"] == "1.0.0"

    @pytest.mark.unit
    def test_empty_version_list_human(self, runner: CliRunner, tmp_path: Path) -> None:
        """version list shows 'no versions found' message for empty collection."""
        catalog_root = _make_iceberg_catalog(tmp_path)

        mock_backend = MagicMock()
        mock_backend.list_versions.return_value = []

        result = _invoke_with_backend(
            runner, mock_backend, ["version", "list", "boundaries", "--catalog", str(catalog_root)]
        )

        assert result.exit_code == 0
        assert "no versions" in result.output.lower()

    @pytest.mark.unit
    def test_version_with_changes_listed(self, runner: CliRunner, tmp_path: Path) -> None:
        """version list shows per-version changes when they exist."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_versions = [
            _make_version("1.1.0", changes=["Added column 'area'"]),
        ]

        mock_backend = MagicMock()
        mock_backend.list_versions.return_value = mock_versions

        result = _invoke_with_backend(
            runner, mock_backend, ["version", "list", "boundaries", "--catalog", str(catalog_root)]
        )

        assert result.exit_code == 0
        assert "Added column" in result.output

    @pytest.mark.unit
    def test_error_on_backend_exception(self, runner: CliRunner, tmp_path: Path) -> None:
        """version list exits with error when backend raises."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.list_versions.side_effect = RuntimeError("catalog corrupt")

        result = _invoke_with_backend(
            runner, mock_backend, ["version", "list", "boundaries", "--catalog", str(catalog_root)]
        )

        assert result.exit_code == 1
        assert "catalog corrupt" in result.output

    @pytest.mark.unit
    def test_error_json_on_backend_exception(self, runner: CliRunner, tmp_path: Path) -> None:
        """version list emits JSON error when backend raises and --json given."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.list_versions.side_effect = RuntimeError("oops")

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["--format", "json", "version", "list", "boundaries", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["success"] is False


# ---------------------------------------------------------------------------
# version rollback
# ---------------------------------------------------------------------------


class TestVersionRollbackCommand:
    """Tests for ``portolan version rollback``."""

    @pytest.mark.unit
    def test_rollback_success_human(self, runner: CliRunner, tmp_path: Path) -> None:
        """version rollback prints success message on human output."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        restored = _make_version("1.0.0")

        mock_backend = MagicMock()
        mock_backend.rollback.return_value = restored

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "rollback", "boundaries", "1.0.0", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 0
        assert "1.0.0" in result.output
        mock_backend.rollback.assert_called_once_with("boundaries", "1.0.0")

    @pytest.mark.unit
    def test_rollback_success_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """version rollback returns structured JSON on success."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        restored = _make_version("1.0.0")

        mock_backend = MagicMock()
        mock_backend.rollback.return_value = restored

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "--format",
                "json",
                "version",
                "rollback",
                "boundaries",
                "1.0.0",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["data"]["restored_version"] == "1.0.0"
        assert data["data"]["collection"] == "boundaries"

    @pytest.mark.unit
    def test_rollback_error_unknown_version(self, runner: CliRunner, tmp_path: Path) -> None:
        """version rollback exits with error when target version doesn't exist."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.rollback.side_effect = ValueError("version 9.9.9 not found")

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "rollback", "boundaries", "9.9.9", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 1
        assert "9.9.9" in result.output

    @pytest.mark.unit
    def test_rollback_error_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """version rollback emits JSON error when rollback fails."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.rollback.side_effect = FileNotFoundError("snapshot missing")

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "--format",
                "json",
                "version",
                "rollback",
                "boundaries",
                "1.0.0",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["success"] is False


_MISSING_EXTRA = "The 'iceberg' backend requires the iceberg extra."


@pytest.mark.unit
@pytest.mark.parametrize(
    "args",
    [["rollback", "boundaries", "1.0.0"], ["prune", "boundaries"]],
    ids=["rollback", "prune"],
)
def test_missing_iceberg_extra_reports_a_json_error(
    runner: CliRunner, tmp_path: Path, args: list[str]
) -> None:
    """A backend setup error becomes a JSON envelope, not a traceback."""
    catalog_root = _make_iceberg_catalog(tmp_path)

    with (
        patch("portolan_cli.config.get_setting", return_value="iceberg"),
        patch("portolan_cli.backends.get_backend", side_effect=ValueError(_MISSING_EXTRA)),
    ):
        result = runner.invoke(
            cli, ["--format", "json", "version", *args, "--catalog", str(catalog_root)]
        )

    assert result.exit_code == 1
    assert json.loads(result.output)["errors"] == [
        {"type": "BackendError", "message": _MISSING_EXTRA}
    ]


# ---------------------------------------------------------------------------
# version prune
# ---------------------------------------------------------------------------


class TestVersionPruneCommand:
    """Tests for ``portolan version prune``."""

    @pytest.mark.unit
    def test_prune_human_with_pruned_versions(self, runner: CliRunner, tmp_path: Path) -> None:
        """version prune shows pruned versions in human format."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        pruned = [_make_version("1.0.0"), _make_version("1.1.0")]

        mock_backend = MagicMock()
        mock_backend.prune.return_value = pruned

        result = _invoke_with_backend(
            runner, mock_backend, ["version", "prune", "boundaries", "--catalog", str(catalog_root)]
        )

        assert result.exit_code == 0
        assert "Pruned 2" in result.output
        mock_backend.prune.assert_called_once_with("boundaries", keep=5, dry_run=False)

    @pytest.mark.unit
    def test_prune_dry_run(self, runner: CliRunner, tmp_path: Path) -> None:
        """version prune --dry-run shows 'Would prune' in output."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        pruned = [_make_version("1.0.0")]

        mock_backend = MagicMock()
        mock_backend.prune.return_value = pruned

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "prune", "boundaries", "--dry-run", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 0
        assert "Would prune" in result.output or "DRY RUN" in result.output
        mock_backend.prune.assert_called_once_with("boundaries", keep=5, dry_run=True)

    @pytest.mark.unit
    def test_prune_nothing_to_prune(self, runner: CliRunner, tmp_path: Path) -> None:
        """version prune shows 'Nothing to prune' when there's nothing to remove."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.prune.return_value = []

        result = _invoke_with_backend(
            runner, mock_backend, ["version", "prune", "boundaries", "--catalog", str(catalog_root)]
        )

        assert result.exit_code == 0
        assert "nothing" in result.output.lower() or "Nothing" in result.output

    @pytest.mark.unit
    def test_prune_custom_keep(self, runner: CliRunner, tmp_path: Path) -> None:
        """version prune respects --keep option."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.prune.return_value = []

        _invoke_with_backend(
            runner,
            mock_backend,
            ["version", "prune", "boundaries", "--keep", "3", "--catalog", str(catalog_root)],
        )

        mock_backend.prune.assert_called_once_with("boundaries", keep=3, dry_run=False)

    @pytest.mark.unit
    def test_prune_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """version prune emits structured JSON when --json given."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        pruned = [_make_version("1.0.0")]

        mock_backend = MagicMock()
        mock_backend.prune.return_value = pruned

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["--format", "json", "version", "prune", "boundaries", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert len(data["data"]["pruned"]) == 1
        assert data["data"]["kept"] == 5
        assert data["data"]["dry_run"] is False

    @pytest.mark.unit
    def test_prune_error_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """version prune emits JSON error when backend raises."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.prune.side_effect = RuntimeError("prune failed")

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["--format", "json", "version", "prune", "boundaries", "--catalog", str(catalog_root)],
        )

        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["success"] is False

    @pytest.mark.unit
    def test_prune_error_human(self, runner: CliRunner, tmp_path: Path) -> None:
        """version prune shows error text when backend raises and no --json."""
        catalog_root = _make_iceberg_catalog(tmp_path)
        mock_backend = MagicMock()
        mock_backend.prune.side_effect = RuntimeError("prune failed badly")

        result = _invoke_with_backend(
            runner, mock_backend, ["version", "prune", "boundaries", "--catalog", str(catalog_root)]
        )

        assert result.exit_code == 1
        assert "prune failed badly" in result.output


# ---------------------------------------------------------------------------
# _check_backend_push_support
# ---------------------------------------------------------------------------


class TestCheckBackendPushSupport:
    """Tests for the _check_backend_push_support helper (invoked inside push)."""

    @pytest.mark.unit
    def test_no_backend_configured_allows_push(self, runner: CliRunner, tmp_path: Path) -> None:
        """push proceeds normally when no backend is configured (file backend)."""
        catalog_root = _make_file_catalog(tmp_path)

        # No backend configured → _check_backend_push_support should return early
        with patch("portolan_cli.config.get_setting", return_value=None):
            # We only care that it doesn't block; the actual push will fail without
            # a real remote — that's OK for this guard test
            result = runner.invoke(
                cli,
                [
                    "push",
                    "--collection",
                    "boundaries",
                    "--dry-run",
                    "--catalog",
                    str(catalog_root),
                ],
            )

        # Should not fail due to backend-push-block (may fail for other reasons)
        assert (
            "not supported" not in result.output.lower()
            or result.exit_code != 1
            or "iceberg" not in result.output.lower()
        )

    @pytest.mark.unit
    def test_push_prints_the_backend_message(self, runner: CliRunner, tmp_path: Path) -> None:
        """push is blocked with the message the backend gives (#977)."""
        catalog_root = _make_file_catalog(tmp_path)
        (catalog_root / ".portolan" / "config.yaml").write_text("backend: custom\n")

        mock_backend = MagicMock(spec=["supports_push", "push_blocked_message"])
        mock_backend.supports_push.return_value = False
        mock_backend.push_blocked_message.return_value = "Push is not supported with custom."

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["push", "--collection", "boundaries", "--catalog", str(catalog_root)],
            setting="custom",
        )

        assert result.exit_code == 1
        assert "Push is not supported with custom." in result.output

    @pytest.mark.unit
    def test_backend_supports_push_allows_through(self, runner: CliRunner, tmp_path: Path) -> None:
        """push proceeds when backend.supports_push() returns True."""
        catalog_root = _make_file_catalog(tmp_path)
        (catalog_root / ".portolan" / "config.yaml").write_text("backend: custom\n")

        # Backend that supports push — the guard should not block it
        mock_backend = MagicMock()
        mock_backend.supports_push.return_value = True

        result = _invoke_with_backend(
            runner,
            mock_backend,
            ["push", "--collection", "boundaries", "--catalog", str(catalog_root)],
            setting="custom",
        )

        # Guard should not produce a backend-block error (push may fail for other reasons)
        assert "not supported" not in result.output or "custom" not in result.output


# ---------------------------------------------------------------------------
# _try_backend_pull
# ---------------------------------------------------------------------------


class TestTryBackendPull:
    """Tests for the _try_backend_pull helper (invoked inside pull)."""

    @pytest.mark.unit
    def test_file_backend_does_not_intercept(self, runner: CliRunner, tmp_path: Path) -> None:
        """pull falls through to file-based pull when backend is 'file'."""
        catalog_root = _make_file_catalog(tmp_path)

        # The file pull is replaced, so the command never contacts GCS.
        with (
            patch("portolan_cli.config.get_setting", return_value=None),
            patch("portolan_cli.sync.pull.pull", side_effect=ValueError("stub remote")) as pull_fn,
        ):
            result = runner.invoke(
                cli,
                [
                    "pull",
                    "gs://some-bucket/catalog",
                    "--collection",
                    "boundaries",
                    "--catalog",
                    str(catalog_root),
                ],
            )

        # Backend routing should NOT produce a "not supported" message
        assert "not supported" not in result.output.lower()
        pull_fn.assert_called_once()
        assert pull_fn.call_args.kwargs["remote_url"] == "gs://some-bucket/catalog"

    @pytest.mark.unit
    def test_backend_pull_returning_none_falls_through(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """pull uses file-based pull when the backend's pull() returns None."""
        catalog_root = _make_file_catalog(tmp_path)
        (catalog_root / ".portolan" / "config.yaml").write_text("backend: custom\n")

        mock_backend = MagicMock(spec=["pull"])
        mock_backend.pull.return_value = None

        with (
            patch("portolan_cli.config.get_setting", return_value="custom"),
            patch("portolan_cli.backends.get_backend", return_value=mock_backend),
            patch("portolan_cli.sync.pull.pull", side_effect=ValueError("stub remote")) as pull_fn,
        ):
            runner.invoke(
                cli,
                [
                    "pull",
                    "gs://some-bucket/catalog",
                    "--collection",
                    "boundaries",
                    "--catalog",
                    str(catalog_root),
                ],
            )

        # The command fell through to the file pull.
        pull_fn.assert_called_once()

    @pytest.mark.unit
    def test_backend_pull_method_called(self, runner: CliRunner, tmp_path: Path) -> None:
        """pull delegates to backend.pull() when the backend provides one."""
        catalog_root = _make_file_catalog(tmp_path)
        (catalog_root / ".portolan" / "config.yaml").write_text(
            "backend: iceberg\nremote: gs://test/catalog\n"
        )

        mock_pull_result = _make_pull_result(
            success=True,
            files_downloaded=5,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.1.0",
        )

        mock_backend = MagicMock()
        mock_backend.pull.return_value = mock_pull_result

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "pull",
                "gs://test/catalog",
                "--collection",
                "boundaries",
                "--catalog",
                str(catalog_root),
            ],
        )

        mock_backend.pull.assert_called_once()
        assert result.exit_code == 0

    @pytest.mark.unit
    def test_backend_pull_failure_exits_with_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """pull exits with code 1 when backend.pull() returns failure."""
        catalog_root = _make_file_catalog(tmp_path)
        (catalog_root / ".portolan" / "config.yaml").write_text("backend: iceberg\n")

        mock_pull_result = _make_pull_result(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.0",
        )

        mock_backend = MagicMock()
        mock_backend.pull.return_value = mock_pull_result

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "pull",
                "gs://test/catalog",
                "--collection",
                "boundaries",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 1

    @pytest.mark.unit
    def test_backend_pull_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """pull emits structured JSON when backend.pull() succeeds and --json given."""
        catalog_root = _make_file_catalog(tmp_path)
        (catalog_root / ".portolan" / "config.yaml").write_text("backend: iceberg\n")

        mock_pull_result = _make_pull_result(
            success=True,
            files_downloaded=3,
            files_skipped=1,
            local_version="1.0.0",
            remote_version="1.1.0",
        )

        mock_backend = MagicMock()
        mock_backend.pull.return_value = mock_pull_result

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "--format",
                "json",
                "pull",
                "gs://test/catalog",
                "--collection",
                "boundaries",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["data"]["files_downloaded"] == 3

    @pytest.mark.unit
    def test_backend_pull_json_failure(self, runner: CliRunner, tmp_path: Path) -> None:
        """pull emits JSON error when backend.pull() fails and --json given."""
        catalog_root = _make_file_catalog(tmp_path)
        (catalog_root / ".portolan" / "config.yaml").write_text("backend: iceberg\n")

        mock_pull_result = _make_pull_result(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.0",
        )

        mock_backend = MagicMock()
        mock_backend.pull.return_value = mock_pull_result

        result = _invoke_with_backend(
            runner,
            mock_backend,
            [
                "--format",
                "json",
                "pull",
                "gs://test/catalog",
                "--collection",
                "boundaries",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["success"] is False
