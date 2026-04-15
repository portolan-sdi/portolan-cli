"""Integration tests for error handling in the CLI-to-Iceberg pipeline."""

from unittest.mock import patch

import pytest

from portolan_cli.cli import cli
from tests.iceberg.integration.conftest import (
    invoke_add,
    place_geojson_in_collection,
)


@pytest.mark.integration
def test_add_nonexistent_path_reports_error(initialized_iceberg_catalog, runner):
    """Adding a nonexistent file should fail gracefully."""
    catalog_root = initialized_iceberg_catalog
    fake_path = catalog_root / "nonexistent" / "fake.geojson"

    result = invoke_add(runner, catalog_root, fake_path)
    assert (
        result.exit_code != 0 or "error" in result.output.lower() or "skip" in result.output.lower()
    )


@pytest.mark.integration
def test_rollback_nonexistent_version_exits_error(initialized_iceberg_catalog, runner):
    """Rollback to a version that doesn't exist should exit with error."""
    catalog_root = initialized_iceberg_catalog

    # First, add something so the collection exists
    geojson = place_geojson_in_collection(catalog_root, "rollback_err")
    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0

    # Try to rollback to nonexistent version
    with patch("portolan_cli.backends.iceberg.config._get_external_config", return_value=None):
        result = runner.invoke(
            cli,
            [
                "version",
                "rollback",
                "rollback_err",
                "99.0.0",
                "--catalog",
                str(catalog_root),
            ],
        )
    assert result.exit_code != 0, f"Expected error, got: {result.output}"


@pytest.mark.integration
def test_version_commands_without_iceberg_backend_error(tmp_path, runner):
    """Running version commands on a file-backend catalog should report error."""
    # Init with default file backend (not iceberg)
    result = runner.invoke(
        cli,
        ["init", str(tmp_path), "--auto"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Try version current -- should fail because file backend doesn't support it
    result = runner.invoke(
        cli,
        ["version", "current", "some_collection", "--catalog", str(tmp_path)],
    )
    assert result.exit_code != 0, f"Expected error for file backend, got: {result.output}"
