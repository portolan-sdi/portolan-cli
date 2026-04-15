"""Integration tests for `portolan version` subcommands via CLI.

Tests rollback, prune, list, and current commands through the full
CLI pipeline with the iceberg backend.
"""

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from tests.iceberg.integration.conftest import (
    _POINTS_GEOJSON,
    _POINTS_GEOJSON_V2,
    invoke_add,
    load_test_catalog,
    place_geojson_in_collection,
)


def _invoke_version_cmd(runner: CliRunner, catalog_root, args: list[str]):
    """Run a `portolan version` subcommand with config isolation."""
    with patch("portolan_cli.backends.iceberg.config._get_external_config", return_value=None):
        return runner.invoke(
            cli,
            ["version", *args, "--catalog", str(catalog_root)],
            catch_exceptions=False,
        )


def _add_two_versions(initialized_iceberg_catalog, runner):
    """Add two versions to 'series' collection. Returns catalog_root."""
    catalog_root = initialized_iceberg_catalog
    g1 = place_geojson_in_collection(catalog_root, "series", _POINTS_GEOJSON, "v1.geojson")
    result = invoke_add(runner, catalog_root, g1)
    assert result.exit_code == 0, f"First add failed: {result.output}"

    g2 = place_geojson_in_collection(catalog_root, "series", _POINTS_GEOJSON_V2, "v2.geojson")
    result = invoke_add(runner, catalog_root, g2)
    assert result.exit_code == 0, f"Second add failed: {result.output}"
    return catalog_root


@pytest.mark.integration
def test_version_current_shows_latest(initialized_iceberg_catalog, runner):
    """portolan version current shows the latest version."""
    catalog_root = _add_two_versions(initialized_iceberg_catalog, runner)

    result = _invoke_version_cmd(runner, catalog_root, ["current", "series"])
    assert result.exit_code == 0, f"Failed: {result.output}"
    assert "1.1.0" in result.output


@pytest.mark.integration
def test_version_list_shows_all_versions(initialized_iceberg_catalog, runner):
    """portolan version list shows all versions oldest-first."""
    catalog_root = _add_two_versions(initialized_iceberg_catalog, runner)

    result = _invoke_version_cmd(runner, catalog_root, ["list", "series"])
    assert result.exit_code == 0, f"Failed: {result.output}"
    assert "1.0.0" in result.output
    assert "1.1.0" in result.output


@pytest.mark.integration
def test_version_list_json_output(initialized_iceberg_catalog, runner):
    """portolan version list --json returns valid JSON."""
    catalog_root = _add_two_versions(initialized_iceberg_catalog, runner)

    result = _invoke_version_cmd(runner, catalog_root, ["list", "series", "--json"])
    assert result.exit_code == 0, f"Failed: {result.output}"

    import json

    data = json.loads(result.output)
    # Verify it's valid JSON with version data (exact envelope structure may vary)
    assert isinstance(data, (dict, list)), f"Expected dict or list, got {type(data)}"


@pytest.mark.integration
def test_rollback_restores_previous_version(initialized_iceberg_catalog, runner):
    """portolan version rollback restores a previous version as current."""
    catalog_root = _add_two_versions(initialized_iceberg_catalog, runner)

    # Rollback to 1.0.0
    result = _invoke_version_cmd(runner, catalog_root, ["rollback", "series", "1.0.0"])
    assert result.exit_code == 0, f"Rollback failed: {result.output}"

    # Verify current is now 1.0.0
    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.series")
    snap = table.current_snapshot()
    assert snap.summary.additional_properties["portolake.version"] == "1.0.0"


@pytest.mark.integration
def test_prune_dry_run_no_side_effects(initialized_iceberg_catalog, runner):
    """portolan version prune --dry-run reports but does not delete."""
    catalog_root = _add_two_versions(initialized_iceberg_catalog, runner)

    result = _invoke_version_cmd(
        runner, catalog_root, ["prune", "series", "--keep", "1", "--dry-run"]
    )
    assert result.exit_code == 0, f"Prune dry-run failed: {result.output}"

    # Both versions should still exist
    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.series")
    versioned = [
        s
        for s in table.snapshots()
        if s.summary and "portolake.version" in s.summary.additional_properties
    ]
    assert len(versioned) == 2, f"Expected 2 versions after dry-run, got {len(versioned)}"
