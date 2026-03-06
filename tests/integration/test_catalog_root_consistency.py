"""Integration tests for consistent catalog root detection across CLI commands.

Per ADR-0029, all CLI commands should use find_catalog_root() from catalog.py,
which looks for .portolan/config.yaml as the single sentinel. This ensures
consistent behavior where commands either all succeed or all fail.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


def setup_managed_catalog(path: Path) -> None:
    """Create a fully managed Portolan catalog structure.

    Per ADR-0023 and ADR-0027:
    - catalog.json at root (STAC standard)
    - .portolan/config.yaml (sentinel file per ADR-0027)
    - .portolan/state.json (required for MANAGED state)
    """
    # Create .portolan directory with sentinel files
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir(parents=True)
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    (portolan_dir / "state.json").write_text("{}")

    # Create catalog.json at root (STAC standard)
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog for integration tests",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


def setup_unmanaged_stac(path: Path) -> None:
    """Create an UNMANAGED_STAC directory (catalog.json only, no .portolan)."""
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "unmanaged-catalog",
        "description": "Unmanaged STAC catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


class TestCatalogRootConsistency:
    """Integration tests verifying all commands use consistent catalog detection."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.mark.integration
    def test_status_finds_catalog_from_subdirectory(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """status command finds catalog from nested subdirectory."""
        setup_managed_catalog(tmp_path)

        # Create nested subdirectory
        nested_dir = tmp_path / "collection" / "item"
        nested_dir.mkdir(parents=True)

        # Run status from nested directory
        with runner.isolated_filesystem(temp_dir=nested_dir):
            result = runner.invoke(cli, ["status"], catch_exceptions=False)

        # Should succeed (exit code 0) because it finds the parent catalog
        assert result.exit_code == 0, f"status failed: {result.output}"

    @pytest.mark.integration
    def test_list_succeeds_from_subdirectory(self, runner: CliRunner, tmp_path: Path) -> None:
        """list command succeeds from subdirectory (returns empty, doesn't walk up).

        Note: 'list' uses --catalog with default="." rather than find_catalog_root(),
        so it doesn't walk up to find the parent catalog. This test verifies the
        command doesn't error when run from a subdirectory without a catalog.
        See lines 176-178 for the full explanation.
        """
        setup_managed_catalog(tmp_path)

        # Create nested subdirectory
        nested_dir = tmp_path / "collection"
        nested_dir.mkdir()

        # Run list from nested directory - will return empty (no local catalog.json)
        with runner.isolated_filesystem(temp_dir=nested_dir):
            result = runner.invoke(cli, ["list"], catch_exceptions=False)

        # Should succeed (exit code 0) but with empty results
        # list doesn't use find_catalog_root, so it won't find the parent
        assert result.exit_code == 0, f"list failed: {result.output}"

    @pytest.mark.integration
    def test_config_list_finds_catalog_from_subdirectory(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """config list command finds catalog from nested subdirectory."""
        setup_managed_catalog(tmp_path)

        # Create nested subdirectory
        nested_dir = tmp_path / "collection"
        nested_dir.mkdir()

        # Run config list from nested directory
        with runner.isolated_filesystem(temp_dir=nested_dir):
            result = runner.invoke(cli, ["config", "list"], catch_exceptions=False)

        # Should succeed (exit code 0)
        assert result.exit_code == 0, f"config list failed: {result.output}"

    @pytest.mark.integration
    def test_unmanaged_stac_catalog_rejected(self, runner: CliRunner, tmp_path: Path) -> None:
        """Commands fail gracefully for catalog.json-only directories (UNMANAGED_STAC)."""
        setup_unmanaged_stac(tmp_path)

        # Create nested subdirectory
        nested_dir = tmp_path / "collection"
        nested_dir.mkdir()

        # Run status from nested directory (should fail)
        with runner.isolated_filesystem(temp_dir=nested_dir):
            result = runner.invoke(cli, ["status"])

        # Should fail because UNMANAGED_STAC is not detected as a catalog
        assert result.exit_code != 0, "status should fail for UNMANAGED_STAC"

    @pytest.mark.integration
    def test_error_message_suggests_init(self, runner: CliRunner, tmp_path: Path) -> None:
        """Error message explicitly tells user to run 'portolan init'."""
        # Empty directory, no catalog
        nested_dir = tmp_path / "no-catalog"
        nested_dir.mkdir()

        with runner.isolated_filesystem(temp_dir=nested_dir):
            result = runner.invoke(cli, ["status"])

        # Should fail and explicitly mention "portolan init" (not just "init" or "catalog")
        assert result.exit_code != 0
        output_lower = result.output.lower()
        assert "portolan init" in output_lower or "'portolan init'" in output_lower, (
            f"Error should explicitly mention 'portolan init': {result.output}"
        )

    @pytest.mark.integration
    def test_nested_catalogs_finds_nearest(self, runner: CliRunner, tmp_path: Path) -> None:
        """Commands find the nearest catalog when nested catalogs exist."""
        # Create parent catalog with distinguishable config
        setup_managed_catalog(tmp_path)
        parent_config = tmp_path / ".portolan" / "config.yaml"
        parent_config.write_text("# Portolan configuration\nname: parent-catalog\n")

        # Create child catalog inside parent with different config
        child_dir = tmp_path / "child-catalog"
        child_dir.mkdir()
        setup_managed_catalog(child_dir)
        child_config = child_dir / ".portolan" / "config.yaml"
        child_config.write_text("# Portolan configuration\nname: child-catalog\n")

        # Create subdirectory inside child
        nested_dir = child_dir / "collection"
        nested_dir.mkdir()

        # Run config list from inside child catalog
        with runner.isolated_filesystem(temp_dir=nested_dir):
            result = runner.invoke(cli, ["config", "list"], catch_exceptions=False)

        # Should succeed and find child catalog (nearest), not parent
        assert result.exit_code == 0, f"config list failed: {result.output}"
        # Verify the child catalog's config is returned (contains "child-catalog")
        assert "child-catalog" in result.output, (
            f"Expected child-catalog config, but got parent or no config: {result.output}"
        )

    @pytest.mark.integration
    def test_all_commands_consistent_without_catalog(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Commands using find_catalog_root fail consistently when no catalog exists.

        Note: 'list' is excluded because it takes --catalog as explicit argument
        rather than using find_catalog_root. That's a separate issue to address.
        """
        nested_dir = tmp_path / "no-catalog" / "nested"
        nested_dir.mkdir(parents=True)

        # Commands that use find_catalog_root internally
        commands = [
            ["status"],
            ["config", "list"],
        ]

        with runner.isolated_filesystem(temp_dir=nested_dir):
            for cmd in commands:
                result = runner.invoke(cli, cmd)
                assert result.exit_code != 0, f"{' '.join(cmd)} should fail without catalog"

    @pytest.mark.integration
    def test_all_commands_consistent_with_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """Commands using find_catalog_root succeed consistently when catalog exists.

        Note: 'list' is excluded because it takes --catalog as explicit argument
        rather than using find_catalog_root. That's a separate issue to address.
        """
        setup_managed_catalog(tmp_path)

        nested_dir = tmp_path / "collection"
        nested_dir.mkdir()

        # Commands that use find_catalog_root internally
        commands = [
            ["status"],
            ["config", "list"],
        ]

        with runner.isolated_filesystem(temp_dir=nested_dir):
            for cmd in commands:
                result = runner.invoke(cli, cmd, catch_exceptions=False)
                assert result.exit_code == 0, f"{' '.join(cmd)} failed: {result.output}"


class TestCatalogRootEdgeCases:
    """Edge case integration tests for catalog root detection."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.mark.integration
    def test_empty_config_yaml_is_valid(self, runner: CliRunner, tmp_path: Path) -> None:
        """Catalog with empty config.yaml is still valid."""
        # Create minimal catalog with empty config.yaml
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("")  # Empty is valid
        (portolan_dir / "state.json").write_text("{}")
        (tmp_path / "catalog.json").write_text('{"type": "Catalog", "id": "test"}')

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["config", "list"], catch_exceptions=False)

        assert result.exit_code == 0, f"Should work with empty config.yaml: {result.output}"

    @pytest.mark.integration
    def test_partial_portolan_dir_not_detected(self, runner: CliRunner, tmp_path: Path) -> None:
        """Catalog with .portolan but no config.yaml is NOT detected."""
        # Create .portolan without config.yaml (partial setup)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "state.json").write_text("{}")  # Only state.json
        (tmp_path / "catalog.json").write_text('{"type": "Catalog", "id": "test"}')

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["config", "list"])

        # Should fail because config.yaml is missing
        assert result.exit_code != 0, "Should fail without config.yaml"
