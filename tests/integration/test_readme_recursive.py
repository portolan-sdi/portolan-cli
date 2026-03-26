"""Integration tests for portolan readme --recursive flag.

Tests that --recursive regenerates all collection READMEs along with
the catalog README.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestReadmeRecursive:
    """Tests for portolan readme --recursive."""

    @pytest.fixture
    def catalog_with_collections(self, tmp_path: Path) -> Path:
        """Create a catalog with multiple collections."""
        # Initialize catalog structure
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("version: '1.0'\n")

        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "title": "Test Catalog",
                    "description": "A test catalog with multiple collections",
                    "stac_version": "1.0.0",
                }
            )
        )

        # Create two collections
        for coll_id in ["alpha", "beta"]:
            coll_dir = tmp_path / coll_id
            coll_dir.mkdir()
            (coll_dir / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": coll_id,
                        "title": f"Collection {coll_id.upper()}",
                        "description": f"Description for {coll_id}",
                        "stac_version": "1.0.0",
                        "extent": {
                            "spatial": {"bbox": [[0, 0, 1, 1]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                        "links": [],
                        "license": "MIT",
                    }
                )
            )
            # Create .portolan for each collection
            (coll_dir / ".portolan").mkdir()
            (coll_dir / ".portolan" / "metadata.yaml").write_text(
                f"title: Collection {coll_id.upper()}\n"
            )

        return tmp_path

    @pytest.mark.integration
    def test_recursive_generates_all_readmes(self, catalog_with_collections: Path) -> None:
        """--recursive should generate README for catalog and all collections."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            result = runner.invoke(cli, ["readme", "--recursive"])

        assert result.exit_code == 0, result.output

        # Check catalog README exists
        assert (catalog_with_collections / "README.md").exists()

        # Check collection READMEs exist
        assert (catalog_with_collections / "alpha" / "README.md").exists()
        assert (catalog_with_collections / "beta" / "README.md").exists()

    @pytest.mark.integration
    def test_recursive_reports_count(self, catalog_with_collections: Path) -> None:
        """--recursive should report how many READMEs were generated."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            result = runner.invoke(cli, ["readme", "--recursive"])

        assert result.exit_code == 0
        # Should mention generating multiple READMEs
        assert "3" in result.output or "README" in result.output

    @pytest.mark.integration
    def test_without_recursive_only_generates_target(self, catalog_with_collections: Path) -> None:
        """Without --recursive, only the target README is generated."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            result = runner.invoke(cli, ["readme"])

        assert result.exit_code == 0

        # Only catalog README should exist
        assert (catalog_with_collections / "README.md").exists()

        # Collection READMEs should NOT exist (not generated without --recursive)
        assert not (catalog_with_collections / "alpha" / "README.md").exists()
        assert not (catalog_with_collections / "beta" / "README.md").exists()

    @pytest.mark.integration
    def test_recursive_json_output(self, catalog_with_collections: Path) -> None:
        """--recursive with --json should output structured results."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            result = runner.invoke(cli, ["--format", "json", "readme", "--recursive"])

        assert result.exit_code == 0

        output = json.loads(result.output)
        assert output["success"] is True
        assert "generated" in output["data"]
        assert output["data"]["count"] == 3  # catalog + 2 collections

    @pytest.mark.integration
    def test_recursive_check_mode(self, catalog_with_collections: Path) -> None:
        """--recursive --check should check all READMEs."""
        runner = CliRunner()

        # First generate all READMEs
        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            runner.invoke(cli, ["readme", "--recursive"])

        # Then check - should pass
        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            result = runner.invoke(cli, ["readme", "--recursive", "--check"])

        assert result.exit_code == 0

    @pytest.mark.integration
    def test_recursive_check_fails_when_stale(self, catalog_with_collections: Path) -> None:
        """--recursive --check should fail if any README is stale."""
        runner = CliRunner()

        # Generate all READMEs
        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            runner.invoke(cli, ["readme", "--recursive"])

        # Modify a collection to make its README stale
        coll_json = catalog_with_collections / "alpha" / "collection.json"
        data = json.loads(coll_json.read_text())
        data["title"] = "MODIFIED TITLE"
        coll_json.write_text(json.dumps(data))

        # Check should now fail
        with runner.isolated_filesystem(temp_dir=catalog_with_collections):
            result = runner.invoke(cli, ["readme", "--recursive", "--check"])

        assert result.exit_code == 1
        assert "stale" in result.output.lower() or "alpha" in result.output
