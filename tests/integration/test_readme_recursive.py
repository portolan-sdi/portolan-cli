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


class TestReadmeVerbose:
    """Integration tests for portolan readme --verbose flag."""

    @pytest.fixture
    def catalog_with_metadata(self, tmp_path: Path) -> Path:
        """Create a catalog with collections and metadata.yaml files."""
        # Initialize catalog structure
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("version: '1.0'\n")
        (tmp_path / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Test\n  email: test@example.com\nlicense: MIT\n"
        )

        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "verbose-test",
                    "title": "Verbose Test Catalog",
                    "description": "Testing verbose output",
                    "stac_version": "1.0.0",
                }
            )
        )

        # Create collection with metadata
        coll_dir = tmp_path / "mydata"
        coll_dir.mkdir()
        (coll_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "mydata",
                    "title": "My Data Collection",
                    "description": "Test collection",
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
        (coll_dir / ".portolan").mkdir()
        (coll_dir / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Test\n  email: test@example.com\nlicense: MIT\n"
        )

        return tmp_path

    @pytest.mark.integration
    def test_verbose_shows_file_reads_for_collection(self, catalog_with_metadata: Path) -> None:
        """--verbose should show which files are being read for a collection."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            result = runner.invoke(cli, ["readme", "mydata", "--verbose"])

        assert result.exit_code == 0
        assert "collection.json" in result.output
        assert "metadata.yaml" in result.output
        assert "mydata" in result.output

    @pytest.mark.integration
    def test_verbose_shows_file_reads_for_catalog(self, catalog_with_metadata: Path) -> None:
        """--verbose should show which files are being read for catalog root."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            result = runner.invoke(cli, ["readme", "--verbose"])

        assert result.exit_code == 0
        assert "catalog.json" in result.output
        assert "metadata.yaml" in result.output

    @pytest.mark.integration
    def test_verbose_recursive_shows_all_file_reads(self, catalog_with_metadata: Path) -> None:
        """--recursive --verbose should show file reads for every entity."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            result = runner.invoke(cli, ["readme", "--recursive", "--verbose"])

        assert result.exit_code == 0
        # Should show processing messages
        assert "Processing" in result.output
        # Should show file-level reads for catalog root
        assert "catalog.json" in result.output
        # Should show file-level reads for collection
        assert "collection.json" in result.output
        # Should show metadata reads
        assert "metadata.yaml" in result.output

    @pytest.mark.integration
    def test_verbose_with_json_produces_valid_json(self, catalog_with_metadata: Path) -> None:
        """--verbose --json should not corrupt JSON output."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            result = runner.invoke(cli, ["--format", "json", "readme", "--verbose"])

        assert result.exit_code == 0
        # Must be valid JSON
        output = json.loads(result.output)
        assert output["success"] is True
        # Verbose text should NOT appear in JSON output
        assert "Reading" not in result.output

    @pytest.mark.integration
    def test_verbose_recursive_json_produces_valid_json(self, catalog_with_metadata: Path) -> None:
        """--recursive --verbose --json should produce valid JSON."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            result = runner.invoke(cli, ["--format", "json", "readme", "--recursive", "--verbose"])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["success"] is True
        assert "generated" in output["data"]

    @pytest.mark.integration
    def test_default_no_verbose_is_quiet(self, catalog_with_metadata: Path) -> None:
        """Without --verbose, file read messages should not appear."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            result = runner.invoke(cli, ["readme"])

        assert result.exit_code == 0
        # Should NOT show "Reading" messages
        assert "Reading" not in result.output

    @pytest.mark.integration
    def test_verbose_check_mode_shows_reads(self, catalog_with_metadata: Path) -> None:
        """--check --verbose should show file reads during check."""
        runner = CliRunner()

        # First generate
        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            runner.invoke(cli, ["readme"])

        # Then check with verbose
        with runner.isolated_filesystem(temp_dir=catalog_with_metadata):
            result = runner.invoke(cli, ["readme", "--check", "--verbose"])

        assert result.exit_code == 0
        assert "catalog.json" in result.output or "metadata.yaml" in result.output
