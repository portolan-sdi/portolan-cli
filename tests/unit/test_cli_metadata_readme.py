"""Tests for `portolan metadata` and `portolan readme` CLI commands.

These tests verify the CLI behavior for:
- `portolan metadata init` - Generate metadata.yaml template
- `portolan metadata validate` - Validate metadata.yaml (contact + license required)
- `portolan readme` - Generate README.md from STAC + metadata
- `portolan readme --check` - Check README freshness for CI
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestMetadataInit:
    """Tests for `portolan metadata init` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_creates_metadata_yaml_at_catalog_root(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init --no-recursive should create .portolan/metadata.yaml at catalog root."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["metadata", "init", "--no-recursive"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert Path(".portolan/metadata.yaml").exists()

    @pytest.mark.unit
    def test_creates_metadata_yaml_at_collection(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init PATH --no-recursive should create .portolan/metadata.yaml at collection level."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("demographics").mkdir()

            result = runner.invoke(cli, ["metadata", "init", "demographics", "--no-recursive"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert Path("demographics/.portolan/metadata.yaml").exists()

    @pytest.mark.unit
    def test_does_not_overwrite_existing(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init --no-recursive should not overwrite existing metadata.yaml."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path(".portolan/metadata.yaml").write_text("license: CC-BY-4.0\n")

            runner.invoke(cli, ["metadata", "init", "--no-recursive"])

            # Should either error or warn, not overwrite
            content = Path(".portolan/metadata.yaml").read_text()
            assert "CC-BY-4.0" in content  # Original content preserved

    @pytest.mark.unit
    def test_force_overwrites_existing(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init --force --no-recursive should overwrite existing metadata.yaml."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path(".portolan/metadata.yaml").write_text("license: Old\n")

            result = runner.invoke(cli, ["metadata", "init", "--force", "--no-recursive"])

            assert result.exit_code == 0
            content = Path(".portolan/metadata.yaml").read_text()
            assert "Old" not in content  # Overwritten
            assert "contact" in content.lower()  # Template content

    @pytest.mark.unit
    def test_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init --format json --no-recursive should output JSON envelope."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["--format", "json", "metadata", "init", "--no-recursive"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "metadata init"

    @pytest.mark.unit
    def test_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["metadata", "init"])

            assert result.exit_code != 0

    # Recursive behavior tests (recursive is now default)

    @pytest.mark.unit
    def test_recursive_creates_metadata_at_all_levels(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """metadata init (recursive by default) should create templates at all STAC levels."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Set up catalog with subcatalog and collection
            runner.invoke(cli, ["init", "--auto"])
            # Subcatalog
            Path("climate").mkdir()
            Path("climate/catalog.json").write_text('{"type": "Catalog"}')
            # Collection under subcatalog
            Path("climate/hittekaart").mkdir()
            Path("climate/hittekaart/collection.json").write_text('{"type": "Collection"}')
            # Direct collection
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')

            result = runner.invoke(cli, ["metadata", "init"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            # Root
            assert Path(".portolan/metadata.yaml").exists()
            # Subcatalog
            assert Path("climate/.portolan/metadata.yaml").exists()
            # Collection under subcatalog
            assert Path("climate/hittekaart/.portolan/metadata.yaml").exists()
            # Direct collection
            assert Path("demographics/.portolan/metadata.yaml").exists()

    @pytest.mark.unit
    def test_recursive_skips_existing_metadata(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init (recursive by default) should skip directories with existing metadata.yaml."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Create collection with existing metadata
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')
            Path("demographics/.portolan").mkdir()
            Path("demographics/.portolan/metadata.yaml").write_text("license: CC-BY-4.0\n")

            result = runner.invoke(cli, ["metadata", "init"])

            assert result.exit_code == 0
            # Existing metadata should be preserved
            content = Path("demographics/.portolan/metadata.yaml").read_text()
            assert "CC-BY-4.0" in content
            # Root should still be created
            assert Path(".portolan/metadata.yaml").exists()

    @pytest.mark.unit
    def test_recursive_skips_items(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init (recursive by default) should NOT create metadata for items."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Collection with an item
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')
            Path("demographics/census-2020").mkdir()
            Path("demographics/census-2020/item.json").write_text('{"type": "Feature"}')

            result = runner.invoke(cli, ["metadata", "init"])

            assert result.exit_code == 0
            # Collection should have metadata
            assert Path("demographics/.portolan/metadata.yaml").exists()
            # Item should NOT have metadata
            assert not Path("demographics/census-2020/.portolan/metadata.yaml").exists()

    @pytest.mark.unit
    def test_recursive_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init --json (recursive by default) should report created and skipped paths."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')

            result = runner.invoke(cli, ["--format", "json", "metadata", "init"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert "created" in output["data"]
            assert isinstance(output["data"]["created"], list)

    @pytest.mark.unit
    def test_recursive_with_explicit_path(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init PATH (recursive by default) should start from specified path."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Subcatalog with nested collection
            Path("climate").mkdir()
            Path("climate/catalog.json").write_text('{"type": "Catalog"}')
            Path("climate/hittekaart").mkdir()
            Path("climate/hittekaart/collection.json").write_text('{"type": "Collection"}')
            # Another top-level collection (should NOT be touched)
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')

            result = runner.invoke(cli, ["metadata", "init", "climate"])

            assert result.exit_code == 0
            # climate subtree should have metadata
            assert Path("climate/.portolan/metadata.yaml").exists()
            assert Path("climate/hittekaart/.portolan/metadata.yaml").exists()
            # Root should NOT have metadata (we started from climate)
            assert not Path(".portolan/metadata.yaml").exists()
            # demographics should NOT have metadata
            assert not Path("demographics/.portolan/metadata.yaml").exists()

    @pytest.mark.unit
    def test_recursive_nonexistent_path_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init with non-existent path should fail with clear error."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["metadata", "init", "nonexistent"])

            assert result.exit_code != 0
            assert "does not exist" in result.output.lower()

    @pytest.mark.unit
    def test_recursive_nonexistent_path_json_output(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """metadata init with non-existent path should output JSON error."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["--format", "json", "metadata", "init", "nonexistent"])

            assert result.exit_code != 0
            output = json.loads(result.output)
            assert output["success"] is False
            assert any("PathNotFoundError" in e["type"] for e in output["errors"])

    @pytest.mark.unit
    def test_recursive_force_overwrites_content(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init --force (recursive by default) should actually overwrite existing content."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')
            Path("demographics/.portolan").mkdir()
            original_content = "# Custom content\nlicense: CC-BY-4.0\n"
            Path("demographics/.portolan/metadata.yaml").write_text(original_content)

            result = runner.invoke(cli, ["metadata", "init", "--force"])

            assert result.exit_code == 0
            # Content should be overwritten with template
            new_content = Path("demographics/.portolan/metadata.yaml").read_text()
            assert new_content != original_content
            assert "contact:" in new_content  # Template has contact field

    @pytest.mark.unit
    def test_recursive_json_output_complete_schema(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init --json (recursive by default) should have complete schema fields."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Create collection with existing metadata (to get skipped paths)
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')
            Path("demographics/.portolan").mkdir()
            Path("demographics/.portolan/metadata.yaml").write_text("license: MIT\n")
            # Create another collection (to get created paths)
            Path("climate").mkdir()
            Path("climate/collection.json").write_text('{"type": "Collection"}')

            result = runner.invoke(cli, ["--format", "json", "metadata", "init"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            data = output["data"]
            # Verify all schema fields exist
            assert "mode" in data
            assert data["mode"] == "recursive"
            assert "created" in data
            assert "skipped" in data
            assert "permission_errors" in data
            assert "count" in data
            # Verify count matches created list length
            assert data["count"] == len(data["created"])
            # Verify types
            assert isinstance(data["created"], list)
            assert isinstance(data["skipped"], list)
            assert isinstance(data["permission_errors"], list)

    @pytest.mark.unit
    def test_recursive_skips_symlinks(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata init (recursive by default) should skip symlinks to prevent infinite loops."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("climate").mkdir()
            Path("climate/catalog.json").write_text('{"type": "Catalog"}')
            # Create a symlink that would cause infinite loop
            try:
                Path("climate/loop").symlink_to(Path.cwd() / "climate")
            except OSError:
                pytest.skip("Symlinks not supported on this platform")

            result = runner.invoke(cli, ["metadata", "init"])

            # Should complete without hanging/crashing
            assert result.exit_code == 0
            assert Path("climate/.portolan/metadata.yaml").exists()


class TestMetadataValidate:
    """Tests for `portolan metadata validate` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_passes_for_valid_metadata(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --no-recursive should pass for valid metadata.yaml with contact + license."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Only contact and license are required now
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: Test User\n  email: test@example.org\nlicense: CC-BY-4.0\n"
            )

            result = runner.invoke(cli, ["metadata", "validate", "--no-recursive"])

            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    def test_fails_for_missing_required_fields(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --no-recursive should fail when required fields (contact, license) are missing."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path(".portolan/metadata.yaml").write_text(
                "citation: Some citation\n"  # Missing contact and license
            )

            result = runner.invoke(cli, ["metadata", "validate", "--no-recursive"])

            assert result.exit_code != 0
            # Should mention missing fields
            assert "contact" in result.output.lower() or "license" in result.output.lower()

    @pytest.mark.unit
    def test_fails_for_invalid_email(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --no-recursive should fail for invalid email format."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: Test\n  email: not-an-email\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["metadata", "validate", "--no-recursive"])

            assert result.exit_code != 0
            assert "email" in result.output.lower()

    @pytest.mark.unit
    def test_validates_collection_level(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate PATH --no-recursive should validate at collection level."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("demographics/.portolan").mkdir(parents=True)
            Path("demographics/.portolan/metadata.yaml").write_text(
                "contact:\n  name: Team\n  email: team@org.com\nlicense: CC0-1.0\n"
            )

            result = runner.invoke(cli, ["metadata", "validate", "demographics", "--no-recursive"])

            assert result.exit_code == 0

    @pytest.mark.unit
    def test_json_output_success(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --format json --no-recursive should output JSON on success."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(
                cli, ["--format", "json", "metadata", "validate", "--no-recursive"]
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["data"]["valid"] is True
            assert output["data"]["errors"] == []

    @pytest.mark.unit
    def test_json_output_failure(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --format json --no-recursive should output errors in JSON."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path(".portolan/metadata.yaml").write_text("citation: Test\n")  # Incomplete

            result = runner.invoke(
                cli, ["--format", "json", "metadata", "validate", "--no-recursive"]
            )

            output = json.loads(result.output)
            assert output["data"]["valid"] is False
            assert len(output["data"]["errors"]) > 0

    # --no-recursive flag tests (recursive is now default)

    @pytest.mark.unit
    def test_recursive_validates_all_levels(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate (recursive by default) should validate all STAC levels."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            valid_metadata = "contact:\n  name: Test\n  email: test@example.org\nlicense: MIT\n"
            # Root metadata
            Path(".portolan/metadata.yaml").write_text(valid_metadata)
            # Subcatalog
            Path("climate").mkdir()
            Path("climate/catalog.json").write_text('{"type": "Catalog"}')
            Path("climate/.portolan").mkdir()
            Path("climate/.portolan/metadata.yaml").write_text(valid_metadata)
            # Collection
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')
            Path("demographics/.portolan").mkdir()
            Path("demographics/.portolan/metadata.yaml").write_text(valid_metadata)

            result = runner.invoke(cli, ["metadata", "validate"])

            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    def test_recursive_aggregates_errors(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate should aggregate errors from multiple invalid files.

        Note: metadata uses hierarchical resolution, so children inherit from parents.
        To test aggregated errors, children must explicitly override with INVALID values
        (not just missing fields, which would inherit from parent).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            valid_metadata = "contact:\n  name: Test\n  email: test@example.org\nlicense: MIT\n"
            # Invalid metadata: override with invalid email (can't be fixed by inheritance)
            invalid_email_metadata = "contact:\n  name: Test\n  email: not-an-email\nlicense: MIT\n"
            # Root: valid
            Path(".portolan/metadata.yaml").write_text(valid_metadata)
            # Collection 1: invalid email
            Path("climate").mkdir()
            Path("climate/collection.json").write_text('{"type": "Collection"}')
            Path("climate/.portolan").mkdir()
            Path("climate/.portolan/metadata.yaml").write_text(invalid_email_metadata)
            # Collection 2: invalid email
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text('{"type": "Collection"}')
            Path("demographics/.portolan").mkdir()
            Path("demographics/.portolan/metadata.yaml").write_text(invalid_email_metadata)

            result = runner.invoke(cli, ["metadata", "validate"])

            assert result.exit_code != 0
            # Should mention multiple failures (2 invalid collections)
            assert "2" in result.output or "invalid" in result.output.lower()

    @pytest.mark.unit
    def test_recursive_json_output_schema(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --json should have complete recursive schema."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            valid_metadata = "contact:\n  name: Test\n  email: test@example.org\nlicense: MIT\n"
            invalid_metadata = "citation: Test\n"
            # Root: valid
            Path(".portolan/metadata.yaml").write_text(valid_metadata)
            # Collection: invalid
            Path("climate").mkdir()
            Path("climate/collection.json").write_text('{"type": "Collection"}')
            Path("climate/.portolan").mkdir()
            Path("climate/.portolan/metadata.yaml").write_text(invalid_metadata)

            result = runner.invoke(cli, ["--format", "json", "metadata", "validate"])

            output = json.loads(result.output)
            assert output["command"] == "metadata validate"
            data = output["data"]
            # Verify recursive schema
            assert data["mode"] == "recursive"
            assert "results" in data
            assert isinstance(data["results"], list)
            assert "summary" in data
            assert "total" in data["summary"]
            assert "valid" in data["summary"]
            assert "invalid" in data["summary"]

    @pytest.mark.unit
    def test_recursive_exit_code_on_any_failure(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate should exit 1 if ANY file is invalid.

        Note: metadata uses hierarchical resolution, so children inherit from parents.
        To test actual errors, children must explicitly override with INVALID values.
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            valid_metadata = "contact:\n  name: Test\n  email: test@example.org\nlicense: MIT\n"
            # Invalid: override with invalid email (can't be fixed by inheritance)
            invalid_email = "contact:\n  name: Test\n  email: not-valid\nlicense: MIT\n"
            # Root: valid
            Path(".portolan/metadata.yaml").write_text(valid_metadata)
            # Collection: invalid (only one)
            Path("climate").mkdir()
            Path("climate/collection.json").write_text('{"type": "Collection"}')
            Path("climate/.portolan").mkdir()
            Path("climate/.portolan/metadata.yaml").write_text(invalid_email)

            result = runner.invoke(cli, ["metadata", "validate"])

            assert result.exit_code != 0  # Fails if ANY invalid

    @pytest.mark.unit
    def test_no_recursive_limits_to_single_path(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --no-recursive should only validate target path."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            valid_metadata = "contact:\n  name: Test\n  email: test@example.org\nlicense: MIT\n"
            invalid_metadata = "citation: Test\n"
            # Root: valid
            Path(".portolan/metadata.yaml").write_text(valid_metadata)
            # Collection: invalid (should NOT be checked with --no-recursive)
            Path("climate").mkdir()
            Path("climate/collection.json").write_text('{"type": "Collection"}')
            Path("climate/.portolan").mkdir()
            Path("climate/.portolan/metadata.yaml").write_text(invalid_metadata)

            result = runner.invoke(cli, ["metadata", "validate", "--no-recursive"])

            # Should pass because we only check root (which is valid)
            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    def test_recursive_skips_directories_without_metadata(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """metadata validate should skip STAC levels without metadata.yaml."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            valid_metadata = "contact:\n  name: Test\n  email: test@example.org\nlicense: MIT\n"
            # Root: valid
            Path(".portolan/metadata.yaml").write_text(valid_metadata)
            # Collection: no metadata.yaml (should be skipped, not fail)
            Path("climate").mkdir()
            Path("climate/collection.json").write_text('{"type": "Collection"}')
            # No .portolan/metadata.yaml created

            result = runner.invoke(cli, ["metadata", "validate"])

            assert result.exit_code == 0, f"Failed: {result.output}"

    @pytest.mark.unit
    def test_no_recursive_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate --no-recursive --json should output single-path schema."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(
                cli, ["--format", "json", "metadata", "validate", "--no-recursive"]
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            # Non-recursive mode uses original schema (no "mode" or "results")
            assert output["data"]["valid"] is True


class TestReadmeGenerate:
    """Tests for `portolan readme` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_generates_readme_at_catalog_root(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme should generate README.md at catalog root."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Create catalog.json with title
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "my-catalog",
                        "title": "My Catalog",
                        "description": "A test catalog",
                    }
                )
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: Test User\n  email: test@example.org\nlicense: CC-BY-4.0\n"
            )

            result = runner.invoke(cli, ["readme"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert Path("README.md").exists()
            content = Path("README.md").read_text()
            assert "# My Catalog" in content  # Title from STAC

    @pytest.mark.unit
    def test_generates_readme_at_collection(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme PATH should generate README.md at collection level."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("demographics/.portolan").mkdir(parents=True)
            Path("demographics/collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": "demographics",
                        "title": "Demographics",
                        "description": "Census data",
                    }
                )
            )
            Path("demographics/.portolan/metadata.yaml").write_text(
                "contact:\n  name: Team\n  email: team@org.com\nlicense: CC0-1.0\n"
            )

            result = runner.invoke(cli, ["readme", "demographics"])

            assert result.exit_code == 0
            assert Path("demographics/README.md").exists()

    @pytest.mark.unit
    def test_stdout_option(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --stdout --no-recursive should print README to stdout without writing file.

        Note: --stdout requires --no-recursive since you can't print multiple READMEs to stdout.
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "My Catalog"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["readme", "--stdout", "--no-recursive"])

            assert result.exit_code == 0
            assert "# My Catalog" in result.output
            assert not Path("README.md").exists()

    @pytest.mark.unit
    def test_check_mode_passes_when_fresh(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --check should pass (exit 0) when README is up-to-date."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )
            # Generate README first
            runner.invoke(cli, ["readme"])

            result = runner.invoke(cli, ["readme", "--check"])

            assert result.exit_code == 0

    @pytest.mark.unit
    def test_check_mode_fails_when_stale(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --check should fail (exit 1) when README is stale."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )
            # Write a different README
            Path("README.md").write_text("# Old Content\n")

            result = runner.invoke(cli, ["readme", "--check"])

            assert result.exit_code != 0

    @pytest.mark.unit
    def test_check_mode_fails_when_missing(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --check should fail (exit 1) when README doesn't exist."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["readme", "--check"])

            assert result.exit_code != 0

    @pytest.mark.unit
    def test_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --format json should output JSON envelope."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["--format", "json", "readme"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "readme"

    @pytest.mark.unit
    def test_uses_stac_title_not_metadata(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme should use title from STAC, not metadata.yaml."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "root",
                        "title": "STAC Title",
                        "description": "STAC description",
                    }
                )
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["readme"])

            assert result.exit_code == 0
            content = Path("README.md").read_text()
            assert "# STAC Title" in content
            assert "Portolan" in content  # Attribution footer

    @pytest.mark.unit
    def test_verbose_shows_file_reads(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --verbose should show which files are being read."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("demographics").mkdir()
            Path("demographics/collection.json").write_text(
                json.dumps({"type": "Collection", "id": "demographics", "title": "Demographics"})
            )
            Path("demographics/.portolan").mkdir()
            Path("demographics/.portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["readme", "demographics", "--verbose"])

            assert result.exit_code == 0
            # Should show reading collection.json
            assert "collection.json" in result.output
            # Should show reading metadata.yaml
            assert "metadata.yaml" in result.output
            # Should show generating
            assert "Generating" in result.output or "README.md" in result.output

    @pytest.mark.unit
    def test_verbose_short_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme -v should work as shorthand for --verbose."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["readme", "-v"])

            assert result.exit_code == 0
            # Should show verbose output
            assert "catalog.json" in result.output or "metadata.yaml" in result.output

    @pytest.mark.unit
    def test_verbose_recursive_shows_per_collection_progress(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """readme --verbose (recursive by default) should show progress for each collection."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "root", "title": "Root"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )
            # Create two collections
            for name in ["demographics", "climate"]:
                Path(name).mkdir()
                Path(f"{name}/collection.json").write_text(
                    json.dumps({"type": "Collection", "id": name, "title": name.title()})
                )
                Path(f"{name}/.portolan").mkdir()
                Path(f"{name}/.portolan/metadata.yaml").write_text(
                    "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
                )

            result = runner.invoke(cli, ["readme", "--verbose"])

            assert result.exit_code == 0
            # Should mention both collections in verbose output
            assert "demographics" in result.output
            assert "climate" in result.output

    @pytest.mark.unit
    def test_verbose_check_shows_reads_even_when_fresh(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """readme --check --verbose should show file reads even when README is fresh."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )
            # Generate README first
            runner.invoke(cli, ["readme"])

            result = runner.invoke(cli, ["readme", "--check", "--verbose"])

            assert result.exit_code == 0
            # Should still show what was read
            assert "catalog.json" in result.output or "metadata.yaml" in result.output

    @pytest.mark.unit
    def test_verbose_ignored_with_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --verbose --json should not include verbose messages in JSON."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["--format", "json", "readme", "--verbose"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            # JSON output should be valid JSON (verbose text not mixed in)
            assert "command" in output

    @pytest.mark.unit
    def test_default_no_verbose_is_minimal(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme without --verbose should produce minimal output."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            Path("catalog.json").write_text(
                json.dumps({"type": "Catalog", "id": "test", "title": "Test"})
            )
            Path(".portolan/metadata.yaml").write_text(
                "contact:\n  name: N\n  email: a@b.c\nlicense: MIT\n"
            )

            result = runner.invoke(cli, ["readme"])

            assert result.exit_code == 0
            # Should NOT show reading messages (only success)
            assert "Reading" not in result.output
            # Should show success message
            assert "README.md" in result.output or "Generated" in result.output


class TestPathTraversalHardening:
    """ADR-0030: user-supplied PATH args must not escape the catalog root."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_metadata_init_recursive_rejects_traversal(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """metadata init (recursive default) rejects a PATH escaping the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["metadata", "init", "../escape"])

            assert result.exit_code != 0
            assert "traversal" in result.output.lower()

    @pytest.mark.unit
    def test_metadata_init_no_recursive_rejects_traversal(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """metadata init --no-recursive rejects a PATH escaping the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["metadata", "init", "../escape", "--no-recursive"])

            assert result.exit_code != 0
            assert "traversal" in result.output.lower()

    @pytest.mark.unit
    def test_metadata_validate_rejects_traversal(self, runner: CliRunner, tmp_path: Path) -> None:
        """metadata validate rejects a PATH escaping the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["metadata", "validate", "../escape"])

            assert result.exit_code != 0
            assert "traversal" in result.output.lower()

    @pytest.mark.unit
    def test_metadata_validate_no_recursive_rejects_traversal(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """metadata validate --no-recursive rejects a PATH escaping the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["metadata", "validate", "../escape", "--no-recursive"])

            assert result.exit_code != 0
            assert "traversal" in result.output.lower()

    @pytest.mark.unit
    def test_readme_rejects_traversal(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme rejects a PATH escaping the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["readme", "../escape"])

            assert result.exit_code != 0
            assert "traversal" in result.output.lower()

    @pytest.mark.unit
    def test_readme_no_recursive_rejects_traversal(self, runner: CliRunner, tmp_path: Path) -> None:
        """readme --no-recursive rejects a PATH escaping the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["readme", "../escape", "--no-recursive"])

            assert result.exit_code != 0
            assert "traversal" in result.output.lower()

    @pytest.mark.unit
    def test_recursive_readme_traversal_json_uses_correct_command(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """JSON error for a recursive readme traversal must be labeled 'readme'."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["--format", "json", "readme", "../escape"])

            assert result.exit_code != 0
            payload = json.loads(result.output)
            assert payload["command"] == "readme"

    @pytest.mark.unit
    def test_recursive_validate_traversal_json_uses_correct_command(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """JSON error for a recursive validate traversal must be labeled 'metadata validate'."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["--format", "json", "metadata", "validate", "../escape"])

            assert result.exit_code != 0
            payload = json.loads(result.output)
            assert payload["command"] == "metadata validate"
