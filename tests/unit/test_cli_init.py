"""Tests for `portolan init` CLI command.

These tests verify the CLI behavior of the init command. For the new v2 file
structure, catalog.json is at ROOT level (not in .portolan).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestCliInit:
    """Tests for the `portolan init` CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_init_creates_catalog_in_current_directory(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """portolan init should create catalog.json at root and .portolan directory."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            # New structure: catalog.json at root
            assert Path("catalog.json").exists()
            # .portolan directory with config.yaml (per issue #290, state.json removed)
            assert Path(".portolan").exists()
            assert Path(".portolan/config.yaml").exists()
            # Note: state.json no longer created (removed per issue #290)

    @pytest.mark.unit
    def test_init_prints_success_message(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init should print a success message."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            assert result.exit_code == 0
            assert "Initialized" in result.output or "\u2713" in result.output

    @pytest.mark.unit
    def test_init_fails_if_managed_catalog_exists(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init should fail if MANAGED catalog exists."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog (config.yaml alone is sufficient per issue #290)
            portolan = Path(".portolan")
            portolan.mkdir()
            (portolan / "config.yaml").write_text("# Portolan config\n")

            # Use --auto to skip interactive prompts and test error path
            result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            assert result.exit_code == 1
            assert "already" in result.output.lower()

    @pytest.mark.unit
    def test_init_fails_if_unmanaged_stac_exists(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init should fail if unmanaged STAC catalog exists."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create unmanaged STAC catalog
            Path("catalog.json").write_text('{"type": "Catalog"}')

            # Use --auto to skip interactive prompts and test error path
            result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            assert result.exit_code == 1
            output_lower = result.output.lower()
            assert "stac" in output_lower or "catalog" in output_lower

    @pytest.mark.unit
    def test_init_accepts_path_argument(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init PATH should create catalog at specified path."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            target = Path("my-catalog")
            target.mkdir()

            result = runner.invoke(cli, ["init", "--auto", str(target), "--license", "CC-BY-4.0"])

            assert result.exit_code == 0
            assert (target / "catalog.json").exists()
            assert (target / ".portolan").exists()

    @pytest.mark.unit
    def test_init_with_title_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init --title should set catalog title."""
        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["init", "--auto", "--title", "My Test Catalog", "--license", "CC-BY-4.0"]
            )

            assert result.exit_code == 0
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("title") == "My Test Catalog"

    @pytest.mark.unit
    def test_init_with_description_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init --description should set catalog description."""
        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli,
                ["init", "--auto", "--description", "Test description", "--license", "CC-BY-4.0"],
            )

            assert result.exit_code == 0
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("description") == "Test description"

    @pytest.mark.unit
    def test_init_with_id_flag(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init --id should set the catalog id."""
        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["init", "--auto", "--id", "phl-housing", "--license", "CC-BY-4.0"]
            )

            assert result.exit_code == 0
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("id") == "phl-housing"

    @pytest.mark.unit
    def test_id_flag_overrides_directory_name(self, runner: CliRunner, tmp_path: Path) -> None:
        """--id wins over the directory name the id is otherwise derived from."""
        import json

        target = tmp_path / "publish"
        target.mkdir()
        result = runner.invoke(
            cli, ["init", str(target), "--auto", "--id", "phl-housing", "--license", "CC-BY-4.0"]
        )

        assert result.exit_code == 0
        data = json.loads((target / "catalog.json").read_text())
        assert data.get("id") == "phl-housing"

    @pytest.mark.unit
    def test_invalid_id_is_rejected(self, runner: CliRunner, tmp_path: Path) -> None:
        """An --id outside the STAC identifier pattern fails instead of being coerced."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["init", "--auto", "--id", "my catalog", "--license", "CC-BY-4.0"]
            )

            assert result.exit_code != 0
            assert not Path("catalog.json").exists()

    @pytest.mark.unit
    def test_warns_when_derived_id_is_a_tooling_artifact(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """A directory named 'publish' warns that the derived id names no data (#821)."""
        target = tmp_path / "publish"
        target.mkdir()
        result = runner.invoke(cli, ["init", str(target), "--auto", "--license", "CC-BY-4.0"])

        assert result.exit_code == 0
        assert "tooling artifact" in result.output
        assert "--id" in result.output

    @pytest.mark.unit
    def test_no_artifact_warning_when_id_is_explicit(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """An explicit --id is the caller's decision, so the artifact warning stays quiet."""
        target = tmp_path / "publish"
        target.mkdir()
        result = runner.invoke(
            cli, ["init", str(target), "--auto", "--id", "phl-housing", "--license", "CC-BY-4.0"]
        )

        assert result.exit_code == 0
        assert "tooling artifact" not in result.output


class TestCliInitInteractive:
    """Tests for interactive prompting in the init command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_interactive_prompts_for_title_and_description(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Without --auto, init should prompt for title and description."""
        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Provide input for title and description prompts
            result = runner.invoke(
                cli,
                ["init"],
                input="Interactive Title\nInteractive Description\nCC-BY-4.0\n",
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("title") == "Interactive Title"
            assert data.get("description") == "Interactive Description"

    @pytest.mark.unit
    def test_interactive_derives_title_on_empty_input(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Empty title input derives a human-readable title (Issue #502).

        Titles are mandatory, so an empty prompt falls back to a title
        humanized from the catalog directory name rather than None.
        """
        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Empty title (just Enter), then description
            result = runner.invoke(
                cli,
                ["init"],
                input="\nCustom Description\nCC-BY-4.0\n",
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            data = json.loads(Path("catalog.json").read_text())
            # Title is mandatory and must be present + human-readable.
            title = data.get("title")
            assert title, "Expected a derived title, got falsy value"
            assert "_" not in title  # humanized, not a raw slug
            assert data.get("description") == "Custom Description"

    @pytest.mark.unit
    def test_interactive_uses_default_description(self, runner: CliRunner, tmp_path: Path) -> None:
        """Empty description input should use the default value."""
        import json

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Empty title, empty description (use default)
            result = runner.invoke(
                cli,
                ["init"],
                input="\n\nCC-BY-4.0\n",
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("description") == "A Portolan-managed STAC catalog"

    @pytest.mark.unit
    def test_json_mode_skips_prompts(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output mode should skip prompts."""
        import json as json_module

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # JSON mode should not prompt
            result = runner.invoke(cli, ["--format", "json", "init", "--license", "CC-BY-4.0"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            # Verify JSON output
            output = json_module.loads(result.output)
            assert output["success"] is True
