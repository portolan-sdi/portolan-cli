"""Tests for `portolan quickstart` and `portolan agent-instructions` CLI commands.

These commands print agent-friendly instructions for AI coding tools (kata-style).
"""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestQuickstartCommand:
    """Tests for the `portolan quickstart` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_quickstart_prints_content(self, runner: CliRunner) -> None:
        """portolan quickstart should print agent instructions."""
        result = runner.invoke(cli, ["quickstart"])

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "Portolan Agent Quickstart" in result.output

    @pytest.mark.unit
    def test_quickstart_contains_stac_terminology(self, runner: CliRunner) -> None:
        """Quickstart should include STAC terminology table."""
        result = runner.invoke(cli, ["quickstart"])

        assert result.exit_code == 0
        assert "Catalog" in result.output
        assert "Collection" in result.output
        assert "Item" in result.output
        assert "Asset" in result.output

    @pytest.mark.unit
    def test_quickstart_contains_example_session(self, runner: CliRunner) -> None:
        """Quickstart should include example commands."""
        result = runner.invoke(cli, ["quickstart"])

        assert result.exit_code == 0
        assert "portolan init" in result.output
        assert "portolan add" in result.output
        assert "portolan scan" in result.output
        assert "portolan check" in result.output

    @pytest.mark.unit
    def test_quickstart_mentions_json_format(self, runner: CliRunner) -> None:
        """Quickstart should mention --format=json for agent parsing."""
        result = runner.invoke(cli, ["quickstart"])

        assert result.exit_code == 0
        assert "--format=json" in result.output or "--json" in result.output

    @pytest.mark.unit
    def test_quickstart_json_output(self, runner: CliRunner) -> None:
        """portolan quickstart --json should return structured JSON."""
        result = runner.invoke(cli, ["quickstart", "--json"])

        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["command"] == "quickstart"
        assert "content" in data["data"]
        assert "Portolan Agent Quickstart" in data["data"]["content"]


class TestAgentInstructionsCommand:
    """Tests for the `portolan agent-instructions` alias command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_agent_instructions_is_alias_for_quickstart(self, runner: CliRunner) -> None:
        """portolan agent-instructions should produce same output as quickstart."""
        quickstart_result = runner.invoke(cli, ["quickstart"])
        alias_result = runner.invoke(cli, ["agent-instructions"])

        assert quickstart_result.exit_code == 0
        assert alias_result.exit_code == 0
        assert quickstart_result.output == alias_result.output

    @pytest.mark.unit
    def test_agent_instructions_json_output(self, runner: CliRunner) -> None:
        """portolan agent-instructions --json should work like quickstart --json."""
        result = runner.invoke(cli, ["agent-instructions", "--json"])

        assert result.exit_code == 0, f"Failed: {result.output}"
        data = json.loads(result.output)
        assert data["success"] is True
        # Command name reflects the alias used
        assert data["command"] == "agent-instructions"
