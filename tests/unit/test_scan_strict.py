"""Unit tests for the scan --strict flag.

These tests verify that --strict elevates warnings to errors,
causing the command to exit with code 1 when warnings are present.

Test cases:
1. No --strict + warnings → exit 0 (warnings don't fail)
2. --strict + warnings → exit 1 (warnings elevated to errors)
3. --strict + clean (no issues) → exit 0
4. JSON output includes fix_commands structure when --strict
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


@pytest.fixture
def dir_with_warnings(tmp_path: Path) -> Path:
    """Create a directory with files that trigger warnings.

    Uses invalid characters in filenames to generate INVALID_CHARACTERS warnings.
    """
    # File with space in name → triggers INVALID_CHARACTERS warning
    (tmp_path / "data (copy).geojson").write_text('{"type": "FeatureCollection", "features": []}')
    return tmp_path


@pytest.fixture
def dir_clean(tmp_path: Path) -> Path:
    """Create a directory with a single clean geo-asset (no warnings).

    Single file with valid name = no issues.
    """
    # Valid filename, single file = no warnings
    (tmp_path / "clean_data.geojson").write_text('{"type": "FeatureCollection", "features": []}')
    return tmp_path


@pytest.fixture
def dir_with_errors(tmp_path: Path) -> Path:
    """Create a directory with files that trigger errors.

    Incomplete shapefile (missing .dbf) triggers ERROR severity.
    """
    # Shapefile without required sidecars → ERROR
    (tmp_path / "incomplete.shp").write_bytes(b"\x00" * 100)
    (tmp_path / "incomplete.shx").write_bytes(b"\x00" * 100)
    # Missing .dbf → incomplete_shapefile ERROR
    return tmp_path


# =============================================================================
# Test: Exit codes without --strict
# =============================================================================


@pytest.mark.unit
class TestScanWithoutStrict:
    """Tests for scan command without --strict flag (default behavior)."""

    def test_warnings_exit_0(self, runner: CliRunner, dir_with_warnings: Path) -> None:
        """Scan with warnings (no --strict) should exit 0.

        Warnings are informational and don't cause failure by default.
        This is the expected behavior for an informational scan.
        """
        result = runner.invoke(cli, ["scan", str(dir_with_warnings)])

        assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}: {result.output}"

    def test_clean_exit_0(self, runner: CliRunner, dir_clean: Path) -> None:
        """Scan with no issues should exit 0."""
        result = runner.invoke(cli, ["scan", str(dir_clean)])

        assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}: {result.output}"

    def test_errors_without_strict_still_exit_0(
        self, runner: CliRunner, dir_with_errors: Path
    ) -> None:
        """Scan with errors (no --strict) should still exit 0.

        Even errors don't cause non-zero exit by default because scan
        is informational. The --strict flag changes this behavior.
        """
        result = runner.invoke(cli, ["scan", str(dir_with_errors)])

        # Note: Current behavior is exit 0 even with errors
        # This test documents that behavior
        assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}: {result.output}"


# =============================================================================
# Test: Exit codes with --strict
# =============================================================================


@pytest.mark.unit
class TestScanWithStrict:
    """Tests for scan command with --strict flag."""

    def test_strict_with_warnings_exits_1(self, runner: CliRunner, dir_with_warnings: Path) -> None:
        """Scan --strict with warnings should exit 1.

        --strict elevates warnings to errors, causing non-zero exit.
        """
        result = runner.invoke(cli, ["scan", "--strict", str(dir_with_warnings)])

        assert result.exit_code == 1, (
            f"Expected exit 1 (warnings elevated), got {result.exit_code}: {result.output}"
        )

    def test_strict_clean_exits_0(self, runner: CliRunner, dir_clean: Path) -> None:
        """Scan --strict with no issues should exit 0.

        Clean scan passes even with --strict.
        """
        result = runner.invoke(cli, ["scan", "--strict", str(dir_clean)])

        assert result.exit_code == 0, f"Expected exit 0, got {result.exit_code}: {result.output}"

    def test_strict_with_errors_exits_1(self, runner: CliRunner, dir_with_errors: Path) -> None:
        """Scan --strict with errors should exit 1.

        Errors always cause exit 1 when --strict is enabled.
        """
        result = runner.invoke(cli, ["scan", "--strict", str(dir_with_errors)])

        assert result.exit_code == 1, f"Expected exit 1, got {result.exit_code}: {result.output}"


# =============================================================================
# Test: JSON output with --strict
# =============================================================================


@pytest.mark.unit
class TestScanStrictJsonOutput:
    """Tests for JSON output with --strict flag."""

    def test_strict_json_success_false_on_warnings(
        self, runner: CliRunner, dir_with_warnings: Path
    ) -> None:
        """JSON output should have success=false when --strict + warnings.

        The JSON envelope reflects the elevated severity.
        """
        result = runner.invoke(cli, ["scan", "--strict", "--json", str(dir_with_warnings)])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["success"] is False, "Expected success=false with --strict + warnings"

    def test_strict_json_success_true_when_clean(self, runner: CliRunner, dir_clean: Path) -> None:
        """JSON output should have success=true when --strict + no issues."""
        result = runner.invoke(cli, ["scan", "--strict", "--json", str(dir_clean)])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["success"] is True, "Expected success=true with --strict + clean"

    def test_json_includes_fix_commands_structure(
        self, runner: CliRunner, dir_with_warnings: Path
    ) -> None:
        """JSON output should include fix_commands array for agent consumption.

        fix_commands provides structured remediation commands that agents
        can parse and execute automatically.
        """
        result = runner.invoke(cli, ["scan", "--strict", "--json", str(dir_with_warnings)])

        output = json.loads(result.output)
        data = output.get("data", {})

        # fix_commands should be present in JSON output
        assert "fix_commands" in data, "Expected fix_commands in JSON data"
        assert isinstance(data["fix_commands"], list), "fix_commands should be a list"

    def test_fix_commands_structure_has_required_fields(
        self, runner: CliRunner, dir_with_warnings: Path
    ) -> None:
        """Each fix_command should have command, args, and reason fields.

        Structure per ADR:
        {"command": "scan", "args": ["--fix"], "reason": "Invalid characters"}
        """
        result = runner.invoke(cli, ["scan", "--strict", "--json", str(dir_with_warnings)])

        output = json.loads(result.output)
        fix_commands = output.get("data", {}).get("fix_commands", [])

        # Should have at least one fix command for the invalid filename
        assert len(fix_commands) > 0, "Expected at least one fix_command"

        for cmd in fix_commands:
            assert "command" in cmd, "fix_command missing 'command' field"
            assert "args" in cmd, "fix_command missing 'args' field"
            assert "reason" in cmd, "fix_command missing 'reason' field"


# =============================================================================
# Test: Human-readable output with --strict
# =============================================================================


@pytest.mark.unit
class TestScanStrictHumanOutput:
    """Tests for human-readable output with --strict flag."""

    def test_strict_shows_elevated_warning_message(
        self, runner: CliRunner, dir_with_warnings: Path
    ) -> None:
        """Human output should indicate warnings were elevated with --strict."""
        result = runner.invoke(cli, ["scan", "--strict", str(dir_with_warnings)])

        # Should exit 1 (not 2 from usage error) AND show strict mode message
        assert result.exit_code == 1, f"Expected exit 1, got {result.exit_code}"
        # Output should indicate strict mode is active (not just contain "warning")
        assert "treated as error" in result.output.lower() or (
            "strict" in result.output.lower() and "warning" in result.output.lower()
        ), f"Expected strict mode indication in output: {result.output}"


# =============================================================================
# Test: --strict interaction with --fix
# =============================================================================


@pytest.mark.unit
class TestScanStrictWithFix:
    """Tests for --strict combined with --fix flag."""

    def test_strict_fix_dryrun_exits_1_with_warnings(
        self, runner: CliRunner, dir_with_warnings: Path
    ) -> None:
        """--strict --fix --dry-run should still exit 1 with warnings.

        Even when showing fixes, strict mode should fail on warnings.
        """
        result = runner.invoke(
            cli, ["scan", "--strict", "--fix", "--dry-run", str(dir_with_warnings)]
        )

        assert result.exit_code == 1, (
            f"Expected exit 1 with --strict, got {result.exit_code}: {result.output}"
        )
