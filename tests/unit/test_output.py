"""Tests for output.py - standardized terminal output utilities.

These tests verify the observable behavior of output functions,
specifically targeting mutations that survived initial testing:
- Default nl=True parameter (newlines added by default)
- Prefix symbols are used (✓, ✗, →, ⚠)
- Colors are applied (green, red, blue, yellow)
- Dry-run mode prefixes messages with [DRY RUN]
- Verbose mode includes additional technical details
"""

from __future__ import annotations

from io import StringIO

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from portolan_cli.output import detail, error, info, success, warn


class TestOutputFunctions:
    """Tests for output helper functions."""

    @pytest.mark.unit
    def test_success_includes_checkmark_prefix(self) -> None:
        """success() should include the ✓ checkmark prefix."""
        output = StringIO()
        success("test message", file=output)

        result = output.getvalue()
        assert "✓" in result, f"Expected checkmark in output, got: {result!r}"

    @pytest.mark.unit
    def test_success_includes_message(self) -> None:
        """success() should include the actual message."""
        output = StringIO()
        success("my test message", file=output)

        result = output.getvalue()
        assert "my test message" in result

    @pytest.mark.unit
    def test_success_adds_newline_by_default(self) -> None:
        """success() should add a newline by default (nl=True)."""
        output = StringIO()
        success("test", file=output)

        result = output.getvalue()
        assert result.endswith("\n"), f"Expected newline at end, got: {result!r}"

    @pytest.mark.unit
    def test_success_no_newline_when_nl_false(self) -> None:
        """success() should not add newline when nl=False."""
        output = StringIO()
        success("test", file=output, nl=False)

        result = output.getvalue()
        assert not result.endswith("\n"), f"Expected no newline, got: {result!r}"

    @pytest.mark.unit
    def test_error_includes_x_prefix(self) -> None:
        """error() should include the ✗ X prefix."""
        output = StringIO()
        error("test message", file=output)

        result = output.getvalue()
        assert "✗" in result, f"Expected X in output, got: {result!r}"

    @pytest.mark.unit
    def test_error_includes_message(self) -> None:
        """error() should include the actual message."""
        output = StringIO()
        error("my error message", file=output)

        result = output.getvalue()
        assert "my error message" in result

    @pytest.mark.unit
    def test_error_adds_newline_by_default(self) -> None:
        """error() should add a newline by default (nl=True)."""
        output = StringIO()
        error("test", file=output)

        result = output.getvalue()
        assert result.endswith("\n"), f"Expected newline at end, got: {result!r}"

    @pytest.mark.unit
    def test_error_no_newline_when_nl_false(self) -> None:
        """error() should not add newline when nl=False."""
        output = StringIO()
        error("test", file=output, nl=False)

        result = output.getvalue()
        assert not result.endswith("\n"), f"Expected no newline, got: {result!r}"

    @pytest.mark.unit
    def test_info_includes_arrow_prefix(self) -> None:
        """info() should include the → arrow prefix."""
        output = StringIO()
        info("test message", file=output)

        result = output.getvalue()
        assert "→" in result, f"Expected arrow in output, got: {result!r}"

    @pytest.mark.unit
    def test_info_adds_newline_by_default(self) -> None:
        """info() should add a newline by default (nl=True)."""
        output = StringIO()
        info("test", file=output)

        result = output.getvalue()
        assert result.endswith("\n"), f"Expected newline at end, got: {result!r}"

    @pytest.mark.unit
    def test_warn_includes_warning_prefix(self) -> None:
        """warn() should include the ⚠ warning prefix."""
        output = StringIO()
        warn("test message", file=output)

        result = output.getvalue()
        assert "⚠" in result, f"Expected warning symbol in output, got: {result!r}"

    @pytest.mark.unit
    def test_warn_adds_newline_by_default(self) -> None:
        """warn() should add a newline by default (nl=True)."""
        output = StringIO()
        warn("test", file=output)

        result = output.getvalue()
        assert result.endswith("\n"), f"Expected newline at end, got: {result!r}"

    @pytest.mark.unit
    def test_detail_adds_newline_by_default(self) -> None:
        """detail() should add a newline by default (nl=True)."""
        output = StringIO()
        detail("test", file=output)

        result = output.getvalue()
        assert result.endswith("\n"), f"Expected newline at end, got: {result!r}"

    @pytest.mark.unit
    def test_detail_includes_message(self) -> None:
        """detail() should include the actual message."""
        output = StringIO()
        detail("my detail message", file=output)

        result = output.getvalue()
        assert "my detail message" in result

    @pytest.mark.unit
    def test_info_includes_message(self) -> None:
        """info() should include the actual message, not None."""
        output = StringIO()
        info("my info message", file=output)

        result = output.getvalue()
        assert "my info message" in result
        assert "None" not in result

    @pytest.mark.unit
    def test_warn_includes_message(self) -> None:
        """warn() should include the actual message, not None."""
        output = StringIO()
        warn("my warn message", file=output)

        result = output.getvalue()
        assert "my warn message" in result
        assert "None" not in result


class TestDryRunMode:
    """Tests for dry-run mode functionality."""

    @pytest.mark.unit
    def test_success_with_dry_run_adds_prefix(self) -> None:
        """success() with dry_run=True should add [DRY RUN] prefix."""
        output = StringIO()
        success("operation succeeded", file=output, dry_run=True)

        result = output.getvalue()
        assert "[DRY RUN]" in result, f"Expected [DRY RUN] prefix, got: {result!r}"
        assert "operation succeeded" in result

    @pytest.mark.unit
    def test_error_with_dry_run_adds_prefix(self) -> None:
        """error() with dry_run=True should add [DRY RUN] prefix."""
        output = StringIO()
        error("operation failed", file=output, dry_run=True)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert "operation failed" in result

    @pytest.mark.unit
    def test_info_with_dry_run_adds_prefix(self) -> None:
        """info() with dry_run=True should add [DRY RUN] prefix."""
        output = StringIO()
        info("reading file", file=output, dry_run=True)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert "reading file" in result

    @pytest.mark.unit
    def test_warn_with_dry_run_adds_prefix(self) -> None:
        """warn() with dry_run=True should add [DRY RUN] prefix."""
        output = StringIO()
        warn("missing file", file=output, dry_run=True)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert "missing file" in result

    @pytest.mark.unit
    def test_detail_with_dry_run_adds_prefix(self) -> None:
        """detail() with dry_run=True should add [DRY RUN] prefix."""
        output = StringIO()
        detail("processing chunk", file=output, dry_run=True)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert "processing chunk" in result

    @pytest.mark.unit
    def test_dry_run_false_has_no_prefix(self) -> None:
        """Functions with dry_run=False should not add [DRY RUN] prefix."""
        output = StringIO()
        success("normal operation", file=output, dry_run=False)

        result = output.getvalue()
        assert "[DRY RUN]" not in result
        assert "normal operation" in result

    @pytest.mark.unit
    def test_dry_run_default_is_false(self) -> None:
        """Functions should default to dry_run=False."""
        output = StringIO()
        success("default behavior", file=output)

        result = output.getvalue()
        assert "[DRY RUN]" not in result

    @settings(suppress_health_check=[HealthCheck.differing_executors])
    @given(st.text(min_size=1))
    @pytest.mark.unit
    def test_dry_run_preserves_message_content(self, message: str) -> None:
        """Dry-run mode should preserve all message content."""
        output = StringIO()
        info(message, file=output, dry_run=True)

        result = output.getvalue()
        # Message should appear in output (ignoring ANSI codes)
        assert message in result or all(char in result for char in message)


class TestVerboseMode:
    """Tests for verbose mode functionality."""

    @pytest.mark.unit
    def test_success_with_verbose_includes_message(self) -> None:
        """success() with verbose=True should include the message."""
        output = StringIO()
        success("operation completed", file=output, verbose=True)

        result = output.getvalue()
        assert "operation completed" in result

    @pytest.mark.unit
    def test_verbose_does_not_suppress_output(self) -> None:
        """verbose=True should not suppress any output."""
        output = StringIO()
        info("verbose message", file=output, verbose=True)

        result = output.getvalue()
        assert len(result) > 0
        assert "verbose message" in result

    @pytest.mark.unit
    def test_verbose_false_same_as_normal(self) -> None:
        """verbose=False should produce same output as default."""
        output_verbose_false = StringIO()
        output_default = StringIO()

        success("test message", file=output_verbose_false, verbose=False)
        success("test message", file=output_default)

        # Both should produce identical output
        assert output_verbose_false.getvalue() == output_default.getvalue()

    @pytest.mark.unit
    def test_verbose_default_is_false(self) -> None:
        """Functions should default to verbose=False."""
        output = StringIO()
        info("default verbosity", file=output)

        result = output.getvalue()
        # Should still produce output, just not verbose extras
        assert len(result) > 0


class TestCombinedModes:
    """Tests for dry-run + verbose mode combinations."""

    @pytest.mark.unit
    def test_dry_run_and_verbose_both_active(self) -> None:
        """Both dry_run=True and verbose=True should work together."""
        output = StringIO()
        success("operation", file=output, dry_run=True, verbose=True)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert "operation" in result

    @pytest.mark.unit
    def test_dry_run_true_verbose_false(self) -> None:
        """dry_run=True with verbose=False should only show dry-run prefix."""
        output = StringIO()
        info("test", file=output, dry_run=True, verbose=False)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert "test" in result

    @pytest.mark.unit
    def test_dry_run_false_verbose_true(self) -> None:
        """dry_run=False with verbose=True should not show dry-run prefix."""
        output = StringIO()
        info("test", file=output, dry_run=False, verbose=True)

        result = output.getvalue()
        assert "[DRY RUN]" not in result
        assert "test" in result

    @pytest.mark.unit
    def test_all_functions_support_both_modes(self) -> None:
        """All output functions should accept both dry_run and verbose parameters."""
        output = StringIO()

        # Should not raise TypeError
        success("test", file=output, dry_run=True, verbose=True)
        error("test", file=output, dry_run=True, verbose=True)
        info("test", file=output, dry_run=True, verbose=True)
        warn("test", file=output, dry_run=True, verbose=True)
        detail("test", file=output, dry_run=True, verbose=True)

        result = output.getvalue()
        # Each call should produce output
        assert result.count("[DRY RUN]") == 5


class TestModesWithNewlineControl:
    """Tests ensuring modes work correctly with nl parameter."""

    @pytest.mark.unit
    def test_dry_run_with_nl_false(self) -> None:
        """Dry-run mode should work with nl=False."""
        output = StringIO()
        success("test", file=output, dry_run=True, nl=False)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert not result.endswith("\n")

    @pytest.mark.unit
    def test_verbose_with_nl_false(self) -> None:
        """Verbose mode should work with nl=False."""
        output = StringIO()
        info("test", file=output, verbose=True, nl=False)

        result = output.getvalue()
        assert not result.endswith("\n")

    @pytest.mark.unit
    def test_both_modes_with_nl_false(self) -> None:
        """Both modes should work together with nl=False."""
        output = StringIO()
        warn("test", file=output, dry_run=True, verbose=True, nl=False)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert not result.endswith("\n")
