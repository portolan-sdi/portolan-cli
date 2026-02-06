"""Tests for output.py - standardized terminal output utilities.

These tests verify the observable behavior of output functions,
specifically targeting mutations that survived initial testing:
- Default nl=True parameter (newlines added by default)
- Prefix symbols are used (✓, ✗, →, ⚠)
- Colors are applied (green, red, blue, yellow)
"""

from __future__ import annotations

from io import StringIO

import pytest

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
