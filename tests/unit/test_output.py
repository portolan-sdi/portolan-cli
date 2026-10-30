"""Tests for output.py - standardized terminal output utilities.

These tests verify the observable behavior of output functions,
specifically targeting behavior that earlier tests did not catch:
- Default nl=True parameter (newlines added by default)
- Prefix symbols are used (✓, ✗, →, ⚠)
- Colors are applied (green, red, blue, yellow)
- Dry-run mode prefixes messages with [DRY RUN]
- Verbose mode includes additional technical details
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from io import StringIO
from typing import TYPE_CHECKING

import pytest
from hypothesis import given
from hypothesis import strategies as st

from portolan_cli.output import detail, error, info, success, warn

if TYPE_CHECKING:
    from collections.abc import Iterator


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

    @given(st.text(min_size=1))
    @pytest.mark.unit
    def test_dry_run_preserves_message_content(self, message: str) -> None:
        """Dry-run mode should preserve all message content."""
        output = StringIO()
        info(message, file=output, dry_run=True)

        result = output.getvalue()
        # Message should appear in output (ignoring ANSI codes)
        assert message in result or all(char in result for char in message)


class TestDryRunAcrossFunctions:
    """Tests that every output function applies the dry-run prefix."""

    @pytest.mark.unit
    def test_dry_run_false_omits_prefix(self) -> None:
        """dry_run=False should not show the dry-run prefix."""
        output = StringIO()
        info("test", file=output, dry_run=False)

        result = output.getvalue()
        assert "[DRY RUN]" not in result
        assert "test" in result

    @pytest.mark.unit
    def test_all_functions_support_dry_run(self) -> None:
        """All output functions should accept dry_run."""
        output = StringIO()

        success("test", file=output, dry_run=True)
        error("test", file=output, dry_run=True)
        info("test", file=output, dry_run=True)
        warn("test", file=output, dry_run=True)
        detail("test", file=output, dry_run=True)

        result = output.getvalue()
        # Each call should produce output
        assert result.count("[DRY RUN]") == 5


class TestModesWithNewlineControl:
    """Tests ensuring dry-run mode works correctly with the nl parameter."""

    @pytest.mark.unit
    def test_dry_run_with_nl_false(self) -> None:
        """Dry-run mode should work with nl=False."""
        output = StringIO()
        success("test", file=output, dry_run=True, nl=False)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert not result.endswith("\n")

    @pytest.mark.unit
    def test_warn_dry_run_with_nl_false(self) -> None:
        """warn() should apply the dry-run prefix with nl=False."""
        output = StringIO()
        warn("test", file=output, dry_run=True, nl=False)

        result = output.getvalue()
        assert "[DRY RUN]" in result
        assert not result.endswith("\n")


class _TTY(StringIO):
    """A StringIO that reports itself as a terminal."""

    def isatty(self) -> bool:
        return True


@pytest.fixture
def default_writer() -> Iterator[None]:
    """Install the click-free writer for one test, then restore the previous one.

    Importing ``portolan_cli.cli`` installs the click writer for the whole
    session, so a test of the default writer installs it explicitly.
    """
    from portolan_cli import output

    previous = output.get_writer()
    output.set_writer(output.default_writer)
    try:
        yield
    finally:
        output.set_writer(previous)


class TestWriter:
    """The writer that turns a styled message into terminal output (#947).

    Library callers must not load Click, so ``output`` has its own writer.
    ``cli.py`` installs a writer that uses ``click.echo``.
    """

    @pytest.mark.unit
    @pytest.mark.usefixtures("default_writer")
    def test_default_writer_omits_color_when_not_a_terminal(self) -> None:
        """A pipe or a file gets the plain prefix and message."""
        stream = StringIO()

        success("hello", file=stream)

        assert stream.getvalue() == "✓ hello\n"

    @pytest.mark.unit
    @pytest.mark.usefixtures("default_writer")
    def test_default_writer_colors_a_terminal_like_click(self) -> None:
        """A terminal gets the same ANSI codes that ``click.style`` makes."""
        import click

        stream = _TTY()

        success("hello", file=stream)
        detail("step", file=stream, nl=False)

        expected = (
            f"{click.style('✓', fg='green')} {click.style('hello', fg='green')}\n"
            f"{click.style(' ', dim=True)} {click.style('step', dim=True)}"
        )
        assert stream.getvalue() == expected

    @pytest.mark.unit
    @pytest.mark.usefixtures("default_writer")
    def test_default_writer_resolves_stdout_at_call_time(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A replaced ``sys.stdout`` receives the message, as ``click.echo`` does."""
        import sys

        stream = StringIO()
        monkeypatch.setattr(sys, "stdout", stream)

        info("now")

        assert stream.getvalue() == "→ now\n"

    @pytest.mark.unit
    def test_set_writer_routes_every_message(self) -> None:
        """An installed writer receives the prefix, message, and style."""
        from portolan_cli import output

        calls: list[tuple[str, str, str | None, bool, bool]] = []

        def _record(
            prefix: str,
            message: str,
            *,
            fg: str | None,
            dim: bool,
            file: object,
            nl: bool,
        ) -> None:
            calls.append((prefix, message, fg, dim, nl))

        previous = output.get_writer()
        output.set_writer(_record)
        try:
            error("bad", nl=False)
            detail("quiet")
        finally:
            output.set_writer(previous)

        assert calls == [("✗", "bad", "red", False, False), (" ", "quiet", None, True, True)]

    @pytest.mark.unit
    def test_importing_cli_installs_the_click_writer(self) -> None:
        """The CLI writes through ``click.echo``, so CliRunner and Windows behave as before."""
        from portolan_cli import output

        cli_module = importlib.import_module("portolan_cli.cli")

        assert output.get_writer() is cli_module.click_writer

    @pytest.mark.unit
    def test_output_module_does_not_import_click(self) -> None:
        """``import portolan_cli.output`` leaves Click unloaded."""
        code = "import sys, portolan_cli.output; print('click' in sys.modules)"

        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        )

        assert result.stdout.strip() == "False"


class TestStdoutToStderr:
    """JSON mode keeps stdout for the envelope alone (#1005).

    Library code calls ``info``, ``success``, ``detail``, and ``progress``,
    and does not know the output mode. While ``stdout_to_stderr`` is active,
    those messages go to stderr.
    """

    @pytest.mark.unit
    @pytest.mark.usefixtures("default_writer")
    def test_stdout_bound_messages_go_to_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Every stdout-bound helper writes to stderr, and stdout stays empty."""
        from portolan_cli import output

        with output.stdout_to_stderr():
            success("done")
            info("note")
            detail("step")
            output.progress("a.shp", current=1, total=2)

        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == "✓ done\n→ note\n  step\n→ Processing file 1 of 2: a.shp\n"

    @pytest.mark.unit
    @pytest.mark.usefixtures("default_writer")
    def test_route_ends_with_the_block(self, capsys: pytest.CaptureFixture[str]) -> None:
        """After the block, stdout-bound messages reach stdout again."""
        from portolan_cli import output

        with output.stdout_to_stderr():
            pass
        info("back")

        captured = capsys.readouterr()
        assert (captured.out, captured.err) == ("→ back\n", "")

    @pytest.mark.unit
    @pytest.mark.usefixtures("default_writer")
    def test_explicit_stream_is_kept(self, capsys: pytest.CaptureFixture[str]) -> None:
        """A caller that names a stream still gets that stream."""
        from portolan_cli import output

        stream = StringIO()
        with output.stdout_to_stderr():
            info("to the stream", file=stream)

        assert stream.getvalue() == "→ to the stream\n"
        assert capsys.readouterr() == ("", "")
