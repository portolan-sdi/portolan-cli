"""Unit tests for batched failure output in _output_add_human / _print_add_failures_batched.

Per GitHub issue #199: repeated failures with the same error message should be
grouped into a single batch entry rather than printing one line per failure.
This reduces noise when many files share the same error (e.g. 6 GDB files all
missing CRS).

Expected behaviour (batched, 6 files with the same error):

    ✗ 6 items failed:
      Reading failed: No CRS found (6 files):
        Examples: file1.gdb, file2.gdb, file3.gdb (and 3 more)

Expected behaviour (two distinct error messages, 2 + 1 files):

    ✗ 3 items failed:
      Reading failed: No CRS found (2 files):
        Examples: file1.gdb, file2.gdb
      Permission denied (1 files):
        - file3.gdb
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from portolan_cli.dataset import AddFailure

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_failure(name: str, error: str = "Reading failed: No CRS found") -> AddFailure:
    return AddFailure(path=Path(f"/catalog/collection/{name}"), error=error)


def capture_add_output(failures: list[AddFailure]) -> str:
    """Call _print_add_failures_batched and capture printed output."""
    from portolan_cli.cli import _print_add_failures_batched

    lines: list[str] = []

    with patch("portolan_cli.cli.error", side_effect=lambda m: lines.append(str(m))):
        with patch("portolan_cli.cli.warn", side_effect=lambda m: lines.append(str(m))):
            with patch("portolan_cli.cli.detail", side_effect=lambda m: lines.append(str(m))):
                _print_add_failures_batched(failures)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Core batching behaviour
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAddFailuresBatching:
    """_print_add_failures_batched groups failures by error message."""

    def test_no_failures_produces_no_output(self) -> None:
        """Empty failures list produces no output."""
        output = capture_add_output([])
        assert output.strip() == ""

    def test_single_failure_shown_directly(self) -> None:
        """A single failure prints its path and error inline (no batching overhead)."""
        failures = [_make_failure("file1.gdb")]
        output = capture_add_output(failures)

        assert "file1.gdb" in output
        assert "Reading failed: No CRS found" in output

    def test_single_failure_total_count_header(self) -> None:
        """A single failure shows '1 item failed:' header."""
        failures = [_make_failure("file1.gdb")]
        output = capture_add_output(failures)

        assert "1 item failed" in output

    def test_multiple_same_error_batched(self) -> None:
        """Multiple failures with the same error message are shown as one batch group."""
        failures = [_make_failure(f"file{i}.gdb") for i in range(6)]
        output = capture_add_output(failures)

        # Header shows total count
        assert "6 items failed" in output
        # Should NOT print 6 separate path lines — at most 3 examples shown
        visible = sum(1 for i in range(6) if f"file{i}.gdb" in output)
        assert visible <= 3

    def test_shows_three_examples_by_default(self) -> None:
        """Default view shows exactly 3 example paths per batch group."""
        failures = [_make_failure(f"file{i:02d}.gdb") for i in range(10)]
        output = capture_add_output(failures)

        visible = sum(1 for i in range(10) if f"file{i:02d}.gdb" in output)
        assert visible == 3

    def test_shows_and_n_more_when_truncated(self) -> None:
        """Output includes '(and N more)' when there are more than 3 failures."""
        failures = [_make_failure(f"file{i}.gdb") for i in range(6)]
        output = capture_add_output(failures)

        assert "more" in output.lower()
        assert "3" in output  # 6 - 3 = 3 remaining

    def test_exactly_three_failures_no_truncation_suffix(self) -> None:
        """Exactly 3 failures shows all paths with no truncation suffix."""
        failures = [_make_failure(f"file{i}.gdb") for i in range(3)]
        output = capture_add_output(failures)

        assert "more" not in output.lower()
        for i in range(3):
            assert f"file{i}.gdb" in output

    def test_different_error_messages_separate_groups(self) -> None:
        """Failures with different error messages produce separate batch groups."""
        failures = [
            _make_failure("a1.gdb", "Reading failed: No CRS found"),
            _make_failure("a2.gdb", "Reading failed: No CRS found"),
            _make_failure("b1.gpkg", "Permission denied"),
        ]
        output = capture_add_output(failures)

        # Both error types must be represented
        assert "No CRS found" in output
        assert "Permission denied" in output

    def test_total_count_header_always_shown(self) -> None:
        """The 'N items failed:' header always shows the total failure count."""
        failures = [_make_failure(f"file{i}.gdb") for i in range(8)]
        output = capture_add_output(failures)

        assert "8 items failed" in output

    def test_single_failure_no_truncation_suffix(self) -> None:
        """A single failure does not show '(and N more)'."""
        failures = [_make_failure("lone.gdb")]
        output = capture_add_output(failures)

        assert "more" not in output.lower()

    def test_error_message_shown_in_batch_header(self) -> None:
        """The shared error message appears in the batch group header."""
        failures = [_make_failure(f"file{i}.gdb") for i in range(5)]
        output = capture_add_output(failures)

        assert "Reading failed: No CRS found" in output
        # Should appear only once (not once per file)
        assert output.count("Reading failed: No CRS found") == 1

    def test_two_groups_both_shown(self) -> None:
        """Two distinct error groups are both visible in the output."""
        failures_crs = [_make_failure(f"geo{i}.gdb", "No CRS found") for i in range(4)]
        failures_csv = [
            _make_failure(f"meta{i}.csv", "Cannot track: no companion geospatial file")
            for i in range(2)
        ]
        output = capture_add_output(failures_crs + failures_csv)

        # Both group headers present
        assert "No CRS found" in output
        assert "Cannot track" in output

        # Total count = 6
        assert "6 items failed" in output


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAddFailuresBatchingEdgeCases:
    """Edge cases for the add-failures batching logic."""

    def test_all_unique_errors_shown_individually(self) -> None:
        """When every failure has a unique error message, each shows individually."""
        failures = [_make_failure(f"file{i}.gdb", f"Unique error {i}") for i in range(3)]
        output = capture_add_output(failures)

        # All 3 paths appear (each in its own single-item group)
        for i in range(3):
            assert f"file{i}.gdb" in output

    def test_large_batch_count_in_header(self) -> None:
        """A large batch still shows the total count correctly."""
        failures = [_make_failure(f"f{i:03d}.gdb") for i in range(50)]
        output = capture_add_output(failures)

        assert "50 items failed" in output
        # 50 - 3 = 47 more
        assert "47" in output
