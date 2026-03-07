"""Unit tests for batched failure output in _print_add_failures_batched.

Tests verify that repeated add failures with the same error message are grouped
together instead of printing one line per failure. This reduces noise when many
files fail with the same error (e.g., 6 GDB files all missing CRS).

Expected behavior (batched):
    ✗ 6 items failed:
      Reading failed: No CRS found (6 files):
        Examples: file1.gdb, file2.gdb, file3.gdb (and 3 more)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from portolan_cli.dataset import AddFailure

# =============================================================================
# Helpers
# =============================================================================


def make_failure(path: str, error_msg: str = "Reading failed: No CRS found") -> AddFailure:
    """Create an AddFailure with the given path and error message."""
    return AddFailure(path=Path(path), error=error_msg)


def capture_batched_output(failures: list[AddFailure]) -> str:
    """Call _print_add_failures_batched and capture printed output."""
    from portolan_cli.cli import _print_add_failures_batched

    lines: list[str] = []

    with patch("portolan_cli.cli.error", side_effect=lambda m: lines.append(m)):
        with patch("portolan_cli.cli.detail", side_effect=lambda m: lines.append(m)):
            _print_add_failures_batched(failures)

    return "\n".join(lines)


# =============================================================================
# Core batching behaviour
# =============================================================================


@pytest.mark.unit
class TestAddFailuresBatching:
    """_print_add_failures_batched groups failures by error message."""

    def test_empty_failures_no_output(self) -> None:
        """Empty failures list produces no output at all."""
        output = capture_batched_output([])
        assert output == ""

    def test_single_failure_shown_inline(self) -> None:
        """A single failure prints path and error inline (no batch overhead)."""
        failures = [make_failure("historic/file1.gdb", "Reading failed: No CRS found")]
        output = capture_batched_output(failures)

        assert "historic/file1.gdb" in output
        assert "Reading failed: No CRS found" in output
        # Single failure should show "1 item failed" (singular)
        assert "1 item failed:" in output

    def test_single_failure_singular_form(self) -> None:
        """Header says 'item' (singular) for exactly one failure."""
        failures = [make_failure("a.shp", "Error")]
        output = capture_batched_output(failures)
        assert "1 item failed:" in output
        assert "items" not in output

    def test_multiple_same_error_batched(self) -> None:
        """Many failures with the same error are batched into one group."""
        failures = [make_failure(f"historic/file{i}.gdb") for i in range(6)]
        output = capture_batched_output(failures)

        # Should show count in header
        assert "6 items failed:" in output
        # Should show the error with file count
        assert "6 files" in output

    def test_shows_three_examples_by_default(self) -> None:
        """Default view shows exactly 3 example paths when more exist."""
        failures = [make_failure(f"dir/file{i}.gdb") for i in range(10)]
        output = capture_batched_output(failures)

        # Count how many file paths appear in the Examples line
        example_count = sum(1 for i in range(10) if f"file{i}.gdb" in output)
        assert example_count == 3

    def test_shows_and_n_more_when_truncated(self) -> None:
        """When truncated, shows '(and N more)' with correct count."""
        failures = [make_failure(f"dir/file{i}.gdb") for i in range(6)]
        output = capture_batched_output(failures)

        assert "(and 3 more)" in output

    def test_no_and_more_when_at_limit(self) -> None:
        """When exactly at limit (3 files), no '(and N more)' text."""
        failures = [make_failure(f"dir/file{i}.gdb") for i in range(3)]
        output = capture_batched_output(failures)

        # 3 files means we're at the limit but still batched
        assert "3 files" in output
        assert "(and" not in output

    def test_two_files_same_error_batched(self) -> None:
        """Two files with the same error are batched (threshold is >1)."""
        failures = [
            make_failure("a.shp", "Missing CRS"),
            make_failure("b.shp", "Missing CRS"),
        ]
        output = capture_batched_output(failures)

        assert "2 files" in output
        assert "a.shp" in output
        assert "b.shp" in output
        assert "(and" not in output  # Only 2 files, both shown as examples

    def test_different_errors_separate_groups(self) -> None:
        """Different error messages get separate groups."""
        failures = [
            make_failure("a.shp", "Missing CRS"),
            make_failure("b.shp", "Missing CRS"),
            make_failure("c.tif", "Invalid format"),
        ]
        output = capture_batched_output(failures)

        assert "3 items failed:" in output
        assert "Missing CRS" in output
        assert "Invalid format" in output
        # The CRS failures should be batched as 2 files
        assert "2 files" in output

    def test_mixed_single_and_batch_groups(self) -> None:
        """Mix of unique and repeated errors: unique shown inline, repeated batched."""
        failures = [
            make_failure("a.gdb", "No CRS"),
            make_failure("b.gdb", "No CRS"),
            make_failure("c.gdb", "No CRS"),
            make_failure("d.tif", "Corrupt file"),
        ]
        output = capture_batched_output(failures)

        assert "4 items failed:" in output
        # "No CRS" group should be batched
        assert "3 files" in output
        # "Corrupt file" should show inline with its path
        assert "d.tif" in output
        assert "Corrupt file" in output

    def test_preserves_error_message_exactly(self) -> None:
        """Error message text is preserved exactly in output."""
        msg = "Reading failed: No CRS found in layer 'boundaries'"
        failures = [make_failure("test.shp", msg)]
        output = capture_batched_output(failures)

        assert msg in output

    def test_total_count_header_accurate(self) -> None:
        """Header count reflects total failures across all groups."""
        failures = [
            make_failure("a.shp", "Error A"),
            make_failure("b.shp", "Error A"),
            make_failure("c.tif", "Error B"),
            make_failure("d.tif", "Error B"),
            make_failure("e.gpkg", "Error C"),
        ]
        output = capture_batched_output(failures)
        assert "5 items failed:" in output

    def test_paths_rendered_as_strings(self) -> None:
        """Path objects are properly converted to strings in output."""
        failures = [make_failure("subdir/data.shp", "Error")]
        output = capture_batched_output(failures)
        assert "subdir/data.shp" in output


# =============================================================================
# Edge cases
# =============================================================================


@pytest.mark.unit
class TestAddFailuresBatchingEdgeCases:
    """Edge cases for add failure batching."""

    def test_all_unique_errors_no_batching(self) -> None:
        """When every failure has a unique error, each is shown inline."""
        failures = [make_failure(f"file{i}.shp", f"Unique error {i}") for i in range(5)]
        output = capture_batched_output(failures)

        assert "5 items failed:" in output
        for i in range(5):
            assert f"file{i}.shp" in output
            assert f"Unique error {i}" in output
        # No batching should occur — no "(N files)" grouping
        assert "files)" not in output

    def test_large_batch_shows_correct_remainder(self) -> None:
        """A large batch (100 files) correctly shows remainder count."""
        failures = [make_failure(f"f{i}.shp", "Same error") for i in range(100)]
        output = capture_batched_output(failures)

        assert "100 items failed:" in output
        assert "100 files" in output
        assert "(and 97 more)" in output

    def test_empty_error_message(self) -> None:
        """An empty error string still groups correctly."""
        failures = [make_failure(f"f{i}.shp", "") for i in range(3)]
        output = capture_batched_output(failures)
        assert "3 items failed:" in output
        assert "3 files" in output


# =============================================================================
# Property-based tests (Hypothesis)
# =============================================================================


_EXTENSIONS = ("shp", "gdb", "tif", "gpkg")

_ERROR_PREFIXES = (
    "Reading failed",
    "Missing CRS",
    "Invalid format",
    "Corrupt file",
    "No geometry column",
)


@st.composite
def add_failure_strategy(draw: st.DrawFn) -> AddFailure:
    """Generate a random AddFailure.

    Uses simple sampled strategies (faster than ``from_regex``) to avoid
    Hypothesis ``too_slow`` health-check failures.
    """
    dirname = draw(st.text(alphabet="abcdefghij", min_size=1, max_size=6))
    basename = draw(st.text(alphabet="abcdefghij0123456789", min_size=1, max_size=6))
    ext = draw(st.sampled_from(_EXTENSIONS))
    path = f"{dirname}/{basename}.{ext}"

    prefix = draw(st.sampled_from(_ERROR_PREFIXES))
    suffix = draw(st.text(alphabet="abcdefghij ", min_size=0, max_size=15))
    error_msg = f"{prefix}: {suffix}" if suffix.strip() else prefix
    return AddFailure(path=Path(path), error=error_msg)


@pytest.mark.unit
class TestAddFailuresBatchingProperties:
    """Property-based tests for add failure batching."""

    @given(failures=st.lists(add_failure_strategy(), min_size=1, max_size=50))
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_header_count_matches_total(self, failures: list[AddFailure]) -> None:
        """Header always shows correct total count."""
        output = capture_batched_output(failures)
        n = len(failures)
        expected_word = "item" if n == 1 else "items"
        assert f"{n} {expected_word} failed:" in output

    @given(failures=st.lists(add_failure_strategy(), min_size=1, max_size=50))
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_every_error_message_appears(self, failures: list[AddFailure]) -> None:
        """Every unique error message appears at least once in the output."""
        output = capture_batched_output(failures)
        unique_errors = {f.error for f in failures}
        for err in unique_errors:
            assert err in output, f"Error message {err!r} not found in output"

    @given(failures=st.lists(add_failure_strategy(), min_size=0, max_size=50))
    @settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow])
    def test_no_crash_on_any_input(self, failures: list[AddFailure]) -> None:
        """Function never crashes regardless of input."""
        # Just verify no exception is raised
        capture_batched_output(failures)
