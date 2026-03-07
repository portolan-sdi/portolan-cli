"""Unit tests for batched warning output in _print_issues_with_fixability.

Tests verify that repeated warnings of the same IssueType are grouped together
instead of printing one line per issue. This reduces noise when many files share
the same issue (e.g., 265 directories with uppercase names).

Expected behavior (batched):
    ⚠ 265 warnings
      [--fix] 265 files have invalid_collection_id
      Examples: USA, GBR, CHN (and 262 more)
      Hint: STAC recommends lowercase for cross-platform compatibility
      Run: portolan scan --fix to auto-rename
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.scan import IssueType, ScanIssue, ScanResult
from portolan_cli.scan import Severity as ScanSeverity

# =============================================================================
# Helpers
# =============================================================================

_ROOT = Path("/tmp/test-root")


def make_scan_result(issues: list[ScanIssue]) -> ScanResult:
    """Build a minimal ScanResult with the given issues."""
    return ScanResult(
        root=_ROOT,
        ready=[],
        issues=issues,
        skipped=[],
        directories_scanned=1,
    )


def make_issue(
    name: str,
    issue_type: IssueType,
    severity: ScanSeverity = ScanSeverity.WARNING,
    message: str = "Test issue",
    suggestion: str | None = None,
) -> ScanIssue:
    """Create a ScanIssue with the given relative path name."""
    return ScanIssue(
        path=_ROOT / name,
        relative_path=name,
        issue_type=issue_type,
        severity=severity,
        message=message,
        suggestion=suggestion,
    )


def capture_output(result: ScanResult, *, show_all: bool = False) -> str:
    """Call _print_issues_with_fixability and capture printed output."""
    from portolan_cli.cli import _print_issues_with_fixability

    lines: list[str] = []

    def fake_echo(msg: str = "", **kwargs: object) -> None:  # type: ignore[misc]
        lines.append(str(msg))

    with patch("portolan_cli.cli.warn", side_effect=lambda m: lines.append(m)):
        with patch("portolan_cli.cli.error", side_effect=lambda m: lines.append(m)):
            with patch("portolan_cli.cli.info_output", side_effect=lambda m: lines.append(m)):
                with patch("portolan_cli.cli.detail", side_effect=lambda m: lines.append(m)):
                    _print_issues_with_fixability(result, show_all=show_all)

    return "\n".join(lines)


# =============================================================================
# Core batching behaviour
# =============================================================================


@pytest.mark.unit
class TestBatchedWarningOutput:
    """_print_issues_with_fixability groups issues by (severity, issue_type)."""

    def test_single_issue_shown_directly(self) -> None:
        """A single issue prints its path inline (no batching needed)."""
        issues = [make_issue("usa", IssueType.INVALID_COLLECTION_ID)]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert "usa" in output

    def test_multiple_same_type_batched(self) -> None:
        """Many issues of the same type are shown as a single batched group."""
        names = [f"country_{i:03d}" for i in range(10)]
        issues = [make_issue(n, IssueType.INVALID_COLLECTION_ID) for n in names]
        result = make_scan_result(issues)
        output = capture_output(result)

        # Should mention the count (10 files)
        assert "10" in output
        # Should NOT print 10 separate path lines — count is batched
        # At most 3 examples should appear
        example_count = sum(1 for n in names if n in output)
        assert example_count <= 3

    def test_shows_three_examples_by_default(self) -> None:
        """Default view shows exactly 3 example paths."""
        names = [f"dir_{i:03d}" for i in range(20)]
        issues = [make_issue(n, IssueType.INVALID_COLLECTION_ID) for n in names]
        result = make_scan_result(issues)
        output = capture_output(result)

        example_count = sum(1 for n in names if n in output)
        assert example_count == 3

    def test_shows_and_n_more_when_truncated(self) -> None:
        """Output includes '(and N more)' when there are more than 3 issues."""
        names = [f"dir_{i:03d}" for i in range(10)]
        issues = [make_issue(n, IssueType.INVALID_COLLECTION_ID) for n in names]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert "more" in output.lower()
        assert "7" in output  # 10 - 3 = 7

    def test_show_all_lists_every_path(self) -> None:
        """With show_all=True, every path is listed (no truncation)."""
        names = [f"dir_{i:03d}" for i in range(10)]
        issues = [make_issue(n, IssueType.INVALID_COLLECTION_ID) for n in names]
        result = make_scan_result(issues)
        output = capture_output(result, show_all=True)

        for n in names:
            assert n in output

    def test_different_issue_types_separate_groups(self) -> None:
        """Issues of different IssueTypes are displayed as separate batched groups."""
        issues = [
            make_issue("USA", IssueType.INVALID_COLLECTION_ID),
            make_issue("GBR", IssueType.INVALID_COLLECTION_ID),
            make_issue("file with spaces.shp", IssueType.INVALID_CHARACTERS),
        ]
        result = make_scan_result(issues)
        output = capture_output(result)

        # Both issue types must be represented
        assert "invalid_collection_id" in output.lower() or "2" in output
        assert "invalid_characters" in output.lower() or "spaces" in output

    def test_severity_ordering_preserved(self) -> None:
        """Errors appear before warnings in output."""
        issues = [
            make_issue("warn_dir", IssueType.INVALID_COLLECTION_ID, ScanSeverity.WARNING),
            make_issue("err_file.shp", IssueType.INCOMPLETE_SHAPEFILE, ScanSeverity.ERROR),
        ]
        result = make_scan_result(issues)
        output = capture_output(result)

        err_pos = output.find("err_file.shp")
        warn_pos = output.find("warn_dir")
        # Errors come before warnings; if both are in batches, the error batch is first
        assert err_pos < warn_pos or (err_pos == -1 and warn_pos == -1) or err_pos < warn_pos

    def test_hint_shown_for_batch(self) -> None:
        """The suggestion/hint from issues is displayed once per batch group."""
        issues = [
            make_issue(
                f"dir_{i}",
                IssueType.INVALID_COLLECTION_ID,
                suggestion="Use lowercase letters",
            )
            for i in range(5)
        ]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert "lowercase" in output.lower()
        # Hint should appear only once per group
        assert output.lower().count("lowercase") == 1

    def test_no_issues_produces_no_output(self) -> None:
        """Empty issues list produces no output."""
        result = make_scan_result([])
        output = capture_output(result)

        assert output.strip() == ""

    def test_single_item_no_truncation_suffix(self) -> None:
        """A single issue does not show '(and N more)'."""
        issues = [make_issue("lone_dir", IssueType.INVALID_COLLECTION_ID)]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert "more" not in output.lower()

    def test_exactly_three_items_no_truncation_suffix(self) -> None:
        """Exactly 3 issues shows all paths with no truncation suffix."""
        issues = [make_issue(f"dir_{i}", IssueType.INVALID_COLLECTION_ID) for i in range(3)]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert "more" not in output.lower()
        for i in range(3):
            assert f"dir_{i}" in output

    def test_fixability_label_shown_per_group(self) -> None:
        """Each batch group shows the fixability label [--fix] or [manual] etc."""
        issues = [make_issue(f"dir_{i}", IssueType.INVALID_COLLECTION_ID) for i in range(5)]
        result = make_scan_result(issues)
        output = capture_output(result)

        # INVALID_COLLECTION_ID is FIX_FLAG
        assert "[--fix]" in output

    def test_severity_count_header(self) -> None:
        """The severity-level header shows the total issue count."""
        issues = [make_issue(f"dir_{i}", IssueType.INVALID_COLLECTION_ID) for i in range(8)]
        result = make_scan_result(issues)
        output = capture_output(result)

        # Header should say "8 warnings" or similar
        assert "8" in output

    def test_mixed_severities_each_group_separate(self) -> None:
        """Errors and warnings in the same IssueType produce separate severity groups."""
        issues = [
            make_issue("e1", IssueType.INCOMPLETE_SHAPEFILE, ScanSeverity.ERROR),
            make_issue("e2", IssueType.INCOMPLETE_SHAPEFILE, ScanSeverity.ERROR),
            make_issue("w1", IssueType.INVALID_COLLECTION_ID, ScanSeverity.WARNING),
            make_issue("w2", IssueType.INVALID_COLLECTION_ID, ScanSeverity.WARNING),
        ]
        result = make_scan_result(issues)
        output = capture_output(result)

        # Both severity headers should be present (2 errors, 2 warnings)
        assert "2 error" in output or "errors" in output
        assert "2 warning" in output or "warnings" in output


# =============================================================================
# Edge cases
# =============================================================================


@pytest.mark.unit
class TestBatchingEdgeCases:
    """Edge cases for the batching logic."""

    def test_info_severity_batched(self) -> None:
        """INFO severity issues are also batched."""
        issues = [
            make_issue(f"dir_{i}", IssueType.FILEGDB_DETECTED, ScanSeverity.INFO) for i in range(6)
        ]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert "6" in output
        example_count = sum(1 for i in range(6) if f"dir_{i}" in output)
        assert example_count <= 3

    def test_no_duplicate_hints(self) -> None:
        """When multiple groups have the same hint, it's still shown once per group."""
        issues_a = [
            make_issue(f"a_{i}", IssueType.INVALID_COLLECTION_ID, suggestion="Hint A")
            for i in range(4)
        ]
        issues_b = [
            make_issue(f"b_{i}", IssueType.INVALID_CHARACTERS, suggestion="Hint B")
            for i in range(4)
        ]
        result = make_scan_result(issues_a + issues_b)
        output = capture_output(result)

        assert output.count("Hint A") == 1
        assert output.count("Hint B") == 1

    def test_batch_with_all_same_suggestions(self) -> None:
        """If all issues in a batch share the same suggestion, it appears once."""
        issues = [
            make_issue(f"dir_{i}", IssueType.INVALID_COLLECTION_ID, suggestion="Use lowercase")
            for i in range(5)
        ]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert output.count("Use lowercase") == 1

    def test_batch_with_no_suggestions(self) -> None:
        """Issues without suggestions produce no Hint line."""
        issues = [
            make_issue(f"dir_{i}", IssueType.MULTIPLE_PRIMARIES, suggestion=None) for i in range(5)
        ]
        result = make_scan_result(issues)
        output = capture_output(result)

        assert "hint" not in output.lower()


# =============================================================================
# Hypothesis property-based tests
# =============================================================================


def _issue_type_strategy() -> st.SearchStrategy[IssueType]:
    return st.sampled_from(list(IssueType))


def _severity_strategy() -> st.SearchStrategy[ScanSeverity]:
    return st.sampled_from([ScanSeverity.ERROR, ScanSeverity.WARNING, ScanSeverity.INFO])


@st.composite
def scan_issue_strategy(draw: st.DrawFn) -> ScanIssue:
    """Hypothesis strategy that produces valid ScanIssue instances."""
    name = draw(
        st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), min_codepoint=65),
            min_size=1,
            max_size=20,
        )
    )
    issue_type = draw(_issue_type_strategy())
    severity = draw(_severity_strategy())
    return make_issue(name, issue_type, severity)


@pytest.mark.unit
class TestBatchingProperties:
    """Property-based tests using Hypothesis."""

    @given(issues=st.lists(scan_issue_strategy(), min_size=1, max_size=50))
    @settings(max_examples=100, deadline=2000)
    def test_all_issues_represented_in_batch_output(self, issues: list[ScanIssue]) -> None:
        """Every issue must be counted (total count in headers) even if not shown by name."""
        result = make_scan_result(issues)
        output = capture_output(result)

        # The total count of each severity group must appear in the output
        for severity in [ScanSeverity.ERROR, ScanSeverity.WARNING, ScanSeverity.INFO]:
            sev_issues = [i for i in issues if i.severity == severity]
            if sev_issues:
                assert str(len(sev_issues)) in output, (
                    f"Expected count {len(sev_issues)} for {severity} to appear in output"
                )

    @given(issues=st.lists(scan_issue_strategy(), min_size=1, max_size=50))
    @settings(max_examples=100, deadline=2000)
    def test_show_all_shows_all_paths(self, issues: list[ScanIssue]) -> None:
        """With show_all=True, every unique relative_path appears in output."""
        result = make_scan_result(issues)
        output = capture_output(result, show_all=True)

        for issue in issues:
            assert issue.relative_path in output, (
                f"Expected path {issue.relative_path!r} in output with show_all=True"
            )

    @given(issues=st.lists(scan_issue_strategy(), min_size=4, max_size=50))
    @settings(max_examples=50, deadline=2000)
    def test_default_truncates_examples_to_at_most_three_per_group(
        self, issues: list[ScanIssue]
    ) -> None:
        """Without show_all, each (severity, issue_type) group shows <= 3 unique paths."""
        result = make_scan_result(issues)
        output = capture_output(result)

        # Group by (severity, issue_type)
        from itertools import groupby

        sorted_issues = sorted(issues, key=lambda i: (i.severity.value, i.issue_type.value))
        for (sev, it), group_iter in groupby(
            sorted_issues, key=lambda i: (i.severity.value, i.issue_type.value)
        ):
            group = list(group_iter)
            unique_paths = list(dict.fromkeys(i.relative_path for i in group))
            if len(unique_paths) > 3:
                # Count how many of the unique paths actually appear in the output.
                # We count unique paths that appear (not issue count), so duplicated
                # relative_path values don't inflate the count.
                visible_unique = [p for p in unique_paths if p in output]
                assert len(visible_unique) <= 3, (
                    f"Group ({sev}, {it}) shows {len(visible_unique)} unique paths > 3"
                )
