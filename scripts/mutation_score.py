#!/usr/bin/env python3
"""Score a mutmut run against the ``.mutation-baseline`` floor.

Both mutation jobs share this module so the parse-and-enforce logic lives in one
tested place instead of duplicated shell: the nightly sweep and the PR-scoped
diff job each run ``mutmut``, export ``mutmut-cicd-stats.json``, then call this
script to enforce the floor and render a GitHub step-summary table.

Scoring:
    killed_total = killed + timeout + suspicious   # the suite reacted
    testable     = killed_total + survived          # no_tests excluded
    kill_rate    = killed_total / testable * 100

``no_tests`` mutants (no covering test at all) are outside the suite's reach and
are excluded from the rate rather than counted as failures. ``timeout`` and
``suspicious`` mutants provoked a reaction from the suite, so they count as
killed. Zero testable mutants means mutmut produced or parsed nothing — that is
mutation testing being broken, never a pass.

Usage:
    python scripts/mutation_score.py \\
        --stats mutants/mutmut-cicd-stats.json \\
        --baseline .mutation-baseline \\
        [--summary "$GITHUB_STEP_SUMMARY"] [--label "changed files"]

Exit codes: 0 = at or above floor; 1 = below floor, zero testable, or bad input.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

_STAT_KEYS = ("killed", "survived", "no_tests", "timeout", "suspicious")


def read_floor(text: str) -> int:
    """Return the floor from ``.mutation-baseline`` contents.

    The first non-comment, non-blank line is the floor. A ``#`` comment or blank
    line is skipped. A non-integer floor raises ``ValueError`` rather than
    silently defaulting.
    """
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        return int(line)  # raises ValueError on a non-integer line
    raise ValueError("no floor value found in baseline file")


@dataclass(frozen=True)
class Score:
    """Outcome of scoring one mutmut run against a floor."""

    killed: int
    survived: int
    no_tests: int
    timeout: int
    suspicious: int
    floor: int

    @property
    def killed_total(self) -> int:
        """Mutants the suite reacted to (clean kill, timeout, or suspicious)."""
        return self.killed + self.timeout + self.suspicious

    @property
    def testable(self) -> int:
        """Mutants a test could kill (excludes ``no_tests``)."""
        return self.killed_total + self.survived

    @property
    def kill_rate(self) -> float | None:
        """Kill rate as a percentage, or ``None`` when nothing is testable."""
        if self.testable == 0:
            return None
        return round(self.killed_total * 100 / self.testable, 2)

    @property
    def ok(self) -> bool:
        """True only when there are testable mutants and the floor is met."""
        rate = self.kill_rate
        return rate is not None and rate >= self.floor


def evaluate(stats: Mapping[str, int], floor: int) -> Score:
    """Build a :class:`Score` from a stats mapping and floor."""
    return Score(
        killed=int(stats.get("killed", 0)),
        survived=int(stats.get("survived", 0)),
        no_tests=int(stats.get("no_tests", 0)),
        timeout=int(stats.get("timeout", 0)),
        suspicious=int(stats.get("suspicious", 0)),
        floor=floor,
    )


def render_summary(score: Score, label: str) -> str:
    """Render a GitHub-flavored Markdown table for the step summary."""
    rate = "n/a" if score.kill_rate is None else f"{score.kill_rate}%"
    scope = f" ({label})" if label else ""
    lines = [
        f"## Mutation Testing{scope}",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Kill rate | {rate} |",
        f"| Floor | {score.floor}% |",
        f"| Killed | {score.killed_total} |",
        f"| Survived | {score.survived} |",
        f"| Testable | {score.testable} |",
        f"| No tests | {score.no_tests} |",
        "",
    ]
    return "\n".join(lines)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enforce the mutmut kill-rate floor.")
    parser.add_argument("--stats", required=True, type=Path, help="mutmut-cicd-stats.json")
    parser.add_argument("--baseline", required=True, type=Path, help=".mutation-baseline")
    parser.add_argument("--summary", type=Path, help="GitHub step-summary file to append")
    parser.add_argument("--label", default="", help="scope label, e.g. 'changed files'")
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help=(
            "Treat zero testable mutants as a pass, not a broken run. Use for a "
            "diff-scoped run where the changed files may contain no mutable code; "
            "omit for a full sweep, where zero mutants means mutation testing broke."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _parse_args(argv)

    try:
        stats = json.loads(args.stats.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"::error::Could not read mutation stats {args.stats}: {exc}")
        return 1

    try:
        floor = read_floor(args.baseline.read_text())
    except (OSError, ValueError) as exc:
        print(f"::error::Invalid .mutation-baseline floor: {exc}")
        return 1

    score = evaluate(stats, floor)

    summary = render_summary(score, args.label)
    if args.summary is not None:
        with args.summary.open("a", encoding="utf-8") as handle:
            handle.write(summary + "\n")
    print(summary)

    if score.testable == 0:
        if args.allow_empty:
            print("No mutable code in scope; nothing to test.")
            return 0
        print(
            "::error::No testable mutants were generated or parsed — mutation "
            "testing is broken. See portolan-sdi/portolan-cli#612."
        )
        return 1

    if not score.ok:
        print(
            f"::error::Mutation kill rate {score.kill_rate}% is below the "
            f"{score.floor}% floor from .mutation-baseline."
        )
        return 1

    print(f"Mutation kill rate {score.kill_rate}% meets the {score.floor}% floor.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
