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

Two floors:
    ``.mutation-baseline`` holds one repo-wide floor, the catastrophe guard every
    run must clear. The nightly sweep scores a rotating ``1/NUM_SHARDS`` slice
    whose kill rate depends on which modules land in it — measured shard rates
    span 14% to 95% — so one repo-wide number either flaps or gates nothing.
    ``--shard`` adds the per-shard floor recorded in ``.mutation-shards.json``:
    a shard must hold its own recorded rate (minus a tolerance for timeout
    jitter), which catches a regression the repo-wide floor would sleep through.
    A shard with no recorded rate yet is gated by the repo-wide floor alone, and
    the run prints the line to record. See portolan-sdi/portolan-cli#612.

Usage:
    python scripts/mutation_score.py \\
        --stats mutants/mutmut-cicd-stats.json \\
        --baseline .mutation-baseline \\
        [--shards .mutation-shards.json --shard 8 --num-shards 25] \\
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
class ShardBaselines:
    """Per-shard kill rates recorded from earlier sweeps.

    Attributes:
        num_shards: Shard count the rates were measured under. Changing
            ``NUM_SHARDS`` re-partitions the tree, so rates measured under a
            different count describe different file sets and cannot be compared.
        tolerance: Percentage points a shard may fall below its recorded rate
            without failing. Absorbs run-to-run jitter — a mutant that times out
            on a slow runner but is killed on a fast one moves the rate slightly.
        rates: Shard index (as a string key, since JSON has no integer keys)
            mapped to its recorded kill rate.
    """

    num_shards: int
    tolerance: float
    rates: Mapping[str, float]

    def floor_for(self, shard: int, repo_floor: float) -> tuple[float, str]:
        """Return the floor for ``shard`` and a human-readable source for it.

        An unrecorded shard falls back to ``repo_floor``. A recorded shard uses
        whichever is higher: its own rate minus the tolerance, or ``repo_floor``.
        Taking the maximum keeps a shard recorded below the repo floor (measured
        before the floor rose) from quietly weakening the gate.
        """
        recorded = self.rates.get(str(shard))
        if recorded is None:
            return repo_floor, ".mutation-baseline (no recorded rate for this shard)"
        adjusted = round(recorded - self.tolerance, 2)
        if repo_floor >= adjusted:
            return repo_floor, f".mutation-baseline (above shard {shard}'s recorded rate)"
        return adjusted, f"shard {shard}'s recorded {recorded}% less {self.tolerance}pp tolerance"


def read_shard_baselines(text: str) -> ShardBaselines:
    """Parse ``.mutation-shards.json``.

    Raises:
        ValueError: The document is malformed, or a rate is not a number.
    """
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("expected a JSON object at the top level")

    try:
        num_shards = int(data["num_shards"])
        tolerance = float(data["tolerance"])
        raw_rates = data["shards"]
    except KeyError as exc:
        raise ValueError(f"missing required key: {exc}") from exc
    except (TypeError, ValueError) as exc:
        raise ValueError(f"num_shards and tolerance must be numbers: {exc}") from exc

    if not isinstance(raw_rates, dict):
        raise ValueError("'shards' must be an object keyed by shard index")

    rates: dict[str, float] = {}
    for key, entry in raw_rates.items():
        if not isinstance(entry, dict) or "kill_rate" not in entry:
            raise ValueError(f"shard {key}: expected an object with a 'kill_rate'")
        try:
            rates[str(key)] = float(entry["kill_rate"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"shard {key}: kill_rate must be a number: {exc}") from exc

    return ShardBaselines(num_shards=num_shards, tolerance=tolerance, rates=rates)


@dataclass(frozen=True)
class Score:
    """Outcome of scoring one mutmut run against a floor."""

    killed: int
    survived: int
    no_tests: int
    timeout: int
    suspicious: int
    floor: float

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


def evaluate(stats: Mapping[str, int], floor: float) -> Score:
    """Build a :class:`Score` from a stats mapping and floor."""
    return Score(
        killed=int(stats.get("killed", 0)),
        survived=int(stats.get("survived", 0)),
        no_tests=int(stats.get("no_tests", 0)),
        timeout=int(stats.get("timeout", 0)),
        suspicious=int(stats.get("suspicious", 0)),
        floor=floor,
    )


def render_summary(score: Score, label: str, floor_source: str = "") -> str:
    """Render a GitHub-flavored Markdown table for the step summary."""
    rate = "n/a" if score.kill_rate is None else f"{score.kill_rate}%"
    scope = f" ({label})" if label else ""
    source = f" — {floor_source}" if floor_source else ""
    lines = [
        f"## Mutation Testing{scope}",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Kill rate | {rate} |",
        f"| Floor | {score.floor}%{source} |",
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
    parser.add_argument("--shards", type=Path, help=".mutation-shards.json (with --shard)")
    parser.add_argument("--shard", type=int, help="shard index being scored")
    parser.add_argument(
        "--num-shards",
        type=int,
        help="shard count this run used; must match the recorded baselines",
    )
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

    floor_source = ".mutation-baseline"
    baselines: ShardBaselines | None = None
    if args.shard is not None:
        if args.shards is None or args.num_shards is None:
            print("::error::--shard requires both --shards and --num-shards.")
            return 1
        try:
            baselines = read_shard_baselines(args.shards.read_text())
        except (OSError, ValueError) as exc:
            print(f"::error::Invalid shard baselines {args.shards}: {exc}")
            return 1
        if baselines.num_shards != args.num_shards:
            print(
                f"::error::{args.shards} records rates for {baselines.num_shards} "
                f"shards but this run used {args.num_shards}. Changing the shard "
                "count re-partitions the tree, so the recorded rates describe "
                "different files; re-measure before enforcing them."
            )
            return 1
        floor, floor_source = baselines.floor_for(args.shard, floor)

    score = evaluate(stats, floor)

    summary = render_summary(score, args.label, floor_source)
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
            f"{score.floor}% floor — {floor_source}."
        )
        return 1

    print(f"Mutation kill rate {score.kill_rate}% meets the {score.floor}% floor.")
    if baselines is not None and str(args.shard) not in baselines.rates:
        # Ratchet prompt: until this shard has a recorded rate, only the
        # repo-wide floor guards it, and a regression inside the gap between the
        # two goes unnoticed.
        print(
            f"::notice::Shard {args.shard} has no recorded rate. Add "
            f'"{args.shard}": {{"kill_rate": {score.kill_rate}, "measured": '
            f'"<date>", "run": "<run id>"}} to {args.shards} to gate it against '
            "this sweep."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
