#!/usr/bin/env python3
"""Prove that each quality gate fails on the fault it exists to catch (#1035).

A gate that nobody has seen fail can be dead: the nightly mutation sweep ran
green for seven weeks while it tested no mutant, and the benchmark job compared
each run with itself. This script injects one known fault per gate and expects
the gate to go red.

For each fault the script:

1. Copies ``HEAD`` to a temporary directory with ``git archive`` and applies
   the fault. A fault that does not apply is an ERROR.
2. Copies ``HEAD`` again, applies the clean variant of the fault, and runs
   each gate. The gate must pass. A gate that fails on a clean tree proves
   nothing, so the row is BROKEN.
3. Runs each gate on the faulty copy. Each gate must fail. A gate that stays
   green is MISSED.

The gates run in the copy with the caller's virtual environment.
``UV_PROJECT_ENVIRONMENT`` points ``uv run`` at it, ``UV_NO_SYNC`` keeps uv
from installing the copy into it, and ``PYTHONPATH`` puts the copy ahead of
the editable install. A symlinked ``.venv`` would work on Linux, but Windows
needs extra privileges to create one, and the tools would walk into it.

Usage::

    uv run python scripts/gate_selftest.py                 # every fault
    uv run python scripts/gate_selftest.py --fault mutant  # one fault

The exit status is 1 when a row is MISSED, BROKEN, or ERROR.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

GATE_TIMEOUT_S = 1200
_STRIPPED_ENV_PREFIXES = ("COV_CORE_", "PYTEST_", "MUTANT_UNDER_TEST")


@dataclass(frozen=True)
class Gate:
    """One command that must pass on a clean tree and fail on the fault."""

    command: tuple[str, ...]
    caught: Callable[[subprocess.CompletedProcess[str]], bool] = lambda proc: proc.returncode != 0


@dataclass(frozen=True)
class Fault:
    """A known defect and the gates that must catch it.

    ``apply(root, faulty)`` writes the fault into the copy at ``root`` when
    ``faulty`` is true, and the clean variant otherwise. ``control`` is false
    when a clean run of the gate has no meaning.
    """

    name: str
    apply: Callable[[Path, bool], None]
    gates: tuple[Gate, ...]
    control: bool = True


@dataclass(frozen=True)
class Result:
    """The outcome of one fault: CAUGHT, MISSED, BROKEN, or ERROR."""

    fault: str
    status: str
    detail: str


class FaultError(RuntimeError):
    """The fault could not be applied, so its gates were not tested."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def gate_env(root: Path) -> dict[str, str]:
    """Return the environment that runs a tool in the copy at ``root``."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(_STRIPPED_ENV_PREFIXES)}
    env.update(
        UV_NO_SYNC="1",
        UV_PROJECT_ENVIRONMENT=sys.prefix,
        VIRTUAL_ENV=sys.prefix,
        PYTHONPATH=str(root),
    )
    return env


def run(
    root: Path, command: Sequence[str], cwd: Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Run ``command`` against the copy at ``root``."""
    return subprocess.run(
        list(command),
        cwd=cwd or root,
        env=gate_env(root),
        capture_output=True,
        text=True,
        check=False,
        timeout=GATE_TIMEOUT_S,
    )


def _setup(root: Path, command: Sequence[str], cwd: Path | None = None) -> None:
    proc = run(root, command, cwd)
    if proc.returncode != 0:
        raise FaultError(f"{' '.join(command)} exited {proc.returncode}: {_tail(proc)}")


def _tail(proc: subprocess.CompletedProcess[str], lines: int = 3) -> str:
    text = (proc.stdout + proc.stderr).strip().splitlines()
    return " / ".join(line.strip() for line in text[-lines:])


def _replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise FaultError(f"{path.name}: expected one {old!r}, found {text.count(old)}")
    path.write_text(text.replace(old, new), encoding="utf-8")


def _append(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)


def copy_head(repo: Path, dest: Path) -> Path:
    """Extract ``HEAD`` of ``repo`` into ``dest`` and return ``dest``."""
    archive = subprocess.run(
        ["git", "archive", "--format=tar", "HEAD"], cwd=repo, capture_output=True, check=True
    ).stdout
    dest.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        if hasattr(tarfile, "data_filter"):
            tar.extractall(dest, filter="data")
        else:  # Python before 3.10.12 has no extraction filters
            tar.extractall(dest)  # noqa: S202 - the archive is this repository's own HEAD
    return dest


# ---------------------------------------------------------------------------
# Faults
# ---------------------------------------------------------------------------

BBOX = "portolan_cli/bbox.py"
MUTANT_GLOB = "portolan_cli.bbox.x_is_valid_bbox__mutmut_*"


def _apply_mutant(root: Path, faulty: bool) -> None:
    # Narrow the test selection to the bbox tests, so the stats phase takes
    # seconds and not the whole suite. The mutants and the runner are the
    # nightly sweep's own.
    _replace_once(
        root / "pyproject.toml",
        'pytest_add_cli_args_test_selection = ["tests/"]',
        'pytest_add_cli_args_test_selection = ["tests/unit/test_bbox.py"]',
    )
    if faulty:
        _setup(root, ("uv", "run", "mutmut", "run", MUTANT_GLOB))


def _killed(proc: subprocess.CompletedProcess[str]) -> bool:
    killed = re.findall(
        r"^\s*portolan_cli\.bbox\.x_is_valid_bbox__mutmut_\d+: killed$", proc.stdout, re.MULTILINE
    )
    return proc.returncode == 0 and len(killed) >= 1


def _apply_library_print(root: Path, faulty: bool) -> None:
    if faulty:
        _append(root / BBOX, '\nprint("x")\n')


def _apply_library_output_import(root: Path, faulty: bool) -> None:
    if faulty:
        _append(root / BBOX, "\nfrom portolan_cli.output import info\n")


def _apply_dead_function(root: Path, faulty: bool) -> None:
    if faulty:
        _append(root / BBOX, "\n\ndef _gate_selftest_dead() -> int:\n    return 1\n")


CATALOG = "_selftest_catalog"
CATALOG_FIXTURE = "tests/fixtures/vector/valid/points.parquet"


def _apply_stale_file_size(root: Path, faulty: bool) -> None:
    catalog = root / CATALOG
    (catalog / "demo").mkdir(parents=True)
    shutil.copy(root / CATALOG_FIXTURE, catalog / "demo" / "points.parquet")
    _setup(root, ("uv", "run", "portolan", "init", "--auto", "--license", "CC0-1.0"), catalog)
    _setup(root, ("uv", "run", "portolan", "add", "demo/points.parquet"), catalog)
    if not faulty:
        return
    # One file in a collection directory is a collection-level asset, so the
    # stamp sits on collection.json. Tamper with the first stamped asset of
    # any STAC object.
    for stac_path in sorted(catalog.rglob("*.json")):
        stac = json.loads(stac_path.read_text(encoding="utf-8"))
        assets = stac.get("assets", {}) if isinstance(stac, dict) else {}
        for asset in assets.values():
            if "file:size" in asset:
                asset["file:size"] = 1
                stac_path.write_text(json.dumps(stac, indent=2), encoding="utf-8")
                return
    raise FaultError("no asset in the catalog carries file:size")


DUPLICATE_MODULE = "portolan_cli/_dup_selftest.py"
# About 70 jscpd tokens, above the 50-token minTokens in .jscpd.json.
_DUPLICATE_BODY = """
    total = 0
    for value in values:
        if value > limit:
            total += value * 2
        elif value < -limit:
            total -= value * 3
        else:
            total -= value
    if total > limit * 10:
        return limit * 10
    return total
"""


def _apply_duplicate_block(root: Path, faulty: bool) -> None:
    names = ("first_total", "second_total") if faulty else ("first_total",)
    source = '"""Gate self-test module."""\n'
    for name in names:
        source += f"\n\ndef {name}(values: list[int], limit: int) -> int:{_DUPLICATE_BODY}"
    (root / DUPLICATE_MODULE).write_text(source, encoding="utf-8")


def _apply_ci_success_wiring(root: Path, faulty: bool) -> None:
    if faulty:
        _replace_once(root / ".github/workflows/ci.yml", "test, plugin-test, ", "test, ")


MARKER_TEST = "tests/unit/test_gate_selftest_marker.py"


def _apply_unknown_marker(root: Path, faulty: bool) -> None:
    marker = "nonexistent" if faulty else "unit"
    (root / MARKER_TEST).write_text(
        f'"""Gate self-test module."""\n\nimport pytest\n\n\n'
        f"@pytest.mark.{marker}\ndef test_marked() -> None:\n    assert True\n",
        encoding="utf-8",
    )


UNREACHABLE_MODULE = "portolan_cli/_unreachable_selftest.py"


def _apply_unreachable_branch(root: Path, faulty: bool) -> None:
    tail = (
        '    if isinstance(value, str):\n        return value\n    return "never"\n'
        if faulty
        else "    return value\n"
    )
    (root / UNREACHABLE_MODULE).write_text(
        '"""Gate self-test module."""\n\n\n'
        "def describe(value: int | str) -> str:\n"
        '    """Name the value."""\n'
        '    if isinstance(value, int):\n        return "int"\n' + tail,
        encoding="utf-8",
    )


FAULTS = (
    Fault(
        "mutant",
        _apply_mutant,
        (Gate(("uv", "run", "mutmut", "results", "--all", "true"), _killed),),
        control=False,
    ),
    Fault(
        "library-print",
        _apply_library_print,
        (Gate(("uv", "run", "ruff", "check", "--select", "T20", BBOX)),),
    ),
    Fault(
        "library-output-import",
        _apply_library_output_import,
        (
            Gate(("uv", "run", "ruff", "check", "--select", "TID251", BBOX)),
            Gate(("uv", "run", "lint-imports")),
        ),
    ),
    Fault(
        "dead-function",
        _apply_dead_function,
        (Gate(("uv", "run", "vulture")),),
    ),
    Fault(
        "stale-file-size",
        _apply_stale_file_size,
        (Gate(("uv", "run", "python", "-m", "tests.catalog_invariants", CATALOG)),),
    ),
    Fault(
        "duplicate-block",
        _apply_duplicate_block,
        (
            Gate(
                (
                    "uvx",
                    "jscpd@5.2.1",
                    "portolan_cli",
                    "tests",
                    "--baseline",
                    ".jscpd-baseline.json",
                    "--fail-on-new-clones",
                    "0",
                )
            ),
        ),
    ),
    Fault(
        "ci-success-wiring",
        _apply_ci_success_wiring,
        (
            Gate(
                (
                    "uv",
                    "run",
                    "pytest",
                    "tests/docs/test_ci_wiring.py::test_every_job_feeds_ci_success",
                    "-q",
                    "-p",
                    "no:cacheprovider",
                )
            ),
        ),
    ),
    Fault(
        "unknown-marker",
        _apply_unknown_marker,
        (Gate(("uv", "run", "pytest", MARKER_TEST, "--co", "-q", "-p", "no:cacheprovider")),),
    ),
    Fault(
        "unreachable-branch",
        _apply_unreachable_branch,
        (Gate(("uv", "run", "mypy", UNREACHABLE_MODULE, "--warn-unreachable")),),
    ),
)
FAULTS_BY_NAME = {fault.name: fault for fault in FAULTS}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_fault(fault: Fault, repo: Path, workdir: Path) -> Result:
    """Test one fault against copies of ``repo`` under ``workdir``."""
    try:
        faulty_root = copy_head(repo, workdir / f"{fault.name}-faulty")
        fault.apply(faulty_root, True)
        if fault.control:
            clean_root = copy_head(repo, workdir / f"{fault.name}-clean")
            fault.apply(clean_root, False)
            for gate in fault.gates:
                proc = run(clean_root, gate.command)
                if gate.caught(proc):
                    return Result(
                        fault.name,
                        "BROKEN",
                        f"{' '.join(gate.command)} fails on a clean tree: {_tail(proc)}",
                    )
        for gate in fault.gates:
            proc = run(faulty_root, gate.command)
            if not gate.caught(proc):
                return Result(
                    fault.name, "MISSED", f"{' '.join(gate.command)} stayed green: {_tail(proc)}"
                )
    except (FaultError, subprocess.SubprocessError, OSError) as error:
        return Result(fault.name, "ERROR", str(error))
    return Result(fault.name, "CAUGHT", "")


def main(argv: list[str] | None = None) -> int:
    """Run the faults and print one Markdown table row per fault."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--fault", action="append", choices=sorted(FAULTS_BY_NAME), help="Run only this fault."
    )
    args = parser.parse_args(argv)

    repo = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True
        ).stdout.strip()
    )
    faults = [FAULTS_BY_NAME[name] for name in args.fault] if args.fault else list(FAULTS)

    print("| Fault | Gates | Result | Detail |")
    print("|---|---|---|---|")
    any_failed = False
    with tempfile.TemporaryDirectory(prefix="gate-selftest-") as workdir:
        for fault in faults:
            result = run_fault(fault, repo, Path(workdir))
            gates = "<br>".join(f"`{' '.join(gate.command)}`" for gate in fault.gates)
            detail = result.detail.replace("|", "\\|")
            print(f"| {fault.name} | {gates} | {result.status} | {detail} |", flush=True)
            any_failed |= result.status != "CAUGHT"
    if any_failed:
        print("\nA gate did not catch its fault.")
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
