"""Guard for Hypothesis property tests under mutmut's repeated pytest sessions.

mutmut drives pytest in-process and runs the suite several times per invocation
(stats, clean, forced-fail, then one pass per mutant). Hypothesis remembers the
instance that ran a ``@given`` method and fails ``HealthCheck.differing_executors``
the second time the same method runs against a fresh instance, which sinks the
clean pass and the whole sweep. See portolan-sdi/portolan-cli#612.

``tests/conftest.py`` suppresses that check when ``MUTANT_UNDER_TEST`` is in the
environment, which mutmut sets in every phase and nothing else sets.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_ROOT = REPO_ROOT / "tests"

# Each case spawns two more pytest sessions. Under mutmut those sessions import
# the mutated tree, where every call goes through a trampoline, and the pair
# blows the timeout and takes the sweep's stats phase with it. The guard being
# tested is what lets that phase run at all, so exercising it there is circular
# as well as slow.
pytestmark = pytest.mark.skipif(
    "MUTANT_UNDER_TEST" in os.environ,
    reason="nested pytest sessions are too slow against the mutated tree",
)


def _first_class_based_property_test() -> str:
    """Return the node ID of a class-based ``@given`` test in this suite.

    Found by scanning rather than hardcoded so a rename cannot silently turn
    this into a test of nothing.
    """
    for path in sorted(TESTS_ROOT.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            for member in node.body:
                if not isinstance(member, ast.FunctionDef):
                    continue
                names = {
                    getattr(d.func if isinstance(d, ast.Call) else d, "id", None)
                    for d in member.decorator_list
                }
                if "given" in names:
                    rel = path.relative_to(REPO_ROOT).as_posix()
                    return f"{rel}::{node.name}::{member.name}"
    pytest.fail("no class-based @given test found; the guard now protects nothing")


TWO_SESSIONS = textwrap.dedent(
    """
    import sys
    import pytest

    args = ["--no-cov", "-q", "-p", "no:cacheprovider", sys.argv[1]]
    first = pytest.main(args)
    second = pytest.main(args)
    print(f"first={first} second={second}")
    sys.exit(0 if (first, second) == (0, 0) else 1)
    """
)


def _run_two_sessions(*, under_mutmut: bool) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env.pop("MUTANT_UNDER_TEST", None)
    if under_mutmut:
        # mutmut sets this to '' for the clean pass; presence is the signal.
        env["MUTANT_UNDER_TEST"] = ""
    return subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, "-c", TWO_SESSIONS, _first_class_based_property_test()],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )


@pytest.mark.integration
def test_property_test_survives_a_second_session_under_mutmut() -> None:
    """Two in-process sessions both pass when MUTANT_UNDER_TEST is set."""
    result = _run_two_sessions(under_mutmut=True)

    assert "first=0 second=0" in result.stdout, result.stdout + result.stderr
    assert "differing_executors" not in result.stdout


@pytest.mark.integration
def test_health_check_still_fires_outside_mutmut() -> None:
    """The guard is scoped to mutmut, so an ordinary run keeps the check."""
    result = _run_two_sessions(under_mutmut=False)

    assert "first=0 second=1" in result.stdout, result.stdout + result.stderr
    assert "differing_executors" in result.stdout
