"""S7: the heavy modules that a help command loads in a cold process (#994).

``portolan --help`` and the help of a command should load no geospatial
stack. Today ``cli.py`` imports the whole add stack at the top, so each help
command loads all of it. ``expected/imports.py`` records the set. Lane F
moves the imports into the commands and empties it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from tests.perf.expected.imports import EXPECTED, HEAVY_MODULES
from tests.perf.scenarios import REPO_ROOT

pytestmark = pytest.mark.integration

_PROBE = """
import json, sys
from portolan_cli import cli
try:
    cli.main(args=sys.argv[1:], prog_name="portolan")
except SystemExit:
    pass
heavy = json.loads(sys.stdin.read())
sys.stderr.write("HEAVY " + json.dumps([m for m in heavy if m in sys.modules]) + "\\n")
"""


def heavy_modules_after(args: tuple[str, ...]) -> list[str]:
    """Return the heavy modules in ``sys.modules`` after ``portolan <args>`` in a cold process."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(REPO_ROOT), env.get("PYTHONPATH"))))
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE, *args],
        input=json.dumps(HEAVY_MODULES),
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    line = next(line for line in proc.stderr.splitlines() if line.startswith("HEAVY "))
    loaded: list[str] = json.loads(line.removeprefix("HEAVY "))
    return loaded


@pytest.mark.parametrize("args", sorted(EXPECTED))
def test_help_loads_the_recorded_heavy_modules(args: tuple[str, ...]) -> None:
    assert heavy_modules_after(args) == EXPECTED[args]
