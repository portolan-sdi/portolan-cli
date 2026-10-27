"""``portolan --help`` scans each entry point group once per process (#947).

Each scan reads every installed distribution, so the cost grows with the
environment. ``plugins.manager._scan`` caches one scan per group. This test
counts the scans in a cold process with 300 extra distributions installed. It
replaces a 0.25 s wall-clock ceiling that measured the runner, not the code.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from tests.perf.scenarios import REPO_ROOT
from tests.support.fake_dist import write_fake_dist

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration

EXTRA_DISTRIBUTIONS = 300

_PROBE = """
import importlib.metadata as metadata, json, sys
scans = []
entry_points = metadata.entry_points
def counting_entry_points(**params):
    scans.append(params.get("group"))
    return entry_points(**params)
metadata.entry_points = counting_entry_points
from portolan_cli import cli
try:
    cli.main(args=["--help"], prog_name="portolan")
except SystemExit:
    pass
installed = sum(1 for _ in metadata.distributions())
sys.stderr.write("SCANS " + json.dumps({"scans": sorted(scans), "installed": installed}) + "\\n")
"""


def test_help_scans_each_group_once(tmp_path: Path) -> None:
    for index in range(EXTRA_DISTRIBUTIONS):
        group = {"portolan": {f"fake{index}": "fake_module"}} if index == 0 else {}
        write_fake_dist(tmp_path, f"fake-dist-{index}", {"console_scripts": {}, **group})
    env = dict(os.environ)
    env.pop("PORTOLAN_PLUGINS", None)
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(tmp_path), str(REPO_ROOT), env.get("PYTHONPATH")))
    )

    proc = subprocess.run(
        [sys.executable, "-c", _PROBE], capture_output=True, text=True, check=True, env=env
    )
    line = next(line for line in proc.stderr.splitlines() if line.startswith("SCANS "))
    result = json.loads(line.removeprefix("SCANS "))

    assert result["installed"] > EXTRA_DISTRIBUTIONS
    assert result["scans"] == ["portolan.commands"]
