"""The sync transport modules load without the scan and conversion stack.

clone, remote listing, push, and pull move bytes. Only the sync orchestration
in ``sync.core`` runs scan and check. A cold interpreter proves that the
transport modules pull in no ``portolan_cli.scan`` or ``portolan_cli.convert``
module, directly or through another import.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

pytestmark = pytest.mark.integration

TRANSPORT_MODULES = (
    "portolan_cli.sync.transport",
    "portolan_cli.sync.push",
    "portolan_cli.sync.pull",
    "portolan_cli.sync.upload",
    "portolan_cli.sync.download",
)

PROBE = """
import importlib, json, sys
for name in sys.argv[1:]:
    importlib.import_module(name)
heavy = ("portolan_cli.scan", "portolan_cli.convert")
print(json.dumps(sorted(
    m for m in sys.modules if m in heavy or m.startswith(tuple(h + "." for h in heavy))
)))
"""


def test_transport_modules_load_no_scan_or_convert_module() -> None:
    """Importing every transport module loads no scan or convert module."""
    completed = subprocess.run(  # noqa: S603 - fixed arguments, no shell
        [sys.executable, "-c", PROBE, *TRANSPORT_MODULES],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(completed.stdout) == []


def test_the_probe_sees_a_scan_import() -> None:
    """The probe reports scan modules when a module does load them."""
    completed = subprocess.run(  # noqa: S603 - fixed arguments, no shell
        [sys.executable, "-c", PROBE, "portolan_cli.sync.core"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "portolan_cli.scan.check" in json.loads(completed.stdout)
