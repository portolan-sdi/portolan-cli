"""The peak RSS of a scenario must be the peak of its own child process."""

from __future__ import annotations

import subprocess
import sys

import pytest

from tests.perf.scenarios import REPO_ROOT

pytestmark = pytest.mark.integration

PROBE = "from tests.perf.runner import _peak_rss_mb; print(_peak_rss_mb())"


@pytest.mark.skipif(sys.platform != "linux", reason="Linux keeps ru_maxrss across exec")
def test_a_child_does_not_report_the_peak_of_its_parent() -> None:
    """Linux ru_maxrss carries the parent peak into a child. The nightly saw one value for each run."""
    ballast = b"\x01" * (600 * 1024 * 1024)  # every page resident
    child = subprocess.run(
        [sys.executable, "-c", PROBE], cwd=REPO_ROOT, capture_output=True, text=True, check=True
    )
    del ballast
    assert float(child.stdout) < 300, "the child reported the peak RSS of its parent"
