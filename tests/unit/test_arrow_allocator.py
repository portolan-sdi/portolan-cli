"""Importing portolan_cli turns off mimalloc's eager arena commit in PyArrow.

PyArrow 26 bundles mimalloc 3.3.1. On Linux, mimalloc commits each arena
eagerly and marks it for transparent huge pages, so every arena chunk that a
thread touches becomes resident in 2 MiB pages. The nightly add-rasters
scenario grew by about 45 MB from that alone. Each check runs in a fresh
interpreter, because mimalloc reads its options once, when PyArrow loads.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit

_OPTION = "MIMALLOC_ARENA_EAGER_COMMIT"

# MIMALLOC_VERBOSE makes mimalloc print every option at process init.
_PROBE = """
import sys
if sys.argv[1] == "portolan":
    import portolan_cli  # noqa: F401
import pyarrow as pa
pa.allocate_buffer(1 << 20)
"""


def _eager_commit(first_import: str, value: str | None = None) -> int:
    env = dict(os.environ)
    env.pop(_OPTION, None)
    env["MIMALLOC_VERBOSE"] = "1"
    if value is not None:
        env[_OPTION] = value
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, first_import],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    match = re.search(r"option 'arena_eager_commit': (-?\d+)", result.stderr)
    if match is None:
        pytest.skip("this PyArrow build does not use mimalloc")
    return int(match.group(1))


def test_pyarrow_alone_commits_arenas_eagerly() -> None:
    """The control: without portolan_cli, mimalloc keeps its default of 2."""
    assert _eager_commit("pyarrow") == 2


def test_importing_portolan_cli_first_turns_eager_commit_off() -> None:
    assert _eager_commit("portolan") == 0


def test_a_value_the_user_sets_wins() -> None:
    assert _eager_commit("portolan", value="1") == 1
