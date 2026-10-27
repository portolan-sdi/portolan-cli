"""The scenario inputs are the same bytes for the same seed (#992).

The gates compare counts across runs and machines. Inputs that change from
run to run would change a count that depends on the data, such as the number
of row groups to read.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest

from tests.perf.scenarios import SCENARIOS, SEED

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration

#: One scenario for each input writer: its size and the number of files it writes.
WRITERS = {
    "add-many-collections": (3, 3),
    "add-with-companions": (3, 6),
    "add-large-vector": (2_500, 1),
    "add-rasters": (3, 3),
}


def digests(root: Path) -> dict[str, str]:
    """Return the SHA-256 of every file under ``root``, by relative path."""
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.mark.parametrize("name", sorted(WRITERS))
def test_same_seed_gives_same_bytes(name: str, tmp_path: Path) -> None:
    inputs = SCENARIOS[name].inputs
    n, files = WRITERS[name]

    inputs(tmp_path / "first", n, SEED)
    inputs(tmp_path / "second", n, SEED)
    inputs(tmp_path / "other", n, SEED + 1)

    first = digests(tmp_path / "first")
    assert len(first) == files
    assert digests(tmp_path / "second") == first
    # The seed reaches every file, so the gate does not pass on constant data.
    other = digests(tmp_path / "other")
    assert other.keys() == first.keys()
    assert [key for key in first if other[key] == first[key]] == []
