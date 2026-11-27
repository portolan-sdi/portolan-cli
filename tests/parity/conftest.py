"""The `parity` fixture. `tests/parity/harness.py` gives the convention."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest

# The harness asserts outside a test module. Rewrite it, so a failure shows the diff.
pytest.register_assert_rewrite("tests.parity.harness")

from tests.parity.harness import Parity  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import VerifyingRunCli


@pytest.fixture
def parity(run_cli: VerifyingRunCli, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> Parity:
    """Compare a command with its library function. See `tests/parity/harness.py`."""
    return Parity(run_cli, capsys, tmp_path)


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, that holds one GeoParquet asset."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build
