"""The `parity` fixture. `tests/parity/harness.py` gives the convention."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

# The harness asserts outside a test module. Rewrite it, so a failure shows the diff.
pytest.register_assert_rewrite("tests.parity.harness")

from tests.parity.harness import Parity  # noqa: E402

if TYPE_CHECKING:
    from pathlib import Path

    from tests.parity.harness import VerifyingRunCli


@pytest.fixture
def parity(run_cli: VerifyingRunCli, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> Parity:
    """Compare a command with its library function. See `tests/parity/harness.py`."""
    return Parity(run_cli, capsys, tmp_path)
