"""Command parity for `skills list`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.skills import list_skill_entries

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


def test_skills_list(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """`skills list` reports the skills and the repository that holds them."""
    parity(
        "skills list",
        [],
        catalog=build_conformant_catalog,
        call=lambda root: list_skill_entries(),
    )
