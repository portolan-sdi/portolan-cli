"""Command parity for `skills list` and `skills show`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.skills import list_skill_entries, show_skill

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


def test_skills_show(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """`skills show` reports the named skill and the repository that holds it."""
    parity(
        "skills show",
        ["portolan-cli"],
        catalog=build_conformant_catalog,
        call=lambda root: show_skill("portolan-cli"),
    )
