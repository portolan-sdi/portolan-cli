"""Command parity for `logo`: the success path and a missing source."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.errors import LogoSourceNotFoundError
from portolan_cli.logo import set_catalog_logo

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


def test_logo_missing_source(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """A missing source fails both sides with the same error type, message, and code."""
    parity.error(
        "logo",
        ["missing.png"],
        catalog=build_conformant_catalog,
        call=lambda root: set_catalog_logo(root, "missing.png", title=None),
        raises=LogoSourceNotFoundError,
    )


def test_logo_svg(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """An SVG logo reports its link, its relative path, and the SVG warning."""

    def catalog(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "brand.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')

    parity(
        "logo",
        ["brand.svg", "--title", "Acme"],
        catalog=catalog,
        call=lambda root: set_catalog_logo(root, "brand.svg", title="Acme"),
    )
