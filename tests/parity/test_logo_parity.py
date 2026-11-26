"""Command parity for `logo`: the success path and a missing source."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from portolan_cli.errors import LogoSourceNotFoundError
from portolan_cli.logo import set_catalog_logo

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.logo import LogoResult
    from tests.catalog_invariants import RunCli
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


SVG = '<svg xmlns="http://www.w3.org/2000/svg"/>'


def assert_logo(root: Path) -> None:
    """Fail unless the image is in _assets/ and catalog.json links it as the one icon."""
    catalog = json.loads((root / "catalog.json").read_text())
    assert (root / "_assets" / "brand.svg").read_text() == SVG
    assert [link for link in catalog["links"] if link["rel"] == "icon"] == [
        {"rel": "icon", "href": "./_assets/brand.svg", "type": "image/svg+xml", "title": "Acme"}
    ]


def test_logo_svg(
    parity: Parity,
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    tmp_path: Path,
) -> None:
    """An SVG logo reports its link, its relative path, and the SVG warning."""

    def catalog(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "brand.svg").write_text(SVG)

    def publish(root: Path) -> LogoResult:
        result = set_catalog_logo(root, "brand.svg", title="Acme")
        assert_logo(root)
        return result

    parity("logo", ["brand.svg", "--title", "Acme"], catalog=catalog, call=publish)

    # The harness removes the catalog that the command wrote, so check a second run.
    command_root = tmp_path / "command-side"
    catalog(command_root)
    run_cli("logo", "brand.svg", "--title", "Acme", cwd=command_root)
    assert_logo(command_root)
