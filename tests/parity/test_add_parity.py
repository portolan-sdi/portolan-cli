"""Command parity for `add` and `add-external`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.external import add_external

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

EXTERNAL_URL = "https://example.org/data/buildings.parquet"


def test_add_external(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """`add-external <url>` registers the remote asset as a collection."""
    parity(
        "add-external",
        [EXTERNAL_URL, "--license", "CC-BY-4.0"],
        catalog=build_conformant_catalog,
        call=lambda root: add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0"),
    )


def test_add_external_exists(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """A collection that exists already fails both sides with the same error."""

    def with_collection(root: Path) -> None:
        build_conformant_catalog(root)
        add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0")

    parity.error(
        "add-external",
        [EXTERNAL_URL, "--license", "CC-BY-4.0"],
        catalog=with_collection,
        call=lambda root: add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0"),
        raises=FileExistsError,
    )
