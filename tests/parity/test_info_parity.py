"""Command parity for `info`: each target kind against its inspect function."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.inspect import inspect_catalog, inspect_collection, inspect_file, inspect_target

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


def test_info_catalog(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info` with no target reports the catalog at `--catalog`, which defaults to `.`."""
    parity("info", [], catalog=points_catalog, call=lambda root: inspect_catalog(Path()))


def test_info_collection(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info <collection dir>` reports the collection."""
    parity(
        "info",
        ["points"],
        catalog=points_catalog,
        call=lambda root: inspect_collection(Path("points")),
    )


def test_info_file(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info <file>` reports the file, with its version from the catalog at `.`."""
    target = Path("points") / "points.parquet"
    parity(
        "info",
        [str(target)],
        catalog=points_catalog,
        call=lambda root: inspect_file(target, catalog_root=Path()),
    )


@pytest.mark.parametrize("target", [None, Path(), Path("points"), Path("points/points.parquet")])
def test_info_target(
    parity: Parity, points_catalog: Callable[[Path], None], target: Path | None
) -> None:
    """`info` picks the report for each target kind, as `inspect_target` does."""
    parity(
        "info",
        [] if target is None else [str(target)],
        catalog=points_catalog,
        call=lambda root: inspect_target(target, catalog_root=Path()),
    )


def test_info_missing_target(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info` on a path that does not exist reports FileNotFoundError."""
    parity.error(
        "info",
        ["missing"],
        catalog=points_catalog,
        call=lambda root: inspect_target(Path("missing"), catalog_root=Path()),
        raises=FileNotFoundError,
    )


def test_info_plain_directory(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info` on a directory with no catalog.json or collection.json reports ValueError."""

    def catalog(root: Path) -> None:
        points_catalog(root)
        (root / "plain").mkdir()

    parity.error(
        "info",
        ["plain"],
        catalog=catalog,
        call=lambda root: inspect_target(Path("plain"), catalog_root=Path()),
        raises=ValueError,
    )
