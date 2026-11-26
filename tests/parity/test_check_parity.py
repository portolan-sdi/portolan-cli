"""Command parity for `check`: the check, the fix, and the dry run against `run_check_workflow`."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.errors import InvalidLiveUrlError
from portolan_cli.validation.workflow import run_check_workflow

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def conformant(build_conformant_catalog: Callable[..., Path]) -> Callable[[Path], object]:
    """Build a catalog that passes the check, so the command exits 0."""
    return build_conformant_catalog


@pytest.fixture
def untitled(build_conformant_catalog: Callable[..., Path]) -> Callable[[Path], None]:
    """Build a catalog whose catalog.json has no title, which the `titles` fixer restores."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        catalog_file = root / "catalog.json"
        catalog = json.loads(catalog_file.read_text())
        del catalog["title"]
        catalog_file.write_text(json.dumps(catalog, indent=2))

    return build


def test_check(parity: Parity, conformant: Callable[[Path], object]) -> None:
    """`check` validates the catalog at `.` and its source files."""
    parity("check", [], catalog=conformant, call=lambda root: run_check_workflow(Path()))


def test_check_metadata(parity: Parity, conformant: Callable[[Path], object]) -> None:
    """`check --metadata --no-data` runs only the metadata pass."""
    parity(
        "check",
        ["--metadata", "--no-data"],
        catalog=conformant,
        call=lambda root: run_check_workflow(Path(), metadata=True, data=False),
    )


def test_check_fix(parity: Parity, untitled: Callable[[Path], None]) -> None:
    """`check --fix` runs the check, the fix, and the re-check."""
    parity(
        "check",
        ["--fix"],
        catalog=untitled,
        call=lambda root: run_check_workflow(Path(), fix=True),
    )


def test_check_fix_dry_run(parity: Parity, conformant: Callable[[Path], object]) -> None:
    """`check --fix --dry-run` previews the fixes from the first check and writes nothing."""
    parity(
        "check",
        ["--fix", "--dry-run"],
        catalog=conformant,
        call=lambda root: run_check_workflow(Path(), fix=True, dry_run=True),
    )


def test_check_invalid_live_url(parity: Parity, conformant: Callable[[Path], object]) -> None:
    """An http live URL fails before any pass runs."""
    parity.error(
        "check",
        ["--live", "--url", "http://example.org/"],
        catalog=conformant,
        call=lambda root: run_check_workflow(Path(), live=True, public_url="http://example.org/"),
        raises=InvalidLiveUrlError,
    )
