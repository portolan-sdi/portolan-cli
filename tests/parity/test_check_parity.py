"""Command parity for `check`: the check, the fix, and the dry run against `run_check_workflow`."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.errors import InvalidLiveUrlError
from portolan_cli.validation.workflow import CheckWorkflowResult, run_check_workflow

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.catalog_invariants import RunCli
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


@pytest.fixture
def retitled(build_conformant_catalog: Callable[..., Path]) -> Callable[[Path], None]:
    """Build a catalog whose metadata.yaml title differs from catalog.json.

    No rule reports the drift, so the check passes. The title sweep of `--fix`
    rewrites catalog.json, and `--fix --dry-run` must leave it alone.
    """

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        with (root / ".portolan" / "metadata.yaml").open("a") as metadata:
            metadata.write("title: Renamed Catalog\n")

    return build


#: The commit takes the root index lock and opens this empty file. It holds no data.
LOCK_FILE = ".portolan/.versions.lock"


def snapshot(root: Path) -> dict[str, bytes]:
    """Return the bytes of every file below ``root``, keyed by relative path."""
    files = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }
    assert files.pop(LOCK_FILE, b"") == b""
    return files


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


def test_check_fix_dry_run(parity: Parity, retitled: Callable[[Path], None]) -> None:
    """`check --fix --dry-run` previews the title fix and writes no file."""

    def dry_run(root: Path) -> CheckWorkflowResult:
        before = snapshot(root)
        result = run_check_workflow(Path(), fix=True, dry_run=True)
        assert snapshot(root) == before
        return result

    parity("check", ["--fix", "--dry-run"], catalog=retitled, call=dry_run)


def test_check_fix_dry_run_command_writes_nothing(
    run_cli: RunCli, retitled: Callable[[Path], None], tmp_path: Path
) -> None:
    """The command side of the dry run: a fix is pending, and no file changes."""
    root = tmp_path / "catalog"
    retitled(root)
    before = snapshot(root)

    result = run_cli("check", str(root), "--fix", "--dry-run", "--json")

    fixers = json.loads(result.stdout)["data"]["fix"]["fixers"]
    assert fixers["total_count"] == 1
    assert snapshot(root) == before


def test_check_invalid_live_url(parity: Parity, conformant: Callable[[Path], object]) -> None:
    """An http live URL fails before any pass runs."""
    parity.error(
        "check",
        ["--live", "--url", "http://example.org/"],
        catalog=conformant,
        call=lambda root: run_check_workflow(Path(), live=True, public_url="http://example.org/"),
        raises=InvalidLiveUrlError,
    )
