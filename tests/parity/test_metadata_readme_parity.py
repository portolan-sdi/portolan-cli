"""Command parity for `metadata init`, `metadata validate`, and `readme`."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest
import yaml

from portolan_cli.input_hardening import InputValidationError
from portolan_cli.metadata.yaml_tree import init_metadata_templates, validate_metadata_files
from portolan_cli.readme import PathNotFoundError, UnsupportedOptionError, generate_readmes

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, that holds one GeoParquet item."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


@pytest.fixture
def contact_catalog(points_catalog: Callable[[Path], None]) -> Callable[[Path], None]:
    """Build `points_catalog` with a contact, so its metadata.yaml is valid."""

    def build(root: Path) -> None:
        points_catalog(root)
        metadata_file = root / ".portolan" / "metadata.yaml"
        metadata = yaml.safe_load(metadata_file.read_text(encoding="utf-8"))
        metadata["contact"] = {"name": "Data Team", "email": "data@example.org"}
        metadata_file.write_text(yaml.safe_dump(metadata), encoding="utf-8")

    return build


@pytest.fixture
def readme_catalog(
    points_catalog: Callable[[Path], None], run_cli: RunCli
) -> Callable[[Path], None]:
    """Build `points_catalog` with fresh READMEs, so `readme --check` exits 0."""

    def build(root: Path) -> None:
        points_catalog(root)
        run_cli("readme", cwd=root)

    return build


def test_metadata_init_recursive(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`metadata init` walks the catalog and reports what it created and skipped."""
    parity(
        "metadata init",
        ["--force"],
        catalog=points_catalog,
        call=lambda root: init_metadata_templates(root, force=True),
    )


def test_metadata_init_single(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`metadata init <path> --no-recursive` writes one template."""
    parity(
        "metadata init",
        ["points", "--no-recursive", "--force"],
        catalog=points_catalog,
        call=lambda root: init_metadata_templates(root, "points", force=True, recursive=False),
    )


def test_metadata_init_traversal(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """A PATH that escapes the catalog fails both sides the same way."""
    parity.error(
        "metadata init",
        ["../escape"],
        catalog=points_catalog,
        call=lambda root: init_metadata_templates(root, "../escape"),
        raises=InputValidationError,
    )


def test_metadata_validate_recursive(
    parity: Parity, contact_catalog: Callable[[Path], None]
) -> None:
    """`metadata validate` reports each metadata.yaml in the tree."""
    parity(
        "metadata validate",
        [],
        catalog=contact_catalog,
        call=lambda root: validate_metadata_files(root),
    )


def test_metadata_validate_single(parity: Parity, contact_catalog: Callable[[Path], None]) -> None:
    """`metadata validate <path> --no-recursive` validates the merged metadata of one level."""
    parity(
        "metadata validate",
        ["points", "--no-recursive"],
        catalog=contact_catalog,
        call=lambda root: validate_metadata_files(root, "points", recursive=False),
    )


def test_metadata_validate_missing_path(
    parity: Parity, points_catalog: Callable[[Path], None]
) -> None:
    """A recursive run from a missing PATH fails both sides the same way."""
    parity.error(
        "metadata validate",
        ["missing"],
        catalog=points_catalog,
        call=lambda root: validate_metadata_files(root, "missing"),
        raises=PathNotFoundError,
    )


def test_readme_recursive(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`readme` writes a README for the catalog and each collection."""
    parity("readme", [], catalog=points_catalog, call=lambda root: generate_readmes(root))


def test_readme_recursive_check(parity: Parity, readme_catalog: Callable[[Path], None]) -> None:
    """`readme --check` reports the fresh and the stale READMEs."""
    parity(
        "readme",
        ["--check"],
        catalog=readme_catalog,
        call=lambda root: generate_readmes(root, check=True),
    )


def test_readme_single(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`readme <path> --no-recursive` writes one README."""
    parity(
        "readme",
        ["points", "--no-recursive"],
        catalog=points_catalog,
        call=lambda root: generate_readmes(root, "points", recursive=False),
    )


def test_readme_single_check(parity: Parity, readme_catalog: Callable[[Path], None]) -> None:
    """`readme --no-recursive --check` reports whether the root README is fresh."""
    parity(
        "readme",
        ["--no-recursive", "--check"],
        catalog=readme_catalog,
        call=lambda root: generate_readmes(root, check=True, recursive=False),
    )


def test_readme_recursive_stdout(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`--stdout` without `--no-recursive` fails both sides the same way."""
    parity.error(
        "readme",
        ["--stdout"],
        catalog=points_catalog,
        call=lambda root: generate_readmes(root, stdout=True),
        raises=UnsupportedOptionError,
    )
