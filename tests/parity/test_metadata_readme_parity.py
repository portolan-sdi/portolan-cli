"""Command parity for `metadata init`, `metadata validate`, and `readme`.

Each success test starts from a catalog that needs work, and it checks the
effect of that work after the parity comparison. A run that exits 1 with a
result (a stale README in check mode, an invalid metadata.yaml) cannot use the
`parity` fixture, which expects exit 0. `_exit_one` compares those runs.
"""

from __future__ import annotations

import contextlib
import json
import shutil
from typing import TYPE_CHECKING, Any, TypeVar

import pytest
import yaml

from portolan_cli.input_hardening import InputValidationError
from portolan_cli.metadata.yaml_tree import init_metadata_templates, validate_metadata_files
from portolan_cli.metadata_yaml import generate_metadata_template
from portolan_cli.readme import (
    UnsupportedOptionError,
    generate_catalog_readme,
    generate_readme_for_collection,
    generate_readmes,
)
from portolan_core.errors import PathNotFoundError

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity, SupportsToDict

pytestmark = pytest.mark.integration

T = TypeVar("T")

STALE = "stale\n"
READMES = ["README.md", "points/README.md"]
ROOT_METADATA = ".portolan/metadata.yaml"
POINTS_METADATA = "points/.portolan/metadata.yaml"


def _keep(results: list[T], result: T) -> T:
    """Record the library result, so the test can check it after the parity run."""
    results.append(result)
    return result


def _metadata_files(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("metadata.yaml"))


def _exit_one(
    run_cli: RunCli,
    root: Path,
    build: Callable[[Path], None],
    args: list[str],
    call: Callable[[Path], SupportsToDict],
    check_tree: Callable[[Path], None],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run a command that exits 1 with a result, then its library call, on fresh catalogs.

    Both sides use the same path, as in the `parity` fixture. ``check_tree``
    checks the catalog after each side.

    Returns:
        The command envelope and the library ``to_dict()``.
    """
    build(root)
    envelope: dict[str, Any] = json.loads(run_cli(*args, "--json", cwd=root, expect=1).stdout)
    check_tree(root)
    shutil.rmtree(root)
    build(root)
    with contextlib.chdir(root):
        data = json.loads(json.dumps(call(root).to_dict()))
    check_tree(root)
    return envelope, data


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, that holds one GeoParquet item.

    The root holds a metadata.yaml template with a license. `points` holds no
    metadata.yaml.
    """

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        source = root / "points" / "points.geojson"
        source.parent.mkdir()
        shutil.copy(valid_points_geojson, source)
        run_cli("add", str(source), cwd=root)

    return build


@pytest.fixture
def stale_catalog(points_catalog: Callable[[Path], None]) -> Callable[[Path], None]:
    """Build `points_catalog` with both READMEs out of date."""

    def build(root: Path) -> None:
        points_catalog(root)
        for readme in READMES:
            (root / readme).write_text(STALE, encoding="utf-8")

    return build


@pytest.fixture
def fresh_catalog(
    points_catalog: Callable[[Path], None], run_cli: RunCli
) -> Callable[[Path], None]:
    """Build `points_catalog` with fresh READMEs, so `readme --check` exits 0."""

    def build(root: Path) -> None:
        points_catalog(root)
        run_cli("readme", cwd=root)

    return build


@pytest.fixture
def contact_catalog(points_catalog: Callable[[Path], None]) -> Callable[[Path], None]:
    """Build `points_catalog` with a contact, so its metadata.yaml is valid."""

    def build(root: Path) -> None:
        points_catalog(root)
        metadata_file = root / ROOT_METADATA
        metadata = yaml.safe_load(metadata_file.read_text(encoding="utf-8"))
        metadata["contact"] = {"name": "Data Team", "email": "data@example.org"}
        metadata_file.write_text(yaml.safe_dump(metadata), encoding="utf-8")

    return build


@pytest.fixture
def bad_email_catalog(contact_catalog: Callable[[Path], None]) -> Callable[[Path], None]:
    """Build `contact_catalog` with a `points` metadata.yaml whose contact email is invalid."""

    def build(root: Path) -> None:
        contact_catalog(root)
        (root / POINTS_METADATA).parent.mkdir()
        (root / POINTS_METADATA).write_text(
            yaml.safe_dump({"contact": {"name": "Data Team", "email": "not-an-email"}}),
            encoding="utf-8",
        )

    return build


def test_metadata_init_recursive(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`metadata init` writes the missing template and skips the existing one."""
    results: list[Any] = []
    parity(
        "metadata init",
        [],
        catalog=points_catalog,
        call=lambda root: _keep(results, init_metadata_templates(root)),
    )
    assert results[0].to_dict() == {
        "mode": "recursive",
        "created": [POINTS_METADATA],
        "skipped": [ROOT_METADATA],
        "permission_errors": [],
        "count": 1,
    }
    assert _metadata_files(parity.root) == [ROOT_METADATA, POINTS_METADATA]
    assert (parity.root / POINTS_METADATA).read_text() == generate_metadata_template()
    assert 'license: "CC-BY-4.0"' in (parity.root / ROOT_METADATA).read_text()


def test_metadata_init_single(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`metadata init <path> --no-recursive` writes one template at that path."""
    results: list[Any] = []
    parity(
        "metadata init",
        ["points", "--no-recursive"],
        catalog=points_catalog,
        call=lambda root: _keep(results, init_metadata_templates(root, "points", recursive=False)),
    )
    assert results[0].to_dict() == {"path": POINTS_METADATA, "overwritten": False}
    assert _metadata_files(parity.root) == [ROOT_METADATA, POINTS_METADATA]
    assert (parity.root / POINTS_METADATA).read_text() == generate_metadata_template()


def test_metadata_init_single_force(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`metadata init --no-recursive --force` replaces the root template and says so."""
    results: list[Any] = []
    parity(
        "metadata init",
        ["--no-recursive", "--force"],
        catalog=points_catalog,
        call=lambda root: _keep(
            results, init_metadata_templates(root, force=True, recursive=False)
        ),
    )
    assert results[0].to_dict() == {"path": ROOT_METADATA, "overwritten": True}
    # The fixture template names a license. The fresh template leaves it blank.
    assert (parity.root / ROOT_METADATA).read_text() == generate_metadata_template()
    assert _metadata_files(parity.root) == [ROOT_METADATA]


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
    results: list[Any] = []
    parity(
        "metadata validate",
        [],
        catalog=contact_catalog,
        call=lambda root: _keep(results, validate_metadata_files(root)),
    )
    assert results[0].to_dict() == {
        "mode": "recursive",
        "results": [{"path": ".", "valid": True, "errors": []}],
        "summary": {"total": 1, "valid": 1, "invalid": 0},
    }


def test_metadata_validate_recursive_invalid(
    run_cli: RunCli, tmp_path: Path, bad_email_catalog: Callable[[Path], None]
) -> None:
    """An invalid metadata.yaml fails the run with its errors in the envelope and the data."""
    envelope, data = _exit_one(
        run_cli,
        tmp_path / "parity-catalog",
        bad_email_catalog,
        ["metadata", "validate"],
        validate_metadata_files,
        check_tree=lambda root: None,
    )
    assert envelope["success"] is False
    assert envelope["data"] == data
    points = data["results"][1]
    assert data["results"][0] == {"path": ".", "valid": True, "errors": []}
    assert points["path"] == "points"
    assert points["valid"] is False
    assert any("email" in e for e in points["errors"])
    assert data["summary"] == {"total": 2, "valid": 1, "invalid": 1}
    assert envelope["errors"] == [
        {"type": "ValidationError", "message": f"points: {e}"} for e in points["errors"]
    ]


def test_metadata_validate_single(parity: Parity, contact_catalog: Callable[[Path], None]) -> None:
    """`metadata validate <path> --no-recursive` validates the merged metadata of one level."""
    results: list[Any] = []
    parity(
        "metadata validate",
        ["points", "--no-recursive"],
        catalog=contact_catalog,
        call=lambda root: _keep(results, validate_metadata_files(root, "points", recursive=False)),
    )
    assert results[0].to_dict() == {"valid": True, "errors": [], "path": "points"}


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


def test_readme_recursive(parity: Parity, stale_catalog: Callable[[Path], None]) -> None:
    """`readme` rewrites the stale README of the catalog and of each collection."""
    results: list[Any] = []
    parity(
        "readme",
        [],
        catalog=stale_catalog,
        call=lambda root: _keep(results, generate_readmes(root)),
    )
    assert results[0].to_dict() == {"generated": READMES, "count": 2}
    root = parity.root
    assert (root / "README.md").read_text() == generate_catalog_readme(root)
    assert (root / "points/README.md").read_text() == generate_readme_for_collection(
        root / "points", root
    )
    assert "points" in (root / "README.md").read_text()


def test_readme_recursive_check_fresh(
    parity: Parity, fresh_catalog: Callable[[Path], None]
) -> None:
    """`readme --check` on fresh READMEs reports them all fresh."""
    results: list[Any] = []
    parity(
        "readme",
        ["--check"],
        catalog=fresh_catalog,
        call=lambda root: _keep(results, generate_readmes(root, check=True)),
    )
    assert results[0].to_dict() == {"fresh": True, "checked": READMES, "stale": []}


def _readmes_untouched(root: Path) -> None:
    for readme in READMES:
        assert (root / readme).read_text() == STALE, readme


def test_readme_recursive_check_stale(
    run_cli: RunCli, tmp_path: Path, stale_catalog: Callable[[Path], None]
) -> None:
    """`readme --check` on stale READMEs exits 1, lists them, and writes nothing."""
    envelope, data = _exit_one(
        run_cli,
        tmp_path / "parity-catalog",
        stale_catalog,
        ["readme", "--check"],
        lambda root: generate_readmes(root, check=True),
        check_tree=_readmes_untouched,
    )
    assert envelope["success"] is True
    assert envelope["data"] == data
    assert data == {"fresh": False, "checked": READMES, "stale": READMES}


def test_readme_single(parity: Parity, stale_catalog: Callable[[Path], None]) -> None:
    """`readme <path> --no-recursive` rewrites the README at that path alone."""
    results: list[Any] = []
    parity(
        "readme",
        ["points", "--no-recursive"],
        catalog=stale_catalog,
        call=lambda root: _keep(results, generate_readmes(root, "points", recursive=False)),
    )
    assert results[0].to_dict() == {"path": "points/README.md", "generated": True}
    root = parity.root
    assert (root / "points/README.md").read_text() == generate_readme_for_collection(
        root / "points", root
    )
    assert (root / "README.md").read_text() == STALE


def test_readme_single_check_fresh(parity: Parity, fresh_catalog: Callable[[Path], None]) -> None:
    """`readme --no-recursive --check` reports a fresh root README."""
    results: list[Any] = []
    parity(
        "readme",
        ["--no-recursive", "--check"],
        catalog=fresh_catalog,
        call=lambda root: _keep(results, generate_readmes(root, check=True, recursive=False)),
    )
    assert results[0].to_dict() == {"fresh": True, "path": "README.md"}


def test_readme_single_check_stale(
    run_cli: RunCli, tmp_path: Path, stale_catalog: Callable[[Path], None]
) -> None:
    """`readme --no-recursive --check` on a stale root README exits 1 and writes nothing."""
    envelope, data = _exit_one(
        run_cli,
        tmp_path / "parity-catalog",
        stale_catalog,
        ["readme", "--no-recursive", "--check"],
        lambda root: generate_readmes(root, check=True, recursive=False),
        check_tree=_readmes_untouched,
    )
    assert envelope["success"] is True
    assert envelope["data"] == data
    assert data == {"fresh": False, "path": "README.md"}


def test_readme_recursive_stdout(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`--stdout` without `--no-recursive` fails both sides the same way."""
    parity.error(
        "readme",
        ["--stdout"],
        catalog=points_catalog,
        call=lambda root: generate_readmes(root, stdout=True),
        raises=UnsupportedOptionError,
    )
