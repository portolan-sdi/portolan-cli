"""Command parity for the local catalog commands: list, status, init, rm, config, clean.

`info` has its own module, `test_info_parity.py`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.catalog_list import list_catalog_contents
from portolan_cli.clean import clean_metadata
from portolan_cli.config import (
    clear_setting,
    read_setting,
    read_settings,
    set_setting,
    store_setting,
)
from portolan_cli.errors import CatalogAlreadyExistsError, ConfigError
from portolan_cli.inspect import initialize_catalog
from portolan_cli.remove import SafetyError, remove_path
from portolan_cli.status import get_catalog_status

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


def _tree(root: Path) -> dict[str, bytes]:
    """Map each file below ``root`` to its bytes."""
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


@pytest.fixture
def points_catalog(points_catalog: Callable[[Path], None]) -> Callable[[Path], None]:
    """The `points` catalog of conftest.py, with one untracked file in the collection."""

    def build(root: Path) -> None:
        points_catalog(root)
        (root / "points" / "notes.txt").write_text("not tracked\n", encoding="utf-8")

    return build


@pytest.mark.parametrize(
    ("args", "tracked_only", "untracked_only"),
    [([], False, False), (["--tracked-only"], True, False), (["--untracked-only"], False, True)],
)
def test_list(
    parity: Parity,
    points_catalog: Callable[[Path], None],
    args: list[str],
    tracked_only: bool,
    untracked_only: bool,
) -> None:
    """`list` reports the files of each collection, with the status filter applied."""
    parity(
        "list",
        args,
        catalog=points_catalog,
        call=lambda root: list_catalog_contents(
            root, tracked_only=tracked_only, untracked_only=untracked_only
        ),
    )


def test_list_collection(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`list --collection` reports one collection."""
    parity(
        "list",
        ["--collection", "points"],
        catalog=points_catalog,
        call=lambda root: list_catalog_contents(root, collection_id="points"),
    )


@pytest.mark.parametrize(
    ("args", "collection", "offline"),
    [([], None, False), (["--offline"], None, True), (["-c", "points"], "points", False)],
)
def test_status(
    parity: Parity,
    points_catalog: Callable[[Path], None],
    args: list[str],
    collection: str | None,
    offline: bool,
) -> None:
    """`status` reports each collection. The catalog has no remote, so none is read."""
    parity(
        "status",
        args,
        catalog=points_catalog,
        call=lambda root: get_catalog_status(root, collection, offline=offline),
    )


def test_status_no_collections(
    parity: Parity, build_conformant_catalog: Callable[..., Path]
) -> None:
    """`status` on a catalog with no collection reports an empty list."""
    parity(
        "status",
        [],
        catalog=build_conformant_catalog,
        call=lambda root: get_catalog_status(root),
    )


def _empty_directory_with_logo(root: Path) -> None:
    root.mkdir()
    (root / "brand.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"/>\n', encoding="utf-8"
    )


def test_init(parity: Parity) -> None:
    """`init --auto` creates the catalog and reports its id, logo, and warnings."""
    parity(
        "init",
        ["--auto", "--id", "parity", "--license", "CC-BY-4.0", "--logo", "brand.svg"],
        catalog=_empty_directory_with_logo,
        call=lambda root: initialize_catalog(
            Path(), catalog_id="parity", license_id="CC-BY-4.0", logo="brand.svg"
        ),
    )


def test_init_derived_id(parity: Parity) -> None:
    """`init` with no --id derives the id from the directory name."""
    parity(
        "init",
        ["--auto", "--license", "CC-BY-4.0"],
        catalog=lambda root: root.mkdir(),
        call=lambda root: initialize_catalog(Path(), license_id="CC-BY-4.0"),
    )


def test_init_existing_catalog(
    parity: Parity, build_conformant_catalog: Callable[..., Path]
) -> None:
    """`init` in a Portolan catalog reports CatalogAlreadyExistsError."""
    parity.error(
        "init",
        ["--auto", "--license", "CC-BY-4.0"],
        catalog=build_conformant_catalog,
        call=lambda root: initialize_catalog(Path(), license_id="CC-BY-4.0"),
        raises=CatalogAlreadyExistsError,
    )


@pytest.mark.parametrize(
    ("args", "keep", "dry_run"),
    [(["--keep"], True, False), (["--dry-run"], False, True), (["--force"], False, False)],
)
def test_rm(
    parity: Parity,
    points_catalog: Callable[[Path], None],
    args: list[str],
    keep: bool,
    dry_run: bool,
) -> None:
    """`rm` untracks the file, and deletes it unless --keep or --dry-run is given."""
    target = Path("points") / "points.geojson"
    parity(
        "rm",
        [*args, str(target)],
        catalog=points_catalog,
        call=lambda root: remove_path(
            target, catalog_root=root, keep=keep, dry_run=dry_run, force=not (keep or dry_run)
        ),
    )


def test_rm_without_force(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`rm` that would delete a file and has no --force reports a SafetyError."""
    target = Path("points") / "points.geojson"
    parity.error(
        "rm",
        [str(target)],
        catalog=points_catalog,
        call=lambda root: remove_path(target, catalog_root=root),
        raises=SafetyError,
    )


def test_rm_without_force_changes_nothing(
    points_catalog: Callable[[Path], None], tmp_path: Path
) -> None:
    """`remove_path` without force refuses before it touches the catalog."""
    root = tmp_path / "catalog"
    points_catalog(root)
    before = _tree(root)
    with pytest.raises(SafetyError) as caught:
        remove_path(root / "points" / "points.geojson", catalog_root=root)
    assert str(caught.value) == (
        "Destructive rm requires --force flag. Use --keep to preserve files, "
        "or --dry-run to preview."
    )
    assert _tree(root) == before


def test_rm_corrupt_versions(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`rm` reports the ValueError of a versions.json that is not JSON."""

    def catalog(root: Path) -> None:
        points_catalog(root)
        (root / "points" / "versions.json").write_text("{", encoding="utf-8")

    target = Path("points") / "points.geojson"
    parity.error(
        "rm",
        ["--keep", str(target)],
        catalog=catalog,
        call=lambda root: remove_path(target, catalog_root=root, keep=True),
        raises=ValueError,
    )


@pytest.fixture
def configured_catalog(points_catalog: Callable[[Path], None]) -> Callable[[Path], None]:
    """The points catalog, with one catalog setting and one collection setting."""

    def build(root: Path) -> None:
        points_catalog(root)
        set_setting(root, "statistics.enabled", "false")
        set_setting(root, "pmtiles.enabled", "true", collection="points")

    return build


def _with_config_text(text: str) -> Callable[[Callable[[Path], None]], Callable[[Path], None]]:
    def wrap(build: Callable[[Path], None]) -> Callable[[Path], None]:
        def catalog(root: Path) -> None:
            build(root)
            with (root / ".portolan" / "config.yaml").open("a", encoding="utf-8") as f:
                f.write(text)

        return catalog

    return wrap


@pytest.mark.parametrize(
    ("key", "collection"),
    [("statistics.enabled", None), ("backend", None), ("pmtiles.enabled", "points")],
)
def test_config_get(
    parity: Parity,
    configured_catalog: Callable[[Path], None],
    key: str,
    collection: str | None,
) -> None:
    """`config get` reports the value and its source."""
    parity(
        "config get",
        [key] if collection is None else [key, "--collection", collection],
        catalog=configured_catalog,
        call=lambda root: read_setting(key, root, collection),
    )


# The message of commit f7f8214, before the move. It has no code prefix.
SENSITIVE_REMOTE = (
    "'remote' found in config.yaml but sensitive settings cannot be read from config "
    "files. Use environment variable PORTOLAN_REMOTE or .env file."
)


def test_config_get_sensitive_in_file(
    run_cli: RunCli, configured_catalog: Callable[[Path], None], tmp_path: Path
) -> None:
    """`config get` reports a sensitive setting in config.yaml as it did before the move."""
    root = tmp_path / "catalog"
    _with_config_text("remote: s3://bucket/catalog\n")(configured_catalog)(root)
    result = run_cli("config", "get", "remote", "--json", cwd=root, expect=1)
    assert json.loads(result.stdout)["errors"] == [
        {"type": "ConfigError", "message": SENSITIVE_REMOTE}
    ]
    with pytest.raises(ValueError) as caught:
        read_setting("remote", root)
    assert str(caught.value) == SENSITIVE_REMOTE


def test_config_get_bad_yaml(parity: Parity, configured_catalog: Callable[[Path], None]) -> None:
    """`config get` reports a config.yaml that does not parse."""
    parity.error(
        "config get",
        ["backend"],
        catalog=_with_config_text("  bad: [\n")(configured_catalog),
        call=lambda root: read_setting("backend", root),
        raises=ConfigError,
    )


@pytest.mark.parametrize(
    ("args", "key", "value", "collection"),
    [
        (["statistics.enabled", "true"], "statistics.enabled", "true", None),
        (["pmtiles.enabled", "false", "-c", "points"], "pmtiles.enabled", "false", "points"),
    ],
)
def test_config_set(
    parity: Parity,
    configured_catalog: Callable[[Path], None],
    args: list[str],
    key: str,
    value: str,
    collection: str | None,
) -> None:
    """`config set` reports the stored value in the type of its setting."""
    parity(
        "config set",
        args,
        catalog=configured_catalog,
        call=lambda root: store_setting(root, key, value, collection),
    )


def test_config_set_unknown_key(parity: Parity, configured_catalog: Callable[[Path], None]) -> None:
    """`config set` of an unknown key reports a ConfigError."""
    parity.error(
        "config set",
        ["no.such.key", "1"],
        catalog=configured_catalog,
        call=lambda root: store_setting(root, "no.such.key", "1"),
        raises=ConfigError,
    )


def test_config_set_unparseable_value(
    parity: Parity, configured_catalog: Callable[[Path], None]
) -> None:
    """`config set` of an integer too long to parse reports the ValueError of the parser."""
    value = "9" * 5000
    parity.error(
        "config set",
        ["pmtiles.max_zoom", value],
        catalog=configured_catalog,
        call=lambda root: store_setting(root, "pmtiles.max_zoom", value),
        raises=ValueError,
    )
    with pytest.raises(ValueError) as caught:
        store_setting(parity.root, "pmtiles.max_zoom", value)
    assert str(caught.value) == (
        "Exceeds the limit (4300 digits) for integer string conversion: value has 5000 "
        "digits; use sys.set_int_max_str_digits() to increase the limit"
    )


@pytest.mark.parametrize("collection", [None, "points"])
def test_config_list(
    parity: Parity, configured_catalog: Callable[[Path], None], collection: str | None
) -> None:
    """`config list` reports each setting with its source."""
    parity(
        "config list",
        [] if collection is None else ["--collection", collection],
        catalog=configured_catalog,
        call=lambda root: read_settings(root, collection),
    )


def test_config_list_bad_yaml(parity: Parity, configured_catalog: Callable[[Path], None]) -> None:
    """`config list` reports a config.yaml that does not parse."""
    parity.error(
        "config list",
        [],
        catalog=_with_config_text("  bad: [\n")(configured_catalog),
        call=lambda root: read_settings(root),
        raises=ConfigError,
    )


@pytest.mark.parametrize(
    ("args", "key", "collection"),
    [
        (["statistics.enabled"], "statistics.enabled", None),
        (["pmtiles.enabled", "-c", "points"], "pmtiles.enabled", "points"),
        (["backend"], "backend", None),
    ],
)
def test_config_unset(
    parity: Parity,
    configured_catalog: Callable[[Path], None],
    args: list[str],
    key: str,
    collection: str | None,
) -> None:
    """`config unset` reports whether the key was set."""
    parity(
        "config unset",
        args,
        catalog=configured_catalog,
        call=lambda root: clear_setting(root, key, collection),
    )


def test_config_unset_bad_yaml(parity: Parity, configured_catalog: Callable[[Path], None]) -> None:
    """`config unset` reports a config.yaml that does not parse."""
    parity.error(
        "config unset",
        ["backend"],
        catalog=_with_config_text("  bad: [\n")(configured_catalog),
        call=lambda root: clear_setting(root, "backend"),
        raises=ConfigError,
    )


@pytest.mark.parametrize("dry_run", [True, False])
def test_clean(parity: Parity, points_catalog: Callable[[Path], None], dry_run: bool) -> None:
    """`clean` removes the metadata, or lists it with --dry-run, and keeps the data."""
    parity(
        "clean",
        ["--dry-run"] if dry_run else [],
        catalog=points_catalog,
        call=lambda root: clean_metadata(root, dry_run=dry_run),
    )
