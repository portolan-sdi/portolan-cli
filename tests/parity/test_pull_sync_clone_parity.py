"""Command parity for `pull`, `sync`, and `clone` against a local directory remote.

The remote is a directory. Each object-store setup returns a ``LocalStore``
rooted above it, so ``s3://bucket/catalog`` resolves to ``<remote>/catalog``.
No test sends a network request.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from obstore.store import LocalStore

from portolan_cli.sync.pull import pull_catalog

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

REMOTE = "s3://bucket/catalog"
ASSET = Path("points") / "points.parquet"


@pytest.fixture
def remote_dir(tmp_path: Path) -> Iterator[Path]:
    """Route every object-store setup of push and pull to a directory remote."""
    bucket = tmp_path / "bucket"
    bucket.mkdir()

    def local_store(*_args: Any, **_kwargs: Any) -> tuple[LocalStore, dict[str, Any]]:
        return LocalStore(prefix=bucket), {}

    with (
        patch("portolan_cli.sync.upload._setup_store_and_kwargs", side_effect=local_store),
        patch("portolan_cli.sync.pull._setup_store_and_kwargs", side_effect=local_store),
        patch("portolan_cli.sync.download._setup_store_and_kwargs", side_effect=local_store),
    ):
        yield bucket / "catalog"


@pytest.fixture
def source_catalog(
    tmp_path: Path,
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Path:
    """Build one catalog with one collection, `points`, that holds one GeoParquet item."""
    root = tmp_path / "source"
    build_conformant_catalog(root)
    (root / "points").mkdir()
    shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
    run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))
    return root


@pytest.fixture
def published(source_catalog: Path, remote_dir: Path) -> Callable[[], None]:
    """Return a function that publishes the source catalog to the remote again."""

    def publish() -> None:
        if remote_dir.exists():
            shutil.rmtree(remote_dir)
        shutil.copytree(source_catalog, remote_dir, ignore=shutil.ignore_patterns(".portolan"))

    return publish


@pytest.fixture
def local_missing_asset(
    source_catalog: Path, published: Callable[[], None]
) -> Callable[[Path], None]:
    """Build a local copy of the published catalog that lost its data asset."""

    def build(root: Path) -> None:
        published()
        shutil.copytree(source_catalog, root)
        (root / ASSET).unlink()

    return build


@pytest.fixture
def local_in_sync(source_catalog: Path, published: Callable[[], None]) -> Callable[[Path], None]:
    """Build a local copy of the published catalog."""

    def build(root: Path) -> None:
        published()
        shutil.copytree(source_catalog, root)

    return build


def test_pull_collection_restore(
    parity: Parity, local_missing_asset: Callable[[Path], None]
) -> None:
    """`pull -c points --restore` downloads the asset that the local copy lost."""
    parity(
        "pull",
        [REMOTE, "-c", "points", "--restore"],
        catalog=local_missing_asset,
        call=lambda root: pull_catalog(REMOTE, Path(), "points", restore=True),
    )


def test_pull_catalog_restore(parity: Parity, local_missing_asset: Callable[[Path], None]) -> None:
    """`pull --restore` without a collection pulls each collection of the catalog."""
    parity(
        "pull",
        [REMOTE, "--restore"],
        catalog=local_missing_asset,
        call=lambda root: pull_catalog(REMOTE, Path(), restore=True),
    )


def test_pull_invalid_remote(parity: Parity, local_in_sync: Callable[[Path], None]) -> None:
    """A remote URL with an unknown scheme fails on both sides with the same error."""
    parity.error(
        "pull",
        ["ftp://bucket/catalog", "-c", "points"],
        catalog=local_in_sync,
        call=lambda root: pull_catalog("ftp://bucket/catalog", Path(), "points"),
        raises=ValueError,
    )
