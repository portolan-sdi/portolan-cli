"""Command parity for `pull`, `sync`, and `clone` against an in-memory remote.

Each object-store setup returns one ``MemoryStore``. The fixture fills it from
a local source catalog under the key prefix ``catalog``, so
``s3://bucket/catalog`` resolves to that catalog. No test sends a network
request. A ``LocalStore`` does not take the conditional puts of push, so the
remote is in memory.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from obstore.store import MemoryStore

from portolan_cli.sync.core import sync
from portolan_cli.sync.pull import pull_catalog
from portolan_cli.sync.transport import clone

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

REMOTE = "s3://bucket/catalog"
ASSET = Path("points") / "points.parquet"


@pytest.fixture
def remote() -> Iterator[list[MemoryStore]]:
    """Route every object-store setup of push and pull to the current remote store."""
    current = [MemoryStore()]

    def memory_store(*_args: Any, **_kwargs: Any) -> tuple[MemoryStore, dict[str, Any]]:
        return current[0], {}

    with (
        patch("portolan_cli.sync.upload._setup_store_and_kwargs", side_effect=memory_store),
        patch("portolan_cli.sync.pull._setup_store_and_kwargs", side_effect=memory_store),
        patch("portolan_cli.sync.download._setup_store_and_kwargs", side_effect=memory_store),
    ):
        yield current


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
def published(source_catalog: Path, remote: list[MemoryStore]) -> Callable[[], None]:
    """Return a function that publishes the source catalog to a fresh remote."""

    def publish() -> None:
        store = MemoryStore()
        for path in sorted(source_catalog.rglob("*")):
            rel = path.relative_to(source_catalog)
            if path.is_file() and rel.parts[0] != ".portolan":
                store.put(f"catalog/{rel.as_posix()}", path.read_bytes())
        remote[0] = store

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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "#1133: sync.push still prints through portolan_cli.output. The push change "
        "of #1133 removes that output. Delete this mark when it merges."
    ),
)
def test_sync_in_sync(parity: Parity, local_in_sync: Callable[[Path], None]) -> None:
    """`sync -c points` runs pull, init, scan, check, and push on a catalog in sync."""
    parity(
        "sync",
        [REMOTE, "-c", "points"],
        catalog=local_in_sync,
        call=lambda root: sync(Path(), "points", REMOTE),
    )


def test_clone_all(parity: Parity, published: Callable[[], None]) -> None:
    """`clone <remote> cloned` copies every collection into a new catalog."""

    def empty(root: Path) -> None:
        published()
        root.mkdir()

    parity(
        "clone",
        [REMOTE, "cloned"],
        catalog=empty,
        call=lambda root: clone(REMOTE, Path("cloned")),
    )


def test_clone_reports_the_catalog_download(tmp_path: Path, published: Callable[[], None]) -> None:
    """clone sends the download lines of the remote catalog.json to its report."""
    published()
    lines: list[tuple[str, str]] = []

    result = clone(
        REMOTE, tmp_path / "cloned", report=lambda level, msg: lines.append((level, msg))
    )

    assert result.success
    downloading = [
        msg for level, msg in lines if level == "info" and msg.startswith("Downloading tmp")
    ]
    assert len(downloading) == 1
    assert re.fullmatch(
        r"Downloading tmp\w+\.json \(\d+\.\d\d MB\) <- catalog/catalog\.json", downloading[0]
    )
    complete = [msg for level, msg in lines if level == "success" and msg.startswith("Download ")]
    assert len(complete) == 1
    assert re.fullmatch(r"Download complete \(\d+\.\d\d MB/s\)", complete[0])
