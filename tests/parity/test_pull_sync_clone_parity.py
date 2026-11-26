"""Command parity for `pull`, `sync`, and `clone` against an in-memory remote.

Each object-store setup returns one ``MemoryStore``. The fixture fills it from
a local source catalog under the key prefix ``catalog``, so
``s3://bucket/catalog`` resolves to that catalog. No test sends a network
request. A ``LocalStore`` does not take the conditional puts of push, so the
remote is in memory.

Parity proves that the command and the library return the same data. Each test
also pins that data and the files on disk after the library call, so an empty
result on both sides cannot pass.
"""

from __future__ import annotations

import contextlib
import re
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn
from unittest.mock import patch

import pytest
from obstore.store import MemoryStore

from portolan_cli.sync.core import sync
from portolan_cli.sync.pull import PullAllResult, PullResult, pull_catalog
from portolan_cli.sync.transport import CloneResult, clone

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

REMOTE = "s3://bucket/catalog"
ASSET = Path("points") / "points.parquet"


def tree(root: Path) -> dict[str, bytes]:
    """Return the bytes of each file below ``root``, outside ``.portolan``."""
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.relative_to(root).parts[0] != ".portolan"
    }


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
        for rel, data in tree(source_catalog).items():
            store.put(f"catalog/{rel}", data)
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
    parity: Parity, local_missing_asset: Callable[[Path], None], source_catalog: Path
) -> None:
    """`pull -c points --restore` downloads the asset that the local copy lost."""
    results: list[PullResult] = []

    def call(root: Path) -> PullResult:
        result = pull_catalog(REMOTE, Path(), "points", restore=True)
        assert isinstance(result, PullResult)
        results.append(result)
        return result

    parity(
        "pull",
        [REMOTE, "-c", "points", "--restore"],
        catalog=local_missing_asset,
        call=call,
    )

    assert results[0].to_dict() == {
        "files_downloaded": 1,
        "files_skipped": 0,
        "files_restored": 1,
        "local_version": "1.0.0",
        "remote_version": "1.0.0",
        "up_to_date": False,
    }
    assert (parity.root / ASSET).read_bytes() == (source_catalog / ASSET).read_bytes()
    assert tree(parity.root) == tree(source_catalog)


def test_pull_catalog_restore(
    parity: Parity, local_missing_asset: Callable[[Path], None], source_catalog: Path
) -> None:
    """`pull --restore` without a collection pulls each collection of the catalog."""
    results: list[PullAllResult] = []

    def call(root: Path) -> PullAllResult:
        result = pull_catalog(REMOTE, Path(), restore=True)
        assert isinstance(result, PullAllResult)
        results.append(result)
        return result

    parity("pull", [REMOTE, "--restore"], catalog=local_missing_asset, call=call)

    assert results[0].to_dict() == {
        "total_collections": 1,
        "successful_collections": 1,
        "failed_collections": 0,
        "total_files_downloaded": 1,
        "collection_errors": {},
    }
    assert tree(parity.root) == tree(source_catalog)


def test_pull_dry_run_touches_no_store_and_writes_nothing(
    parity: Parity, local_in_sync: Callable[[Path], None], source_catalog: Path
) -> None:
    """`pull -c points --dry-run` sends no store request and changes no local file."""

    def no_store_call(*_args: Any, **_kwargs: Any) -> NoReturn:
        raise AssertionError("a dry run called the object store")

    results: list[PullResult] = []

    def call(root: Path) -> PullResult:
        result = pull_catalog(REMOTE, Path(), "points", dry_run=True)
        assert isinstance(result, PullResult)
        results.append(result)
        return result

    # pull and download call obstore through the module, so one patch per name
    # covers both.
    with contextlib.ExitStack() as stack:
        for name in ("get", "get_async", "head", "head_async", "list"):
            stack.enter_context(patch(f"obstore.{name}", side_effect=no_store_call))
        parity("pull", [REMOTE, "-c", "points", "--dry-run"], catalog=local_in_sync, call=call)

    assert results[0].dry_run is True
    assert results[0].to_dict() == {
        "files_downloaded": 0,
        "files_skipped": 0,
        "files_restored": 0,
        "local_version": "1.0.0",
        "remote_version": None,
        "up_to_date": False,
    }
    assert tree(parity.root) == tree(source_catalog)


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


def test_clone_all(parity: Parity, published: Callable[[], None], source_catalog: Path) -> None:
    """`clone <remote> cloned` copies every collection into a new catalog."""
    results: list[CloneResult] = []

    def empty(root: Path) -> None:
        published()
        root.mkdir()

    def call(root: Path) -> CloneResult:
        result = clone(REMOTE, Path("cloned"))
        results.append(result)
        return result

    parity("clone", [REMOTE, "cloned"], catalog=empty, call=call)

    source_files = tree(source_catalog)
    assert results[0].collections_cloned == ["points"]
    assert results[0].total_files_downloaded == len(source_files)
    assert tree(parity.root / "cloned") == source_files


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
