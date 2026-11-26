"""Command parity for `push`: each push path against `run_push`.

Push writes to an in-memory store. The catalog factory replaces the store, so
the command and the library call each start from an empty remote.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest
from obstore.store import MemoryStore

from portolan_cli.sync.push import run_push

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.sync.notices import NoticeLevel
    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

DESTINATION = "s3://parity-bucket/catalog"


def _quiet(_level: NoticeLevel, _message: str) -> None:
    """Drop each notice. The CLI prints them, and the library call must not."""


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, and an empty remote for it."""
    remote: dict[str, MemoryStore] = {}
    monkeypatch.setattr(
        "portolan_cli.sync.push.setup_store", lambda *_a, **_k: (remote["store"], "catalog")
    )

    def build(root: Path) -> None:
        remote["store"] = MemoryStore()
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


def test_push_all_collections(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`push` with no `--collection` pushes every collection."""
    parity(
        "push",
        [DESTINATION],
        catalog=points_catalog,
        call=lambda root: run_push(root, DESTINATION, notify=_quiet),
    )


def test_push_one_collection(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`push --collection` pushes that collection only."""
    parity(
        "push",
        [DESTINATION, "--collection", "points"],
        catalog=points_catalog,
        call=lambda root: run_push(root, DESTINATION, collection="points", notify=_quiet),
    )


def test_push_dry_run(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`push --dry-run` reports the upper bound and writes nothing."""
    parity(
        "push",
        [DESTINATION, "--dry-run", "--collection", "points"],
        catalog=points_catalog,
        call=lambda root: run_push(
            root, DESTINATION, collection="points", dry_run=True, notify=_quiet
        ),
    )


def test_push_missing_collection(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`push --collection` for a collection without versions.json fails."""
    parity.error(
        "push",
        [DESTINATION, "--collection", "missing"],
        catalog=points_catalog,
        call=lambda root: run_push(root, DESTINATION, collection="missing", notify=_quiet),
        raises=FileNotFoundError,
    )
