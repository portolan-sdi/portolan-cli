"""Fixtures shared by the RunState, commit, and catalog-scale unit tests (issue #995)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from portolan_cli import trace
from portolan_cli.versions import Asset, Version, VersionsFile, write_versions

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


@pytest.fixture
def counters() -> Iterator[Callable[[str], int]]:
    """Trace the test with ``trace.tracing()`` and return a reader for one counter."""
    with trace.tracing():
        yield lambda name: trace.counters().get(name, 0)


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    """A minimal file-backend catalog with an empty catalog-level index."""
    (tmp_path / ".portolan").mkdir()
    (tmp_path / ".portolan" / "config.yaml").write_text("{}\n")
    (tmp_path / "versions.json").write_text(json.dumps({"collections": {}}))
    return tmp_path


@pytest.fixture
def write_snapshot() -> Callable[[Path, dict[str, Asset]], None]:
    """Return a writer of a versions.json with one version, 1.0.0, of the assets."""

    def write(collection_dir: Path, assets: dict[str, Asset]) -> None:
        collection_dir.mkdir(parents=True, exist_ok=True)
        version = Version(
            version="1.0.0",
            created=datetime(2026, 1, 1, tzinfo=timezone.utc),
            breaking=False,
            assets=assets,
            changes=list(assets),
        )
        write_versions(
            collection_dir / "versions.json",
            VersionsFile(spec_version="1.0.0", current_version="1.0.0", versions=[version]),
        )

    return write
