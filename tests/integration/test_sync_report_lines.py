"""The sync commands print the lines that the sync library reports."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from obstore.store import MemoryStore

from tests.conftest import aws_files_environ

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests.catalog_invariants import RunCli

pytestmark = pytest.mark.integration

REMOTE = "s3://bucket/catalog"
ENDPOINT_LINE = "  Endpoint https://s3.example.test from AWS profile 'default'"


@pytest.fixture
def remote(tmp_path: Path) -> Iterator[MemoryStore]:
    """Serve every S3 store from one MemoryStore, with an endpoint from the profile."""
    aws_dir = tmp_path / ".aws"
    aws_dir.mkdir()
    (aws_dir / "config").write_text(
        "[default]\nendpoint_url = https://s3.example.test\nregion = us-east-1\n"
    )
    store = MemoryStore()
    with (
        patch.object(Path, "home", return_value=tmp_path),
        aws_files_environ(aws_dir),
        patch.dict("os.environ", {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}),
        patch("portolan_cli.sync.upload.S3Store", return_value=store),
    ):
        yield store


@pytest.fixture
def catalog(
    tmp_path: Path,
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Path:
    """A catalog with one collection, ``points``."""
    root = build_conformant_catalog(tmp_path / "catalog")
    (root / "points").mkdir()
    shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
    run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))
    return root


def test_push_prints_the_endpoint_line(remote: MemoryStore, catalog: Path, run_cli: RunCli) -> None:
    """``push`` names the profile that supplies the endpoint, before its first line."""
    lines = run_cli("push", REMOTE, "-c", "points", cwd=catalog).stdout.splitlines()

    assert lines[:2] == [ENDPOINT_LINE, "→ Checking remote state: s3://bucket/catalog"]


def test_status_prints_the_download_of_the_remote_versions(
    remote: MemoryStore, catalog: Path, run_cli: RunCli
) -> None:
    run_cli("push", REMOTE, "-c", "points", cwd=catalog)

    with patch.dict("os.environ", {"PORTOLAN_REMOTE": REMOTE}):
        lines = run_cli("status", cwd=catalog).stdout.splitlines()

    assert lines[0] == ENDPOINT_LINE
    assert lines[1].startswith("→ Downloading ")
    assert lines[1].endswith(".json (0.00 MB) <- catalog/points/versions.json")
    assert lines[2].startswith("✓ Download complete (")
    assert lines[3:6] == [
        "→ Collection: points",
        "→   Local version: 1.0.0",
        "→   Remote version: 1.0.0",
    ]
