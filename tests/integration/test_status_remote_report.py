"""`portolan status` prints the download lines of the remote versions.json.

Commit f7f8214 printed them through `portolan_cli.output`. The store is an
in-memory obstore ``MemoryStore``, so no test sends a network request.
"""

from __future__ import annotations

import json
import re
import shutil
from typing import TYPE_CHECKING

import pytest
from obstore.store import MemoryStore

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli

pytestmark = pytest.mark.integration

DOWNLOAD = re.compile(
    r"→ Downloading tmp\w+\.json \(0\.00 MB\) <- catalog/points/versions\.json\n"
    r"✓ Download complete \(\d+\.\d\d MB/s\)\n"
)


@pytest.fixture
def remote_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """A catalog with one collection, and a remote that holds its versions.json."""
    root = tmp_path / "catalog"
    build_conformant_catalog(root)
    (root / "points").mkdir()
    shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
    run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))
    memory = MemoryStore()
    memory.put("catalog/points/versions.json", (root / "points" / "versions.json").read_bytes())
    monkeypatch.setenv("PORTOLAN_REMOTE", "s3://bucket/catalog")
    monkeypatch.setattr(
        "portolan_cli.sync.download._setup_store_and_kwargs", lambda *_, **__: (memory, {})
    )
    return root


@pytest.mark.parametrize("flags", [[], ["--quiet"], ["-v"]])
def test_status_prints_the_download_lines(
    run_cli: RunCli, remote_catalog: Path, flags: list[str]
) -> None:
    """Text mode prints the download lines on stdout, before the report."""
    result = run_cli(*flags, "status", cwd=remote_catalog)
    assert DOWNLOAD.match(result.stdout), result.stdout
    assert "Remote version: 1.0.0" in result.stdout


def test_status_json_sends_the_download_lines_to_stderr(
    run_cli: RunCli, remote_catalog: Path
) -> None:
    """JSON mode keeps stdout for the envelope and prints the lines on stderr."""
    result = run_cli("status", "--json", cwd=remote_catalog)
    assert json.loads(result.stdout)["data"]["collections"][0]["remote_version"] == "1.0.0"
    assert DOWNLOAD.search(result.stderr), result.stderr
