"""A catalog-wide pull with no report callback writes nothing to the terminal.

Collection discovery lives in ``sync.push.discover_collections``. On a
``PermissionError`` it still prints a warning through ``portolan_cli.output``,
so that line bypasses the ``report`` callback of ``pull_catalog``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from portolan_cli.sync.pull import PullAllResult, pull_catalog
from portolan_cli.sync.report import silent

pytestmark = pytest.mark.unit


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    """Build the smallest catalog root that discovery accepts."""
    (tmp_path / ".portolan").mkdir()
    (tmp_path / ".portolan" / "config.yaml").write_text("{}\n", encoding="utf-8")
    catalog_json = {"type": "Catalog", "id": "c", "stac_version": "1.1.0", "links": []}
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_json), encoding="utf-8")
    return tmp_path


def _deny_versions_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the recursive search for versions.json raise PermissionError."""
    real_rglob = Path.rglob

    def rglob(self: Path, pattern: str, *args: Any, **kwargs: Any) -> Any:
        if pattern == "versions.json":
            raise PermissionError("denied")
        return real_rglob(self, pattern, *args, **kwargs)

    monkeypatch.setattr(Path, "rglob", rglob)


@pytest.mark.xfail(
    strict=True,
    reason="#1133: sync.push.discover_collections still prints until the push branch merges",
)
def test_a_denied_discovery_writes_nothing(
    catalog: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """With ``report=silent``, a discovery PermissionError writes nothing."""
    _deny_versions_scan(monkeypatch)

    result = pull_catalog("s3://bucket/catalog", catalog, report=silent)

    assert isinstance(result, PullAllResult)
    assert result.total_collections == 0
    assert capsys.readouterr() == ("", "")
