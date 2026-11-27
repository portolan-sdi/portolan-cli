"""Which data files the ``check --fix`` geo-asset pass rewrites in place."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from rashid_core.api import Finding, Severity

from portolan_cli.validation.workflow import _reoptimize_targets

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def _collection(root: Path, assets: dict[str, dict[str, str]]) -> None:
    collection = root / "c" / "collection.json"
    collection.parent.mkdir(parents=True)
    collection.write_text(json.dumps({"assets": assets}))


def _finding(rule: str, key: str, severity: Severity = Severity.ERROR) -> Finding:
    return Finding(rule, severity, "", "c/collection.json", json_pointer=f"/assets/{key}")


def test_only_the_files_that_auto_convert_findings_name(tmp_path: Path) -> None:
    """A checksum finding, an INFO, and a remote href name no file to rewrite."""
    _collection(
        tmp_path,
        {
            "a/b": {"href": "./a.tif"},
            "remote": {"href": "https://example.com/r.tif"},
            "info": {"href": "i.parquet"},
            "sum": {"href": "s.tif"},
        },
    )

    targets = _reoptimize_targets(
        tmp_path,
        [
            _finding("PTL-DAT-011", "a~1b"),
            _finding("PTL-DAT-013", "remote"),
            _finding("PTL-DAT-006", "info", Severity.INFO),
            _finding("PTL-DAT-001", "sum"),
        ],
    )

    assert targets == (frozenset({(tmp_path / "c" / "a.tif").resolve()}), frozenset())


def test_a_named_mirror_goes_to_the_mirror_writer(tmp_path: Path) -> None:
    _collection(tmp_path, {"mirror": {"href": "./items.parquet"}})

    targets = _reoptimize_targets(tmp_path, [_finding("PTL-DAT-007", "mirror")])

    assert targets == (frozenset(), frozenset({(tmp_path / "c" / "items.parquet").resolve()}))


@pytest.mark.parametrize(
    ("config", "rule", "needs"),
    [
        (
            "vector:\n    sort: none",
            "PTL-DAT-006",
            "conversion.vector.sort set to hilbert or quadkey",
        ),
        ("vector:\n    add_bbox: false", "PTL-DAT-007", "conversion.vector.add_bbox set to true"),
        ("cog:\n    tile_size: 1024", "PTL-DAT-013", "conversion.cog.tile_size of 512 or less"),
    ],
)
def test_a_file_the_settings_cannot_repair_is_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: str, rule: str, needs: str
) -> None:
    from portolan_cli import report

    (tmp_path / ".portolan").mkdir()
    (tmp_path / ".portolan" / "config.yaml").write_text(f"conversion:\n  {config}\n")
    _collection(tmp_path, {"data": {"href": "d.parquet"}})
    said: list[tuple[str, str]] = []
    monkeypatch.setattr(report, "emit", lambda level, text: said.append((level, text)))

    kept = _reoptimize_targets(tmp_path, [_finding(rule, "data")])
    rewritten = _reoptimize_targets(
        tmp_path, [_finding(rule, "data"), _finding("PTL-DAT-008", "data")]
    )

    assert kept == (frozenset(), frozenset())
    assert said[0] == ("warn", f"Kept d.parquet: {rule} needs {needs} in .portolan/config.yaml")
    assert rewritten == (frozenset({(tmp_path / "c" / "d.parquet").resolve()}), frozenset())
