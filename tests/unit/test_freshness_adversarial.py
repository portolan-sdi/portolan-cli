"""Regressions for metadata repair and fractional-clock freshness checks."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from portolan_cli.metadata.detection import detect_changes, is_stale, judge_staleness
from portolan_cli.metadata.models import FileMetadataState, MetadataCheckResult, MetadataStatus
from portolan_cli.metadata.update import update_versions_tracking
from portolan_cli.run_state import RunState
from portolan_cli.versions import Asset, Version, VersionsFile, read_versions, write_versions

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("contents", [b"new", b"longer"])
@pytest.mark.parametrize("caller", ["update", "fix"])
def test_repair_refreshes_content_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, contents: bytes, caller: str
) -> None:
    """A new mtime must never certify an old hash or size as the current bytes."""
    from portolan_cli import trace
    from portolan_cli.metadata.fix import _fix_single_file

    collection = tmp_path / "scenes"
    path = collection / "scene" / "data.bin"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"old")
    versions_path = collection / "versions.json"
    write_versions(
        versions_path,
        VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2024, 1, 1, tzinfo=UTC),
                    breaking=False,
                    assets={
                        "scene/data.bin": Asset(
                            sha256=hashlib.sha256(b"old").hexdigest(),
                            size_bytes=3,
                            href="scenes/scene/data.bin",
                            mtime=1_700_000_000.0,
                        )
                    },
                    changes=["scene/data.bin"],
                )
            ],
        ),
    )
    path.write_bytes(contents)
    os.utime(path, (1_700_000_010.5, 1_700_000_010.5))
    run = RunState.open(tmp_path)
    run.checksum(path)
    with trace.tracing():
        if caller == "fix":
            # Isolate the tracking side of the fixer from format-specific extraction.
            monkeypatch.setattr(
                "portolan_cli.metadata.fix.update_item_metadata", lambda *args, **kwargs: None
            )
            result = _fix_single_file(
                MetadataCheckResult(path, MetadataStatus.STALE, "changed"), collection, state=run
            )
            assert result.success, result.message
        else:
            update_versions_tracking(path, versions_path)
        hashes = trace.counters()["checksum.files"]

    recorded = read_versions(versions_path).versions[-1].assets["scene/data.bin"]
    assert recorded.sha256 == hashlib.sha256(contents).hexdigest(), "repair retained stale checksum"
    assert recorded.size_bytes == len(contents), "repair retained stale size"
    assert recorded.mtime == recorded.source_mtime == path.stat().st_mtime
    assert run.unchanged(path, recorded)
    assert hashes == (0 if caller == "fix" else 1)


@pytest.mark.parametrize("signal", ["schema", "bbox", "feature_count"])
def test_fractional_edit_is_stale_inside_coarse_window(tmp_path: Path, signal: str) -> None:
    state = FileMetadataState(
        file_path=tmp_path / "data.parquet",
        current_mtime=1_700_000_000.5,
        stored_mtime=1_700_000_000.0,
        current_bbox=[0, 0, 1, 2] if signal == "bbox" else [0, 0, 1, 1],
        stored_bbox=[0, 0, 1, 1],
        current_feature_count=2 if signal == "feature_count" else 1,
        stored_feature_count=1,
        current_schema_fingerprint="new" if signal == "schema" else "old",
        stored_schema_fingerprint="old",
    )
    expected = "schema_changed" if signal == "schema" else "content_changed"
    assert is_stale(state) == (True, expected)
    assert judge_staleness(state, resized=False) == (True, expected)
    assert state.mtime_changed
    assert "mtime" in detect_changes(state)


def test_model_reports_fractional_edit(tmp_path: Path) -> None:
    state = FileMetadataState(
        tmp_path / "data.parquet",
        1_700_000_000.5,
        1_700_000_000.0,
        None,
        None,
        1,
        1,
        "schema",
        "schema",
    )
    assert state.mtime_changed, "fractional mtime received coarse-clock tolerance"


def test_remember_replaces_existing_entry_without_evicting_other_kinds(tmp_path: Path) -> None:
    from portolan_cli.formats import in_run, remember

    path = tmp_path / "data.bin"
    path.write_bytes(b"data")
    run = RunState.open(tmp_path)
    assert run.memo("metadata", path, lambda _: "old") == "old"
    with in_run(run):
        remember("statistics", path, "statistics")
        remember("metadata", path, "new")
    assert run.memo("metadata", path, lambda _: "miss") == "new"
    assert run.memo("statistics", path, lambda _: "miss") == "statistics"


def test_remember_supersedes_in_progress_loader(tmp_path: Path) -> None:
    """A loader that started before a writer remembered cannot restore stale data."""
    from portolan_cli.formats import in_run, remember

    path = tmp_path / "data.bin"
    path.write_bytes(b"data")
    run = RunState.open(tmp_path)

    def loader(file: Path) -> str:
        remember("metadata", file, "new")
        return "old"

    with in_run(run):
        assert run.memo("metadata", path, loader) == "old"
    assert run.memo("metadata", path, lambda _: "miss") == "new"


def test_item_freshness_reuses_run_checksum(tmp_path: Path, valid_points_parquet: Path) -> None:
    """The fallback hash check must share the hash already read by this run."""
    import shutil

    from portolan_cli import trace
    from portolan_cli.metadata.detection import check_file_metadata

    path = tmp_path / "scene/data.parquet"
    path.parent.mkdir()
    shutil.copyfile(valid_points_parquet, path)
    path.with_suffix(".json").write_text(json.dumps({"type": "Feature", "id": "data"}))
    (tmp_path / "versions.json").write_text(
        json.dumps(
            {
                "spec_version": "1.0.0",
                "current_version": "1.0.0",
                "versions": [
                    {
                        "version": "1.0.0",
                        "created": "2024-01-01T00:00:00Z",
                        "breaking": False,
                        "assets": {
                            "scene/data.parquet": {
                                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                "size_bytes": path.stat().st_size,
                                "href": "scene/data.parquet",
                                "mtime": 0,
                            }
                        },
                        "changes": ["scene/data.parquet"],
                    }
                ],
            }
        )
    )
    run = RunState.open(tmp_path)
    run.checksum(path)
    with trace.tracing():
        result = check_file_metadata(path, tmp_path, state=run)
        hashes = trace.counters().get("checksum.files", 0)
    assert result.status == MetadataStatus.FRESH
    assert hashes == 0, "item freshness rehashed a file already cached by the run"
