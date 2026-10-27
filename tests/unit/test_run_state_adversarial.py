"""Regressions for reentrant reads and failures during a run commit."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.run_state import RunState
from portolan_cli.version_ops import commit_run
from portolan_cli.versions import Asset, read_versions

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.versions import VersionsFile

pytestmark = pytest.mark.unit


def _stage(state: RunState, collection: str) -> Asset:
    path = state.collection_dir(collection) / "a.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"old")
    os.utime(path, (1_700_000_000, 1_700_000_000))
    asset = Asset(
        sha256=hashlib.sha256(b"old").hexdigest(),
        size_bytes=3,
        href=f"{collection}/a.bin",
        mtime=path.stat().st_mtime,
    )
    state.stage(collection, assets={"a.bin": asset}, message="add")
    return asset


def test_partial_commit_updates_index(catalog: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A later collection failure must not hide an already committed collection."""
    from portolan_cli import versions

    state = RunState.open(catalog)
    for collection in ("a", "b"):
        _stage(state, collection)
    write = versions.write_versions

    def fail_second(path: Path, record: VersionsFile) -> None:
        if path.parent.name == "b":
            raise OSError("injected commit failure")
        write(path, record)

    monkeypatch.setattr(versions, "write_versions", fail_second)
    with pytest.raises(OSError, match="injected commit failure"):
        commit_run(state)

    assert read_versions(catalog / "a/versions.json").current_version == "1.0.0"
    index = json.loads((catalog / "versions.json").read_text())
    assert index["collections"]["a"]["current_version"] == "1.0.0"


def test_failed_commit_keeps_unpublished_stages(
    catalog: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retry commits the unfinished collections without publishing successes twice."""
    from portolan_cli import versions

    state = RunState.open(catalog)
    for collection in ("a", "b", "c"):
        _stage(state, collection)
    write = versions.write_versions

    def fail_second(path: Path, record: VersionsFile) -> None:
        if path.parent.name == "b":
            raise OSError("injected commit failure")
        write(path, record)

    monkeypatch.setattr(versions, "write_versions", fail_second)
    with pytest.raises(OSError, match="injected commit failure"):
        commit_run(state)
    assert set(state.staged()) == {"b", "c"}

    monkeypatch.setattr(versions, "write_versions", write)
    assert set(commit_run(state)) == {"b", "c"}
    for collection in ("a", "b", "c"):
        assert len(read_versions(catalog / collection / "versions.json").versions) == 1


def _four_workers(call: Callable[[], Any]) -> list[Any]:
    """Bound deadlock repros so a regression cannot hang pytest itself."""
    start = threading.Barrier(4)
    results: list[Any] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            start.wait(timeout=5)
            results.append(call())
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1)
    assert not any(thread.is_alive() for thread in threads), "RunState loader deadlocked"
    assert errors == []
    return results


def test_memo_loader_can_reenter_same_key(tmp_path: Path) -> None:
    path = tmp_path / "a.bin"
    path.write_text("value")
    state = RunState.open(tmp_path)
    loads: list[Path] = []

    def inner(file: Path) -> str:
        loads.append(file)
        return file.read_text()

    def outer(file: Path) -> str:
        return state.memo("value", file, inner)

    assert _four_workers(lambda: state.memo("value", path, outer)) == ["value"] * 4
    assert loads == [path]


def test_invalidation_during_load_is_not_lost(tmp_path: Path) -> None:
    """A read started before a writer invalidates cannot repopulate its old entry."""
    path = tmp_path / "a.bin"
    path.write_bytes(b"old")
    state = RunState.open(tmp_path)
    calls: list[Path] = []

    def loader(file: Path) -> int:
        calls.append(file)
        if len(calls) == 1:
            state.invalidate(file)
        return len(calls)

    assert state.memo("value", path, loader) == 1
    assert state.memo("value", path, loader) == 2


def test_config_loader_can_read_metadata(catalog: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from portolan_cli import config

    state = RunState.open(catalog)
    metadata = catalog / ".portolan/metadata.yaml"
    metadata.write_text("license: MIT\n")
    parse = config.parse_yaml

    def parse_with_metadata(path: Path) -> Any:
        if path.name == "config.yaml":
            assert state.metadata(catalog)["license"] == "MIT"
        return parse(path)

    monkeypatch.setattr(config, "parse_yaml", parse_with_metadata)
    assert _four_workers(lambda: state.config(catalog)) == [state.config(catalog)] * 4


def test_config_merge_does_not_hold_lock_while_waiting_for_loader(
    catalog: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A YAML flight owned by a different worker can itself read metadata."""
    from portolan_cli import config

    state = RunState.open(catalog)
    config_path = catalog / ".portolan/config.yaml"
    (catalog / ".portolan/metadata.yaml").write_text("license: MIT\n")
    started = threading.Event()
    merging = threading.Event()
    choose = threading.Lock()
    assigned: list[bool] = []
    yaml_read = state._yaml

    def signal_merge(path: Path) -> Any:
        if path.name == "config.yaml":
            merging.set()
        return yaml_read(path)

    monkeypatch.setattr(state, "_yaml", signal_merge)

    def loader(path: Path) -> Any:
        started.set()
        assert merging.wait(timeout=5)
        assert state.metadata(catalog)["license"] == "MIT"
        return config.parse_yaml(path)

    def read() -> Any:
        with choose:
            direct = not assigned
            assigned.append(True)
        if direct:
            return state.memo("config.yaml", config_path, loader)
        assert started.wait(timeout=5)
        return state.config(catalog)

    assert _four_workers(read) == [config.parse_yaml(config_path)] * 4


@pytest.mark.parametrize("reader", ["unchanged", "status", "pull"])
def test_coarse_record_does_not_hide_fractional_edit(catalog: Path, reader: str) -> None:
    """A whole-second record does not prove the next write uses a coarse clock."""
    from portolan_cli.status import detect_modified_files
    from portolan_cli.sync.pull import detect_uncommitted_changes

    state = RunState.open(catalog)
    asset = _stage(state, "c")
    commit_run(state)
    path = catalog / "c/a.bin"
    path.write_bytes(b"new")
    os.utime(path, (1_700_000_001.5, 1_700_000_001.5))

    if reader == "unchanged":
        assert state.unchanged(path, asset) is False
    elif reader == "status":
        assert detect_modified_files(path.parent, read_versions(path.parent / "versions.json")) == [
            "a.bin"
        ]
    else:
        assert detect_uncommitted_changes(catalog, "c") == ["a.bin"]


def test_is_current_sees_first_staged_version(catalog: Path) -> None:
    from portolan_cli.query import is_current

    state = RunState.open(catalog)
    _stage(state, "c")
    collection = catalog / "c"
    assert not (collection / "versions.json").exists()
    assert is_current(collection / "a.bin", collection / "versions.json", state=state) is True


def test_backend_does_not_reuse_checksum_for_another_path(catalog: Path) -> None:
    from portolan_cli.backends.json_file import JsonFileBackend

    state = RunState.open(catalog)
    _stage(state, "c")
    commit_run(state)
    replacement = catalog / "replacement.bin"
    replacement.write_bytes(b"new")
    os.utime(replacement, (1_700_000_000, 1_700_000_000))

    version = JsonFileBackend(catalog).publish(
        "c",
        assets={"a.bin": str(replacement)},
        schema={"columns": [], "types": {}, "hash": "unknown"},
        breaking=False,
        message="replacement",
    )

    assert version.assets["a.bin"].sha256 == hashlib.sha256(b"new").hexdigest()


def test_plugin_partial_failure_retries_only_unpublished(
    catalog: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import UTC, datetime

    from portolan_cli import version_ops
    from portolan_cli.versions import Version

    (catalog / ".portolan/config.yaml").write_text("backend: fake\n")
    state = RunState.open(catalog)
    for collection in ("a", "b", "c"):
        _stage(state, collection)
    attempts: list[str] = []

    def publish(collection: str, **_kwargs: Any) -> Version:
        attempts.append(collection)
        if attempts == ["a", "b"]:
            raise OSError("injected plugin failure")
        return Version(
            version="1.0.0", created=datetime.now(UTC), breaking=False, assets={}, changes=[]
        )

    monkeypatch.setattr(version_ops, "publish_version", publish)
    with pytest.raises(OSError, match="injected plugin failure"):
        commit_run(state)
    assert set(state.staged()) == {"b", "c"}
    assert set(commit_run(state)) == {"b", "c"}
    assert attempts == ["a", "b", "b", "c"]


def test_failed_commit_preserves_newer_stage_calls(
    catalog: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from portolan_cli import versions

    state = RunState.open(catalog)
    original = _stage(state, "c")
    state.stage("c", assets={"remove.bin": original}, removed={"revive.bin"})
    newer = replace(original, sha256="f" * 64)

    def fail_with_newer_stage(_path: Path, _record: VersionsFile) -> None:
        state.stage(
            "c",
            assets={"a.bin": newer, "revive.bin": newer},
            removed={"remove.bin"},
            message="newer",
        )
        raise OSError("injected commit failure")

    monkeypatch.setattr(versions, "write_versions", fail_with_newer_stage)
    with pytest.raises(OSError, match="injected commit failure"):
        commit_run(state)

    assert state.tracked_assets(catalog / "c") == {"a.bin": newer, "revive.bin": newer}
    assert state.staged()["c"].message == "add\nnewer"


def test_projections_do_not_count_as_publications(
    catalog: Path, counters: Callable[[str], int]
) -> None:
    state = RunState.open(catalog)
    asset = _stage(state, "c")
    for _ in range(4):
        assert state.versions(catalog / "c").current_version == "1.0.0"
        assert state.tracked_assets(catalog / "c") == {"a.bin": asset}
    assert counters("versions.published") == 0
    commit_run(state)
    assert counters("versions.published") == 1
    assert commit_run(state) == {}
    assert counters("versions.published") == 1

    state.stage("c", assets={"copy.bin": asset}, amend=True)
    commit_run(state)
    assert counters("versions.published") == 1
    assert len(read_versions(catalog / "c/versions.json").versions) == 1
