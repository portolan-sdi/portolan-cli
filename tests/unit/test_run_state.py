"""Unit tests for RunState, the run-scoped memo and version stage (issue #995)."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from portolan_cli import trace
from portolan_cli.constants import MTIME_TOLERANCE_SECONDS
from portolan_cli.run_state import RunState, mtime_matches, stat_matches
from portolan_cli.versions import Asset, Version, VersionsFile, read_versions, write_versions

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

pytestmark = pytest.mark.unit

_SEC = 1_000_000_000


@pytest.fixture
def counters() -> Iterator[Callable[[str], int]]:
    """Turn the trace counters on and return a reader for one counter."""
    trace.enable()
    try:
        yield lambda name: trace.counters().get(name, 0)
    finally:
        trace.reset()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _asset(path: Path, catalog_root: Path, data: bytes) -> Asset:
    st = path.stat()
    return Asset(
        sha256=_sha(data),
        size_bytes=st.st_size,
        href=path.relative_to(catalog_root).as_posix(),
        mtime=st.st_mtime,
    )


def _catalog(root: Path) -> Path:
    """A minimal file-backend catalog with an empty catalog-level index."""
    (root / ".portolan").mkdir(parents=True)
    (root / ".portolan" / "config.yaml").write_text("{}\n")
    (root / "versions.json").write_text(json.dumps({"collections": {}}))
    return root


def _write_versions(collection_dir: Path, assets: dict[str, Asset]) -> None:
    collection_dir.mkdir(parents=True, exist_ok=True)
    write_versions(
        collection_dir / "versions.json",
        VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    breaking=False,
                    assets=assets,
                    changes=list(assets),
                )
            ],
        ),
    )


def _stat_with_mtime(path: Path, mtime: float) -> os.stat_result:
    os.utime(path, (mtime, mtime))
    return path.stat()


class TestStatMatches:
    """One mtime predicate: 1 us with a fraction, 2.0 s on whole seconds."""

    def test_fractional_record_allows_less_than_one_microsecond(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"abc")
        recorded = 1_700_000_000.25
        st = _stat_with_mtime(f, recorded)
        assert stat_matches(st, recorded, 3) is True
        assert mtime_matches(recorded + 5e-7, recorded) is True
        assert mtime_matches(recorded + 2e-6, recorded) is False

    def test_fractional_record_rejects_one_millisecond(self, tmp_path: Path) -> None:
        """The old pull fast path accepted 1 ms. A fine file system never needs it."""
        f = tmp_path / "a.bin"
        f.write_bytes(b"abc")
        st = _stat_with_mtime(f, 1_700_000_000.251)
        assert stat_matches(st, 1_700_000_000.25, 3) is False

    def test_whole_second_record_uses_coarse_window(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"abc")
        recorded = 1_700_000_000.0
        just_inside = recorded + MTIME_TOLERANCE_SECONDS - 0.001
        assert stat_matches(_stat_with_mtime(f, just_inside), recorded, 3) is True

    def test_whole_second_window_is_exactly_two_seconds(self) -> None:
        recorded = 1_700_000_000.0
        assert MTIME_TOLERANCE_SECONDS == 2.0
        assert mtime_matches(recorded + 2.0, recorded) is False
        assert mtime_matches(recorded + 2.0 + 1e-6, recorded) is False
        assert mtime_matches(recorded - 2.0 - 1e-6, recorded) is False
        assert mtime_matches(recorded + 2.0 - 1e-6, recorded) is True

    def test_size_must_match(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"abc")
        st = _stat_with_mtime(f, 1_700_000_000.25)
        assert stat_matches(st, 1_700_000_000.25, 4) is False

    def test_missing_record_never_matches(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"abc")
        st = f.stat()
        assert stat_matches(st, None, 3) is False
        assert stat_matches(st, st.st_mtime, None) is False


class TestChecksumMemo:
    """A file is hashed once per run while its stat stays the same."""

    def test_second_call_does_not_hash(
        self, tmp_path: Path, counters: Callable[[str], int]
    ) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"hello")
        state = RunState.open(tmp_path)
        assert state.checksum(f) == _sha(b"hello")
        assert state.checksum(f) == _sha(b"hello")
        assert counters("checksum.files") == 1

    def test_rewrite_one_nanosecond_later_forces_rehash(
        self, tmp_path: Path, counters: Callable[[str], int]
    ) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"hello")
        os.utime(f, ns=(1_700_000_000 * _SEC, 1_700_000_000 * _SEC))
        state = RunState.open(tmp_path)
        assert state.checksum(f) == _sha(b"hello")

        # Same size, same inode, mtime 1 ns later.
        with f.open("r+b") as fh:
            fh.write(b"HELLO")
        os.utime(f, ns=(1_700_000_000 * _SEC + 1, 1_700_000_000 * _SEC + 1))

        assert state.checksum(f) == _sha(b"HELLO")
        assert counters("checksum.files") == 2

    def test_directory_asset_is_fingerprinted_each_call(self, tmp_path: Path) -> None:
        gdb = tmp_path / "data.gdb"
        gdb.mkdir()
        (gdb / "a0000001.gdbtable").write_bytes(b"x")
        state = RunState.open(tmp_path)
        first = state.checksum(gdb)
        (gdb / "a0000002.gdbtable").write_bytes(b"y")
        assert state.checksum(gdb) != first


class TestUnchanged:
    """The stat check runs before any hash."""

    def test_size_change_needs_no_hash(
        self, tmp_path: Path, counters: Callable[[str], int]
    ) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"hello")
        asset = _asset(f, tmp_path, b"hello")
        f.write_bytes(b"hello world")
        assert RunState.open(tmp_path).unchanged(f, asset) is False
        assert counters("checksum.files") == 0

    def test_stat_match_needs_no_hash(self, tmp_path: Path, counters: Callable[[str], int]) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"hello")
        asset = _asset(f, tmp_path, b"hello")
        assert RunState.open(tmp_path).unchanged(f, asset) is True
        assert counters("checksum.files") == 0

    def test_touched_file_with_same_bytes_is_unchanged(
        self, tmp_path: Path, counters: Callable[[str], int]
    ) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"hello")
        asset = _asset(f, tmp_path, b"hello")
        os.utime(f, ((asset.mtime or 0) + 10, (asset.mtime or 0) + 10))
        assert RunState.open(tmp_path).unchanged(f, asset) is True
        assert counters("checksum.files") == 1

    def test_same_size_new_bytes_is_changed(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"hello")
        asset = _asset(f, tmp_path, b"hello")
        f.write_bytes(b"HELLO")
        os.utime(f, ((asset.mtime or 0) + 10, (asset.mtime or 0) + 10))
        assert RunState.open(tmp_path).unchanged(f, asset) is False

    def test_missing_file_is_changed(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"hello")
        asset = _asset(f, tmp_path, b"hello")
        f.unlink()
        assert RunState.open(tmp_path).unchanged(f, asset) is False


class TestVersions:
    """versions.json is read once per run and re-read only when it changes."""

    def test_read_once(self, tmp_path: Path, counters: Callable[[str], int]) -> None:
        coll = tmp_path / "c"
        f = tmp_path / "x.bin"
        f.write_bytes(b"x")
        _write_versions(coll, {"x.bin": _asset(f, tmp_path, b"x")})
        state = RunState.open(tmp_path)
        for _ in range(5):
            assert state.versions(coll).current_version == "1.0.0"
        assert counters("versions_json.reads") == 1

    def test_atomic_rewrite_is_read_again(
        self, tmp_path: Path, counters: Callable[[str], int]
    ) -> None:
        coll = tmp_path / "c"
        _write_versions(coll, {})
        state = RunState.open(tmp_path)
        state.versions(coll)
        f = tmp_path / "x.bin"
        f.write_bytes(b"x")
        _write_versions(coll, {"x.bin": _asset(f, tmp_path, b"x")})
        assert list(state.versions(coll).versions[-1].assets) == ["x.bin"]
        assert counters("versions_json.reads") == 2

    def test_missing_file_gives_empty_record(self, tmp_path: Path) -> None:
        vf = RunState.open(tmp_path).versions(tmp_path / "none")
        assert (vf.current_version, vf.versions) == (None, [])

    def test_caller_edit_does_not_change_the_memo(self, tmp_path: Path) -> None:
        coll = tmp_path / "c"
        _write_versions(coll, {})
        state = RunState.open(tmp_path)
        state.versions(coll).versions.clear()
        assert len(state.versions(coll).versions) == 1

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        coll = tmp_path / "c"
        coll.mkdir()
        (coll / "versions.json").write_text("{")
        with pytest.raises(ValueError, match="Invalid JSON"):
            RunState.open(tmp_path).versions(coll)


class TestStage:
    """Staged assets show in versions() and merge into one version."""

    def test_staged_asset_is_visible_before_commit(self, tmp_path: Path) -> None:
        coll = tmp_path / "c"
        _write_versions(coll, {})
        f = coll / "a.bin"
        f.write_bytes(b"a")
        state = RunState.open(tmp_path)
        state.stage("c", assets={"a.bin": _asset(f, tmp_path, b"a")})

        projected = state.versions(coll)
        assert projected.current_version == "1.0.1"
        assert list(projected.versions[-1].assets) == ["a.bin"]
        assert state.versions(coll, include_staged=False).current_version == "1.0.0"
        assert read_versions(coll / "versions.json").current_version == "1.0.0"

    def test_calls_merge(self, tmp_path: Path) -> None:
        coll = tmp_path / "c"
        coll.mkdir()
        a, b = coll / "a.bin", coll / "b.bin"
        a.write_bytes(b"a")
        b.write_bytes(b"b")
        state = RunState.open(tmp_path)
        state.stage("c", assets={"a.bin": _asset(a, tmp_path, b"a")}, message="one")
        state.stage("c", removed={"a.bin"}, message="two")
        state.stage("c", assets={"b.bin": _asset(b, tmp_path, b"b")}, message="one")
        state.stage("c", removed={"gone"})
        state.stage("c", assets={"gone": _asset(b, tmp_path, b"b")}, breaking=True)

        staged = state.staged()["c"]
        assert sorted(staged.assets) == ["b.bin", "gone"]
        assert staged.removed == {"a.bin"}
        assert staged.message == "one\ntwo"
        assert staged.breaking is True

    def test_projection_follows_new_stage_calls(self, tmp_path: Path) -> None:
        coll = tmp_path / "c"
        coll.mkdir()
        a, b = coll / "a.bin", coll / "b.bin"
        a.write_bytes(b"a")
        b.write_bytes(b"b")
        state = RunState.open(tmp_path)
        state.stage("c", assets={"a.bin": _asset(a, tmp_path, b"a")})
        assert list(state.versions(coll).versions[-1].assets) == ["a.bin"]
        state.stage("c", assets={"b.bin": _asset(b, tmp_path, b"b")})
        assert sorted(state.versions(coll).versions[-1].assets) == ["a.bin", "b.bin"]

    def test_concurrent_stage_keeps_every_asset(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"a")
        asset = _asset(f, tmp_path, b"a")
        state = RunState.open(tmp_path)

        def worker(n: int) -> None:
            for i in range(200):
                state.stage("c", assets={f"{n}-{i}": asset})

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(state.staged()["c"].assets) == 8 * 200

    def test_concurrent_checksums_are_correct(self, tmp_path: Path) -> None:
        files = []
        for i in range(64):
            f = tmp_path / f"{i}.bin"
            f.write_bytes(str(i).encode() * 100)
            files.append(f)
        state = RunState.open(tmp_path)
        results: dict[str, str] = {}
        lock = threading.Lock()

        def worker() -> None:
            for f in files:
                digest = state.checksum(f)
                with lock:
                    results[f.name] = digest

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert results == {f.name: _sha(f.read_bytes()) for f in files}


class TestMemo:
    """The generic memo is keyed on kind plus the file's stat."""

    def test_loader_runs_once_until_the_file_changes(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"a")
        os.utime(f, ns=(1_700_000_000 * _SEC, 1_700_000_000 * _SEC))
        calls: list[Path] = []

        def loader(p: Path) -> int:
            calls.append(p)
            return len(calls)

        state = RunState.open(tmp_path)
        assert state.memo("k", f, loader) == 1
        assert state.memo("k", f, loader) == 1
        assert state.memo("other", f, loader) == 2
        os.utime(f, ns=(1_700_000_000 * _SEC + 1, 1_700_000_000 * _SEC + 1))
        assert state.memo("k", f, loader) == 3

    def test_invalidate_drops_the_entry(self, tmp_path: Path) -> None:
        f = tmp_path / "a.bin"
        f.write_bytes(b"a")
        calls: list[int] = []

        def loader(_p: Path) -> int:
            calls.append(1)
            return len(calls)

        state = RunState.open(tmp_path)
        state.memo("k", f, loader)
        state.invalidate(f)
        assert state.memo("k", f, loader) == 2

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            RunState.open(tmp_path).memo("k", tmp_path / "none", lambda p: p)


class TestConfigSeam:
    """config(), metadata(), and setting() keep the config.py precedence."""

    def test_setting_reads_the_hierarchy(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        (tmp_path / ".portolan" / "config.yaml").write_text("backend: root\n")
        coll = tmp_path / "c"
        (coll / ".portolan").mkdir(parents=True)
        (coll / ".portolan" / "config.yaml").write_text("backend: coll\n")
        state = RunState.open(tmp_path)
        assert state.setting("backend") == "root"
        assert state.setting("backend", collection_dir=coll) == "coll"
        assert state.setting("backend", collection_dir=coll, cli_value="cli") == "cli"
        assert state.config(coll)["backend"] == "coll"

    def test_metadata_drops_inherited_title(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        (tmp_path / ".portolan" / "metadata.yaml").write_text("title: Root\nlicense: MIT\n")
        coll = tmp_path / "c"
        coll.mkdir()
        assert RunState.open(tmp_path).metadata(coll) == {"license": "MIT"}
