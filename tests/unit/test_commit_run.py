"""Unit tests for version_ops.commit_run and the batched catalog index (issue #995)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli import trace
from portolan_cli.catalog import CollectionSummary, update_catalog_index
from portolan_cli.run_state import RunState
from portolan_cli.version_ops import commit_run, owned_run
from portolan_cli.versions import Asset, read_versions

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

pytestmark = pytest.mark.unit


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    (tmp_path / ".portolan").mkdir()
    (tmp_path / ".portolan" / "config.yaml").write_text("{}\n")
    (tmp_path / "versions.json").write_text(json.dumps({"collections": {}}))
    return tmp_path


@pytest.fixture
def counters() -> Iterator[Callable[[str], int]]:
    """Turn the trace counters on and return a reader for one counter."""
    trace.enable()
    try:
        yield lambda name: trace.counters().get(name, 0)
    finally:
        trace.reset()


def _asset(catalog: Path, rel: str, data: bytes) -> Asset:
    path = catalog / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    st = path.stat()
    return Asset(sha256=f"{len(data):064x}", size_bytes=st.st_size, href=rel, mtime=st.st_mtime)


def _index(catalog: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((catalog / "versions.json").read_text())
    return data


class TestCommitRunFileBackend:
    def test_many_stage_calls_write_one_version(self, catalog: Path) -> None:
        state = RunState.open(catalog)
        for i in range(5):
            state.stage("c", assets={f"{i}.bin": _asset(catalog, f"c/{i}.bin", b"x" * i)})

        published = commit_run(state)

        vf = read_versions(catalog / "c" / "versions.json")
        assert [v.version for v in vf.versions] == ["1.0.0"]
        assert sorted(vf.versions[0].assets) == [f"{i}.bin" for i in range(5)]
        assert published["c"].version == "1.0.0"
        assert _index(catalog)["collections"]["c"]["asset_count"] == 5

    def test_one_write_per_collection_and_one_index_write(
        self, catalog: Path, counters: Callable[[str], int], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import portolan_cli.catalog as catalog_mod
        import portolan_cli.versions as versions_mod
        from portolan_cli.json_io import write_json_atomic

        writes: list[str] = []

        def record(path: Path, data: Any) -> None:
            writes.append(path.relative_to(catalog).as_posix())
            write_json_atomic(path, data)

        monkeypatch.setattr(versions_mod, "write_json_atomic", record)
        monkeypatch.setattr(catalog_mod, "write_json_atomic", record)

        state = RunState.open(catalog)
        for coll in ("a", "b", "nested/c"):
            for i in range(3):
                rel = f"{coll}/{i}.bin"
                state.stage(coll, assets={f"{i}.bin": _asset(catalog, rel, b"y" * (i + 1))})
        commit_run(state)

        assert sorted(writes) == [
            "a/versions.json",
            "b/versions.json",
            "nested/c/versions.json",
            "versions.json",
        ]
        assert set(_index(catalog)["collections"]) == {"a", "b", "nested/c"}
        # One read of the catalog index, inside the lock. Collections with no
        # versions.json are not read.
        assert counters("versions_json.reads") == 1

    def test_second_commit_does_nothing(self, catalog: Path) -> None:
        state = RunState.open(catalog)
        state.stage("c", assets={"a.bin": _asset(catalog, "c/a.bin", b"a")})
        commit_run(state)
        assert commit_run(state) == {}

    def test_restaging_unchanged_assets_writes_nothing(self, catalog: Path) -> None:
        asset = _asset(catalog, "c/a.bin", b"a")
        first = RunState.open(catalog)
        first.stage("c", assets={"a.bin": asset})
        commit_run(first)
        before = (catalog / "c" / "versions.json").stat().st_mtime_ns
        index_before = (catalog / "versions.json").read_bytes()

        again = RunState.open(catalog)
        again.stage("c", assets={"a.bin": asset})
        assert commit_run(again) == {}
        assert (catalog / "c" / "versions.json").stat().st_mtime_ns == before
        assert (catalog / "versions.json").read_bytes() == index_before

    def test_removed_keys_leave_the_snapshot(self, catalog: Path) -> None:
        first = RunState.open(catalog)
        first.stage(
            "c",
            assets={
                "a.bin": _asset(catalog, "c/a.bin", b"a"),
                "b.bin": _asset(catalog, "c/b.bin", b"bb"),
            },
        )
        commit_run(first)
        second = RunState.open(catalog)
        second.stage("c", removed={"a.bin"}, message="Removed a.bin")
        commit_run(second)

        latest = read_versions(catalog / "c" / "versions.json").versions[-1]
        assert (latest.version, sorted(latest.assets), latest.message) == (
            "1.0.1",
            ["b.bin"],
            "Removed a.bin",
        )


class TestCommitRunAmend:
    """A side step of the command that wrote the latest version folds into it."""

    def _first(self, catalog: Path) -> None:
        state = RunState.open(catalog)
        state.stage("c", assets={"a.bin": _asset(catalog, "c/a.bin", b"a")}, message="add")
        commit_run(state)

    def test_amend_folds_into_latest(self, catalog: Path) -> None:
        self._first(catalog)
        state = RunState.open(catalog)
        state.stage("c", assets={"m.bin": _asset(catalog, "c/m.bin", b"mm")}, amend=True)
        commit_run(state)

        vf = read_versions(catalog / "c" / "versions.json")
        assert [v.version for v in vf.versions] == ["1.0.0"]
        assert (sorted(vf.versions[0].assets), vf.versions[0].changes) == (
            ["a.bin", "m.bin"],
            ["a.bin", "m.bin"],
        )
        assert vf.versions[0].message == "add"
        assert _index(catalog)["collections"]["c"]["asset_count"] == 2

    def test_amend_with_no_version_makes_one(self, catalog: Path) -> None:
        state = RunState.open(catalog)
        state.stage("c", assets={"m.bin": _asset(catalog, "c/m.bin", b"mm")}, amend=True)
        commit_run(state)
        assert read_versions(catalog / "c" / "versions.json").current_version == "1.0.0"

    def test_one_plain_stage_makes_a_new_version(self, catalog: Path) -> None:
        self._first(catalog)
        state = RunState.open(catalog)
        state.stage("c", assets={"m.bin": _asset(catalog, "c/m.bin", b"mm")}, amend=True)
        state.stage("c", assets={"n.bin": _asset(catalog, "c/n.bin", b"nnn")})
        commit_run(state)
        vf = read_versions(catalog / "c" / "versions.json")
        assert [v.version for v in vf.versions] == ["1.0.0", "1.0.1"]

    def test_amend_with_same_record_writes_nothing(self, catalog: Path) -> None:
        self._first(catalog)
        recorded = read_versions(catalog / "c" / "versions.json").versions[0].assets["a.bin"]
        before = (catalog / "c" / "versions.json").read_bytes()
        state = RunState.open(catalog)
        state.stage("c", assets={"a.bin": recorded}, amend=True)
        assert commit_run(state) == {}
        assert (catalog / "c" / "versions.json").read_bytes() == before


class TestCommitRunPluginBackend:
    def test_one_publish_per_collection(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import portolan_cli.version_ops as version_ops

        (catalog / ".portolan" / "config.yaml").write_text("backend: fake\n")
        calls: list[tuple[str, dict[str, str], set[str] | None, str]] = []

        def fake_publish(collection: str, **kwargs: Any) -> Any:
            calls.append((collection, kwargs["assets"], kwargs["removed"], kwargs["message"]))
            assert kwargs["backend_name"] == "fake"
            return object()

        monkeypatch.setattr(version_ops, "publish_version", fake_publish)
        state = RunState.open(catalog)
        state.stage("a", assets={"1.bin": _asset(catalog, "a/1.bin", b"1")}, message="m")
        state.stage("a", assets={"2.bin": _asset(catalog, "a/2.bin", b"22")})
        state.stage("b", removed={"old.bin"})
        commit_run(state)

        assert calls == [
            (
                "a",
                {"1.bin": str(catalog / "a/1.bin"), "2.bin": str(catalog / "a/2.bin")},
                None,
                "m",
            ),
            ("b", {}, {"old.bin"}, ""),
        ]
        assert not (catalog / "a" / "versions.json").exists()


class TestOwnedRun:
    def test_new_state_commits_on_success(self, catalog: Path) -> None:
        with owned_run(catalog) as state:
            state.stage("c", assets={"a.bin": _asset(catalog, "c/a.bin", b"a")})
        assert (catalog / "c" / "versions.json").exists()

    def test_exception_commits_nothing(self, catalog: Path) -> None:
        with pytest.raises(RuntimeError), owned_run(catalog) as state:
            state.stage("c", assets={"a.bin": _asset(catalog, "c/a.bin", b"a")})
            raise RuntimeError("boom")
        assert not (catalog / "c" / "versions.json").exists()

    def test_caller_state_is_not_committed(self, catalog: Path) -> None:
        mine = RunState.open(catalog)
        with owned_run(catalog, mine) as state:
            assert state is mine
            state.stage("c", assets={"a.bin": _asset(catalog, "c/a.bin", b"a")})
        assert not (catalog / "c" / "versions.json").exists()
        assert list(mine.staged()) == ["c"]


class TestUpdateCatalogIndex:
    def test_matching_entries_leave_the_file_alone(self, catalog: Path) -> None:
        assert update_catalog_index(catalog, {"c": CollectionSummary("1.0.0", 2, 10)}) is True
        before = (catalog / "versions.json").read_bytes()
        assert update_catalog_index(catalog, {"c": CollectionSummary("1.0.0", 2, 10)}) is False
        assert (catalog / "versions.json").read_bytes() == before

    def test_keeps_entries_of_other_collections(self, catalog: Path) -> None:
        update_catalog_index(catalog, {"a": CollectionSummary("1.0.0", 1, 1)})
        update_catalog_index(catalog, {"b": CollectionSummary("2.0.0", 3, 4)})
        collections = _index(catalog)["collections"]
        assert {k: v["current_version"] for k, v in collections.items()} == {
            "a": "1.0.0",
            "b": "2.0.0",
        }

    def test_no_index_file_is_a_no_op(self, tmp_path: Path) -> None:
        assert update_catalog_index(tmp_path, {"c": CollectionSummary("1.0.0", 1, 1)}) is False
        assert not (tmp_path / "versions.json").exists()
