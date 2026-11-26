"""Command parity for `push`: each push path against `run_push`.

Push writes to an in-memory store. The catalog factory replaces the store, so
the command and the library call each start from an empty remote. Each store
records the keys of its puts in order, so a test can assert what each run
published and that the manifest went last.
"""

from __future__ import annotations

import contextlib
import json
import shutil
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import obstore
import pytest
from obstore.store import MemoryStore

from portolan_cli.json_output import ErrorDetail
from portolan_cli.sync.push import PushAllResult, PushResult, run_push

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.sync.notices import NoticeLevel
    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity, VerifyingRunCli

pytestmark = pytest.mark.integration

DESTINATION = "s3://parity-bucket/catalog"
PREFIX = "catalog"

COLLECTION_FILES = {
    "catalog/points/points.geojson",
    "catalog/points/points.parquet",
    "catalog/points/points.thumb.jpg",
    "catalog/points/collection.json",
    "catalog/points/AGENTS.md",
    "catalog/points/README.md",
    "catalog/points/versions.json",
}
ROOT_FILES = {"catalog/catalog.json", "catalog/AGENTS.md", "catalog/README.md"}
# The bytes of each collection file are in place before its manifest names them.
BEFORE_MANIFEST = {
    "catalog/points/points.geojson",
    "catalog/points/points.parquet",
    "catalog/points/points.thumb.jpg",
    "catalog/points/collection.json",
}


def _quiet(_level: NoticeLevel, _message: str) -> None:
    """Drop each notice. The CLI prints them, and the library call must not."""


@dataclass
class Remote:
    """The in-memory store of one run, with the keys of its puts in order."""

    store: MemoryStore = field(default_factory=MemoryStore)
    puts: list[str] = field(default_factory=list)

    def stored(self) -> set[str]:
        return {str(meta["path"]) for page in obstore.list(self.store) for meta in page}


@dataclass
class Remotes:
    """One Remote for each catalog that the factory builds, and the put faults."""

    runs: list[Remote] = field(default_factory=list)
    fail_key: str | None = None
    refuse_network: bool = False

    @property
    def current(self) -> Remote:
        return self.runs[-1]


@pytest.fixture
def remotes(monkeypatch: pytest.MonkeyPatch) -> Remotes:
    """Send every push store call to the Remote of the current run."""
    remotes = Remotes()
    real_put, real_put_async = obstore.put, obstore.put_async

    def setup_store(*_args: Any, **_kwargs: Any) -> tuple[MemoryStore, str]:
        if remotes.refuse_network:
            raise AssertionError("a dry run must not set up a store")
        return remotes.current.store, PREFIX

    def record(key: str) -> None:
        if remotes.refuse_network:
            raise AssertionError(f"a dry run must not write {key}")
        remotes.current.puts.append(key)
        if key == remotes.fail_key:
            raise ConnectionError(f"injected failure for {key}")

    def put(store: Any, key: str, *args: Any, **kwargs: Any) -> Any:
        record(key)
        return real_put(store, key, *args, **kwargs)

    async def put_async(store: Any, key: str, *args: Any, **kwargs: Any) -> Any:
        record(key)
        return await real_put_async(store, key, *args, **kwargs)

    monkeypatch.setattr("portolan_cli.sync.push.setup_store", setup_store)
    monkeypatch.setattr(obstore, "put", put)
    monkeypatch.setattr(obstore, "put_async", put_async)
    return remotes


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
    remotes: Remotes,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, and an empty remote for it."""

    def build(root: Path) -> None:
        remotes.runs.append(Remote())
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


def _assert_manifest_after_data(remote: Remote) -> None:
    manifest = remote.puts.index("catalog/points/versions.json")
    assert all(remote.puts.index(key) < manifest for key in BEFORE_MANIFEST), remote.puts


def test_push_all_collections(
    parity: Parity, points_catalog: Callable[[Path], None], remotes: Remotes
) -> None:
    """`push` with no `--collection` pushes every collection, root manifest last."""
    parity(
        "push",
        [DESTINATION],
        catalog=points_catalog,
        call=lambda root: run_push(root, DESTINATION, notify=_quiet),
    )
    assert len(remotes.runs) == 2
    for remote in remotes.runs:
        assert remote.stored() == COLLECTION_FILES | ROOT_FILES | {"catalog/versions.json"}
        assert remote.puts[-1] == "catalog/versions.json"
        assert remote.puts.index("catalog/catalog.json") > max(
            remote.puts.index(key) for key in COLLECTION_FILES
        )
        _assert_manifest_after_data(remote)


def test_push_all_collections_counts(
    points_catalog: Callable[[Path], None], tmp_path: Path, remotes: Remotes
) -> None:
    """The catalog-wide result counts the one collection and its version."""
    points_catalog(tmp_path / "catalog")
    result = run_push(tmp_path / "catalog", DESTINATION, notify=_quiet)
    assert isinstance(result, PushAllResult)
    assert (result.total_collections, result.successful_collections) == (1, 1)
    assert (result.total_versions_pushed, result.total_files_uploaded) == (1, 10)


def test_push_one_collection(
    parity: Parity, points_catalog: Callable[[Path], None], remotes: Remotes
) -> None:
    """`push --collection` pushes that collection, and its manifest after its data."""
    parity(
        "push",
        [DESTINATION, "--collection", "points"],
        catalog=points_catalog,
        call=lambda root: run_push(root, DESTINATION, collection="points", notify=_quiet),
    )
    assert len(remotes.runs) == 2
    for remote in remotes.runs:
        assert remote.stored() == COLLECTION_FILES | ROOT_FILES
        _assert_manifest_after_data(remote)
        assert remote.puts.index("catalog/catalog.json") < remote.puts.index(
            "catalog/points/versions.json"
        )


def test_push_one_collection_counts(
    points_catalog: Callable[[Path], None], tmp_path: Path, remotes: Remotes
) -> None:
    """The result counts the version and the uploaded files, not the manifest."""
    points_catalog(tmp_path / "catalog")
    result = run_push(tmp_path / "catalog", DESTINATION, collection="points", notify=_quiet)
    assert isinstance(result, PushResult)
    assert (result.success, result.versions_pushed, result.files_uploaded) == (True, 1, 9)


def test_push_dry_run(
    parity: Parity, points_catalog: Callable[[Path], None], remotes: Remotes
) -> None:
    """`push --dry-run` reports the upper bound, with no store call at all."""
    remotes.refuse_network = True
    parity(
        "push",
        [DESTINATION, "--dry-run", "--collection", "points"],
        catalog=points_catalog,
        call=lambda root: run_push(
            root, DESTINATION, collection="points", dry_run=True, notify=_quiet
        ),
    )
    assert len(remotes.runs) == 2
    for remote in remotes.runs:
        assert (remote.puts, remote.stored()) == ([], set())


def test_push_missing_collection(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`push --collection` for a collection without versions.json fails."""
    parity.error(
        "push",
        [DESTINATION, "--collection", "missing"],
        catalog=points_catalog,
        call=lambda root: run_push(root, DESTINATION, collection="missing", notify=_quiet),
        raises=FileNotFoundError,
    )


class _BlockedBackend:
    """A backend that refuses a file-based push, as the Iceberg backend does."""

    def supports_push(self) -> bool:
        return False

    def push_blocked_message(self, _remote: str | None) -> str:
        return "push is blocked for this backend"


def test_push_blocked_backend(
    parity: Parity,
    points_catalog: Callable[[Path], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A collection whose backend refuses push fails the same way in both."""
    import portolan_cli.backends as backends

    real_get_backend = backends.get_backend

    def get_backend(name: str, **kwargs: Any) -> Any:
        return _BlockedBackend() if name == "iceberg" else real_get_backend(name, **kwargs)

    monkeypatch.setattr(backends, "get_backend", get_backend)

    def blocked_catalog(root: Path) -> None:
        points_catalog(root)
        (root / "points" / ".portolan").mkdir()
        (root / "points" / ".portolan" / "config.yaml").write_text("backend: iceberg\n")

    parity.error(
        "push",
        [DESTINATION, "--collection", "points"],
        catalog=blocked_catalog,
        call=lambda root: run_push(root, DESTINATION, collection="points", notify=_quiet),
        raises=NotImplementedError,
    )


def test_push_dry_run_constructs_no_backend(
    points_catalog: Callable[[Path], None],
    tmp_path: Path,
    remotes: Remotes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A library dry run never opens the backend, because that is catalog I/O.

    An Iceberg backend opens its catalog and creates its namespace when it is
    built. A dry run reads only the local versions.json. The command still
    refuses the blocked collection with its own early check.
    """
    import portolan_cli.backends as backends

    def get_backend(name: str, **_kwargs: Any) -> Any:
        raise AssertionError(f"a dry run must not build the {name!r} backend")

    root = tmp_path / "catalog"
    points_catalog(root)
    (root / "points" / ".portolan").mkdir()
    (root / "points" / ".portolan" / "config.yaml").write_text("backend: iceberg\n")
    monkeypatch.setattr(backends, "get_backend", get_backend)
    remotes.refuse_network = True

    result = run_push(root, DESTINATION, collection="points", dry_run=True, notify=_quiet)

    assert isinstance(result, PushResult)
    assert (result.success, result.dry_run, result.would_push_versions) == (True, True, 1)
    assert (remotes.current.puts, remotes.current.stored()) == ([], set())


def _returned_failure(
    run_cli: VerifyingRunCli,
    capsys: pytest.CaptureFixture[str],
    build: Callable[[Path], None],
    root: Path,
    args: list[str],
    call: Callable[[Path], PushResult | PushAllResult],
) -> tuple[dict[str, Any], PushResult | PushAllResult]:
    """Run the command and the library on fresh catalogs at ``root``, for a returned failure."""
    build(root)
    command = run_cli("push", *args, "--json", cwd=root, expect=1)
    envelope: dict[str, Any] = json.loads(command.stdout)
    # The rebuild deletes this catalog before the run_cli teardown check, so check it now.
    run_cli.verify()
    shutil.rmtree(root)
    build(root)
    capsys.readouterr()
    with contextlib.chdir(root):
        result = call(root)
    assert capsys.readouterr() == ("", ""), "the library call printed output"
    assert result.success is False
    assert envelope["success"] is False
    assert envelope["data"] == json.loads(json.dumps(result.to_dict()))
    return envelope, result


def test_push_one_collection_returned_failure(
    run_cli: VerifyingRunCli,
    capsys: pytest.CaptureFixture[str],
    points_catalog: Callable[[Path], None],
    remotes: Remotes,
    tmp_path: Path,
) -> None:
    """A failed asset upload returns a failure, and the data is the result `to_dict()`."""
    remotes.fail_key = "catalog/points/points.parquet"
    envelope, result = _returned_failure(
        run_cli,
        capsys,
        points_catalog,
        tmp_path / "catalog",
        [DESTINATION, "--collection", "points"],
        lambda root: run_push(root, DESTINATION, collection="points", notify=_quiet),
    )
    assert envelope["data"] == {}
    assert isinstance(result, PushResult)
    assert result.errors
    assert envelope["errors"] == [ErrorDetail("PushError", m).to_dict() for m in result.errors]
    for remote in remotes.runs:
        assert "catalog/points/versions.json" not in remote.stored()


def test_push_all_collections_returned_failure(
    run_cli: VerifyingRunCli,
    capsys: pytest.CaptureFixture[str],
    points_catalog: Callable[[Path], None],
    remotes: Remotes,
    tmp_path: Path,
) -> None:
    """A failed collection returns a catalog-wide failure with the full counts."""
    remotes.fail_key = "catalog/points/points.parquet"
    envelope, result = _returned_failure(
        run_cli,
        capsys,
        points_catalog,
        tmp_path / "catalog",
        [DESTINATION],
        lambda root: run_push(root, DESTINATION, notify=_quiet),
    )
    assert isinstance(result, PushAllResult)
    assert envelope["data"]["failed_collections"] == 1
    assert envelope["errors"] == [
        ErrorDetail("PushError", f"points: {m}").to_dict()
        for m in result.collection_errors["points"]
    ]
    for remote in remotes.runs:
        assert "catalog/catalog.json" not in remote.stored()
