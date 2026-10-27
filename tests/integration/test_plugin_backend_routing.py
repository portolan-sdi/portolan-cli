"""Plugin publishes and reads must use each collection's backend."""

from __future__ import annotations

import asyncio
import json
import shutil
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.add import add_files
from portolan_cli.backends.json_file import JsonFileBackend
from portolan_cli.cli import cli
from portolan_cli.config import set_setting
from portolan_cli.hookspecs import EnrichContext, hookimpl
from portolan_cli.run_state import RunState
from portolan_cli.sync.pull import PullResult, pull_all_collections_async
from portolan_cli.version_ops import (
    check_backend_supports_push,
    get_current_version,
    list_versions,
    publish_version,
    try_backend_pull,
)
from portolan_cli.versions import Version

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.backends.protocol import PostAddContext

pytestmark = pytest.mark.integration


class RecordingBackend(JsonFileBackend):
    """Keep plugin history separate from the file backend and record lifecycle calls."""

    def __init__(self, catalog_root: Path) -> None:
        super().__init__(catalog_root=catalog_root)
        self.history: dict[str, list[Version]] = {}
        self.publishes: list[tuple[str, dict[str, str]]] = []
        self.hooks: list[PostAddContext] = []
        self.pulls: list[tuple[str, bool]] = []
        self.pulls_on_loop: list[bool] = []
        self.events: list[str] = []

    def publish(
        self, collection: str, assets: dict[str, str], *args: Any, **kwargs: Any
    ) -> Version:
        """Record a publication without writing file-backend history.

        The arguments after ``assets`` are ``schema, breaking, message`` and the
        keywords ``removed`` and ``version``.
        """
        _schema, breaking, message = args[:3]
        version: str | None = kwargs.get("version")
        self.publishes.append((collection, dict(assets)))
        self.events.append("publish")
        history = self.history.setdefault(collection, [])
        result = Version(
            version=version or f"1.{len(history)}.0",
            created=datetime.now(UTC),
            breaking=breaking,
            assets={},
            changes=sorted(assets),
            message=message,
        )
        history.append(result)
        return result

    def get_current_version(self, collection: str) -> Version:
        """Return plugin history only."""
        return self.history[collection][-1]

    def list_versions(self, collection: str) -> list[Version]:
        """Return plugin history only."""
        return self.history.get(collection, [])

    def on_post_add(self, context: PostAddContext) -> None:
        """Record the post-add context and event ordering."""
        self.hooks.append(context)
        self.events.append("post-add")

    def pull(
        self, remote_url: str, local_root: Path, collection: str, *, dry_run: bool = False
    ) -> PullResult:
        """Report a plugin download without object-store I/O."""
        self.pulls.append((collection, dry_run))
        try:
            asyncio.get_running_loop()
            self.pulls_on_loop.append(True)
        except RuntimeError:
            self.pulls_on_loop.append(False)
        return PullResult(
            success=True,
            files_downloaded=3,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.1.0",
            up_to_date=False,
        )

    def push_blocked_message(self, remote: str | None) -> str:
        return "Push is not supported with the recording backend."

    def supports_push(self) -> bool:
        """This backend publishes directly and rejects file push."""
        return False


class _Plugin:
    def __init__(self, backend: RecordingBackend) -> None:
        self.backend = backend

    @hookimpl
    def portolan_backends(self) -> dict[str, Callable[..., RecordingBackend]]:
        return {"recording": lambda **kwargs: self.backend}

    @hookimpl
    def portolan_enrich_item(self, item: dict[str, Any], context: EnrichContext) -> None:
        item["properties"]["test:enriched"] = True
        self.backend.events.append("enrich")


@pytest.fixture
def recording(conformant_catalog: Path, register_plugin: Callable[..., None]) -> RecordingBackend:
    set_setting(conformant_catalog, "conversion.cog.generate_thumbnail", False)
    backend = RecordingBackend(conformant_catalog)
    register_plugin(_Plugin(backend))
    return backend


def _backend(root: Path, collection: str, backend: str) -> None:
    directory = root / collection / ".portolan"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "config.yaml").write_text(f"backend: {backend}\n")


def _raster(root: Path, fixtures: Path, collection: str = "imagery") -> Path:
    item = root / collection / "scene"
    item.mkdir(parents=True)
    shutil.copyfile(fixtures / "raster/valid/rgb.tif", item / "scene.tif")
    (item / "attributes.csv").write_text("id,name\n1,Scene\n")
    return item


def test_plugin_add_publishes_once_with_item_companions(
    conformant_catalog: Path,
    fixtures_dir: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    _backend(root, "imagery", "recording")
    item = _raster(root, fixtures_dir)

    _, _, failures = add_files(paths=[item], catalog_root=root)

    assert failures == []
    assert len(recording.publishes) == 1
    assert set(recording.publishes[0][1]) == {"scene/scene.tif", "scene/attributes.csv"}
    assert len(recording.hooks) == 1
    assert recording.events[-2:] == ["publish", "post-add"]
    assert recording.hooks[0]["collection_id"] == "imagery"
    assert json.loads((item / "scene.json").read_text())["properties"]["test:enriched"] is True


def test_plugin_add_exception_commits_nothing(
    conformant_catalog: Path,
    fixtures_dir: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    _backend(root, "imagery", "recording")
    item = _raster(root, fixtures_dir)
    with (
        patch("portolan_cli.add._write_deferred", side_effect=RuntimeError("deferred failed")),
        pytest.raises(RuntimeError, match="deferred failed"),
    ):
        add_files(paths=[item], catalog_root=root)
    assert recording.publishes == []
    assert recording.hooks == []


@pytest.mark.parametrize("use_state", [False, True])
@pytest.mark.parametrize("operation", ["current", "list", "publish"])
def test_version_entry_points_use_collection_backend(
    conformant_catalog: Path,
    recording: RecordingBackend,
    use_state: bool,
    operation: str,
) -> None:
    root = conformant_catalog
    collection = "group/remote"
    _backend(root, collection, "recording")
    expected = recording.publish(
        collection, {}, {"columns": [], "types": {}, "hash": "x"}, False, ""
    )
    state = RunState.open(root) if use_state else None
    if operation == "current":
        assert get_current_version(collection, catalog_root=root, state=state) == expected
    elif operation == "list":
        assert list_versions(collection, catalog_root=root, state=state) == [expected]
    else:
        publish_version(collection, assets={}, catalog_root=root, state=state)
        assert len(recording.history[collection]) == 2


def test_catalog_push_checks_collection_override(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    _backend(root, "remote", "recording")
    (root / "remote/collection.json").write_text('{"type":"Collection"}')
    assert check_backend_supports_push(root) is not None


def test_catalog_push_honors_file_override(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    set_setting(root, "backend", "recording")
    _backend(root, "local", "file")
    (root / "local/collection.json").write_text('{"type":"Collection"}')
    assert check_backend_supports_push(root) is None


def test_catalog_backend_pull_routes_each_collection(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    _backend(root, "remote", "recording")
    (root / "remote/collection.json").write_text('{"type":"Collection"}')
    result = try_backend_pull(root, "s3://example/catalog", None, dry_run=True)
    assert result.handled is True
    assert recording.pulls == [("remote", True)]


def test_mixed_catalog_pull_uses_plugins_and_file_backend(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    for name in ("remote", "local"):
        _backend(root, name, "recording" if name == "remote" else "file")
        (root / name / "collection.json").write_text('{"type":"Collection"}')
        (root / name / "versions.json").write_text(
            '{"spec_version":"1.0.0","current_version":null,"versions":[]}'
        )
    file_pull = AsyncMock(
        return_value=PullResult(
            success=True,
            files_downloaded=2,
            files_skipped=0,
            local_version=None,
            remote_version="1.0.0",
        )
    )
    with (
        patch("portolan_cli.sync.pull._setup_store_and_kwargs", return_value=(object(), {})),
        patch("portolan_cli.sync.pull.pull_async", file_pull),
    ):
        result = asyncio.run(
            pull_all_collections_async("s3://example/catalog", root, json_mode=True)
        )
    assert result.success is True
    assert recording.pulls == [("remote", False)]
    assert [call.kwargs["collection"] for call in file_pull.call_args_list] == ["local"]
    assert result.total_files_downloaded == 5


@pytest.mark.parametrize("command", ["current", "list"])
def test_cli_version_reads_collection_plugin(
    conformant_catalog: Path,
    recording: RecordingBackend,
    command: str,
) -> None:
    root = conformant_catalog
    _backend(root, "remote", "recording")
    recording.publish("remote", {}, {"columns": [], "types": {}, "hash": "x"}, False, "")
    result = CliRunner().invoke(
        cli, ["version", command, "remote", "--catalog", str(root), "--json"]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["success"] is True
    if command == "list":
        assert [v["version"] for v in data["data"]["versions"]] == ["1.0.0"]


def test_catalog_push_api_rejects_collection_plugin(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    from portolan_cli.sync.push import push_all_collections_async

    root = conformant_catalog
    _backend(root, "remote", "recording")
    (root / "remote/collection.json").write_text('{"type":"Collection"}')
    with pytest.raises(NotImplementedError, match="recording"):
        asyncio.run(push_all_collections_async(root, "s3://example/catalog", dry_run=True))


def test_catalog_pull_finds_plugin_without_versions_json(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    _backend(root, "remote", "recording")
    (root / "remote/collection.json").write_text('{"type":"Collection"}')
    with patch(
        "portolan_cli.sync.pull._setup_store_and_kwargs", return_value=(object(), {})
    ) as setup:
        result = asyncio.run(pull_all_collections_async("s3://example/catalog", root, dry_run=True))
    assert recording.pulls == [("remote", True)]
    assert result.total_collections == 1
    setup.assert_not_called()


def test_standalone_tabular_plugin_runs_post_add_hook(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    _backend(root, "lookup", "recording")
    set_setting(root, "tabular.enabled", True)
    table = root / "lookup/codes.csv"
    table.write_text("code,label\n1,road\n")

    _, _, failures = add_files(paths=[table], catalog_root=root)

    assert failures == []
    assert len(recording.publishes) == 1
    assert len(recording.hooks) == 1
    assert recording.hooks[0]["collection_id"] == "lookup"
    assert recording.events[-2:] == ["publish", "post-add"]


def test_plugin_owned_run_exception_commits_nothing(
    conformant_catalog: Path,
    fixtures_dir: Path,
    recording: RecordingBackend,
) -> None:
    from portolan_cli.version_ops import owned_run

    root = conformant_catalog
    _backend(root, "imagery", "recording")
    item = _raster(root, fixtures_dir)
    with pytest.raises(RuntimeError, match="after add"), owned_run(root) as state:
        add_files(paths=[item], catalog_root=root, state=state)
        raise RuntimeError("after add")
    assert recording.publishes == []
    assert recording.hooks == []


def test_catalog_push_shares_config_reads(conformant_catalog: Path) -> None:
    from portolan_cli import trace

    root = conformant_catalog
    for name in ("a", "b", "c"):
        directory = root / name
        directory.mkdir()
        (directory / "collection.json").write_text('{"type":"Collection"}')
    with trace.tracing():
        assert check_backend_supports_push(root) is None
        assert trace.counters()["config.yaml_parses"] == 1


def test_item_companion_scan_reuses_run_checksums(
    conformant_catalog: Path,
    fixtures_dir: Path,
    recording: RecordingBackend,
) -> None:
    from portolan_cli.sync.checksums import compute_checksum

    root = conformant_catalog
    _backend(root, "imagery", "recording")
    item = _raster(root, fixtures_dir)
    with (
        patch("portolan_cli.preparation.compute_checksum", wraps=compute_checksum) as direct,
        patch("portolan_cli.run_state.compute_checksum", wraps=compute_checksum) as memoized,
    ):
        _, _, failures = add_files(paths=[item], catalog_root=root)
    assert failures == []
    hashed = [call.args[0] for call in direct.call_args_list + memoized.call_args_list]
    assert hashed.count(item / "scene.tif") == 1
    assert hashed.count(item / "attributes.csv") == 1


def test_plugin_item_companion_only_add_runs_post_add_hook(
    conformant_catalog: Path,
    fixtures_dir: Path,
    recording: RecordingBackend,
) -> None:
    root = conformant_catalog
    item = _raster(root, fixtures_dir)
    # Establish the local manifest that a plugin may also maintain. The next
    # add changes only a companion, so no prepared geo item registers a hook.
    _, _, failures = add_files(paths=[item], catalog_root=root)
    assert failures == []
    _backend(root, "imagery", "recording")
    companion = item / "attributes.csv"
    companion.write_text("id,name\n1,Changed\n")

    _, _, failures = add_files(paths=[companion], catalog_root=root)

    assert failures == []
    assert len(recording.publishes) == 1
    assert len(recording.hooks) == 1
    assert [entry["item_id"] for entry in recording.hooks[0]["items"]] == ["scene"]
    assert recording.events[-2:] == ["publish", "post-add"]


@pytest.mark.parametrize("operation", ["current", "list", "publish"])
def test_version_entry_points_use_collection_backend_from_cwd(
    conformant_catalog: Path,
    recording: RecordingBackend,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    root = conformant_catalog
    _backend(root, "remote", "recording")
    monkeypatch.chdir(root)
    expected = recording.publish("remote", {}, {"columns": [], "types": {}, "hash": "x"}, False, "")
    if operation == "current":
        assert get_current_version("remote") == expected
    elif operation == "list":
        assert list_versions("remote") == [expected]
    else:
        publish_version("remote", assets={})
        assert len(recording.history["remote"]) == 2


def test_catalog_pull_runs_a_plugin_pull_off_the_event_loop(
    conformant_catalog: Path,
    recording: RecordingBackend,
) -> None:
    """A synchronous plugin pull must not stop the downloads of other collections."""
    root = conformant_catalog
    _backend(root, "remote", "recording")
    (root / "remote/collection.json").write_text('{"type":"Collection"}')
    with patch("portolan_cli.sync.pull._setup_store_and_kwargs", return_value=(object(), {})):
        asyncio.run(pull_all_collections_async("s3://example/catalog", root, dry_run=True))
    assert recording.pulls_on_loop == [False], "the plugin pull blocked the event loop"
