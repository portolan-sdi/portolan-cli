"""Push under failure, against a real S3 API on moto.

Each test builds the catalog with the real CLI, pushes it with the real CLI,
and reads the bucket back with boto3. A test asserts what a reader of the
bucket sees: the object set, the ETags, and the headers. It does not assert
which push functions ran.

``tests/integration/conftest.py`` holds the harness. A test that fails on
``origin/main`` carries a strict xfail with its issue number. The fix for that
issue removes the marker.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from click.testing import Result

    from tests.catalog_invariants import RunCli
    from tests.integration.conftest import Bucket, PutFaults

pytestmark = [pytest.mark.integration]


CONFLICT_MESSAGE = "Remote changed during push"

# The media type of each suffix that a push uploads (#807). The values come
# from the IANA registry, the STAC best practices for COG, and the PMTiles
# specification.
CONTENT_TYPES = {
    ".parquet": "application/vnd.apache.parquet",
    ".json": "application/json",
    ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".md": "text/markdown",
    ".pmtiles": "application/vnd.pmtiles",
    ".xml": "application/xml",
}


def _is_scene(key: str) -> bool:
    return key.endswith(".tif")


def _push(run_cli: RunCli, root: Path, bucket: Bucket, *extra: str) -> Result:
    return run_cli("push", bucket.url, "--catalog", str(root), *extra, expect=None)


def _other_writer_manifest(bucket: Bucket, key: str) -> bytes:
    """Return the manifest of another writer that published version 9.0.0."""
    body = bucket.client.get_object(Bucket=bucket.name, Key=key)["Body"].read()
    manifest: dict[str, Any] = json.loads(body)
    versions = manifest.setdefault("versions", [])
    other = dict(versions[-1]) if versions else {"assets": {}}
    other.update(version="9.0.0", created="2026-10-23T00:00:00Z", changes=[])
    versions.append(other)
    manifest["current_version"] = "9.0.0"
    return json.dumps(manifest).encode()


def _other_writer_manifest_naming(bucket: Bucket, key: str, asset_key: str, sha256: str) -> bytes:
    """Return the manifest of version 9.0.0, which names ``sha256`` for ``asset_key``."""
    manifest = json.loads(_other_writer_manifest(bucket, key))
    for asset in manifest["versions"][-1]["assets"].values():
        if bucket.key(asset["href"]) == asset_key:
            asset["sha256"] = sha256
    return json.dumps(manifest).encode()


def _bytes_that_disagree_with_the_manifest(
    bucket: Bucket, collection: str, written: set[str]
) -> dict[str, str]:
    """Return each written Asset key whose bytes differ from the remote manifest.

    The manifest is the remote ``<collection>/versions.json`` and its current
    snapshot. The value is the sha256 of the object, or ``<missing>``.
    """
    body = bucket.client.get_object(
        Bucket=bucket.name, Key=bucket.key(f"{collection}/versions.json")
    )
    manifest: dict[str, Any] = json.loads(body["Body"].read())
    versions: list[dict[str, Any]] = manifest["versions"]
    current = next(
        (v for v in versions if v.get("version") == manifest.get("current_version")),
        versions[-1],
    )
    expected = {bucket.key(asset["href"]): asset["sha256"] for asset in current["assets"].values()}
    checked = sorted(written & expected.keys())
    assert checked != [], (sorted(written), sorted(expected))

    present = bucket.keys()
    disagree: dict[str, str] = {}
    for key in checked:
        actual = "<missing>"
        if key in present:
            data = bucket.client.get_object(Bucket=bucket.name, Key=key)["Body"].read()
            actual = hashlib.sha256(data).hexdigest()
        if actual != expected[key]:
            disagree[key] = actual
    return disagree


def _lose_the_manifest_race(
    run_cli: RunCli, root: Path, bucket: Bucket, fault_store: PutFaults, other: Callable[[], None]
) -> None:
    """Run ``other`` before the imagery versions.json write, and assert the push conflict."""
    manifest_key = bucket.key("imagery/versions.json")
    fault_store.arm(1, when=lambda key: key == manifest_key, action=other)

    second = _push(run_cli, root, bucket)

    assert second.exit_code == 1, second.output
    assert CONFLICT_MESSAGE in second.output


@pytest.fixture
def pushed(
    fault_catalog: Path,
    bucket: Bucket,
    run_cli: RunCli,
    fault_store: PutFaults,
    change_scenes: Callable[[Path], None],
) -> Path:
    """Push the catalog once, then record a new imagery version locally."""
    first = _push(run_cli, fault_catalog, bucket)
    assert first.exit_code == 0, first.output
    change_scenes(fault_catalog)
    return fault_catalog


def test_failed_second_push_keeps_first_push_objects(
    pushed: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """A dropped connection on the second Asset put leaves push 1 readable.

    Each key that push 2 wrote holds the bytes that the remote manifest
    describes. A rollback that keeps the key with the bytes of push 2 fails.
    """
    after_first = bucket.keys()
    manifest = bucket.head(bucket.key("imagery/versions.json"))
    calls_before = len(fault_store.calls)
    fault_store.fail_on(2, when=_is_scene)

    second = _push(run_cli, pushed, bucket)

    assert second.exit_code == 1, second.output
    assert "injected fault" in second.output
    assert bucket.head(bucket.key("imagery/versions.json")) == manifest
    written = set(fault_store.keys()[calls_before:]) & after_first
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", written) == {}
    assert sorted(after_first - bucket.keys()) == []
    assert bucket.keys() == after_first


@pytest.mark.parametrize("mode", [(), ("--json",)], ids=["text", "json"])
def test_failed_rollback_reports_the_key_it_could_not_restore(
    pushed: Path,
    bucket: Bucket,
    run_cli: RunCli,
    fault_store: PutFaults,
    monkeypatch: pytest.MonkeyPatch,
    mode: tuple[str, ...],
) -> None:
    """The restore of one scene fails during the rollback of a failed push.

    The push exits 1 and names that scene, its backup key, and the reason in
    its errors. The other written keys hold the bytes of push 1 again. The
    backup of the scene stays, so the old bytes are not lost.
    """
    from portolan_cli.sync import push as push_module

    after_first = bucket.keys()
    calls_before = len(fault_store.calls)
    fault_store.fail_on(2, when=_is_scene)
    real_copy = push_module.obs.copy_async
    refused: list[tuple[str, str]] = []

    async def copy_async(store: Any, src: str, dst: str, **kwargs: Any) -> Any:
        if _is_scene(dst) and "/.portolan/" in src:
            refused.append((dst, src))
            raise PermissionError("injected restore fault")
        return await real_copy(store, src, dst, **kwargs)

    monkeypatch.setattr(push_module.obs, "copy_async", copy_async)

    second = _push(run_cli, pushed, bucket, *mode)

    assert second.exit_code == 1, second.output
    assert len(refused) == 1, refused
    scene, backup = refused[0]
    message = f"Could not restore {scene} from {backup}: injected restore fault"
    if mode:
        envelope = json.loads(second.stdout)
        assert envelope["success"] is False
        assert f"imagery: {message}" in [entry["message"] for entry in envelope["errors"]]
    else:
        # The failure line and the closing summary each list the errors.
        lines = [line for line in second.output.splitlines() if "imagery: Failed" in line]
        assert [message in line for line in lines] == [True, True], second.output
    written = set(fault_store.keys()[calls_before:]) & after_first
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", written - {scene}) == {}
    assert bucket.keys() == after_first | {backup}


def test_conflict_deletes_nothing_of_the_other_writer(
    pushed: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """A push that loses the versions.json race deletes no key.

    Each key that the losing push overwrote holds the bytes that the other
    writer's manifest describes (#965).
    """
    manifest_key = bucket.key("imagery/versions.json")
    before = bucket.keys()
    calls_before = len(fault_store.calls)
    other: dict[str, str] = {}

    def other_push() -> None:
        other["etag"] = bucket.second_writer(
            manifest_key, _other_writer_manifest(bucket, manifest_key)
        )

    fault_store.arm(1, when=_is_scene, action=other_push)

    second = _push(run_cli, pushed, bucket)

    assert second.exit_code == 1, second.output
    assert CONFLICT_MESSAGE in second.output
    assert bucket.head(manifest_key)["ETag"] == other["etag"]
    written = set(fault_store.keys()[calls_before:]) & before
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", written) == {}
    assert sorted(before - bucket.keys()) == []


def test_root_versions_json_is_conditional(
    pushed: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """Another writer's root versions.json survives a concurrent push.

    The collection manifests use ``mode={"e_tag": ...}``. The root manifest
    does not, so the push overwrites the other writer without a conflict. The
    push must read the root ETag before its first upload.
    """
    root_key = bucket.key("versions.json")
    other: dict[str, str] = {}

    def other_push() -> None:
        other["etag"] = bucket.second_writer(root_key, b'{"other_writer": true}')

    fault_store.arm(1, when=_is_scene, action=other_push)

    second = _push(run_cli, pushed, bucket)

    assert second.exit_code == 1, second.output
    assert CONFLICT_MESSAGE in second.output
    assert bucket.head(root_key)["ETag"] == other["etag"]


def test_every_object_has_a_content_type(
    fault_catalog: Path, bucket: Bucket, run_cli: RunCli
) -> None:
    """Every pushed object carries the media type of its suffix."""
    result = _push(run_cli, fault_catalog, bucket)
    assert result.exit_code == 0, result.output

    keys = bucket.keys()
    assert {Path(key).suffix for key in keys} >= {".parquet", ".json", ".tif", ".md"}
    actual = {key: bucket.head(key)["ContentType"] for key in keys}
    expected = {key: CONTENT_TYPES.get(Path(key).suffix, "<no rule>") for key in keys}
    assert actual == expected


def test_thread_pool_is_shut_down(
    fault_catalog: Path,
    bucket: Bucket,
    run_cli: RunCli,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each Asset upload pool that push opens is shut down once."""
    from portolan_cli.sync import push as push_module

    pools: list[RecordingPool] = []

    class RecordingPool(concurrent.futures.ThreadPoolExecutor):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.shutdowns = 0
            pools.append(self)

        def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
            self.shutdowns += 1
            super().shutdown(wait=wait, cancel_futures=cancel_futures)

    def upload_pool(*args: Any, **kwargs: Any) -> RecordingPool:
        return RecordingPool(*args, **kwargs)

    real_upload_assets = push_module._upload_assets_async

    async def upload_assets(*args: Any, **kwargs: Any) -> Any:
        # Only the pool that the Asset upload makes is under test. The
        # catalog-wide push opens its own worker pool.
        with monkeypatch.context() as scoped:
            scoped.setattr(concurrent.futures, "ThreadPoolExecutor", upload_pool)
            scoped.setattr(push_module, "ThreadPoolExecutor", upload_pool, raising=False)
            return await real_upload_assets(*args, **kwargs)

    monkeypatch.setattr(push_module, "_upload_assets_async", upload_assets)

    result = _push(run_cli, fault_catalog, bucket, "--collection", "imagery")

    assert result.exit_code == 0, result.output
    assert len(pools) == 1
    assert [pool.shutdowns for pool in pools] == [1]


@pytest.mark.unit
def test_multipart_chunk_size_keeps_parts_under_10000() -> None:
    """Every size up to 5 TiB fits in 10,000 parts of at least 5 MiB."""
    from portolan_cli.sync.upload import multipart_chunk_size

    for size in (1, 5 * 2**20, 48 * 2**30, 6 * 2**30 * 10, 5 * 2**40):
        chunk = multipart_chunk_size(size)
        assert chunk >= 5 * 2**20, size
        assert chunk * 10_000 >= size, size
    # One byte over 9000 parts of 5 MiB grows the part size by one byte.
    assert multipart_chunk_size(9000 * 5 * 2**20 + 1) == 5 * 2**20 + 1
    assert multipart_chunk_size(1024) == 5 * 2**20


def test_multipart_put_passes_the_chunk_size(
    fault_catalog: Path,
    bucket: Bucket,
    run_cli: RunCli,
    fault_store: PutFaults,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The multipart put of each large Asset passes ``multipart_chunk_size``."""
    from portolan_cli.sync import push as push_module
    from portolan_cli.sync.upload import multipart_chunk_size

    # Route every Asset through the multipart path, so the test needs no
    # 5 MiB fixture.
    monkeypatch.setattr(push_module, "MULTIPART_THRESHOLD", 1)

    result = _push(run_cli, fault_catalog, bucket, "--collection", "imagery")
    assert result.exit_code == 0, result.output

    scene_puts = [call for call in fault_store.calls if _is_scene(call.key)]
    assert len(scene_puts) == 2
    for call in scene_puts:
        local = fault_catalog / bucket.relative(call.key)
        assert call.kwargs.get("chunk_size") == multipart_chunk_size(local.stat().st_size)


def test_dry_run_sends_nothing(
    fault_catalog: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """``push --dry-run`` writes no object."""
    result = _push(run_cli, fault_catalog, bucket, "--dry-run")

    assert result.exit_code == 0, result.output
    assert fault_store.keys() == []
    assert bucket.keys() == set()
    assert "would push up to 1 version(s)" in result.output


def test_list_permission_denied_warns_once(
    fault_catalog: Path,
    bucket: Bucket,
    run_cli: RunCli,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A write-only credential pushes two collections and sees one warning."""
    import obstore
    from obstore.exceptions import PermissionDeniedError

    listed: list[str] = []

    def deny(*args: Any, **kwargs: Any) -> Any:
        listed.append(str(args[1]) if len(args) > 1 else str(kwargs.get("prefix")))
        raise PermissionDeniedError("403 Forbidden: s3:ListBucket")

    for name in ("list", "list_with_delimiter", "list_with_delimiter_async"):
        monkeypatch.setattr(obstore, name, deny)

    result = _push(run_cli, fault_catalog, bucket)

    assert result.exit_code == 0, result.output
    warnings = [line for line in result.output.splitlines() if "s3:ListBucket" in line]
    assert len(warnings) == 1, result.output
    assert listed != []


def test_conflict_keeps_the_bytes_that_the_other_writer_wrote_later(
    pushed: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """A push that loses the race restores no key that the other writer wrote after it.

    The other writer writes a scene and its manifest between the uploads of
    this push and its versions.json write. The scene then holds the bytes
    that the winning manifest names. A restore from the backup puts the bytes
    of version 1.0.0 back over them.
    """
    manifest_key = bucket.key("imagery/versions.json")
    scene_key = bucket.key("imagery/scene-a/rgb.tif")
    winner_bytes = b"bytes of the other writer"

    def other_push() -> None:
        winner_sha = hashlib.sha256(winner_bytes).hexdigest()
        manifest = _other_writer_manifest_naming(bucket, manifest_key, scene_key, winner_sha)
        bucket.second_writer(scene_key, winner_bytes)
        bucket.second_writer(manifest_key, manifest)

    _lose_the_manifest_race(run_cli, pushed, bucket, fault_store, other_push)
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", {scene_key}) == {}
    present = bucket.keys()
    assert sorted(key for key in present if "/.portolan/" in key) == []


def test_interrupted_push_restores_the_keys_and_drops_the_backups(
    pushed: Path, bucket: Bucket, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A push that stops on an interrupt after its uploads leaves push 1 readable.

    The interrupt comes after the Assets and the STAC files upload, before the
    versions.json write. The bucket then holds the keys and the bytes of push 1,
    and no backup key.
    """
    import asyncio

    from portolan_cli.sync import push as push_module

    before = bucket.keys()

    async def interrupt(*_args: Any, **_kwargs: Any) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setattr(push_module, "_upload_metadata_files_async", interrupt)

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(push_module.push_async(pushed, "imagery", bucket.url))

    assert bucket.keys() == before
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", before) == {}


def test_cancelled_backup_leaves_no_backup_key(
    pushed: Path, bucket: Bucket, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A push cancelled while it copies the backups deletes the copies it made.

    The backup runs before the upload and its rollback. Python's asyncio.run
    cancels the main task on SIGINT, so the cancellation reaches the backup
    through its gather. Each later push uses a fresh backup directory, so a
    copy that stays behind is never reclaimed.
    """
    import asyncio

    from portolan_cli.sync import push as push_module

    before = bucket.keys()
    real_copy = push_module.obs.copy_async
    copies: list[str] = []

    async def copy_once_then_cancel(store: Any, src: str, dst: str, **kwargs: Any) -> Any:
        copies.append(src)
        if len(copies) > 1:
            raise asyncio.CancelledError
        # The first copy lands on the server even when the client cancels the
        # call, as a request in flight does on a real store. A cleanup that
        # deletes only the copies it saw finish misses it.
        landed = asyncio.ensure_future(real_copy(store, src, dst, **kwargs))
        try:
            await asyncio.sleep(0.05)
        finally:
            await asyncio.shield(landed)
        return landed.result()

    monkeypatch.setattr(push_module.obs, "copy_async", copy_once_then_cancel)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(push_module.push_async(pushed, "imagery", bucket.url))

    assert len(copies) > 1
    assert bucket.keys() == before


def test_conflict_with_a_push_of_the_same_version_restores_nothing(
    pushed: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """Two pushes of one local version race. The loser keeps the bytes that both wrote.

    The other writer publishes the local versions.json of this push, as a
    second run of the same push does. Each written key then already holds
    the bytes that the winning manifest names.
    """
    manifest_key = bucket.key("imagery/versions.json")
    before = bucket.keys()
    calls_before = len(fault_store.calls)
    local_manifest = (pushed / "imagery" / "versions.json").read_bytes()

    def same_push() -> None:
        bucket.second_writer(manifest_key, local_manifest)

    _lose_the_manifest_race(run_cli, pushed, bucket, fault_store, same_push)
    written = set(fault_store.keys()[calls_before:]) & before
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", written) == {}


def _remote_json(bucket: Bucket, key: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(
        bucket.client.get_object(Bucket=bucket.name, Key=key)["Body"].read()
    )
    return document


def _child_hrefs(document: dict[str, Any]) -> list[str]:
    return sorted(link["href"] for link in document["links"] if link["rel"] == "child")


@pytest.mark.parametrize("scope", [(), ("--collection", "imagery")], ids=["all", "collection"])
def test_root_catalog_keeps_the_child_link_of_another_writer(
    pushed: Path,
    bucket: Bucket,
    run_cli: RunCli,
    fault_store: PutFaults,
    scope: tuple[str, ...],
) -> None:
    """Another writer adds a child link to the root catalog.json during the push.

    The push writes the root catalog.json under the ETag that it read before
    its first upload. On a mismatch it merges the child links of the remote
    copy, so both child links are in the bucket after the push.
    """
    catalog_key = bucket.key("catalog.json")
    other_link = {"rel": "child", "href": "./roads/collection.json", "type": "application/json"}

    def other_push() -> None:
        document = _remote_json(bucket, catalog_key)
        document["links"].append(other_link)
        bucket.second_writer(catalog_key, json.dumps(document).encode())

    fault_store.arm(1, when=_is_scene, action=other_push)

    second = _push(run_cli, pushed, bucket, *scope)

    assert second.exit_code == 0, second.output
    local = json.loads((pushed / "catalog.json").read_text(encoding="utf-8"))
    expected = sorted([*_child_hrefs(local), "./roads/collection.json"])
    assert _child_hrefs(_remote_json(bucket, catalog_key)) == expected


def test_root_catalog_keeps_a_collection_that_the_local_catalog_lacks(
    fault_catalog: Path, bucket: Bucket, run_cli: RunCli, tmp_path: Path
) -> None:
    """A second checkout that lacks a pushed collection keeps its child link.

    The first checkout pushes buildings. The second checkout has no buildings
    collection and pushes imagery. The remote root catalog.json then links
    both collections.
    """
    import shutil

    first = _push(run_cli, fault_catalog, bucket, "--collection", "buildings")
    assert first.exit_code == 0, first.output
    other = tmp_path / "other"
    shutil.copytree(fault_catalog, other)
    shutil.rmtree(other / "buildings")
    catalog = json.loads((other / "catalog.json").read_text(encoding="utf-8"))
    catalog["links"] = [link for link in catalog["links"] if "buildings" not in link["href"]]
    (other / "catalog.json").write_text(json.dumps(catalog, indent=2), encoding="utf-8")

    second = _push(run_cli, other, bucket, "--collection", "imagery")

    assert second.exit_code == 0, second.output
    remote = _remote_json(bucket, bucket.key("catalog.json"))
    assert _child_hrefs(remote) == ["./buildings/collection.json", "./imagery/collection.json"]


def test_push_skips_a_stray_file_and_names_it(
    fault_catalog: Path, bucket: Bucket, run_cli: RunCli
) -> None:
    """A file that no STAC document names stays local, and the push names it once."""
    stray = "imagery/scene-a/notes.txt"
    (fault_catalog / stray).write_text("notes beside a tracked asset\n", encoding="utf-8")

    result = _push(run_cli, fault_catalog, bucket)

    assert result.exit_code == 0, result.output
    present = bucket.keys()
    assert bucket.key(stray) not in present
    assert bucket.key("imagery/scene-a/rgb.tif") in present
    named = [line for line in result.output.splitlines() if stray in line]
    assert named == [f"⚠ Not pushed, because no STAC document names it: {stray}"]


def test_root_readme_keeps_the_bytes_of_another_writer(
    pushed: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """Another writer changes the root README.md during the push.

    The README write is conditional on the ETag that the push read before its
    first upload. On a mismatch the remote bytes stay and the push warns.
    """
    readme_key = bucket.key("README.md")
    (pushed / "README.md").write_text("# Local edit\n", encoding="utf-8")
    other_bytes = b"# Other writer\n"

    def other_push() -> None:
        bucket.second_writer(readme_key, other_bytes)

    fault_store.arm(1, when=_is_scene, action=other_push)

    second = _push(run_cli, pushed, bucket)

    assert second.exit_code == 0, second.output
    remote = bucket.client.get_object(Bucket=bucket.name, Key=readme_key)["Body"].read()
    assert remote == other_bytes
    assert f"Another writer changed {readme_key} during the push" in second.output


# A copy source limit that the small test scenes exceed. S3 refuses a
# CopyObject source above 5 GB. moto does not, so ``refuse_large_copies``
# makes the copy refuse a source above this size.
SMALL_COPY_LIMIT = 16


@pytest.fixture
def refuse_large_copies(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Make CopyObject refuse a source above ``SMALL_COPY_LIMIT``, as S3 does above 5 GB."""
    from portolan_cli.sync import push as push_module

    monkeypatch.setattr(push_module, "COPY_SOURCE_LIMIT", SMALL_COPY_LIMIT)
    real_copy = push_module.obs.copy_async
    refused: list[str] = []

    async def copy_async(store: Any, src: str, dst: str, **kwargs: Any) -> Any:
        meta = await push_module.obs.head_async(store, src)
        if int(meta["size"]) > SMALL_COPY_LIMIT:
            refused.append(src)
            raise ValueError("InvalidRequest: The specified copy source is larger than the limit")
        return await real_copy(store, src, dst, **kwargs)

    monkeypatch.setattr(push_module.obs, "copy_async", copy_async)
    return refused


def test_failed_push_restores_a_key_above_the_copy_limit(
    pushed: Path,
    bucket: Bucket,
    run_cli: RunCli,
    fault_store: PutFaults,
    refuse_large_copies: list[str],
) -> None:
    """A failed push restores a scene that is too large for CopyObject.

    The backup streams such a key through the client. The restore streams it
    back. The bucket then holds the bytes that the remote manifest names.
    """
    after_first = bucket.keys()
    calls_before = len(fault_store.calls)
    # The streamed backups are puts of .tif keys too. The fault hits the
    # second scene upload.
    fault_store.fail_on(2, when=lambda key: _is_scene(key) and "/.portolan/" not in key)

    second = _push(run_cli, pushed, bucket)

    assert second.exit_code == 1, second.output
    assert "injected fault" in second.output
    assert "Cannot back up" not in second.output
    assert refuse_large_copies == []
    written = set(fault_store.keys()[calls_before:]) & after_first
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", written) == {}
    assert bucket.keys() == after_first
    scenes = sorted(key for key in written if _is_scene(key))
    assert [bucket.head(key)["ContentType"] for key in scenes] == [CONTENT_TYPES[".tif"]] * 2


def test_conflict_loser_keeps_an_asset_with_the_bytes_the_winner_names(
    pushed: Path, bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """Two pushes of different snapshots share one asset. The loser keeps that asset.

    The winning manifest names the new bytes of scene-a, as this push does,
    and the old bytes of scene-b. The loser restores scene-b only. A restore
    of scene-a puts bytes back that the winning manifest does not name.
    """
    manifest_key = bucket.key("imagery/versions.json")
    shared_key = bucket.key("imagery/scene-a/rgb.tif")
    shared_sha = hashlib.sha256((pushed / bucket.relative(shared_key)).read_bytes()).hexdigest()

    def other_push() -> None:
        manifest = _other_writer_manifest_naming(bucket, manifest_key, shared_key, shared_sha)
        bucket.second_writer(manifest_key, manifest)

    _lose_the_manifest_race(run_cli, pushed, bucket, fault_store, other_push)
    scenes = {shared_key, bucket.key("imagery/scene-b/singleband.tif")}
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", scenes) == {}


def test_clone_skips_an_object_that_no_document_names(
    fault_catalog: Path, bucket: Bucket, run_cli: RunCli, tmp_path: Path
) -> None:
    """clone downloads the STAC tree and the objects it names, not every object.

    A stale object of a removed asset stays under the collection prefix. The
    clone does not hold it. It holds every other pushed object, byte for byte.
    """
    first = _push(run_cli, fault_catalog, bucket)
    assert first.exit_code == 0, first.output
    stray = bucket.key("imagery/scene-a/removed.tif")
    bucket.second_writer(stray, b"bytes of a removed asset")
    named = bucket.keys() - {stray}
    target = tmp_path / "clone"

    result = run_cli("clone", bucket.url, str(target), expect=None)

    assert result.exit_code == 0, result.output
    cloned = {
        path.relative_to(target).as_posix()
        for path in target.rglob("*")
        if path.is_file() and ".portolan" not in path.relative_to(target).parts
    }
    assert cloned == {bucket.relative(key) for key in named}
    differ = [
        key
        for key in sorted(named)
        if (target / bucket.relative(key)).read_bytes()
        != bucket.client.get_object(Bucket=bucket.name, Key=key)["Body"].read()
    ]
    assert differ == []
