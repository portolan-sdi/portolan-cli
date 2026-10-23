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
        manifest = json.loads(_other_writer_manifest(bucket, manifest_key))
        for asset in manifest["versions"][-1]["assets"].values():
            if bucket.key(asset["href"]) == scene_key:
                asset["sha256"] = hashlib.sha256(winner_bytes).hexdigest()
        bucket.second_writer(scene_key, winner_bytes)
        bucket.second_writer(manifest_key, json.dumps(manifest).encode())

    fault_store.arm(1, when=lambda key: key == manifest_key, action=other_push)

    second = _push(run_cli, pushed, bucket)

    assert second.exit_code == 1, second.output
    assert CONFLICT_MESSAGE in second.output
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", {scene_key}) == {}
    assert sorted(key for key in bucket.keys() if "/.portolan/" in key) == []


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

    fault_store.arm(1, when=lambda key: key == manifest_key, action=same_push)

    second = _push(run_cli, pushed, bucket)

    assert second.exit_code == 1, second.output
    assert CONFLICT_MESSAGE in second.output
    written = set(fault_store.keys()[calls_before:]) & before
    assert _bytes_that_disagree_with_the_manifest(bucket, "imagery", written) == {}
