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
import json
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import Result

if TYPE_CHECKING:
    from tests.integration.conftest import Bucket, PutFaults

pytestmark = [pytest.mark.integration]

RunCli = Callable[..., Result]

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
    return run_cli("push", bucket.url, "--catalog", str(root), *extra)


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


@pytest.fixture
def pushed(
    fault_catalog: Path,
    s3_bucket: Bucket,
    run_cli: RunCli,
    fault_store: PutFaults,
    change_scenes: Callable[[Path], None],
) -> Path:
    """Push the catalog once, then record a new imagery version locally."""
    first = _push(run_cli, fault_catalog, s3_bucket)
    assert first.exit_code == 0, first.output
    change_scenes(fault_catalog)
    return fault_catalog


@pytest.mark.xfail(
    strict=True,
    reason="#965: rollback deletes keys that the remote current version still uses",
)
def test_failed_second_push_keeps_first_push_objects(
    pushed: Path, s3_bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """A dropped connection on the second Asset put leaves push 1 readable."""
    after_first = s3_bucket.keys()
    manifest = s3_bucket.head(s3_bucket.key("imagery/versions.json"))
    fault_store.fail_on(2, when=_is_scene)

    second = _push(run_cli, pushed, s3_bucket)

    assert second.exit_code == 1, second.output
    assert "injected fault" in second.output
    assert s3_bucket.head(s3_bucket.key("imagery/versions.json")) == manifest
    assert sorted(after_first - s3_bucket.keys()) == []
    assert s3_bucket.keys() == after_first


@pytest.mark.xfail(
    strict=True,
    reason="#971: the conflict rollback deletes keys that the other writer's manifest uses",
)
def test_conflict_deletes_nothing_of_the_other_writer(
    pushed: Path, s3_bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """A push that loses the versions.json race deletes no key."""
    manifest_key = s3_bucket.key("imagery/versions.json")
    before = s3_bucket.keys()
    other: dict[str, str] = {}

    def other_push() -> None:
        other["etag"] = s3_bucket.second_writer(
            manifest_key, _other_writer_manifest(s3_bucket, manifest_key)
        )

    fault_store.arm(1, when=_is_scene, action=other_push)

    second = _push(run_cli, pushed, s3_bucket)

    assert second.exit_code == 1, second.output
    assert CONFLICT_MESSAGE in second.output
    assert s3_bucket.head(manifest_key)["ETag"] == other["etag"]
    assert sorted(before - s3_bucket.keys()) == []


@pytest.mark.xfail(
    strict=True,
    reason="#971: the root versions.json write has no ETag precondition",
)
def test_root_versions_json_is_conditional(
    pushed: Path, s3_bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """Another writer's root versions.json survives a concurrent push.

    The collection manifests use ``mode={"e_tag": ...}``. The root manifest
    does not, so the push overwrites the other writer without a conflict. The
    push must read the root ETag before its first upload.
    """
    root_key = s3_bucket.key("versions.json")
    other: dict[str, str] = {}

    def other_push() -> None:
        other["etag"] = s3_bucket.second_writer(root_key, b'{"other_writer": true}')

    fault_store.arm(1, when=_is_scene, action=other_push)

    second = _push(run_cli, pushed, s3_bucket)

    assert second.exit_code == 1, second.output
    assert CONFLICT_MESSAGE in second.output
    assert s3_bucket.head(root_key)["ETag"] == other["etag"]


@pytest.mark.xfail(strict=True, reason="#807: push sets no Content-Type on any object")
def test_every_object_has_a_content_type(
    fault_catalog: Path, s3_bucket: Bucket, run_cli: RunCli
) -> None:
    """Every pushed object carries the media type of its suffix."""
    result = _push(run_cli, fault_catalog, s3_bucket)
    assert result.exit_code == 0, result.output

    keys = s3_bucket.keys()
    assert {Path(key).suffix for key in keys} >= {".parquet", ".json", ".tif", ".md"}
    actual = {key: s3_bucket.head(key)["ContentType"] for key in keys}
    expected = {key: CONTENT_TYPES.get(Path(key).suffix, "<no rule>") for key in keys}
    assert actual == expected


@pytest.mark.xfail(strict=True, reason="#1008: the upload thread pool is never shut down")
def test_thread_pool_is_shut_down(
    fault_catalog: Path,
    s3_bucket: Bucket,
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

    result = _push(run_cli, fault_catalog, s3_bucket, "--collection", "imagery")

    assert result.exit_code == 0, result.output
    assert len(pools) == 1
    assert [pool.shutdowns for pool in pools] == [1]


@pytest.mark.unit
@pytest.mark.xfail(
    strict=True,
    raises=ImportError,
    reason="#908: no multipart_chunk_size helper, so parts stay at 5 MiB",
)
def test_multipart_chunk_size_keeps_parts_under_10000() -> None:
    """Every size up to 5 TiB fits in 10,000 parts of at least 5 MiB."""
    from portolan_cli.sync.upload import multipart_chunk_size

    for size in (1, 5 * 2**20, 48 * 2**30, 6 * 2**30 * 10, 5 * 2**40):
        chunk = multipart_chunk_size(size)
        assert chunk >= 5 * 2**20, size
        assert chunk * 10_000 >= size, size
    assert multipart_chunk_size(6 * 2**30) * 10_000 >= 6 * 2**30
    assert multipart_chunk_size(1024) == 5 * 2**20


@pytest.mark.xfail(
    strict=True,
    raises=ImportError,
    reason="#908: push passes no chunk_size to the multipart put",
)
def test_multipart_put_passes_the_chunk_size(
    fault_catalog: Path,
    s3_bucket: Bucket,
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

    result = _push(run_cli, fault_catalog, s3_bucket, "--collection", "imagery")
    assert result.exit_code == 0, result.output

    scene_puts = [call for call in fault_store.calls if _is_scene(call.key)]
    assert len(scene_puts) == 2
    for call in scene_puts:
        local = fault_catalog / s3_bucket.relative(call.key)
        assert call.kwargs.get("chunk_size") == multipart_chunk_size(local.stat().st_size)


def test_dry_run_sends_nothing(
    fault_catalog: Path, s3_bucket: Bucket, run_cli: RunCli, fault_store: PutFaults
) -> None:
    """``push --dry-run`` writes no object."""
    result = _push(run_cli, fault_catalog, s3_bucket, "--dry-run")

    assert result.exit_code == 0, result.output
    assert "would push up to 1 version(s)" in result.output
    assert fault_store.keys() == []
    assert s3_bucket.keys() == set()


@pytest.mark.xfail(
    strict=True,
    reason="#972: a credential without list permission gets no single clear warning",
)
def test_list_permission_denied_warns_once(
    fault_catalog: Path,
    s3_bucket: Bucket,
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

    result = _push(run_cli, fault_catalog, s3_bucket)

    assert result.exit_code == 0, result.output
    warnings = [line for line in result.output.splitlines() if "s3:ListBucket" in line]
    assert len(warnings) == 1, result.output
    assert listed != []
