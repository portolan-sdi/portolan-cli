"""Fault injection for push against a moto S3 server.

obstore is a Rust client. It sends real HTTP requests and does not see boto3
patches, so the harness runs moto in server mode on the loopback interface.
The tests then read the bucket back with boto3. They assert the object set,
the ETags, and the Content-Type headers that a reader of the bucket sees.

``PutFaults`` wraps ``obstore.put`` and ``obstore.put_async``. Every put site
in ``portolan_cli/sync`` calls them through the ``obstore`` module, so one
``monkeypatch.setattr`` reaches all of them. A test arms an action on the Nth
put whose key matches a predicate. The action raises ``ConnectionError`` to
simulate a dropped connection, or calls ``second_writer`` to simulate a
concurrent push.

``moto_server``, ``s3_bucket``, and ``run_cli`` are private copies of the
shared fixtures that G2 adds to ``tests/conftest.py`` for #1035.
"""

from __future__ import annotations

import logging
import shutil
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner, Result

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
BUILDINGS = FIXTURES / "realdata" / "open-buildings.parquet"
SCENE_A = FIXTURES / "raster" / "valid" / "rgb.tif"
SCENE_B = FIXTURES / "raster" / "valid" / "singleband.tif"
# Other valid COGs. ``change_scenes`` swaps them in, so the imagery
# collection gets a new version with two changed Assets.
SCENE_A_V2 = FIXTURES / "raster" / "valid" / "nodata.tif"
SCENE_B_V2 = FIXTURES / "raster" / "valid" / "float32.tif"
PREFIX = "cat"


@dataclass(frozen=True)
class Bucket:
    """One moto bucket and a boto3 client that reads it."""

    name: str
    endpoint: str
    client: Any

    @property
    def url(self) -> str:
        """Return the push destination for the catalog prefix."""
        return f"s3://{self.name}/{PREFIX}"

    @staticmethod
    def key(relative: str) -> str:
        """Return the object key of a catalog-relative path."""
        return f"{PREFIX}/{relative}"

    @staticmethod
    def relative(key: str) -> str:
        """Return the catalog-relative path of an object key."""
        return key.removeprefix(f"{PREFIX}/")

    def keys(self) -> set[str]:
        """Return every object key in the bucket."""
        found: set[str] = set()
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.name):
            found.update(obj["Key"] for obj in page.get("Contents", []))
        return found

    def head(self, key: str) -> dict[str, str]:
        """Return the ETag and the Content-Type that a reader of ``key`` sees."""
        response = self.client.head_object(Bucket=self.name, Key=key)
        return {"ETag": str(response["ETag"]), "ContentType": str(response["ContentType"])}

    def second_writer(self, key: str, body: bytes) -> str:
        """Overwrite ``key`` as another writer does, and return the new ETag."""
        response = self.client.put_object(Bucket=self.name, Key=key, Body=body)
        return str(response["ETag"])


@dataclass
class PutCall:
    """One call to ``obstore.put`` or ``obstore.put_async``."""

    key: str
    kwargs: dict[str, Any]


@dataclass
class _Trigger:
    nth: int
    when: Callable[[str], bool]
    action: Callable[[], None]
    seen: int = 0


@dataclass
class PutFaults:
    """Count obstore puts and run an armed action on the Nth matching put."""

    calls: list[PutCall] = field(default_factory=list)
    _triggers: list[_Trigger] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def arm(self, nth: int, when: Callable[[str], bool], action: Callable[[], None]) -> None:
        """Run ``action`` before the ``nth`` put whose key satisfies ``when``."""
        self._triggers.append(_Trigger(nth=nth, when=when, action=action))

    def fail_on(self, nth: int, when: Callable[[str], bool]) -> None:
        """Raise ``ConnectionError`` on the ``nth`` put whose key satisfies ``when``."""

        def drop() -> None:
            raise ConnectionError("injected fault: connection reset by peer")

        self.arm(nth, when, drop)

    def keys(self) -> list[str]:
        """Return the key of each put, in call order."""
        return [call.key for call in self.calls]

    def record(self, key: str, kwargs: dict[str, Any]) -> list[Callable[[], None]]:
        """Record one put and return the actions that fire on it."""
        due: list[Callable[[], None]] = []
        with self._lock:
            self.calls.append(PutCall(key=key, kwargs=dict(kwargs)))
            for trigger in self._triggers:
                if trigger.when(key):
                    trigger.seen += 1
                    if trigger.seen == trigger.nth:
                        due.append(trigger.action)
        return due


@pytest.fixture
def fault_store(monkeypatch: pytest.MonkeyPatch) -> PutFaults:
    """Wrap ``obstore.put`` and ``obstore.put_async`` with a ``PutFaults`` counter."""
    import obstore

    faults = PutFaults()
    real_put = obstore.put
    real_put_async = obstore.put_async

    def put(store: Any, path: str, file: Any, **kwargs: Any) -> Any:
        for action in faults.record(path, kwargs):
            action()
        return real_put(store, path, file, **kwargs)

    async def put_async(store: Any, path: str, file: Any, **kwargs: Any) -> Any:
        for action in faults.record(path, kwargs):
            action()
        return await real_put_async(store, path, file, **kwargs)

    monkeypatch.setattr(obstore, "put", put)
    monkeypatch.setattr(obstore, "put_async", put_async)
    return faults


@pytest.fixture(scope="session")
def moto_server() -> Iterator[str]:
    """Start one moto S3 server on the loopback interface for the session."""
    pytest.importorskip("moto")
    from moto.server import ThreadedMotoServer

    # werkzeug logs each request at INFO. A failed test then prints hundreds
    # of request lines above its assertion.
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def s3_bucket(moto_server: str, monkeypatch: pytest.MonkeyPatch) -> Bucket:
    """Create an empty bucket and point the obstore S3 client at moto."""
    boto3 = pytest.importorskip("boto3")

    # The VM exports a real profile and region. They would win over the
    # moto credentials below.
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    for name, value in {
        "AWS_ENDPOINT_URL": moto_server,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }.items():
        monkeypatch.setenv(name, value)

    client = boto3.client(
        "s3",
        endpoint_url=moto_server,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    name = f"push-faults-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=name)
    return Bucket(name=name, endpoint=moto_server, client=client)


@pytest.fixture
def run_cli() -> Callable[..., Result]:
    """Run the real CLI in process and fail on an exception that escapes it.

    A test that expects exit code 1 must see the CLI report the failure. A
    traceback also gives exit code 1, so it must not pass that test.
    """
    from portolan_cli.cli import cli

    runner = CliRunner()

    def run(*args: str) -> Result:
        result = runner.invoke(cli, list(args))
        if result.exception is not None and not isinstance(result.exception, SystemExit):
            raise result.exception
        return result

    return run


def _build_catalog(root: Path, run: Callable[..., Result]) -> None:
    (root / "buildings").mkdir(parents=True)
    shutil.copy(BUILDINGS, root / "buildings" / "open-buildings.parquet")
    for scene, source in (("scene-a", SCENE_A), ("scene-b", SCENE_B)):
        (root / "imagery" / scene).mkdir(parents=True)
        shutil.copy(source, root / "imagery" / scene / source.name)
    result = run(
        "init",
        str(root),
        "--auto",
        "--title",
        "Fault catalog",
        "--description",
        "Catalog for push fault injection.",
        "--license",
        "CC-BY-4.0",
    )
    assert result.exit_code == 0, result.output
    result = run(
        "add",
        "--portolan-dir",
        str(root),
        str(root / "buildings" / "open-buildings.parquet"),
        str(root / "imagery" / "scene-a" / SCENE_A.name),
        str(root / "imagery" / "scene-b" / SCENE_B.name),
    )
    assert result.exit_code == 0, result.output
    assert (root / "imagery" / "collection.json").is_file(), result.output


@pytest.fixture(scope="session")
def _catalog_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from portolan_cli.cli import cli

    runner = CliRunner()

    def run(*args: str) -> Result:
        return runner.invoke(cli, list(args), catch_exceptions=False)

    root = tmp_path_factory.mktemp("fault-template") / "catalog"
    _build_catalog(root, run)
    return root


@pytest.fixture
def fault_catalog(tmp_path: Path, _catalog_template: Path) -> Path:
    """Return a fresh copy of a catalog with a vector and a raster collection.

    ``init --auto`` and ``add`` build it once per session. The collection
    ``buildings`` holds ``open-buildings.parquet``. The collection ``imagery``
    holds two COG scenes as Items.
    """
    root = tmp_path / "catalog"
    shutil.copytree(_catalog_template, root)
    return root


@pytest.fixture
def change_scenes(run_cli: Callable[..., Result]) -> Callable[[Path], None]:
    """Return a function that gives both COG scenes new bytes and a new version."""

    def change(root: Path) -> None:
        shutil.copy(SCENE_A_V2, root / "imagery" / "scene-a" / SCENE_A.name)
        shutil.copy(SCENE_B_V2, root / "imagery" / "scene-b" / SCENE_B.name)
        result = run_cli(
            "add",
            "--portolan-dir",
            str(root),
            str(root / "imagery" / "scene-a" / SCENE_A.name),
            str(root / "imagery" / "scene-b" / SCENE_B.name),
        )
        assert result.exit_code == 0, result.output

    return change
