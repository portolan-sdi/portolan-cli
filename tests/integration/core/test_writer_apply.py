"""Apply a portolan-core write plan to real sinks, then validate the result (#1133)."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from typing import TYPE_CHECKING

import obstore
import pytest
from obstore.store import LocalStore, MemoryStore
from rashid_core.api import ValidationOptions, validate

from portolan_core.writer import (
    LocalSink,
    ObstoreSink,
    PutOperation,
    WritePlan,
    apply_plan,
    plan_catalog,
)
from tests.support.core_catalog import (
    GOLDEN_DIR,
    golden_documentation,
    golden_documents,
    golden_files,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

pytestmark = pytest.mark.integration


def _plan(prior_managed_keys: Sequence[str] = (), key_prefix: str = "") -> WritePlan:
    return plan_catalog(
        golden_documents(),
        golden_documentation(),
        prior_managed_keys=prior_managed_keys,
        key_prefix=key_prefix,
    )


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_git_checks_out_the_golden_files_with_lf_line_endings() -> None:
    """The writer writes LF. A CRLF checkout on Windows would break each byte comparison."""
    files = sorted(str(path) for path in GOLDEN_DIR.rglob("*") if path.is_file())
    result = subprocess.run(
        ["git", "check-attr", "eol", "--", *files],
        cwd=GOLDEN_DIR,
        capture_output=True,
        text=True,
        check=True,
    )

    eol = {line.rpartition(": ")[2] for line in result.stdout.splitlines()}
    assert len(result.stdout.splitlines()) == len(files)
    assert eol == {"lf"}


def test_local_sink_writes_the_golden_catalog(tmp_path: Path) -> None:
    apply_plan(_plan(), LocalSink(tmp_path))

    assert _tree(tmp_path) == golden_files()


def test_local_sink_deletes_prior_managed_keys(tmp_path: Path) -> None:
    stale = tmp_path / "removed" / "collection.json"
    stale.parent.mkdir()
    stale.write_text("{}", encoding="utf-8")
    data = tmp_path / "removed" / "data.parquet"
    data.write_bytes(b"PAR1")

    apply_plan(
        _plan(prior_managed_keys=["removed/collection.json", "removed/README.md"]),
        LocalSink(tmp_path),
    )

    assert not stale.exists()
    assert data.read_bytes() == b"PAR1"


def test_local_sink_deletes_a_symlink_and_not_its_target(tmp_path: Path) -> None:
    data = tmp_path / "data.parquet"
    data.write_bytes(b"PAR1")
    (tmp_path / "old.json").symlink_to(data)

    LocalSink(tmp_path).delete("old.json")

    assert not (tmp_path / "old.json").is_symlink()
    assert data.read_bytes() == b"PAR1"


def test_local_sink_replaces_a_symlink_and_not_its_target(tmp_path: Path) -> None:
    data = tmp_path / "data.parquet"
    data.write_bytes(b"PAR1")
    (tmp_path / "old.json").symlink_to(data)

    LocalSink(tmp_path).put("old.json", b"{}", "application/json")

    assert not (tmp_path / "old.json").is_symlink()
    assert (tmp_path / "old.json").read_bytes() == b"{}"
    assert data.read_bytes() == b"PAR1"


posix_modes = pytest.mark.skipif(
    sys.platform == "win32", reason="Windows keeps a read-only flag, not POSIX permission bits"
)


@posix_modes
def test_local_sink_keeps_the_mode_of_a_replaced_file(tmp_path: Path) -> None:
    target = tmp_path / "catalog.json"
    target.write_bytes(b"{}")
    target.chmod(0o644)

    LocalSink(tmp_path).put("catalog.json", b"{ }", "application/json")

    assert stat.S_IMODE(target.stat().st_mode) == 0o644


@posix_modes
def test_local_sink_never_creates_a_temporary_copy_wider_than_a_private_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "catalog.json"
    target.write_bytes(b"{}")
    target.chmod(0o600)
    modes: list[int] = []
    real_open = os.open

    def spy(path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        modes.append(mode)
        return real_open(path, flags, mode)

    monkeypatch.setattr(os, "open", spy)
    LocalSink(tmp_path).put("catalog.json", b"{ }", "application/json")

    assert modes == [0o600]
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_local_sink_writes_a_file_name_near_the_length_limit(tmp_path: Path) -> None:
    name = "a" * 245 + ".json"  # 250 bytes, under the 255-byte limit of common file systems

    LocalSink(tmp_path).put(name, b"{}", "application/json")

    assert (tmp_path / name).read_bytes() == b"{}"
    assert [path.name for path in tmp_path.iterdir()] == [name]


@posix_modes
def test_local_sink_gives_a_new_file_the_umask_default_mode(tmp_path: Path) -> None:
    umask = os.umask(0o022)
    try:
        LocalSink(tmp_path).put("catalog.json", b"{}", "application/json")
    finally:
        os.umask(umask)

    assert stat.S_IMODE((tmp_path / "catalog.json").stat().st_mode) == 0o644


def test_local_sink_refuses_a_key_outside_its_root(tmp_path: Path) -> None:
    plan = WritePlan(operations=(PutOperation("../escape.json", b"{}", "application/json"),))

    with pytest.raises(ValueError, match="escape.json"):
        apply_plan(plan, LocalSink(tmp_path / "root"))
    assert not (tmp_path / "escape.json").exists()


def test_obstore_sink_writes_the_golden_catalog_to_a_memory_store() -> None:
    store = MemoryStore()

    apply_plan(_plan(key_prefix="catalogs/demo"), ObstoreSink(store))

    listed = sorted(meta["path"] for batch in obstore.list(store) for meta in batch)
    assert listed == sorted(f"catalogs/demo/{key}" for key in golden_files())
    for key, expected in golden_files().items():
        assert bytes(obstore.get(store, f"catalogs/demo/{key}").bytes()) == expected


def test_obstore_sink_sets_the_content_type(tmp_path: Path) -> None:
    store = MemoryStore()

    apply_plan(_plan(), ObstoreSink(store))

    result = obstore.get(store, "environment/air-quality/station-north/station-north.json")
    assert result.attributes["Content-Type"] == "application/geo+json"


def test_obstore_sink_deletes_on_a_local_store_and_tolerates_a_missing_key(
    tmp_path: Path,
) -> None:
    store = LocalStore(tmp_path)
    obstore.put(store, "removed/collection.json", b"{}")  # noqa: TID251 - test setup

    apply_plan(
        _plan(prior_managed_keys=["removed/collection.json", "never/written.json"]),
        ObstoreSink(store),
    )

    assert not (tmp_path / "removed" / "collection.json").exists()
    assert _tree(tmp_path) == golden_files()


def test_obstore_sink_raises_when_a_404_leaves_the_object_in_place(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The Source Cooperative proxy answers a bulk delete on an account with
    # 404 NoSuchBucket. obstore reports that 404 as FileNotFoundError.
    store = MemoryStore()
    obstore.put(store, "parks/collection.json", b"{}")  # noqa: TID251 - test setup

    def bucket_not_found(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError("NoSuchBucket")

    monkeypatch.setattr(obstore, "delete", bucket_not_found)

    with pytest.raises(FileNotFoundError, match="NoSuchBucket"):
        ObstoreSink(store).delete("parks/collection.json")
    ObstoreSink(store).delete("never/written.json")


def test_the_validator_accepts_the_written_catalog(tmp_path: Path) -> None:
    apply_plan(_plan(), LocalSink(tmp_path))

    result = subprocess.run(
        [sys.executable, "-m", "rashid_core", "check", "--no-data", "--json", str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    report = json.loads(result.stdout)
    errors = [f for f in report["findings"] if f["severity"] == "error"]
    in_process = validate(tmp_path, options=ValidationOptions(data=False))

    assert errors == []
    assert result.returncode == 0, result.stderr
    assert in_process.errors == []
    assert sorted(f["rule_id"] for f in report["findings"]) == sorted(
        f.rule_id for f in in_process.findings
    )
