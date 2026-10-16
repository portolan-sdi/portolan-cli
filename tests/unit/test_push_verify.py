"""Tests for `portolan push --verify` (Issue #966).

Push decides to skip an asset from versions.json alone. A store that an older
bug damaged keeps the wrong bytes forever, because the remote current snapshot
records the local sha256. With ``--verify``, push sends a HEAD for each asset
it would skip. It uploads the asset when the object is missing or its size
differs from the ``size_bytes`` that the local current snapshot records.

The tests use an in-memory obstore store, so they do no network I/O.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import obstore as obs
import pytest
from click.testing import CliRunner
from obstore.store import MemoryStore

from portolan_cli.cli import cli
from portolan_cli.sync.push import PushAllResult, PushResult, VerifyReport

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from click.testing import Result

PREFIX = "catalog"
REMOTE = "s3://bucket/catalog"
COLLECTION: dict[str, Any] = {
    "type": "Collection",
    "stac_version": "1.1.0",
    "id": "test",
    "description": "d",
    "license": "CC0-1.0",
    "links": [],
    "extent": {
        "temporal": {"interval": [[None, None]]},
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
    },
}


def _snapshot(version: str, assets: dict[str, bytes]) -> dict[str, Any]:
    """Build one versions.json entry whose assets record real sha256 and size."""
    return {
        "version": version,
        "created": "2026-10-01T00:00:00Z",
        "breaking": False,
        "changes": sorted(assets),
        "assets": {
            name: {
                "sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
                "href": f"test/{name}",
            }
            for name, content in assets.items()
        },
    }


def _versions(current: str, *entries: dict[str, Any]) -> dict[str, Any]:
    """Build a versions.json document."""
    return {"spec_version": "1.0.0", "current_version": current, "versions": list(entries)}


def _write_catalog(root: Path, versions: dict[str, Any], files: dict[str, bytes]) -> None:
    """Write a minimal catalog with one collection named ``test``."""
    (root / ".portolan").mkdir(parents=True, exist_ok=True)
    (root / ".portolan" / "config.yaml").write_text("version: 1\n")
    (root / "catalog.json").write_text(
        json.dumps({"type": "Catalog", "id": "c", "stac_version": "1.1.0", "links": []})
    )
    coll = root / "test"
    coll.mkdir(parents=True, exist_ok=True)
    (coll / "collection.json").write_text(json.dumps(COLLECTION))
    (coll / "versions.json").write_text(json.dumps(versions))
    for name, content in files.items():
        (coll / name).write_bytes(content)


def _counting_head(
    calls: list[str],
) -> Callable[[Any, str], Awaitable[Any]]:
    """Wrap obs.head_async so a test can count the HEAD requests."""
    real_head = obs.head_async

    async def head(store: Any, key: str) -> Any:
        calls.append(key)
        return await real_head(store, key)

    return head


def _assert_one_repair(result: PushResult, store: MemoryStore, expected: bytes) -> None:
    """Assert a push that verified, repaired a.parquet, and pushed no version."""
    assert result.success is True, result.errors
    assert result.versions_pushed == 0
    assert result.verify == VerifyReport(checked=1, mismatched=1)
    assert bytes(obs.get(store, f"{PREFIX}/test/a.parquet").bytes()) == expected


# =============================================================================
# The verify decision
# =============================================================================


class TestVerifySkippedAssets:
    """``_verify_skipped_assets_async`` picks the skipped assets the store lacks."""

    @staticmethod
    def _run(
        store: MemoryStore, local: dict[str, Any], remote: dict[str, Any] | None
    ) -> tuple[frozenset[str], VerifyReport]:
        from portolan_cli.sync.push import _verify_skipped_assets_async

        return asyncio.run(
            _verify_skipped_assets_async(store, PREFIX, local, remote, concurrency=4)
        )

    @pytest.mark.unit
    def test_size_match_keeps_the_skip(self) -> None:
        """An object whose size equals size_bytes stays skipped."""
        content = b"good bytes"
        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": content}))
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/a.parquet", content)

        reupload, report = self._run(store, data, data)

        assert reupload == frozenset()
        assert report == VerifyReport(checked=1, mismatched=0)

    @pytest.mark.unit
    def test_size_mismatch_uploads(self) -> None:
        """An object with another size goes back into the upload set."""
        content = b"good bytes of 1.0.5"
        data = _versions("1.0.5", _snapshot("1.0.5", {"a.parquet": content}))
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/a.parquet", b"stale bytes of 1.0.4, longer")

        reupload, report = self._run(store, data, data)

        assert reupload == frozenset({"test/a.parquet"})
        assert report == VerifyReport(checked=1, mismatched=1)

    @pytest.mark.unit
    def test_missing_object_uploads(self) -> None:
        """An object that the store does not hold goes back into the upload set."""
        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": b"x", "b.parquet": b"y"}))
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/a.parquet", b"x")

        reupload, report = self._run(store, data, data)

        assert reupload == frozenset({"test/b.parquet"})
        assert report == VerifyReport(checked=2, mismatched=1)

    @pytest.mark.unit
    def test_only_skipped_assets_get_a_head(self) -> None:
        """An asset that the snapshot diff already uploads gets no HEAD."""
        local = _versions("1.0.1", _snapshot("1.0.1", {"a.parquet": b"x", "b.parquet": b"new"}))
        remote = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": b"x", "b.parquet": b"old"}))
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/a.parquet", b"x")
        calls: list[str] = []

        with patch("portolan_cli.sync.push.obs.head_async", _counting_head(calls)):
            reupload, report = self._run(store, local, remote)

        assert calls == [f"{PREFIX}/test/a.parquet"]
        assert reupload == frozenset()
        assert report == VerifyReport(checked=1, mismatched=0)

    @pytest.mark.unit
    def test_first_push_sends_no_head(self) -> None:
        """Without a remote versions.json nothing is skipped, so nothing is checked."""
        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": b"x"}))
        calls: list[str] = []

        with patch("portolan_cli.sync.push.obs.head_async", _counting_head(calls)):
            reupload, report = self._run(MemoryStore(), data, None)

        assert calls == []
        assert reupload == frozenset()
        assert report == VerifyReport(checked=0, mismatched=0)

    @pytest.mark.unit
    def test_unknown_size_detects_only_a_missing_object(self) -> None:
        """Without size_bytes, push can only detect a missing object."""
        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": b"x", "b.parquet": b"y"}))
        for asset in data["versions"][0]["assets"].values():
            del asset["size_bytes"]
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/a.parquet", b"other size")

        reupload, report = self._run(store, data, data)

        assert reupload == frozenset({"test/b.parquet"})
        assert report == VerifyReport(checked=2, mismatched=1)

    @pytest.mark.unit
    def test_heads_run_with_bounded_concurrency(self) -> None:
        """No more HEAD requests run at once than the concurrency limit."""
        from portolan_cli.sync.push import _verify_skipped_assets_async

        files = {f"f{i}.parquet": b"x" for i in range(20)}
        data = _versions("1.0.0", _snapshot("1.0.0", files))
        in_flight = 0
        peak = 0

        async def slow_head(store: Any, key: str) -> dict[str, Any]:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
            return {"size": 1}

        with patch("portolan_cli.sync.push.obs.head_async", slow_head):
            reupload, report = asyncio.run(
                _verify_skipped_assets_async(MemoryStore(), PREFIX, data, data, concurrency=3)
            )

        assert peak == 3
        assert reupload == frozenset()
        assert report == VerifyReport(checked=20, mismatched=0)

    @pytest.mark.unit
    def test_head_error_other_than_not_found_fails_the_verify(self) -> None:
        """A HEAD that fails for another reason raises, and it does not pass as a match."""
        from portolan_cli.errors import PushVerifyError

        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": b"x"}))

        async def denied_head(store: Any, key: str) -> dict[str, Any]:
            raise PermissionError("403 Forbidden")

        with (
            patch("portolan_cli.sync.push.obs.head_async", denied_head),
            pytest.raises(PushVerifyError, match="test/a.parquet"),
        ):
            self._run(MemoryStore(), data, data)


class TestReuploadOverridesSkip:
    """``_get_assets_to_upload`` uploads an href in ``reupload`` despite a sha256 match."""

    @pytest.mark.unit
    def test_reupload_href_is_uploaded(self, tmp_path: Path) -> None:
        """A verified mismatch re-enters the upload set."""
        from portolan_cli.sync.push import _get_assets_to_upload

        (tmp_path / "test").mkdir()
        a_file = tmp_path / "test" / "a.parquet"
        a_file.write_bytes(b"x")
        (tmp_path / "test" / "b.parquet").write_bytes(b"y")
        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": b"x", "b.parquet": b"y"}))

        assets = _get_assets_to_upload(tmp_path, data, data, reupload=frozenset({"test/a.parquet"}))

        assert assets == [a_file.resolve()]


# =============================================================================
# push_async with --verify
# =============================================================================


class TestPushAsyncVerify:
    """push_async threads ``verify`` through both the version and no-version paths."""

    @staticmethod
    def _push(root: Path, store: MemoryStore, **kwargs: Any) -> PushResult:
        from portolan_cli.sync.push import push_async

        with patch("portolan_cli.sync.push.setup_store", return_value=(store, PREFIX)):
            return asyncio.run(push_async(root, "test", REMOTE, adaptive=False, **kwargs))

    @staticmethod
    def _seeded(tmp_path: Path, stored: bytes) -> tuple[Path, MemoryStore, bytes, dict[str, Any]]:
        """A catalog whose remote versions.json equals the local one."""
        good = b"good bytes of version 1.0.5"
        data = _versions("1.0.5", _snapshot("1.0.5", {"a.parquet": good}))
        root = tmp_path / "catalog"
        _write_catalog(root, data, {"a.parquet": good})
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/versions.json", json.dumps(data).encode())
        obs.put(store, f"{PREFIX}/test/a.parquet", stored)
        return root, store, good, data

    @pytest.mark.unit
    def test_verify_without_new_version_repairs_the_object(self, tmp_path: Path) -> None:
        """The repair case. No version is local only, and the object has the wrong size."""
        root, store, good, data = self._seeded(tmp_path, b"stale bytes of 1.0.4")

        result = self._push(root, store, verify=True)

        _assert_one_repair(result, store, good)
        remote = json.loads(bytes(obs.get(store, f"{PREFIX}/test/versions.json").bytes()))
        assert remote["current_version"] == "1.0.5"
        assert remote["versions"] == data["versions"]

    @pytest.mark.unit
    def test_verify_with_matching_store_uploads_no_asset(self, tmp_path: Path) -> None:
        """When every object matches, the no-version path stays metadata only."""
        root, store, good, _ = self._seeded(tmp_path, b"good bytes of version 1.0.5")
        puts: list[str] = []
        real_put = obs.put_async

        async def counting_put(s: Any, key: str, *args: Any, **kwargs: Any) -> Any:
            puts.append(key)
            return await real_put(s, key, *args, **kwargs)

        with patch("portolan_cli.sync.push.obs.put_async", counting_put):
            result = self._push(root, store, verify=True)

        assert result.success is True, result.errors
        assert result.verify == VerifyReport(checked=1, mismatched=0)
        assert f"{PREFIX}/test/a.parquet" not in puts
        assert f"{PREFIX}/test/versions.json" not in puts

    @pytest.mark.unit
    def test_verify_with_new_version_checks_skipped_assets(self, tmp_path: Path) -> None:
        """A push with a new version also repairs an unchanged asset with the wrong size."""
        a = b"unchanged bytes of a"
        b_old, b_new = b"old b", b"new bytes of b"
        local = _versions(
            "1.0.1",
            _snapshot("1.0.0", {"a.parquet": a, "b.parquet": b_old}),
            _snapshot("1.0.1", {"a.parquet": a, "b.parquet": b_new}),
        )
        remote = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": a, "b.parquet": b_old}))
        root = tmp_path / "catalog"
        _write_catalog(root, local, {"a.parquet": a, "b.parquet": b_new})
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/versions.json", json.dumps(remote).encode())
        obs.put(store, f"{PREFIX}/test/a.parquet", b"damaged")
        obs.put(store, f"{PREFIX}/test/b.parquet", b_old)

        result = self._push(root, store, verify=True)

        assert result.success is True, result.errors
        assert result.versions_pushed == 1
        assert result.verify == VerifyReport(checked=1, mismatched=1)
        assert bytes(obs.get(store, f"{PREFIX}/test/a.parquet").bytes()) == a
        assert bytes(obs.get(store, f"{PREFIX}/test/b.parquet").bytes()) == b_new

    @pytest.mark.unit
    @pytest.mark.parametrize("force", [False, True], ids=["plain", "force"])
    def test_no_flag_sends_no_head(self, tmp_path: Path, force: bool) -> None:
        """Without --verify, push sends zero HEAD requests and keeps the old behavior."""
        root, store, _, _ = self._seeded(tmp_path, b"stale bytes of 1.0.4")
        calls: list[str] = []

        with patch("portolan_cli.sync.push.obs.head_async", _counting_head(calls)):
            result = self._push(root, store, force=force)

        assert calls == []
        assert result.verify is None
        assert bytes(obs.get(store, f"{PREFIX}/test/a.parquet").bytes()) == (
            b"stale bytes of 1.0.4"
        )

    @pytest.mark.unit
    def test_verify_does_not_run_on_a_conflict(self, tmp_path: Path) -> None:
        """A conflict stops the push before any HEAD request."""
        from portolan_cli.sync.push import PushConflictError

        root, store, good, data = self._seeded(tmp_path, b"stale")
        remote = dict(data)
        remote["versions"] = [*data["versions"], _snapshot("1.0.6", {"a.parquet": b"z"})]
        remote["current_version"] = "1.0.6"
        obs.put(store, f"{PREFIX}/test/versions.json", json.dumps(remote).encode())
        calls: list[str] = []

        with (
            patch("portolan_cli.sync.push.obs.head_async", _counting_head(calls)),
            pytest.raises(PushConflictError),
        ):
            self._push(root, store, verify=True)

        assert calls == []


class TestRepairOnlyPath:
    """A repair without a new version uploads the flagged assets only.

    Push then keeps the remote versions.json as is. The local manifest can
    differ from the remote manifest with the same version list. A plain push
    publishes none of that difference, so a repair must not publish it either.
    """

    @staticmethod
    def _push(root: Path, store: MemoryStore, **kwargs: Any) -> PushResult:
        return TestPushAsyncVerify._push(root, store, **kwargs)

    @pytest.mark.unit
    def test_repair_keeps_the_remote_manifest_and_skips_unflagged_assets(
        self, tmp_path: Path
    ) -> None:
        """Reviewer probe. Local current is 1.0.0, remote current is 1.0.1.

        Both sides list the same versions. The store has no a.parquet. The
        local b.parquet differs from the remote current b.parquet, but verify
        does not flag it. Push must upload a.parquet only, and it must not
        write versions.json.
        """
        a = b"a bytes"
        b_one, b_two = b"b-one", b"b-two-longer"
        entries = (
            _snapshot("1.0.0", {"a.parquet": a, "b.parquet": b_one}),
            _snapshot("1.0.1", {"a.parquet": a, "b.parquet": b_two}),
        )
        local = _versions("1.0.0", *entries)
        remote = _versions("1.0.1", *entries)
        root = tmp_path / "catalog"
        _write_catalog(root, local, {"a.parquet": a, "b.parquet": b_one})
        store = MemoryStore()
        remote_bytes = json.dumps(remote).encode()
        obs.put(store, f"{PREFIX}/test/versions.json", remote_bytes)
        obs.put(store, f"{PREFIX}/test/b.parquet", b_two)

        result = self._push(root, store, verify=True)

        _assert_one_repair(result, store, a)
        assert bytes(obs.get(store, f"{PREFIX}/test/b.parquet").bytes()) == b_two
        stored_manifest = bytes(obs.get(store, f"{PREFIX}/test/versions.json").bytes())
        assert stored_manifest == remote_bytes
        assert json.loads(stored_manifest)["current_version"] == "1.0.1"

    @pytest.mark.unit
    def test_repair_does_not_write_versions_json(self, tmp_path: Path) -> None:
        """The repair-only path never calls the manifest write."""
        root, store, good, _ = TestPushAsyncVerify._seeded(tmp_path, b"stale bytes of 1.0.4")

        with patch("portolan_cli.sync.push._upload_versions_json_async") as mock_manifest:
            result = self._push(root, store, verify=True)

        assert result.success is True, result.errors
        mock_manifest.assert_not_called()
        assert bytes(obs.get(store, f"{PREFIX}/test/a.parquet").bytes()) == good

    @pytest.mark.unit
    def test_repair_upload_failure_fails_and_deletes_nothing(self, tmp_path: Path) -> None:
        """A failed repair upload reports failure, and push deletes no key."""
        root, store, _, _ = TestPushAsyncVerify._seeded(tmp_path, b"stale bytes of 1.0.4")

        async def failed_upload(*args: Any, **kwargs: Any) -> Any:
            from portolan_cli.sync.push import UploadMetrics

            return 0, ["Failed to upload test/a.parquet: boom"], [], UploadMetrics()

        with (
            patch("portolan_cli.sync.push._upload_assets_async", failed_upload),
            patch("portolan_cli.sync.push._cleanup_uploaded_assets") as mock_cleanup,
            patch("portolan_cli.sync.push._upload_versions_json_async") as mock_manifest,
        ):
            result = self._push(root, store, verify=True)

        assert result.success is False
        assert result.errors == ["Failed to upload test/a.parquet: boom"]
        assert result.verify == VerifyReport(checked=1, mismatched=1)
        mock_cleanup.assert_not_called()
        mock_manifest.assert_not_called()
        assert bytes(obs.get(store, f"{PREFIX}/test/a.parquet").bytes()) == (
            b"stale bytes of 1.0.4"
        )

    @pytest.mark.unit
    @pytest.mark.parametrize("force", [False, True], ids=["repair-only", "force"])
    def test_repaired_count_excludes_a_skipped_optional_derivative(
        self, tmp_path: Path, force: bool, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A flagged optional derivative that is missing on disk does not count as repaired."""
        a = b"a bytes"
        tiles = b"pmtiles bytes"
        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": a, "a.pmtiles": tiles}))
        root = tmp_path / "catalog"
        _write_catalog(root, data, {"a.parquet": a})
        store = MemoryStore()
        obs.put(store, f"{PREFIX}/test/versions.json", json.dumps(data).encode())

        result = self._push(root, store, verify=True, force=force)

        assert result.success is True, result.errors
        assert result.verify == VerifyReport(checked=2, mismatched=2)
        assert bytes(obs.get(store, f"{PREFIX}/test/a.parquet").bytes()) == a
        out = capsys.readouterr().out
        assert "Repaired 1 asset(s) (no new versions)" in out
        assert "Repaired 2 asset(s)" not in out


class TestSkipRuleHasOneSource:
    """Verify and the upload rule read the same per-href view of the local snapshot."""

    @staticmethod
    def _duplicate_href(first: bytes, second: bytes) -> dict[str, Any]:
        """A local snapshot with two entries, x and y, for the same href."""
        snap = _snapshot("1.0.0", {"x": first, "y": second})
        for entry in snap["assets"].values():
            entry["href"] = "test/a.parquet"
        return _versions("1.0.0", snap)

    @pytest.mark.unit
    @pytest.mark.parametrize("order", ["remote-first", "remote-last"])
    def test_duplicate_href_is_either_verified_or_uploaded(
        self, tmp_path: Path, order: str
    ) -> None:
        """An href is in the verify set exactly when the upload rule skips it."""
        from portolan_cli.sync.push import _get_assets_to_upload, _skipped_asset_sizes

        aaa, bbb = b"AAA", b"BBB-longer"
        local = (
            self._duplicate_href(aaa, bbb)
            if order == "remote-first"
            else self._duplicate_href(bbb, aaa)
        )
        remote = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": aaa}))
        (tmp_path / "test").mkdir()
        a_file = tmp_path / "test" / "a.parquet"
        a_file.write_bytes(bbb)

        verify_set = _skipped_asset_sizes(local, remote)
        uploads = _get_assets_to_upload(tmp_path, local, remote)

        if order == "remote-first":
            # The last entry (y, BBB) wins. It differs, so it uploads and gets no HEAD.
            assert verify_set == {}
            assert uploads == [a_file.resolve()]
        else:
            # The last entry (y, AAA) wins. It matches, so it skips and gets a HEAD.
            assert verify_set == {"test/a.parquet": len(aaa)}
            assert uploads == []


class TestPushAllVerifyCounts:
    """A catalog-wide push sums the verify counts of every collection that verified."""

    @pytest.mark.unit
    def test_failed_collection_after_verify_adds_its_counts(self) -> None:
        """A collection that verified and then failed its upload still adds its counts."""
        from portolan_cli.sync.push import _push_all_process_result

        stats: dict[str, Any] = {"failed": 0, "errors": {}, "verify": VerifyReport()}
        failed = PushResult(
            success=False,
            files_uploaded=0,
            versions_pushed=0,
            errors=["boom"],
            verify=VerifyReport(checked=4, mismatched=2),
        )

        _push_all_process_result("test", failed, None, 1, 1, stats)

        assert stats["failed"] == 1
        assert stats["errors"] == {"test": ["boom"]}
        assert stats["verify"] == VerifyReport(checked=4, mismatched=2)


class TestDryRunVerifyNetworkIsolation:
    """``push --dry-run --verify`` sends no network request (sync.md dry-run rule)."""

    @pytest.mark.unit
    def test_dry_run_verify_sends_no_request(self, tmp_path: Path) -> None:
        """Dry run returns before setup_store, the remote fetch, and any HEAD."""
        from portolan_cli.sync.push import push

        good = b"good"
        data = _versions("1.0.0", _snapshot("1.0.0", {"a.parquet": good}))
        root = tmp_path / "catalog"
        _write_catalog(root, data, {"a.parquet": good})

        with (
            patch("portolan_cli.sync.push.setup_store") as mock_setup,
            patch("portolan_cli.sync.push._fetch_remote_versions_async") as mock_fetch,
            patch("portolan_cli.sync.push.obs.head_async") as mock_head,
        ):
            result = push(root, "test", REMOTE, dry_run=True, verify=True)

        mock_setup.assert_not_called()
        mock_fetch.assert_not_called()
        mock_head.assert_not_called()
        assert result.dry_run is True
        assert result.verify is None


# =============================================================================
# CLI
# =============================================================================


def _invoke_push(tmp_path: Path, args: list[str]) -> Result:
    """Run ``portolan push`` in a minimal catalog with one collection."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        path = Path.cwd()
        (path / ".portolan").mkdir(parents=True, exist_ok=True)
        (path / ".portolan" / "config.yaml").write_text("version: '1.0'\n")
        (path / "col1").mkdir()
        (path / "col1" / "versions.json").write_text(json.dumps({"versions": []}))
        with patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://test/catalog"}):
            return runner.invoke(cli, ["push", "--catalog", ".", "--json", *args])


class TestCliVerifyFlag:
    """``portolan push --verify`` reaches the library and reports the counts."""

    @pytest.mark.unit
    @pytest.mark.parametrize("flag", [[], ["--verify"]], ids=["default", "verify"])
    @patch("portolan_cli.sync.push.push_async")
    def test_single_collection_passes_verify(
        self, mock_push: MagicMock, tmp_path: Path, flag: list[str]
    ) -> None:
        """The flag reaches push_async, and it defaults to False."""

        async def fake(**kwargs: Any) -> PushResult:
            return PushResult(
                success=True,
                files_uploaded=1,
                versions_pushed=0,
                verify=VerifyReport(checked=3, mismatched=1) if kwargs["verify"] else None,
            )

        mock_push.side_effect = fake
        result = _invoke_push(tmp_path, ["-c", "col1", *flag])

        assert result.exit_code == 0, result.output
        assert mock_push.call_args.kwargs["verify"] is bool(flag)
        envelope = json.loads(result.output)
        if flag:
            assert envelope["data"]["verify"] == {"checked": 3, "mismatched": 1}
        else:
            assert "verify" not in envelope["data"]

    @pytest.mark.unit
    @patch("portolan_cli.sync.push.push_all_collections")
    def test_catalog_wide_passes_verify(self, mock_push_all: MagicMock, tmp_path: Path) -> None:
        """A catalog-wide push forwards --verify and reports the totals in JSON."""
        mock_push_all.return_value = PushAllResult(
            success=True,
            total_collections=1,
            successful_collections=1,
            failed_collections=0,
            total_files_uploaded=1,
            total_versions_pushed=0,
            verify=VerifyReport(checked=5, mismatched=2),
        )
        result = _invoke_push(tmp_path, ["--verify"])

        assert result.exit_code == 0, result.output
        assert mock_push_all.call_args.kwargs["verify"] is True
        assert json.loads(result.output)["data"]["verify"] == {"checked": 5, "mismatched": 2}
