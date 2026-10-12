"""Tests for the plugin conformance checks in `portolan_cli.testing` (#947)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.backends.json_file import JsonFileBackend
from portolan_cli.testing import ConformanceError, check_backend
from portolan_cli.versions import Version

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


class _MemoryBackend:
    """A backend that keeps versions in a dict."""

    def __init__(self, catalog_root: Path | None = None) -> None:
        self.versions: dict[str, list[Version]] = {}

    def get_current_version(self, collection: str) -> Version:
        return self.versions[collection][-1]

    def list_versions(self, collection: str) -> list[Version]:
        return list(self.versions.get(collection, []))

    def publish(
        self,
        collection: str,
        assets: dict[str, str],
        schema: Any,
        breaking: bool,
        message: str,
        removed: set[str] | None = None,
        version: str | None = None,
    ) -> Version:
        history = self.versions.setdefault(collection, [])
        created = Version(
            version=version or f"{len(history) + 1}.0.0",
            created=datetime.now(timezone.utc),
            breaking=breaking,
            assets={},
            changes=sorted(assets),
            message=message,
        )
        history.append(created)
        return created

    def rollback(self, collection: str, target_version: str) -> Version:
        raise NotImplementedError

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        raise NotImplementedError

    def check_drift(self, collection: str) -> dict[str, Any]:
        current = self.versions[collection][-1].version
        return {
            "has_drift": False,
            "local_version": current,
            "remote_version": current,
            "message": "in sync",
        }


class _ListReturnsStrings(_MemoryBackend):
    def list_versions(self, collection: str) -> list[Version]:
        return [v.version for v in super().list_versions(collection)]  # type: ignore[misc]


class _StaleCurrent(_MemoryBackend):
    def get_current_version(self, collection: str) -> Version:
        return self.versions[collection][0]


class _DriftMissingKey(_MemoryBackend):
    def check_drift(self, collection: str) -> dict[str, Any]:
        report = super().check_drift(collection)
        del report["message"]
        return report


def test_file_backend_conforms(tmp_path: Path) -> None:
    """The built-in backend passes. NotImplementedError from rollback is allowed."""
    check_backend(JsonFileBackend, tmp_path)


def test_memory_backend_conforms(tmp_path: Path) -> None:
    check_backend(_MemoryBackend, tmp_path)


@pytest.mark.parametrize(
    ("backend", "problem"),
    [
        (_ListReturnsStrings, r"list_versions returned str items, expected Version"),
        (_StaleCurrent, r"get_current_version returned 1\.0\.0, expected 2\.0\.0"),
        (_DriftMissingKey, r"check_drift is missing the keys: message"),
    ],
)
def test_nonconforming_backend_names_the_method(
    tmp_path: Path, backend: type[_MemoryBackend], problem: str
) -> None:
    with pytest.raises(ConformanceError, match=problem):
        check_backend(backend, tmp_path)


def test_factory_must_return_a_backend(tmp_path: Path) -> None:
    with pytest.raises(ConformanceError, match=r"does not implement VersioningBackend"):
        check_backend(lambda catalog_root: object(), tmp_path)
