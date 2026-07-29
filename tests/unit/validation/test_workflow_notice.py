"""The workflow notice: files on disk the catalog does not account for.

An unregistered data file is not a conformance defect — rashid validates the
catalog it is given, and a file nothing points at is outside that catalog. It is
still worth saying out loud, so `run_check` carries it on a separate channel
that never becomes a ``PTL-*`` finding.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from rashid.model import Report

from portolan_cli.metadata.models import MetadataCheckResult, MetadataReport, MetadataStatus
from portolan_cli.validation.runner import run_check

pytestmark = pytest.mark.unit


def _result(path: Path, status: MetadataStatus) -> MetadataCheckResult:
    return MetadataCheckResult(file_path=path, status=status, message=status.value)


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    """A directory that looks enough like a catalog for the scan to be attempted."""
    (tmp_path / "catalog.json").write_text("{}", encoding="utf-8")
    return tmp_path


@pytest.fixture(autouse=True)
def _no_rashid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the conformance pass: this module is about the other channel."""

    def _validate(root: Path, **kwargs: Any) -> Report:
        return Report(findings=[], files_checked=1)

    monkeypatch.setattr("portolan_cli.validation.runner.validate", _validate)


def _with_scan(monkeypatch: pytest.MonkeyPatch, report: MetadataReport | Exception) -> list[Path]:
    """Route ``scan_catalog_metadata`` to ``report``; return the roots it saw."""
    seen: list[Path] = []

    def _scan(root: Path) -> MetadataReport:
        seen.append(root)
        if isinstance(report, Exception):
            raise report
        return report

    monkeypatch.setattr("portolan_cli.metadata.scan.scan_catalog_metadata", _scan)
    return seen


def _check(root: Path) -> Any:
    return run_check(root, geo_assets=False, data=False, structural=False)


class TestNoticeContents:
    def test_orphaned_files_are_reported_as_unregistered(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _with_scan(
            monkeypatch,
            MetadataReport(
                results=[_result(catalog / "roads" / "stray.parquet", MetadataStatus.ORPHANED)]
            ),
        )

        notice = _check(catalog).workflow_notice

        assert notice is not None
        assert notice.unregistered == ["roads/stray.parquet"]
        assert notice.missing == []

    def test_missing_files_are_reported_separately(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _with_scan(
            monkeypatch,
            MetadataReport(
                results=[_result(catalog / "roads" / "gone.parquet", MetadataStatus.MISSING)]
            ),
        )

        notice = _check(catalog).workflow_notice

        assert notice is not None
        assert notice.missing == ["roads/gone.parquet"]
        assert notice.unregistered == []

    def test_message_names_both_counts(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _with_scan(
            monkeypatch,
            MetadataReport(
                results=[
                    _result(catalog / "a" / "one.parquet", MetadataStatus.ORPHANED),
                    _result(catalog / "a" / "two.parquet", MetadataStatus.ORPHANED),
                    _result(catalog / "a" / "gone.parquet", MetadataStatus.MISSING),
                ]
            ),
        )

        notice = _check(catalog).workflow_notice

        assert notice is not None
        assert "2 data file(s) on disk are not registered" in notice.message
        assert "1 registered asset(s) are missing from disk" in notice.message
        # Neither case is auto-fixable, so the message must not point at --fix
        # (ADR-0041: an orphan is reported, never guessed at).
        assert "portolan add" in notice.message
        assert "--fix" not in notice.message


class TestNoticeAbsence:
    def test_fresh_catalog_has_no_notice(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _with_scan(
            monkeypatch,
            MetadataReport(results=[_result(catalog / "a" / "ok.parquet", MetadataStatus.FRESH)]),
        )

        assert _check(catalog).workflow_notice is None

    @pytest.mark.parametrize("status", [MetadataStatus.STALE, MetadataStatus.BREAKING])
    def test_staleness_stays_on_the_fix_channel(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch, status: MetadataStatus
    ) -> None:
        """STALE/BREAKING are what --fix updates; the notice is about accounting."""
        _with_scan(
            monkeypatch,
            MetadataReport(results=[_result(catalog / "a" / "old.parquet", status)]),
        )

        assert _check(catalog).workflow_notice is None

    def test_non_catalog_path_yields_no_notice(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _with_scan(monkeypatch, FileNotFoundError("no catalog.json"))

        assert _check(catalog).workflow_notice is None

    def test_scan_is_not_run_when_metadata_is_off(
        self, catalog: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen = _with_scan(
            monkeypatch,
            MetadataReport(
                results=[_result(catalog / "a" / "stray.parquet", MetadataStatus.ORPHANED)]
            ),
        )

        outcome = run_check(catalog, metadata=False, geo_assets=False)

        assert outcome.workflow_notice is None
        assert seen == []
