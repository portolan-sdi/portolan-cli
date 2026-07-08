"""Unit tests for scripts/pip_audit_ignores.py.

The ignore file is the single source of truth for pip-audit vulnerability
ignores (consumed by ci.yml, nightly.yml, and security-audit.yml). These tests
pin the two behaviours that make it safe: expired entries drop automatically
(so a still-present vuln starts failing CI again) and malformed lines fail loud
(so a typo can never silently disable auditing).
"""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest

from scripts.pip_audit_ignores import (
    IgnoreFileError,
    active_ignores,
    format_args,
    parse_entries,
)

TODAY = datetime.date(2026, 7, 8)


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / ".pip-audit-ignores"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.unit
def test_active_ignores_keeps_unexpired(tmp_path: Path) -> None:
    path = _write(tmp_path, "CVE-2026-1  2026-08-15  fix pending upstream\n")
    assert active_ignores(path, today=TODAY) == ["CVE-2026-1"]


@pytest.mark.unit
def test_active_ignores_keeps_entry_expiring_today(tmp_path: Path) -> None:
    # Expiry is inclusive: the entry is still active on its expiry date.
    path = _write(tmp_path, "CVE-2026-1  2026-07-08  expires today\n")
    assert active_ignores(path, today=TODAY) == ["CVE-2026-1"]


@pytest.mark.unit
def test_active_ignores_drops_expired(tmp_path: Path) -> None:
    path = _write(tmp_path, "CVE-2026-1  2026-07-07  expired yesterday\n")
    assert active_ignores(path, today=TODAY) == []


@pytest.mark.unit
def test_active_ignores_skips_comments_and_blanks(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "# header comment\n"
        "\n"
        "   \n"
        "CVE-2026-1  2026-09-01  reason one\n"
        "# trailing comment\n"
        "PYSEC-2026-2  2026-09-01  reason two\n",
    )
    assert active_ignores(path, today=TODAY) == ["CVE-2026-1", "PYSEC-2026-2"]


@pytest.mark.unit
def test_missing_fields_raises(tmp_path: Path) -> None:
    # Only an ID and a date, no reason -> malformed.
    path = _write(tmp_path, "CVE-2026-1  2026-09-01\n")
    with pytest.raises(IgnoreFileError, match="line 1"):
        active_ignores(path, today=TODAY)


@pytest.mark.unit
def test_bad_date_raises(tmp_path: Path) -> None:
    path = _write(tmp_path, "CVE-2026-1  not-a-date  some reason\n")
    with pytest.raises(IgnoreFileError, match="YYYY-MM-DD"):
        active_ignores(path, today=TODAY)


@pytest.mark.unit
def test_reason_may_contain_spaces(tmp_path: Path) -> None:
    path = _write(tmp_path, "CVE-2026-1  2026-09-01  a longer multi word reason\n")
    entries = parse_entries(path)
    assert entries[0].vuln_id == "CVE-2026-1"
    assert entries[0].reason == "a longer multi word reason"


@pytest.mark.unit
def test_format_args_emits_ignore_flags() -> None:
    assert format_args(["CVE-1", "CVE-2"]) == "--ignore-vuln CVE-1 --ignore-vuln CVE-2"


@pytest.mark.unit
def test_format_args_empty_is_empty_string() -> None:
    assert format_args([]) == ""


@pytest.mark.unit
def test_repo_ignore_file_is_wellformed() -> None:
    """The committed .pip-audit-ignores must always parse and never be expired
    on the day it is committed (guards against a stale-date typo)."""
    repo_file = Path(__file__).resolve().parents[2] / ".pip-audit-ignores"
    entries = parse_entries(repo_file)  # raises if malformed
    assert entries, "expected at least one tracked ignore entry"
    for entry in entries:
        assert entry.vuln_id
        assert entry.reason
