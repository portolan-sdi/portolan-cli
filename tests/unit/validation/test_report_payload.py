"""The `check --json` payload: rashid findings enriched with remediation.

The payload is the agent contract. A finding passes through rashid's
``to_dict()`` verbatim — same rule_id, same fix_hint, same expected/actual — and
gains three CLI-owned keys telling the agent whether to wait for `--fix` or act
itself.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from rashid.model import Finding, Report, Severity

from portolan_cli.validation.remediation import Bucket
from portolan_cli.validation.report import build_check_payload
from portolan_cli.validation.runner import CheckOutcome, LiveHint

pytestmark = pytest.mark.unit


def _finding(rule_id: str, severity: Severity = Severity.ERROR, **extra: Any) -> Finding:
    return Finding(
        rule_id=rule_id,
        severity=severity,
        message=f"{rule_id} fired",
        path="collection.json",
        **extra,
    )


def _outcome(
    findings: list[Finding],
    *,
    files_checked: int = 3,
    legacy_note: str | None = None,
    live_hint: LiveHint | None = None,
    format_report: Any = None,
) -> CheckOutcome:
    return CheckOutcome(
        report=Report(findings=findings, files_checked=files_checked),
        format_report=format_report,
        legacy_note=legacy_note,
        live_hint=live_hint,
    )


class TestEnvelope:
    def test_shape(self) -> None:
        payload = build_check_payload(_outcome([]), mode="all")
        assert payload["mode"] == "all"
        assert payload["passed"] is True
        assert payload["files_checked"] == 3
        assert payload["findings"] == []
        assert payload["error_count"] == 0
        assert payload["warning_count"] == 0
        assert payload["info_count"] == 0

    def test_validator_is_identified(self) -> None:
        from importlib.metadata import version

        payload = build_check_payload(_outcome([]), mode="all")
        assert payload["validator"] == {"name": "rashid", "version": version("rashid")}

    def test_spec_version_is_reported(self) -> None:
        from portolan_cli.constants import PORTOLAN_SPEC_VERSION

        assert build_check_payload(_outcome([]), mode="all")["spec_version"] == (
            PORTOLAN_SPEC_VERSION
        )

    def test_errors_flip_passed(self) -> None:
        payload = build_check_payload(_outcome([_finding("PTL-CNF-001")]), mode="all")
        assert payload["passed"] is False
        assert payload["error_count"] == 1

    def test_warnings_do_not_flip_passed(self) -> None:
        payload = build_check_payload(
            _outcome([_finding("PTL-TTL-002", Severity.WARNING)]), mode="all"
        )
        assert payload["passed"] is True
        assert payload["warning_count"] == 1

    def test_payload_is_json_serializable(self) -> None:
        payload = build_check_payload(
            _outcome([_finding("PTL-DAT-001", expected="abc", actual="def")], legacy_note="old"),
            mode="all",
        )
        assert json.loads(json.dumps(payload))["findings"][0]["expected"] == "abc"

    def test_metadata_skipped_reports_no_metadata_keys(self) -> None:
        outcome = CheckOutcome(report=None, format_report=None, legacy_note=None, live_hint=None)
        payload = build_check_payload(outcome, mode="geo-assets")
        assert "findings" not in payload
        assert payload["passed"] is True


class TestFindingEnrichment:
    def test_rashid_fields_pass_through_verbatim(self) -> None:
        finding = _finding(
            "PTL-LNK-006",
            json_pointer="/links/3/href",
            fix_hint="Repoint the link at roads/collection.json.",
            expected="roads/collection.json",
            actual="roads/collections.json",
            object_id="roads",
        )
        emitted = build_check_payload(_outcome([finding]), mode="all")["findings"][0]
        for key, value in finding.to_dict().items():
            assert emitted[key] == value

    def test_auto_rule_is_marked_auto_fixable(self) -> None:
        emitted = build_check_payload(_outcome([_finding("PTL-CNF-001")]), mode="all")["findings"][
            0
        ]
        assert emitted["remediation"] == Bucket.AUTO.value
        assert emitted["auto_fixable"] is True
        assert emitted["requirement"].startswith("Declare the versioned Portolan schema URI")

    def test_instruct_rule_is_not_auto_fixable(self) -> None:
        emitted = build_check_payload(_outcome([_finding("PTL-LIC-001")]), mode="all")["findings"][
            0
        ]
        assert emitted["remediation"] == Bucket.INSTRUCT.value
        assert emitted["auto_fixable"] is False

    def test_external_rule_is_not_auto_fixable(self) -> None:
        emitted = build_check_payload(_outcome([_finding("PTL-LIV-001")]), mode="all")["findings"][
            0
        ]
        assert emitted["remediation"] == Bucket.EXTERNAL.value
        assert emitted["auto_fixable"] is False

    def test_unmapped_rule_degrades_to_instruct(self) -> None:
        emitted = build_check_payload(_outcome([_finding("PTL-XXX-999")]), mode="all")["findings"][
            0
        ]
        assert emitted["remediation"] == Bucket.INSTRUCT.value
        assert emitted["requirement"] == ""

    def test_counts_by_remediation(self) -> None:
        payload = build_check_payload(
            _outcome(
                [
                    _finding("PTL-CNF-001"),
                    _finding("PTL-LNK-001"),
                    _finding("PTL-LIC-001"),
                    _finding("PTL-LIV-001"),
                ]
            ),
            mode="all",
        )
        assert payload["counts_by_remediation"] == {"auto": 2, "instruct": 1, "external": 1}

    def test_counts_by_remediation_always_lists_every_bucket(self) -> None:
        payload = build_check_payload(_outcome([]), mode="all")
        assert payload["counts_by_remediation"] == {"auto": 0, "instruct": 0, "external": 0}


class TestOptionalSections:
    def test_legacy_note_included_when_set(self) -> None:
        payload = build_check_payload(_outcome([], legacy_note="This catalog predates"), mode="all")
        assert payload["legacy_note"] == "This catalog predates"

    def test_legacy_note_absent_otherwise(self) -> None:
        assert "legacy_note" not in build_check_payload(_outcome([]), mode="all")

    def test_live_hint_included_when_set(self) -> None:
        hint = LiveHint(base_url="https://data.example.org/c/", message="Run --live")
        payload = build_check_payload(_outcome([], live_hint=hint), mode="all")
        assert payload["live_hint"] == {
            "base_url": "https://data.example.org/c/",
            "message": "Run --live",
        }

    def test_live_hint_absent_otherwise(self) -> None:
        assert "live_hint" not in build_check_payload(_outcome([]), mode="all")

    def test_format_section_included_when_geo_assets_ran(self) -> None:
        class FakeFormatReport:
            def to_dict(self) -> dict[str, Any]:
                return {"total": 2, "files": []}

        payload = build_check_payload(
            _outcome([], format_report=FakeFormatReport()), mode="geo-assets"
        )
        assert payload["format"] == {"total": 2, "files": []}

    def test_format_section_absent_otherwise(self) -> None:
        assert "format" not in build_check_payload(_outcome([]), mode="metadata")
