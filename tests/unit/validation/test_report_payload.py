"""The `check --json` payload: rashid findings enriched with remediation.

The payload is the agent contract. A finding passes through rashid's
``to_dict()`` verbatim — same rule_id, same fix_hint, same expected/actual — and
gains three CLI-owned keys telling the agent whether to wait for `--fix` or act
itself.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from rashid.model import Finding, Report, Severity

from portolan_cli.metadata.fix import FixAction, FixReport, FixResult
from portolan_cli.validation.remediation import Bucket
from portolan_cli.validation.report import (
    annotate_survivors,
    build_check_payload,
    build_fix_payload,
)
from portolan_cli.validation.runner import CheckOutcome, LiveHint, WorkflowNotice

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
    workflow_notice: WorkflowNotice | None = None,
) -> CheckOutcome:
    return CheckOutcome(
        report=Report(findings=findings, files_checked=files_checked),
        format_report=format_report,
        legacy_note=legacy_note,
        live_hint=live_hint,
        workflow_notice=workflow_notice,
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

    def test_a_failed_fix_pass_flips_passed(self) -> None:
        """`--fix` failing (a conversion that errored) exits 1; `passed` must agree."""
        payload = build_check_payload(_outcome([]), mode="all", fix_failed=True)
        assert payload["passed"] is False

    def test_a_failed_fix_pass_flips_passed_even_with_a_clean_report(self) -> None:
        payload = build_check_payload(
            _outcome([_finding("PTL-TTL-002", Severity.WARNING)]), mode="all", fix_failed=True
        )
        assert payload["passed"] is False

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


class TestWorkflowSection:
    def test_notice_is_emitted_when_present(self) -> None:
        notice = WorkflowNotice(
            unregistered=["roads/stray.parquet"],
            missing=[],
            message="1 data file(s) on disk are not registered in the catalog.",
        )
        payload = build_check_payload(_outcome([], workflow_notice=notice), mode="all")
        assert payload["workflow"] == {
            "unregistered": ["roads/stray.parquet"],
            "missing": [],
            "message": "1 data file(s) on disk are not registered in the catalog.",
        }

    def test_unregistered_files_are_not_findings(self) -> None:
        """The notice is a workflow channel: it never becomes a PTL-* finding."""
        notice = WorkflowNotice(unregistered=["roads/stray.parquet"], missing=[], message="x")
        payload = build_check_payload(_outcome([], workflow_notice=notice), mode="all")
        assert payload["findings"] == []
        assert payload["error_count"] == 0
        assert payload["passed"] is True

    def test_workflow_absent_otherwise(self) -> None:
        assert "workflow" not in build_check_payload(_outcome([]), mode="all")


def _fix_payload(
    *,
    fixer_report: FixReport | None = None,
    applied: list[str] | None = None,
    selected: list[str] | None = None,
    skipped: dict[str, list[str]] | None = None,
    pre_findings: list[Finding] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    return build_fix_payload(
        legacy={"metadata_fix": {}},
        fixer_report=fixer_report if fixer_report is not None else FixReport(),
        applied=applied if applied is not None else [],
        selected=selected if selected is not None else [],
        skipped=skipped if skipped is not None else {},
        pre_findings=pre_findings if pre_findings is not None else [],
        dry_run=dry_run,
    )


class TestFixPayloadAccounting:
    """`applied` means changed; a fixer that ran and changed nothing is skipped."""

    def test_selected_and_skipped_are_reported_alongside_applied(self) -> None:
        payload = _fix_payload(
            selected=["titles", "bbox"],
            applied=["titles"],
            skipped={"bbox": ["No child extents to recompute the bbox from"]},
        )
        assert payload["selected"] == ["titles", "bbox"]
        assert payload["applied"] == ["titles"]
        assert payload["skipped"] == {"bbox": ["No child extents to recompute the bbox from"]}

    def test_a_fixer_that_changed_nothing_is_not_applied(self) -> None:
        payload = _fix_payload(selected=["bbox"], applied=[], skipped={"bbox": ["nothing to do"]})
        assert payload["applied"] == []
        assert payload["selected"] == ["bbox"]

    def test_skips_are_not_counted_as_successes(self, tmp_path: Path) -> None:
        report = FixReport(
            results=[FixResult(tmp_path / "collection.json", FixAction.SKIPPED, True, "no bbox")]
        )
        payload = _fix_payload(
            fixer_report=report, selected=["bbox"], skipped={"bbox": ["no bbox"]}
        )
        assert payload["fixers"]["success_count"] == 0
        assert payload["fixers"]["total_count"] == 0
        assert payload["fixers"]["skipped_count"] == 1

    def test_auto_count_denominator_is_the_pre_fix_findings(self) -> None:
        payload = _fix_payload(
            pre_findings=[_finding("PTL-BBX-001"), _finding("PTL-LIC-001")],
        )
        assert payload["auto_count"] == 1

    def test_legacy_and_dry_run_keys_survive(self) -> None:
        payload = _fix_payload(dry_run=True)
        assert payload["metadata_fix"] == {}
        assert payload["dry_run"] is True


class TestSurvivorIndexing:
    def test_two_findings_of_one_rule_on_one_file_both_survive(self) -> None:
        pre = [_finding("PTL-BBX-001"), _finding("PTL-BBX-001")]
        payload = _fix_payload(pre_findings=pre)
        annotate_survivors(payload, _outcome(list(pre)), pre_findings=pre)

        assert [item["index"] for item in payload["survivors"]] == [0, 1]
        assert len(payload["survivors"]) == 2
        assert payload["fixed_count"] == 0

    def test_index_restarts_per_rule_and_path(self) -> None:
        payload = _fix_payload()
        annotate_survivors(
            payload,
            _outcome(
                [
                    _finding("PTL-BBX-001"),
                    _finding("PTL-LNK-001"),
                    _finding("PTL-BBX-001"),
                ]
            ),
            pre_findings=[],
        )

        indexes = {(item["rule_id"], item["index"]) for item in payload["survivors"]}
        assert indexes == {("PTL-BBX-001", 0), ("PTL-BBX-001", 1), ("PTL-LNK-001", 0)}

    def test_json_pointer_still_distinguishes_survivors(self) -> None:
        payload = _fix_payload()
        annotate_survivors(
            payload,
            _outcome(
                [
                    _finding("PTL-BBX-001", json_pointer="/extent"),
                    _finding("PTL-BBX-001", json_pointer="/bbox"),
                ]
            ),
            pre_findings=[],
        )

        assert [item["index"] for item in payload["survivors"]] == [0, 0]


class TestFixedCountCountsOnlyRepairedFindings:
    """``fixed_count`` measures pre-fix defects resolved, not post-fix noise.

    A repair pass can expose a defect that was not reportable before it ran —
    the links fixer writes a child link, and the title rule then fires on it.
    Counting every post-fix AUTO finding as a survivor made those newly exposed
    defects cancel out repairs that really happened, so a pass that fixed
    something reported ``Fixed automatically (0)``.
    """

    def test_a_newly_exposed_finding_does_not_cancel_a_repair(self) -> None:
        pre = [_finding("PTL-BBX-001")]
        payload = _fix_payload(pre_findings=pre)
        annotate_survivors(payload, _outcome([_finding("PTL-LNK-006")]), pre_findings=pre)

        assert [item["rule_id"] for item in payload["survivors"]] == ["PTL-LNK-006"]
        assert payload["fixed_count"] == 1

    def test_only_the_matched_survivors_are_subtracted(self) -> None:
        pre = [_finding("PTL-BBX-001"), _finding("PTL-BBX-001")]
        payload = _fix_payload(pre_findings=pre)
        annotate_survivors(
            payload,
            _outcome([_finding("PTL-BBX-001"), _finding("PTL-LNK-006")]),
            pre_findings=pre,
        )

        assert len(payload["survivors"]) == 2
        assert payload["fixed_count"] == 1

    def test_a_survivor_on_a_different_file_is_not_the_same_defect(self) -> None:
        pre = [_finding("PTL-BBX-001")]
        payload = _fix_payload(pre_findings=pre)
        post = Finding(
            rule_id="PTL-BBX-001",
            severity=Severity.ERROR,
            message="PTL-BBX-001 fired",
            path="roads/collection.json",
        )
        annotate_survivors(payload, _outcome([post]), pre_findings=pre)

        assert payload["fixed_count"] == 1
