"""Build the machine-readable payload `portolan check --json` emits."""

from __future__ import annotations

from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from portolan_cli.validation.remediation import Bucket, remediation_of
from portolan_core.constants import PORTOLAN_SPEC_VERSION

if TYPE_CHECKING:
    from collections.abc import Iterable

    from portolan_cli.metadata.fix import FixReport
    from portolan_cli.validation.runner import CheckOutcome

VALIDATOR_NAME = "rashid"


def _enrich(finding: Any) -> dict[str, Any]:
    payload: dict[str, Any] = finding.to_dict()
    remediation = remediation_of(finding)
    payload["remediation"] = remediation.bucket.value
    payload["auto_fixable"] = remediation.bucket is Bucket.AUTO
    payload["requirement"] = remediation.requirement
    return payload


def build_check_payload(
    outcome: CheckOutcome, *, mode: str, fix_failed: bool = False
) -> dict[str, Any]:
    """Render ``outcome`` as the JSON payload."""
    payload: dict[str, Any] = {
        "mode": mode,
        "spec_version": PORTOLAN_SPEC_VERSION,
        "validator": {"name": VALIDATOR_NAME, "version": version(VALIDATOR_NAME)},
        "passed": not fix_failed,
    }

    report = outcome.report
    if report is not None:
        counts = dict.fromkeys((bucket.value for bucket in Bucket), 0)
        findings = []
        for finding in report.findings:
            enriched = _enrich(finding)
            counts[enriched["remediation"]] += 1
            findings.append(enriched)

        payload.update(
            passed=report.passed and not fix_failed,
            files_checked=report.files_checked,
            error_count=len(report.errors),
            warning_count=len(report.warnings),
            info_count=len(report.infos),
            findings=findings,
            counts_by_remediation=counts,
        )

    if outcome.format_report is not None:
        payload["format"] = outcome.format_report.to_dict()

    if outcome.legacy_note is not None:
        payload["legacy_note"] = outcome.legacy_note
    if outcome.workflow_notice is not None:
        payload["workflow"] = {
            "unregistered": outcome.workflow_notice.unregistered,
            "missing": outcome.workflow_notice.missing,
            "message": outcome.workflow_notice.message,
        }
    if outcome.live_hint is not None:
        payload["live_hint"] = {
            "base_url": outcome.live_hint.base_url,
            "message": outcome.live_hint.message,
        }

    return payload


def build_fix_payload(
    *,
    legacy: dict[str, Any],
    fixer_report: FixReport,
    applied: list[str],
    selected: list[str],
    skipped: dict[str, list[str]],
    pre_findings: Iterable[Any],
    dry_run: bool,
) -> dict[str, Any]:
    """Build the ``fix`` section from the repair pass, before the re-check."""
    payload = dict(legacy)
    payload["fixers"] = fixer_report.to_dict()
    payload["applied"] = applied
    payload["selected"] = selected
    payload["skipped"] = skipped
    payload["auto_count"] = sum(
        1 for finding in pre_findings if remediation_of(finding).bucket is Bucket.AUTO
    )
    payload["dry_run"] = dry_run
    return payload


def _auto_occurrences(findings: Iterable[Any]) -> list[tuple[str, str | None, str | None, int]]:
    """AUTO findings as ``(rule_id, path, json_pointer, occurrence index)`` keys."""
    seen: dict[tuple[str, str | None, str | None], int] = {}
    keys: list[tuple[str, str | None, str | None, int]] = []
    for finding in findings:
        if remediation_of(finding).bucket is not Bucket.AUTO:
            continue
        group = (finding.rule_id, finding.path, finding.json_pointer)
        index = seen.get(group, 0)
        seen[group] = index + 1
        keys.append((*group, index))
    return keys


def annotate_survivors(
    fix_payload: dict[str, Any], outcome: CheckOutcome, *, pre_findings: Iterable[Any]
) -> None:
    """Record which AUTO findings outlived the fixers, and how many did not."""
    findings = outcome.report.findings if outcome.report is not None else []
    post = _auto_occurrences(findings)
    fix_payload["survivors"] = [
        {"rule_id": rule_id, "path": path, "json_pointer": pointer, "index": index}
        for rule_id, path, pointer, index in post
    ]
    before = set(_auto_occurrences(pre_findings))
    repaired = fix_payload.get("auto_count", 0) - sum(1 for key in post if key in before)
    fix_payload["fixed_count"] = max(repaired, 0)
