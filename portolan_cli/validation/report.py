"""Build the machine-readable payload `portolan check --json` emits.

Pure data: no Click, no Rich, no exit codes. ``cli.py`` wraps the returned dict
in the standard output envelope and renders the human view from the same
:class:`~portolan_cli.validation.runner.CheckOutcome`.

The contract, per finding, is rashid's ``Finding.to_dict()`` verbatim plus three
CLI-owned keys:

- ``remediation`` — ``auto`` / ``instruct`` / ``external``.
- ``auto_fixable`` — shorthand for ``remediation == "auto"``.
- ``requirement`` — the imperative sentence for the defect, empty when the rule
  id is not in the remediation table.

An agent reads the loop off ``counts_by_remediation``: run ``--fix`` while
``auto`` is non-zero, then work the ``instruct`` findings by hand.
"""

from __future__ import annotations

from importlib.metadata import version
from typing import Any

from portolan_cli.constants import PORTOLAN_SPEC_VERSION
from portolan_cli.validation.remediation import Bucket, remediation_for
from portolan_cli.validation.runner import CheckOutcome

VALIDATOR_NAME = "rashid"


def _enrich(finding: Any) -> dict[str, Any]:
    payload: dict[str, Any] = finding.to_dict()
    remediation = remediation_for(finding.rule_id)
    payload["remediation"] = remediation.bucket.value
    payload["auto_fixable"] = remediation.bucket is Bucket.AUTO
    payload["requirement"] = remediation.requirement
    return payload


def build_check_payload(outcome: CheckOutcome, *, mode: str) -> dict[str, Any]:
    """Render ``outcome`` as the JSON payload.

    Args:
        outcome: What :func:`~portolan_cli.validation.runner.run_check` produced.
        mode: Scope the run covered — ``metadata``, ``geo-assets``, or ``all``.

    Returns:
        A JSON-serializable dict.
    """
    payload: dict[str, Any] = {
        "mode": mode,
        "spec_version": PORTOLAN_SPEC_VERSION,
        "validator": {"name": VALIDATOR_NAME, "version": version(VALIDATOR_NAME)},
        "passed": True,
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
            passed=report.passed,
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
    if outcome.live_hint is not None:
        payload["live_hint"] = {
            "base_url": outcome.live_hint.base_url,
            "message": outcome.live_hint.message,
        }

    return payload
