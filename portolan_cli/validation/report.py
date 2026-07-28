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

A ``workflow`` section appears when the manifest and the filesystem disagree
about which files exist: ``unregistered`` data files and ``missing`` assets.
Those are not conformance defects and never appear in ``findings`` — see
:class:`~portolan_cli.validation.runner.WorkflowNotice`.

Under ``--fix`` the payload gains a ``fix`` section describing the one repair
pass that ran:

- ``selected`` — the fixer-registry keys the findings called for, in execution
  order.
- ``applied`` — the subset of those that actually changed something.
- ``skipped`` — ``{key: [reason, ...]}`` for every selected key that changed
  nothing, so "ran" is never reported as "fixed".
- ``auto_count`` — AUTO findings the pre-fix check reported.
- ``fixed_count`` — how many of those are gone: ``auto_count - len(survivors)``.
- ``survivors`` — ``{rule_id, path, json_pointer, index}`` for every AUTO
  finding still present after the re-check; ``index`` separates repeat
  occurrences of one rule on one file. A non-empty list is the signal to
  **stop** calling ``--fix``: those defects need a person, whatever their
  bucket says.
- ``fixers`` — the :class:`~portolan_cli.metadata.fix.FixReport` from the
  registry, per-file.
- ``metadata_fix`` / ``conversion`` — the item-freshness and geo-asset reports.
- ``dry_run`` — true when nothing was written and no re-check ran.
"""

from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import version
from typing import Any

from portolan_cli.constants import PORTOLAN_SPEC_VERSION
from portolan_cli.metadata.fix import FixReport
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
    """Build the ``fix`` section from the repair pass, before the re-check.

    ``selected`` and ``applied`` are not the same list, and the difference is
    the point: a fixer the findings called for may run and change nothing —
    a bbox with no child extents to recompute from, a convert pass the
    geo-asset sweep already owns. Reporting the selection as the outcome told
    an agent "Applied: bbox" for a defect still sitting in the catalog.

    Args:
        legacy: The item-freshness and conversion reports (``metadata_fix``,
            ``conversion``).
        fixer_report: What the fixer registry did.
        applied: Fixer keys that actually changed something.
        selected: Fixer keys the findings called for, in execution order.
        skipped: For each selected key that changed nothing, why not.
        pre_findings: Findings from the check that drove the fixers; their AUTO
            count is the denominator ``fixed_count`` is measured against.
        dry_run: Whether anything was written.

    Returns:
        The ``fix`` section, pending :func:`annotate_survivors`.
    """
    payload = dict(legacy)
    payload["fixers"] = fixer_report.to_dict()
    payload["applied"] = applied
    payload["selected"] = selected
    payload["skipped"] = skipped
    payload["auto_count"] = sum(
        1 for finding in pre_findings if remediation_for(finding.rule_id).bucket is Bucket.AUTO
    )
    payload["dry_run"] = dry_run
    return payload


def annotate_survivors(fix_payload: dict[str, Any], outcome: CheckOutcome) -> None:
    """Record which AUTO findings outlived the fixers, and how many did not.

    A survivor is an AUTO finding the re-check still reports. Naming them is what
    lets an agent stop: without this list, ``auto_fixable: true`` invites another
    ``--fix`` pass forever on a defect no fixer resolves.

    Each entry carries an ``index``: its occurrence number within its
    (rule_id, path, json_pointer) group. Two findings of one rule on one file
    with no pointer are distinct defects, and without the index they collapsed
    into a single survivor entry — so only one of them got the "the automatic
    fix did not resolve this" annotation.

    Args:
        fix_payload: The section from :func:`build_fix_payload`; mutated in place.
        outcome: The post-fix check.
    """
    findings = outcome.report.findings if outcome.report is not None else []
    seen: dict[tuple[str, str | None, str | None], int] = {}
    survivors = []
    for finding in findings:
        if remediation_for(finding.rule_id).bucket is not Bucket.AUTO:
            continue
        key = (finding.rule_id, finding.path, finding.json_pointer)
        index = seen.get(key, 0)
        seen[key] = index + 1
        survivors.append(
            {
                "rule_id": finding.rule_id,
                "path": finding.path,
                "json_pointer": finding.json_pointer,
                "index": index,
            }
        )
    fix_payload["survivors"] = survivors
    fix_payload["fixed_count"] = max(fix_payload.get("auto_count", 0) - len(survivors), 0)
