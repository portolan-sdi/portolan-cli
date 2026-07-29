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
- ``fixed_count`` — how many of those are gone: ``auto_count`` less the
  survivors that were already among them. A finding a repair *exposed* is a
  survivor but not a repair undone, so it never decrements this.
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


def build_check_payload(
    outcome: CheckOutcome, *, mode: str, fix_failed: bool = False
) -> dict[str, Any]:
    """Render ``outcome`` as the JSON payload.

    ``passed`` is fed by exactly two inputs: rashid's verdict (absent when
    ``--metadata`` was not run, in which case there is nothing to fail) and
    whether the ``--fix`` pass itself failed. Those are the same inputs the exit
    code uses, so ``passed: true`` never accompanies a non-zero exit. The one
    documented exception is ``--strict``, which escalates warnings and the
    workflow notice to exit 1 without calling the catalog non-conformant.

    Args:
        outcome: What :func:`~portolan_cli.validation.runner.run_check` produced.
        mode: Scope the run covered — ``metadata``, ``geo-assets``, or ``all``.
        fix_failed: Whether the repair pass reported a failure — a conversion
            that errored, or a fixer that could not write. Exits 1, so it must
            not be published as ``passed: true``.

    Returns:
        A JSON-serializable dict.
    """
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


def _auto_occurrences(findings: Iterable[Any]) -> list[tuple[str, str | None, str | None, int]]:
    """AUTO findings as ``(rule_id, path, json_pointer, occurrence index)`` keys.

    The index disambiguates repeat occurrences of one rule on one file with no
    pointer, which are distinct defects that would otherwise collapse into one.
    """
    seen: dict[tuple[str, str | None, str | None], int] = {}
    keys: list[tuple[str, str | None, str | None, int]] = []
    for finding in findings:
        if remediation_for(finding.rule_id).bucket is not Bucket.AUTO:
            continue
        group = (finding.rule_id, finding.path, finding.json_pointer)
        index = seen.get(group, 0)
        seen[group] = index + 1
        keys.append((*group, index))
    return keys


def annotate_survivors(
    fix_payload: dict[str, Any], outcome: CheckOutcome, *, pre_findings: Iterable[Any]
) -> None:
    """Record which AUTO findings outlived the fixers, and how many did not.

    A survivor is an AUTO finding the re-check still reports. Naming them is what
    lets an agent stop: without this list, ``auto_fixable: true`` invites another
    ``--fix`` pass forever on a defect no fixer resolves.

    Each entry carries an ``index``: its occurrence number within its
    (rule_id, path, json_pointer) group. Two findings of one rule on one file
    with no pointer are distinct defects, and without the index they collapsed
    into a single survivor entry — so only one of them got the "the automatic
    fix did not resolve this" annotation.

    ``fixed_count`` subtracts only the survivors that were *there before* the
    fixers ran, matched on (rule_id, path, json_pointer, occurrence index). A
    repair can expose a defect nothing could report earlier — the links fixer
    writes a child link and the title rule then fires on it — and counting that
    against the pass reported ``Fixed automatically (0)`` for work that really
    happened. Newly exposed findings are still listed as survivors: they are the
    reason to stop calling ``--fix``, they are just not repairs undone.

    Args:
        fix_payload: The section from :func:`build_fix_payload`; mutated in place.
        outcome: The post-fix check.
        pre_findings: The findings the fixers were handed, i.e. the same list
            ``build_fix_payload`` measured ``auto_count`` from.
    """
    findings = outcome.report.findings if outcome.report is not None else []
    post = _auto_occurrences(findings)
    fix_payload["survivors"] = [
        {"rule_id": rule_id, "path": path, "json_pointer": pointer, "index": index}
        for rule_id, path, pointer, index in post
    ]
    before = set(_auto_occurrences(pre_findings))
    repaired = fix_payload.get("auto_count", 0) - sum(1 for key in post if key in before)
    fix_payload["fixed_count"] = max(repaired, 0)
