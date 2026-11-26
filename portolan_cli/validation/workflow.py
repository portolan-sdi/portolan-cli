"""Run `portolan check` and `portolan check --fix` as one library call.

:func:`run_check_workflow` takes the options of the command and returns a
:class:`CheckWorkflowResult`. Its ``to_dict()`` is the envelope ``data`` the
command emits. The function prints nothing. The CLI renders the human view
from the result and decides the exit code from it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli.errors import NotACatalogError
from portolan_cli.trace import span

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.convert import ConversionResult
    from portolan_cli.metadata.fix import FixReport
    from portolan_cli.run_state import RunState
    from portolan_cli.scan.check import FixWorkflowOutcome
    from portolan_cli.validation.runner import CheckOutcome

    #: Receives one fix report and the conversion report that goes with it, or None.
    FixReportCallback = Callable[[FixReport | None, Any], None]


@dataclass(frozen=True)
class CheckWorkflowResult:
    """Everything one `portolan check` run produced.

    Attributes:
        outcome: The check that the payload reports. After ``--fix`` it is the
            re-check. After ``--fix --dry-run`` it is the check before the fix.
        payload: The envelope ``data``, with the ``fix`` section under ``--fix``.
        failed: The envelope ``success`` is false. The exit code is 1 when this
            is true or when ``payload["passed"]`` is false.
    """

    outcome: CheckOutcome
    payload: dict[str, Any]
    failed: bool

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of `portolan check --json`."""
        return self.payload


def run_check_workflow(
    path: Path,
    *,
    fix: bool = False,
    dry_run: bool = False,
    remove_legacy: bool = False,
    force: bool = False,
    workers: int | None = None,
    metadata: bool = False,
    geo_assets: bool = False,
    strict: bool = False,
    live: bool = False,
    public_url: str | None = None,
    data: bool = True,
    data_scope: str = "all",
    structural: bool = True,
    schema: bool = False,
    on_progress: Callable[[ConversionResult], None] | None = None,
    on_fix_report: FixReportCallback | None = None,
) -> CheckWorkflowResult:
    """Validate the catalog at ``path``, and with ``fix`` repair it and re-check it.

    Without ``fix`` this validates once.

    With ``fix`` the run is check, fix, re-check **exactly once**, never a
    loop. The first check produces the findings. The AUTO ones dispatch the
    fixer registry. The second check reports what is left. An AUTO finding that
    survives the re-check is a fixer that did not resolve it, so the ``fix``
    section lists it under ``survivors``. Otherwise an agent that keeps calling
    ``--fix`` on it never stops.

    ``dry_run`` reports what the fixers would change, writes nothing, and skips
    the re-check. With no writes there is nothing new to find.

    Args:
        path: Directory to check.
        fix: Apply the repairs, then re-check.
        dry_run: With ``fix``, preview the repairs and write nothing.
        remove_legacy: With ``fix``, remove source files after a conversion.
        force: With ``fix``, re-optimize already-valid COGs.
        workers: Parallel worker processes. None uses the CPU count.
        metadata: Limit the run to the catalog metadata.
        geo_assets: Limit the run to the source files on disk.
        strict: Fail on warnings and on the workflow notice too.
        live: Probe the published host over HTTP.
        public_url: Base URL the catalog is published under.
        data: Run the data pass over asset bytes.
        data_scope: Which assets the data pass may read, ``all`` or ``local``.
        structural: Run the STAC 1.1.0 structural pass.
        schema: Run the Portolan profile schema pass.
        on_progress: Called after each conversion that ``fix`` runs.
        on_fix_report: Called with each fix report as soon as that pass ends.
            First the freshness pass with its conversion report, then the fixer
            registry with None. A later failure, for example in the commit,
            does not hide what the fix already wrote.

    Returns:
        A :class:`CheckWorkflowResult`.

    Raises:
        InvalidLiveUrlError: ``live`` has no valid https base URL. Nothing ran.
        NotACatalogError: A metadata-only ``fix`` found no catalog. Nothing was
            committed.
    """
    # Through the package, as before the move, so tests that patch it still apply.
    from portolan_cli.validation import annotate_survivors, build_check_payload, run_check
    from portolan_cli.validation.runner import validate_live_base_url

    # Auto-detect the worker count when unset, so a large --fix run
    # parallelizes by default (issue #530). An explicit value wins.
    workers = workers if workers is not None else (os.cpu_count() or 1)
    run_metadata, run_geo_assets, mode = _determine_check_mode(metadata, geo_assets)
    validate_metadata = _should_validate(path, run_metadata=run_metadata, fix=fix)
    if live and validate_metadata:
        validate_live_base_url(path, public_url)
    check_kwargs: dict[str, Any] = {
        "data": data,
        "data_scope": data_scope,
        "structural": structural,
        "schema": schema,
        "metadata": validate_metadata,
        "public_url": public_url,
        "workers": workers,
    }

    fix_data: dict[str, Any] | None = None
    fix_failed = False
    pre_outcome = None
    state: RunState | None = None
    if fix:
        from portolan_cli.scan.check import resolve_catalog_root_for_check
        from portolan_cli.version_ops import owned_run

        # One run state for the check, the fix, and the re-check. Each pass
        # parses the config through it and stages its versions in it. The block
        # commits once, before the re-check reads the result (issue #995).
        # An exception in the block commits nothing.
        with owned_run(resolve_catalog_root_for_check(path) or path) as state:
            with span("check.pre"):
                pre_outcome = run_check(
                    path, live=False, geo_assets=False, state=state, **check_kwargs
                )
            with span("check.fix", dry_run=dry_run):
                fix_data, fix_failed = _run_fix_and_repairs(
                    path=path,
                    findings=list(pre_outcome.report.findings) if pre_outcome.report else [],
                    run_metadata=run_metadata,
                    run_geo_assets=run_geo_assets,
                    dry_run=dry_run,
                    remove_legacy=remove_legacy,
                    force=force,
                    workers=workers,
                    on_progress=on_progress,
                    on_fix_report=on_fix_report,
                    state=state,
                )

    if fix and dry_run and pre_outcome is not None:
        outcome = pre_outcome
    else:
        with span("check.recheck" if fix else "check.run"):
            outcome = run_check(
                path,
                live=live,
                # --fix already ran the geo-asset conversion. A re-scan repeats it.
                geo_assets=run_geo_assets and not fix,
                state=state,
                **check_kwargs,
            )
    payload = build_check_payload(outcome, mode=mode, fix_failed=fix_failed)
    if fix_data is not None:
        if dry_run:
            # A survivor is only knowable from a re-check, and a dry run wrote
            # nothing to re-check. Annotating here compares the pre-fix findings
            # against themselves, so every AUTO defect reads as having outlived
            # a pass that never ran, and `fixed_count` lands on 0 next to the
            # fixers the same catalog really does repair. Null reports what a
            # preview knows: nothing was measured.
            fix_data["survivors"] = None
            fix_data["fixed_count"] = None
        else:
            annotate_survivors(
                fix_data,
                outcome,
                pre_findings=list(pre_outcome.report.findings)
                if pre_outcome is not None and pre_outcome.report is not None
                else [],
            )
        payload["fix"] = fix_data
    failed = fix_failed or check_failed(payload, strict=strict, refresh=fix and mode == "metadata")
    return CheckWorkflowResult(outcome=outcome, payload=payload, failed=failed)


def _determine_check_mode(metadata: bool, geo_assets: bool) -> tuple[bool, bool, str]:
    """Determine which checks to run and the mode string.

    The scope flags (--metadata, --geo-assets) determine WHAT to check/fix:
    - Neither flag: check/fix both (default)
    - --metadata: check/fix metadata only
    - --geo-assets: check/fix geo-assets only
    - Both flags: check/fix both (explicit)

    The --fix flag separately controls WHETHER to apply fixes (orthogonal).

    Returns:
        Tuple of (run_metadata, run_geo_assets, mode_string).
    """
    explicit_flags = metadata or geo_assets

    if explicit_flags:
        run_metadata = metadata
        run_geo_assets = geo_assets
    else:
        # No explicit flags: run both
        run_metadata = True
        run_geo_assets = True

    # Determine mode string
    if run_metadata and not run_geo_assets:
        mode = "metadata"
    elif run_geo_assets and not run_metadata:
        mode = "geo-assets"
    else:
        mode = "all"

    return run_metadata, run_geo_assets, mode


def _should_validate(path: Path, *, run_metadata: bool, fix: bool) -> bool:
    """Decide whether to validate, given a path that may not be a catalog at all.

    Mirrors the rule ``check.run_fix_workflow`` already applies to its metadata
    repair: a mixed-scope ``--fix`` over a directory with no catalog.json still
    converts the source files and skips metadata, rather than failing on a
    catalog the user never claimed to have. The re-check has to make the same
    allowance or ``--fix`` would report PTL-GEN-000 on work that succeeded.

    Without ``--fix``, a missing catalog is reported: `check` on a
    non-catalog directory has always exited non-zero.
    """
    if not run_metadata or not fix:
        return run_metadata

    from portolan_cli.scan.check import resolve_catalog_root_for_check

    return resolve_catalog_root_for_check(path) is not None


def check_failed(payload: dict[str, Any], *, strict: bool, refresh: bool = False) -> bool:
    """Decide the envelope's ``success`` from the findings the mode owns.

    A default run fails on any error. Under ``--strict``, where a catalog is
    asserted to be publishable, it also fails on a warning or on the workflow
    notice (unregistered or vanished data files). The notice is not a
    conformance defect, so on its own it never flips a default run.

    ``refresh`` is ``--fix --metadata`` without ``--strict``: the metadata
    refresh (#853). It fails on an AUTO error that is still in the findings,
    which is a repair the refresh did not make. It also fails on a document
    that does not parse or breaks the STAC schema, because the refresh cannot
    use it. The findings keep the two cases apart by rule id. Any other
    INSTRUCT or EXTERNAL error is work for a person or a host. The refresh
    did its job, so ``success`` stays true. The caller still exits 1, because
    the error leaves ``passed`` false. A fixer that could not write fails the
    run through ``fix_failed`` in the caller.
    """
    if strict:
        return any(payload.get(key) for key in ("error_count", "warning_count", "workflow"))
    if refresh:
        return any(
            finding["severity"] == "error"
            and (finding["remediation"] == "auto" or finding["rule_id"] in _UNUSABLE_DOCUMENT_RULES)
            for finding in payload.get("findings", [])
        )
    return bool(payload.get("error_count"))


#: rashid rules for a document the refresh cannot use. PTL-GEN-000 and
#: PTL-GEN-001 report a syntax error: a catalog.json or an object file that
#: does not parse. PTL-STR-001 reports a schema violation: the file parses but
#: breaks the STAC schema. Each one fails the refresh.
_UNUSABLE_DOCUMENT_RULES = frozenset({"PTL-GEN-000", "PTL-GEN-001", "PTL-STR-001"})


def _run_fix_and_repairs(
    *,
    path: Path,
    findings: list[Any],
    run_metadata: bool,
    run_geo_assets: bool,
    dry_run: bool,
    remove_legacy: bool,
    force: bool = False,
    workers: int | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
    on_fix_report: FixReportCallback | None = None,
    state: RunState,
) -> tuple[dict[str, Any], bool]:
    """Apply the fixer registry, then the metadata/geo-asset fix workflow.

    Two halves that do not overlap. The registry
    (:func:`portolan_cli.validation.fixers.apply_fixers`) owns everything a
    rashid finding names — titles, links, schema URI, README/AGENTS.md,
    checksums, extents. ``check.run_fix_workflow`` owns item freshness, which no
    rule reports because it compares the catalog against files on disk rather
    than against the spec. Conversion belongs to both, so the registry's
    ``convert`` key is skipped whenever the geo-asset pass is in scope.

    Args:
        path: Directory to check/fix.
        findings: Findings from the pre-fix check; the AUTO ones pick fixers.
        run_metadata: Whether to fix metadata issues.
        run_geo_assets: Whether to fix geo-asset format issues.
        dry_run: Preview changes without applying them.
        remove_legacy: Passed to ``check.run_fix_workflow``.
        force: Passed to ``check.run_fix_workflow``.
        workers: Passed to ``check.run_fix_workflow``.
        on_progress: Passed to ``check.run_fix_workflow``.
        on_fix_report: Called with each fix report as soon as that pass ends.
        state: The run state of the command. Every pass stages in it, and the
            caller commits it once.

    Returns:
        The JSON ``fix`` section and whether a fix failed.
    """
    from portolan_cli.metadata.fix import FixReport
    from portolan_cli.scan.check import run_fix_workflow
    from portolan_cli.validation import build_fix_payload

    fixer_report, applied, selected, skip_reasons = _run_fixer_registry(
        findings=findings,
        run_metadata=run_metadata,
        run_geo_assets=run_geo_assets,
        dry_run=dry_run,
        state=state,
    )

    with span("check.fix.freshness"):
        outcome = run_fix_workflow(
            path=path,
            run_metadata=run_metadata,
            run_geo_assets=run_geo_assets,
            dry_run=dry_run,
            remove_legacy=remove_legacy,
            force=force,
            workers=workers,
            on_progress=on_progress,
            state=state,
        )
        legacy_data, legacy_failed, written_items = _legacy_fix_result(outcome, on_fix_report)

    # The registry above ran against findings from the pre-fix check, so it
    # never saw an item the freshness pass has only just written. Repair
    # those now, or `--fix` publishes an item with no links and no checksums
    # and reports more defects than it started with (#709).
    if run_metadata and written_items:
        from portolan_cli.validation.fixers import repair_written_items

        with span("check.fix.repair", items=len(written_items)):
            repairs = repair_written_items(
                state.catalog_root, written_items, dry_run=dry_run, state=state
            )
        if repairs:
            fixer_report = FixReport(results=[*fixer_report.results, *repairs])

    if on_fix_report is not None:
        on_fix_report(fixer_report, None)

    fix_data = build_fix_payload(
        legacy=legacy_data,
        fixer_report=fixer_report,
        applied=applied,
        selected=selected,
        skipped=skip_reasons,
        pre_findings=findings,
        dry_run=dry_run,
    )
    return fix_data, legacy_failed or fixer_report.failure_count > 0


def _run_fixer_registry(
    *,
    findings: list[Any],
    run_metadata: bool,
    run_geo_assets: bool,
    dry_run: bool,
    state: RunState,
) -> tuple[FixReport, list[str], list[str], dict[str, list[str]]]:
    """Run the title sweep and the fixers that the AUTO findings call for.

    Returns:
        The fixer report, the applied and selected fixer keys, and the reasons
        that the fixers which changed nothing gave.
    """
    from portolan_cli.catalog import apply_catalog_human_titles
    from portolan_cli.metadata.fix import FixAction, FixReport, FixResult
    from portolan_cli.validation.fixers import apply_fixers

    if not run_metadata:
        return FixReport(), [], [], {}
    catalog_root = state.catalog_root
    # Issue #815: a catalog publishes the title and description a human wrote
    # in its own metadata.yaml. No rashid rule reports the drift, because
    # rashid grades catalog.json alone and never reads metadata.yaml. The
    # sweep therefore runs on its own, like the item-freshness pass, and not
    # through the finding-driven registry.
    with span("check.fix.title_sweep"):
        title_results = [
            FixResult(
                file_path=catalog_path,
                action=FixAction.UPDATED,
                success=True,
                message="Applied the metadata.yaml title/description",
            )
            for catalog_path in apply_catalog_human_titles(
                catalog_root, dry_run=dry_run, state=state
            )
        ]
    if not findings:
        return FixReport(results=title_results), [], [], {}
    skip = {"convert"} if run_geo_assets else set()
    with span("check.fix.fixers", findings=len(findings)):
        run = apply_fixers(catalog_root, findings, dry_run=dry_run, skip=skip, state=state)
    # The report holds what the fixers did, not what the findings asked for. A
    # fixer that ran and changed nothing is a skip with a reason.
    report = FixReport(results=[*title_results, *run.report.results])
    return report, run.applied, run.selected, run.skip_reasons


def _legacy_fix_result(
    outcome: FixWorkflowOutcome,
    on_fix_report: FixReportCallback | None,
) -> tuple[dict[str, Any], bool, list[Path]]:
    """Turn the outcome of ``check.run_fix_workflow`` into the JSON data of the fix.

    Returns:
        The fix reports as JSON data, whether a fix failed, and the data files
        whose items the freshness pass updated. The caller repairs those items,
        which the finding-driven registry ran too early to see (#709).

    Raises:
        NotACatalogError: A metadata-only fix found no catalog root (issue #384).
    """
    from portolan_cli.metadata.fix import FixAction

    if outcome.fatal_error is not None:
        raise NotACatalogError(outcome.fatal_error)

    metadata_fix_report = outcome.metadata_fix_report
    format_fix_report = outcome.format_fix_report

    conversion_failed = bool(
        format_fix_report
        and format_fix_report.conversion_report
        and format_fix_report.conversion_report.failed > 0
    )
    failed = outcome.has_failures or conversion_failed

    if on_fix_report is not None:
        on_fix_report(metadata_fix_report, format_fix_report)

    # A created item went through finalize_items and the mirror writer, as after
    # add, so it needs no repair. A repair would run the catalog-wide sweeps again.
    written_items = [
        result.file_path
        for result in (metadata_fix_report.results if metadata_fix_report else [])
        if result.success and result.action is FixAction.UPDATED
    ]

    return (
        _fix_json_section(
            metadata_fix_report=metadata_fix_report,
            format_fix_report=format_fix_report,
        ),
        failed,
        written_items,
    )


def _fix_json_section(
    *,
    metadata_fix_report: FixReport | None,
    format_fix_report: Any,
) -> dict[str, Any]:
    """Build the ``fix`` section of the check payload.

    Args:
        metadata_fix_report: Results from metadata fix (if run).
        format_fix_report: Results from geo-asset fix (if run).

    Returns:
        The fix results, keyed by which repair produced them.
    """
    from portolan_cli.metadata.fix import FixReport

    data: dict[str, Any] = {}

    if metadata_fix_report is not None:
        if not isinstance(metadata_fix_report, FixReport):
            raise TypeError(f"Expected FixReport, got {type(metadata_fix_report).__name__}")
        data["metadata_fix"] = metadata_fix_report.to_dict()

    if format_fix_report is not None:
        # Use "conversion" key for backward compatibility with existing tests
        data["conversion"] = format_fix_report.to_dict()

    return data
