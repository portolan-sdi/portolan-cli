"""Run `portolan check` and `portolan check --fix` as one library call."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_core.errors import NotACatalogError
from portolan_core.trace import span

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.conversion_config import CogSettings, VectorSettings
    from portolan_cli.convert import ConversionResult
    from portolan_cli.metadata.fix import FixReport
    from portolan_cli.scan.check import FixWorkflowOutcome
    from portolan_cli.validation.runner import CheckOutcome
    from portolan_core.run_state import RunState

    #: Receives one fix report and the conversion report that goes with it, or None.
    FixReportCallback = Callable[[FixReport | None, Any], None]


@dataclass(frozen=True)
class CheckWorkflowResult:
    """Everything one `portolan check` run produced."""

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
    """Validate the catalog at ``path``, and with ``fix`` repair it and re-check it."""
    # Through the package, as before the move, so tests that patch it still apply.
    from portolan_cli.validation import annotate_survivors, build_check_payload, run_check
    from portolan_cli.validation.runner import validate_live_base_url

    # Auto-detect the worker count when unset; an explicit value wins.
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

        # One run state for the check, the fix, and the re-check; the block commits once before the re-check.
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
            # A dry run wrote nothing to re-check, so survivors are unknowable; report null, not a wrong count.
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
    """Determine which checks to run and the mode string."""
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
    """Decide whether to validate, given a path that may not be a catalog at all."""
    if not run_metadata or not fix:
        return run_metadata

    from portolan_cli.scan.check import resolve_catalog_root_for_check

    return resolve_catalog_root_for_check(path) is not None


def check_failed(payload: dict[str, Any], *, strict: bool, refresh: bool = False) -> bool:
    """Decide the envelope's ``success`` from the findings the mode owns."""
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
    """Apply the fixer registry, then the metadata/geo-asset fix workflow."""
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

    rewrites, mirrors = _reoptimize_targets(state.catalog_root, findings, state)
    with span("check.fix.freshness"):
        outcome = run_fix_workflow(
            path=path,
            run_metadata=run_metadata,
            run_geo_assets=run_geo_assets,
            dry_run=dry_run,
            remove_legacy=remove_legacy,
            force=force,
            reoptimize=rewrites,
            workers=workers,
            on_progress=on_progress,
            state=state,
        )
        legacy_data, legacy_failed, written_items = _legacy_fix_result(outcome, on_fix_report)
    # The mirror writer rebuilds a named items.parquet, under the mirror rules.
    written_items.extend(mirrors)

    # The registry ran on pre-fix findings and never saw items the freshness pass just wrote; repair them now.
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


#: The largest internal COG tile that PTL-DAT-013 allows.
_MAX_TILE = 512

#: What each rewrite rule needs of the conversion settings, as a hint and a test.
_REWRITE_NEEDS: dict[str, tuple[str, Callable[[VectorSettings, CogSettings], bool]]] = {
    "PTL-DAT-006": (
        "conversion.vector.sort set to hilbert or quadkey",
        lambda vector, _cog: vector.sort != "none",
    ),
    "PTL-DAT-007": ("conversion.vector.add_bbox set to true", lambda vector, _cog: vector.add_bbox),
    "PTL-DAT-013": (
        "conversion.cog.tile_size of 512 or less",
        lambda _vector, cog: cog.tile_size <= _MAX_TILE,
    ),
}


def _reoptimize_targets(
    root: Path, findings: list[Any], state: RunState | None = None
) -> tuple[frozenset[Path], frozenset[Path]]:
    """The files that AUTO ``convert`` findings name, then the mirrors. Settings that cannot help warn."""
    from rashid_core.api import is_absolute_href

    from portolan_cli.conversion_config import get_cog_settings, get_vector_settings
    from portolan_cli.report import emit
    from portolan_cli.stac_parquet import PARQUET_FILENAME
    from portolan_cli.validation.remediation import Bucket, remediation_of

    named: dict[Path, set[str]] = {}
    for finding in findings:
        remediation = remediation_of(finding)
        if remediation.bucket is not Bucket.AUTO or remediation.fixer != "convert":
            continue
        match (finding.json_pointer or "").split("/"):
            case ["", "assets", key, *_]:
                key = key.replace("~1", "/").replace("~0", "~")
            case _:
                continue
        node = root / finding.path
        try:
            href = json.loads(node.read_text(encoding="utf-8"))["assets"][key]["href"]
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if isinstance(href, str) and not is_absolute_href(href):
            named.setdefault((node.parent / href).resolve(), set()).add(finding.rule_id)

    vector, cog = get_vector_settings(root, state=state), get_cog_settings(root, state=state)
    targets: set[Path] = set()
    mirrors: set[Path] = set()
    for path, rules in named.items():
        if path.name == PARQUET_FILENAME and (path.parent / "collection.json").is_file():
            mirrors.add(path)
            continue
        unmet = {
            rule: _REWRITE_NEEDS[rule][0]
            for rule in rules
            if rule in _REWRITE_NEEDS and not _REWRITE_NEEDS[rule][1](vector, cog)
        }
        if unmet.keys() < rules:
            targets.add(path)
            continue
        needs = "; ".join(f"{rule} needs {need}" for rule, need in sorted(unmet.items()))
        emit("warn", f"Kept {path.name}: {needs} in .portolan/config.yaml")
    return frozenset(targets), frozenset(mirrors)


def _run_fixer_registry(
    *,
    findings: list[Any],
    run_metadata: bool,
    run_geo_assets: bool,
    dry_run: bool,
    state: RunState,
) -> tuple[FixReport, list[str], list[str], dict[str, list[str]]]:
    """Run the title sweep and the fixers that the AUTO findings call for."""
    from portolan_cli.catalog import apply_catalog_human_titles
    from portolan_cli.metadata.fix import FixAction, FixReport, FixResult
    from portolan_cli.validation.fixers import apply_fixers

    if not run_metadata:
        return FixReport(), [], [], {}
    catalog_root = state.catalog_root
    # No rashid rule reports metadata.yaml drift (rashid grades catalog.json alone), so this sweep runs on its own.
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
    """Turn the outcome of ``check.run_fix_workflow`` into the JSON data of the fix."""
    from portolan_cli.convert import ConversionStatus
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
    # A conversion rewrites the bytes after the checksum fixer ran, so refresh file:size and file:checksum.
    conversion_report = format_fix_report.conversion_report if format_fix_report else None
    written_items.extend(
        result.output
        for result in (conversion_report.results if conversion_report else [])
        if result.status is ConversionStatus.SUCCESS and result.output is not None
    )

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
    """Build the ``fix`` section of the check payload."""
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
