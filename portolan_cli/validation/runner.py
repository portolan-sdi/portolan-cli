"""Run rashid over a catalog and return everything `check` needs to render."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from rashid_core.api import LocalOnlyReader, ValidationOptions, validate

from portolan_cli.validation.config import load_public_url, load_rules_config
from portolan_cli.validation.legacy import detect_legacy_notes
from portolan_core.errors import InvalidLiveUrlError
from portolan_core.trace import span

if TYPE_CHECKING:
    from rashid_core.api import Report

    from portolan_core.config import ConfigSource
    from portolan_core.run_state import RunState

#: A structural/schema validator maps one object's raw JSON to schema errors.
#: Injected by tests to keep them offline and independent of schema churn.
Validator = Callable[[dict[str, Any]], list[Any]]


@dataclass(frozen=True)
class LiveHint:
    """The catalog is published but the live pass was not run."""

    base_url: str
    message: str


@dataclass(frozen=True)
class WorkflowNotice:
    """Data files the catalog does not account for — a workflow gap, not a defect."""

    unregistered: list[str]
    missing: list[str]
    message: str


@dataclass(frozen=True)
class CheckOutcome:
    """Everything one `portolan check` run produced."""

    report: Report | None
    format_report: Any
    legacy_note: str | None
    live_hint: LiveHint | None
    workflow_notice: WorkflowNotice | None = None


def _resolve_root(path: Path) -> Path:
    from portolan_cli.scan.check import resolve_catalog_root_for_check

    return resolve_catalog_root_for_check(path) or path


def validate_live_base_url(
    path: Path, public_url: str | None, *, state: ConfigSource | None = None
) -> None:
    """Reject invalid live URLs before validation or any requested repair."""
    base_url = public_url or load_public_url(_resolve_root(path), state=state)
    if base_url is None:
        return
    source = "--url" if public_url else "publish.public_url"
    try:
        parsed = urlparse(base_url)
        # urlparse defers port parsing. Reading .port raises on a bad port.
        _ = parsed.port
    except ValueError as err:
        raise InvalidLiveUrlError(source) from err
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise InvalidLiveUrlError(source)


def _live_hint(
    root: Path, public_url: str | None, *, live: bool, state: ConfigSource
) -> LiveHint | None:
    base_url = public_url or load_public_url(root, state=state)
    if live or base_url is None:
        return None
    return LiveHint(
        base_url=base_url,
        message=(
            f"This catalog is published at {base_url}. Run `portolan check --live` to verify the "
            "host serves the assets with HTTP Range support and CORS headers, and serves every "
            "linked file."
        ),
    )


def _notice_message(unregistered: list[str], missing: list[str]) -> str:
    """Say what is unaccounted for, and who can settle it."""
    parts = []
    if unregistered:
        parts.append(
            f"{len(unregistered)} data file(s) on disk are not registered in the catalog; "
            "run `portolan add` to register them, or delete them"
        )
    if missing:
        parts.append(
            f"{len(missing)} registered asset(s) are missing from disk; restore the files, "
            "or remove the assets from the catalog"
        )
    return f"{'. '.join(parts)}."


def _workflow_notice(root: Path, *, state: RunState) -> WorkflowNotice | None:
    """Account for data files the manifest and the filesystem disagree about."""
    from portolan_cli.metadata.models import MetadataStatus
    from portolan_cli.metadata.scan import scan_catalog_metadata

    try:
        report = scan_catalog_metadata(root, state=state)
    except FileNotFoundError:
        return None

    def _paths(status: MetadataStatus) -> list[str]:
        return [
            PurePath(result.file_path.relative_to(root)).as_posix()
            if result.file_path.is_absolute() and result.file_path.is_relative_to(root)
            else PurePath(result.file_path).as_posix()
            for result in report.results
            if result.status is status
        ]

    unregistered = _paths(MetadataStatus.ORPHANED)
    missing = _paths(MetadataStatus.MISSING)
    if not unregistered and not missing:
        return None
    return WorkflowNotice(
        unregistered=unregistered,
        missing=missing,
        message=_notice_message(unregistered, missing),
    )


def run_check(
    path: Path,
    *,
    data: bool = True,
    data_scope: str = "all",
    live: bool = False,
    structural: bool = True,
    schema: bool = False,
    geo_assets: bool = True,
    metadata: bool = True,
    public_url: str | None = None,
    workers: int | None = None,
    structural_validator: Validator | None = None,
    schema_validator: Validator | None = None,
    live_prober: Any = None,
    state: RunState | None = None,
) -> CheckOutcome:
    """Validate the catalog containing ``path`` and check its source files."""
    from portolan_core.run_state import RunState

    root = _resolve_root(path)
    # One run state, so the three config readers below parse config.yaml once.
    if state is None:
        state = RunState.open(root)

    report: Report | None = None
    notice: WorkflowNotice | None = None
    if metadata:
        if live:
            validate_live_base_url(root, public_url, state=state)
        # One extra manifest walk per run. `run_fix_workflow` walks separately
        # under --fix; deduping the two is a later refactor, not this change.
        notice = _workflow_notice(root, state=state)
        # rashid runs every rule in one call, so the span covers all validators.
        with span("check.validate", structural=structural, schema=schema, data=data, live=live):
            report = validate(
                root,
                options=ValidationOptions(
                    config=load_rules_config(root, state=state),
                    structural=structural,
                    structural_validator=structural_validator,
                    schema=schema,
                    schema_validator=schema_validator,
                    data=data,
                    data_reader_factory=LocalOnlyReader if data_scope == "local" else None,
                    live=live,
                    live_prober=live_prober,
                    live_base_url=public_url or load_public_url(root, state=state),
                ),
            )

    format_report = None
    if geo_assets:
        from portolan_cli.scan.check import check_directory

        with span("check.geo_assets"):
            format_report = check_directory(
                path,
                fix=False,
                dry_run=False,
                workers=workers,
                # The config and its conversion.paths globs belong to the
                # catalog root, also when the check covers a subdirectory.
                catalog_path=root,
                state=state,
            )

    return CheckOutcome(
        report=report,
        format_report=format_report,
        legacy_note=detect_legacy_notes(root),
        live_hint=_live_hint(root, public_url, live=live, state=state),
        workflow_notice=notice,
    )
