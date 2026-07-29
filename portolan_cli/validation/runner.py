"""Run rashid over a catalog and return everything `check` needs to render.

One call, one outcome. The CLI does not orchestrate passes; it says which ones
it wants and gets a :class:`CheckOutcome` back.

Which passes run by default, and why:

- **metadata** (rashid's ``PTL-*`` rule set) — always, it is the point.
- **structural** (STAC 1.1.0) — on. rashid 0.1.1 ships the STAC schema closure
  in its wheel, so this is offline and costs a schema walk.
- **data** (asset bytes: checksum, size, format, COG and GeoParquet internals) —
  on. Verifying that the bytes match what the metadata claims is most of what a
  catalog validator is for, and rashid promoted its geospatial stack to core
  dependencies precisely so the pass could default on.
- **schema** (the Portolan profile JSON Schema) — **off**. rashid maintains a
  parity invariant between the profile schema and its hand-written rules, so
  running both restates every defect twice: once from a rule that names the
  field and suggests a fix, once from a schema error that does neither. For an
  agent working a check → fix → re-check loop that second copy is pure noise.
  ``--schema`` turns it on, which is worth doing when validating a catalog some
  other tool produced.
- **live** (HTTP Range and CORS against the published host) — off, it reaches
  the network. ``--live`` opts in; when the catalog is published and ``--live``
  was not passed, the outcome carries a :class:`LiveHint` saying so.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Any

from rashid import validate
from rashid.model import Report

from portolan_cli.validation.config import load_public_url, load_rules_config
from portolan_cli.validation.legacy import detect_legacy_generation

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
    """Data files the catalog does not account for — a workflow gap, not a defect.

    This is deliberately **not** a conformance finding and must never be
    rendered as a ``PTL-*`` id. rashid validates the catalog it is handed; a
    file nothing in the manifest points at is outside that catalog, so no rule
    can fire on it, and inventing one would mean asserting a spec requirement
    that does not exist. It is still worth telling the operator about, which is
    what this separate channel is for. If you are tempted to promote it into
    ``findings``, write the spec requirement first.

    Attributes:
        unregistered: Catalog-relative posix paths of data files on disk that no
            manifest registers (``MetadataStatus.ORPHANED``).
        missing: Catalog-relative posix paths registered in a manifest whose
            bytes are gone (``MetadataStatus.MISSING``).
        message: One-line human summary naming both counts.
    """

    unregistered: list[str]
    missing: list[str]
    message: str


@dataclass(frozen=True)
class CheckOutcome:
    """Everything one `portolan check` run produced.

    Attributes:
        report: rashid's report, or None when ``metadata=False``.
        format_report: Source-file convertibility (``scan.check.CheckReport``),
            or None when ``geo_assets=False``. This covers files on disk that
            are not yet catalog assets, which rashid by construction cannot see.
        legacy_note: Set when the catalog predates the profile schema URI.
        live_hint: Set when the catalog is published and ``--live`` was skipped.
        workflow_notice: Unregistered or vanished data files, when there are any.
            A workflow channel, never conformance — see :class:`WorkflowNotice`.
    """

    report: Report | None
    format_report: Any
    legacy_note: str | None
    live_hint: LiveHint | None
    workflow_notice: WorkflowNotice | None = None


def _resolve_root(path: Path) -> Path:
    from portolan_cli.scan.check import resolve_catalog_root_for_check

    return resolve_catalog_root_for_check(path) or path


def _live_hint(root: Path, public_url: str | None, *, live: bool) -> LiveHint | None:
    base_url = public_url or load_public_url(root)
    if live or base_url is None:
        return None
    return LiveHint(
        base_url=base_url,
        message=(
            f"This catalog is published at {base_url}. Run `portolan check --live` to verify the "
            "host serves the assets with HTTP Range support and CORS headers."
        ),
    )


def _notice_message(unregistered: list[str], missing: list[str]) -> str:
    """Say what is unaccounted for, and who can settle it.

    Neither case is something ``--fix`` resolves. An orphan is explicitly not
    auto-fixable: fabricating an ``item.json`` for a file nobody
    registered guesses at metadata. A missing asset is bytes that are gone, and
    no repair pass can bring them back.
    """
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


def _workflow_notice(root: Path) -> WorkflowNotice | None:
    """Account for data files the manifest and the filesystem disagree about.

    Deliberately kept off the findings list — see :class:`WorkflowNotice`. Only
    ORPHANED and MISSING land here; STALE and BREAKING describe a *registered*
    asset whose metadata drifted, which is ``--fix``'s freshness pass, not an
    accounting gap.
    """
    from portolan_cli.metadata.models import MetadataStatus
    from portolan_cli.metadata.scan import scan_catalog_metadata

    try:
        report = scan_catalog_metadata(root)
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
) -> CheckOutcome:
    """Validate the catalog containing ``path`` and check its source files.

    Args:
        path: Directory to check. Resolved up to the catalog root, so checking a
            collection subdirectory validates the catalog it belongs to.
        data: Run the data pass over asset bytes.
        live: Probe the published host over HTTP.
        structural: Run the STAC 1.1.0 structural pass.
        schema: Run the Portolan profile schema pass (off by default; see the
            module docstring).
        geo_assets: Check source files on disk for cloud-native convertibility.
        metadata: Run rashid at all. False checks geo-assets only.
        public_url: Base URL the catalog is published under, overriding
            ``publish.public_url`` from config.
        workers: Parallel workers for the source-file scan.
        structural_validator: Injected structural validator (testing).
        schema_validator: Injected schema validator (testing).
        live_prober: Injected HTTP prober (testing).

    Returns:
        A :class:`CheckOutcome`. With ``metadata=True`` it also carries a
        :class:`WorkflowNotice` when the manifest and the filesystem disagree
        about which files exist — a workflow channel, never a conformance
        finding.
    """
    root = _resolve_root(path)

    report: Report | None = None
    notice: WorkflowNotice | None = None
    if metadata:
        # One extra manifest walk per run. `run_fix_workflow` walks separately
        # under --fix; deduping the two is a later refactor, not this change.
        notice = _workflow_notice(root)
        report = validate(
            root,
            config=load_rules_config(root),
            structural=structural,
            structural_validator=structural_validator,
            schema=schema,
            schema_validator=schema_validator,
            data=data,
            live=live,
            live_prober=live_prober,
            live_base_url=public_url or load_public_url(root),
        )

    format_report = None
    if geo_assets:
        from portolan_cli.scan.check import check_directory

        format_report = check_directory(
            path,
            fix=False,
            dry_run=False,
            workers=workers,
            catalog_path=path,
        )

    return CheckOutcome(
        report=report,
        format_report=format_report,
        legacy_note=detect_legacy_generation(root),
        live_hint=_live_hint(root, public_url, live=live),
        workflow_notice=notice,
    )
