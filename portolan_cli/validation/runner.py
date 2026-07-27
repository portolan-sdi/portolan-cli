"""Run rashid over a catalog and return everything `check` needs to render.

One call, one outcome. The CLI does not orchestrate passes; it says which ones
it wants and gets a :class:`CheckOutcome` back (ADR-0007).

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
from pathlib import Path
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
class CheckOutcome:
    """Everything one `portolan check` run produced.

    Attributes:
        report: rashid's report, or None when ``metadata=False``.
        format_report: Source-file convertibility (``scan.check.CheckReport``),
            or None when ``geo_assets=False``. This covers files on disk that
            are not yet catalog assets, which rashid by construction cannot see.
        legacy_note: Set when the catalog predates the profile schema URI.
        live_hint: Set when the catalog is published and ``--live`` was skipped.
    """

    report: Report | None
    format_report: Any
    legacy_note: str | None
    live_hint: LiveHint | None


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
        A :class:`CheckOutcome`.
    """
    root = _resolve_root(path)

    report: Report | None = None
    if metadata:
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
    )
