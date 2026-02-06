"""Portolan CLI - Command-line interface for managing cloud-native geospatial data.

The CLI is a thin wrapper around the Python API (see catalog.py).
All business logic lives in the library; the CLI handles user interaction.
"""

from __future__ import annotations

from pathlib import Path

import click

from portolan_cli.catalog import Catalog, CatalogExistsError
from portolan_cli.output import detail, error, info, success, warn
from portolan_cli.validation import Severity
from portolan_cli.validation import check as validate_catalog


@click.group()
@click.version_option()
def cli() -> None:
    """Portolan - Publish and manage cloud-native geospatial data catalogs."""
    pass


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
def init(path: Path) -> None:
    """Initialize a new Portolan catalog.

    Creates a .portolan directory with a STAC catalog.json file.

    PATH is the directory where the catalog should be created (default: current directory).
    """
    try:
        Catalog.init(path)
        success(f"Initialized Portolan catalog in {path.resolve()}")
    except CatalogExistsError as err:
        error(f"Catalog already exists at {path.resolve()}")
        raise SystemExit(1) from err


@cli.command()
@click.argument("path", type=click.Path(path_type=Path, exists=True), default=".")
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON")
@click.option("--verbose", "-v", is_flag=True, help="Show all validation rules, not just failures")
def check(path: Path, json_output: bool, verbose: bool) -> None:
    """Validate a Portolan catalog.

    Runs validation rules against the catalog and reports any issues.

    PATH is the directory containing the .portolan catalog (default: current directory).
    """
    import json

    report = validate_catalog(path)

    if json_output:
        click.echo(json.dumps(report.to_dict(), indent=2))
    else:
        # Human-readable output
        if verbose:
            # Show all rules
            for result in report.results:
                if result.passed:
                    success(f"{result.rule_name}: {result.message}")
                elif result.severity == Severity.ERROR:
                    error(f"{result.rule_name}: {result.message}")
                    if result.fix_hint:
                        detail(f"  Hint: {result.fix_hint}")
                elif result.severity == Severity.WARNING:
                    warn(f"{result.rule_name}: {result.message}")
                    if result.fix_hint:
                        detail(f"  Hint: {result.fix_hint}")
                else:
                    info(f"{result.rule_name}: {result.message}")
        else:
            # Show only failures
            for result in report.results:
                if not result.passed:
                    if result.severity == Severity.ERROR:
                        error(f"{result.rule_name}: {result.message}")
                    elif result.severity == Severity.WARNING:
                        warn(f"{result.rule_name}: {result.message}")
                    else:
                        info(f"{result.rule_name}: {result.message}")
                    if result.fix_hint:
                        detail(f"  Hint: {result.fix_hint}")

        # Summary
        if report.passed:
            success("All validation checks passed")
        else:
            error_count = len(report.errors)
            warning_count = len(report.warnings)
            parts = []
            if error_count:
                parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
            if warning_count:
                parts.append(f"{warning_count} warning{'s' if warning_count != 1 else ''}")
            error(f"Validation failed: {', '.join(parts)}")

    # Exit code: 1 if any errors (not warnings)
    if report.errors:
        raise SystemExit(1)
