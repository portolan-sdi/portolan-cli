#!/usr/bin/env python3
"""Validate CLI documentation matches actual CLI structure.

This script extracts the CLI structure from Click commands and compares
against a baseline snapshot. Fails CI if CLI changes without doc updates.

Usage:
    python scripts/validate_cli_docs.py           # Validate against snapshot
    python scripts/validate_cli_docs.py --update  # Update snapshot
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from portolan_cli.cli import cli

SNAPSHOT_PATH = Path("docs/.cli-snapshot.json")


def extract_command_info(cmd: click.Command, name: str = "") -> dict[str, Any]:
    """Extract command information recursively."""
    info: dict[str, Any] = {
        "name": name or cmd.name or "cli",
        "help": (cmd.help or "").strip().split("\n")[0],  # First line only
        "options": [],
        "arguments": [],
    }

    # Extract options
    for param in cmd.params:
        if isinstance(param, click.Option):
            option_info = {
                "names": list(param.opts),
                "required": param.required,
                "type": _get_type_name(param.type),
                "help": (param.help or "").strip(),
            }
            if param.default is not None and param.default != ():
                option_info["default"] = _serialize_default(param.default)
            info["options"].append(option_info)
        elif isinstance(param, click.Argument):
            info["arguments"].append(
                {
                    "name": param.name,
                    "required": param.required,
                    "type": _get_type_name(param.type),
                }
            )

    # Extract subcommands for groups
    if isinstance(cmd, click.Group):
        info["subcommands"] = {}
        for subcmd_name, subcmd in sorted(cmd.commands.items()):
            info["subcommands"][subcmd_name] = extract_command_info(subcmd, subcmd_name)

    return info


def _get_type_name(param_type: click.ParamType) -> str:
    """Get a human-readable type name."""
    if isinstance(param_type, click.Choice):
        return f"Choice([{', '.join(param_type.choices)}])"
    if isinstance(param_type, click.Path):
        return "Path"
    return param_type.name.upper()


def _serialize_default(value: Any) -> Any:
    """Serialize default value for JSON."""
    if isinstance(value, Path):
        return str(value)
    if callable(value):
        return "<dynamic>"
    # Handle Click's Sentinel objects and other non-serializable types
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return f"<{type(value).__name__}>"


def generate_snapshot() -> dict[str, Any]:
    """Generate CLI snapshot from current code."""
    return {
        "version": "1.0",
        "cli": extract_command_info(cli),
    }


def load_snapshot() -> dict[str, Any] | None:
    """Load existing snapshot if it exists."""
    if not SNAPSHOT_PATH.exists():
        return None
    return json.loads(SNAPSHOT_PATH.read_text())


def save_snapshot(snapshot: dict[str, Any]) -> None:
    """Save snapshot to file."""
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")


def compare_snapshots(current: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    """Compare two snapshots and return list of differences."""
    differences: list[str] = []
    _compare_commands(current["cli"], baseline["cli"], "portolan", differences)
    return differences


def _compare_commands(
    current: dict[str, Any],
    baseline: dict[str, Any],
    path: str,
    differences: list[str],
) -> None:
    """Recursively compare command structures."""
    # Check for new/removed options
    current_opts = {tuple(o["names"]) for o in current.get("options", [])}
    baseline_opts = {tuple(o["names"]) for o in baseline.get("options", [])}

    for opt in current_opts - baseline_opts:
        differences.append(f"New option in {path}: {opt}")
    for opt in baseline_opts - current_opts:
        differences.append(f"Removed option from {path}: {opt}")

    # Check for new/removed arguments
    current_args = {a["name"] for a in current.get("arguments", [])}
    baseline_args = {a["name"] for a in baseline.get("arguments", [])}

    for arg in current_args - baseline_args:
        differences.append(f"New argument in {path}: {arg}")
    for arg in baseline_args - current_args:
        differences.append(f"Removed argument from {path}: {arg}")

    # Check for new/removed subcommands
    current_subs = set(current.get("subcommands", {}).keys())
    baseline_subs = set(baseline.get("subcommands", {}).keys())

    for sub in current_subs - baseline_subs:
        differences.append(f"New subcommand: {path} {sub}")
    for sub in baseline_subs - current_subs:
        differences.append(f"Removed subcommand: {path} {sub}")

    # Recursively check common subcommands
    for sub in current_subs & baseline_subs:
        _compare_commands(
            current["subcommands"][sub],
            baseline["subcommands"][sub],
            f"{path} {sub}",
            differences,
        )


def main() -> int:
    """Main entry point."""
    update_mode = "--update" in sys.argv

    current = generate_snapshot()

    if update_mode:
        save_snapshot(current)
        print(f"Updated CLI snapshot: {SNAPSHOT_PATH}")
        return 0

    baseline = load_snapshot()

    if baseline is None:
        print(f"No baseline snapshot found at {SNAPSHOT_PATH}")
        print("Run with --update to create initial snapshot")
        return 1

    differences = compare_snapshots(current, baseline)

    if differences:
        print("CLI structure has changed! Update docs and snapshot:")
        print()
        for diff in differences:
            print(f"  - {diff}")
        print()
        print("To update the snapshot, run:")
        print("  uv run python scripts/validate_cli_docs.py --update")
        return 1

    print("CLI snapshot is up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
