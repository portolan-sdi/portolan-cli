#!/usr/bin/env python3
"""Validate quickstart content for required sections and syntax.

This script ensures the agent quickstart content includes all required
sections and has valid markdown structure.

Usage:
    python scripts/validate_quickstart.py           # Validate (exit 1 if invalid)
    python scripts/validate_quickstart.py --check   # Same as above (for prek hook)

Exit codes:
    0: Valid
    1: Invalid or missing required content
"""

from __future__ import annotations

import sys
from pathlib import Path

# Required sections that must be present in quickstart content
REQUIRED_SECTIONS = [
    "# Portolan Agent Quickstart",
    "## Session Setup",
    "## STAC Terminology",
    "## Full Workflow Example",
    "## Key Commands",
    "## JSON Output",
]

# Required terminology (STAC terms must be documented)
REQUIRED_TERMS = ["Catalog", "Collection", "Item", "Asset"]

# Required commands in example session
REQUIRED_COMMANDS = [
    "portolan init",
    "portolan add",
    "portolan scan",
    "portolan check",
]


def get_project_root() -> Path:
    """Find project root by looking for pyproject.toml."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path.cwd()


def validate_quickstart() -> list[str]:
    """Validate quickstart content and return list of errors."""
    errors: list[str] = []

    # Import the quickstart content
    root = get_project_root()
    sys.path.insert(0, str(root))

    try:
        from portolan_cli.quickstart_content import QUICKSTART
    except ImportError as e:
        return [f"Could not import quickstart content: {e}"]

    # Check required sections
    for section in REQUIRED_SECTIONS:
        if section not in QUICKSTART:
            errors.append(f"Missing required section: {section}")

    # Check STAC terminology
    for term in REQUIRED_TERMS:
        if term not in QUICKSTART:
            errors.append(f"Missing STAC term: {term}")

    # Check example commands
    for cmd in REQUIRED_COMMANDS:
        if cmd not in QUICKSTART:
            errors.append(f"Missing example command: {cmd}")

    # Check for JSON format mention (agent parsing)
    if "--format=json" not in QUICKSTART and "--json" not in QUICKSTART:
        errors.append("Missing JSON format documentation for agent parsing")

    # Check markdown code blocks are balanced
    code_block_count = QUICKSTART.count("```")
    if code_block_count % 2 != 0:
        errors.append(f"Unbalanced code blocks: {code_block_count} backtick sequences")

    return errors


def main() -> int:
    """Run validation and print results."""
    errors = validate_quickstart()

    if errors:
        print("Quickstart validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print("Quickstart content valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
