#!/usr/bin/env python3
"""Validate CLAUDE.md references match actual files.

This script checks that:
1. All ADRs in context/shared/adr/ are listed in the CLAUDE.md index
2. All known issues in context/shared/known-issues/ are listed in CLAUDE.md
3. All links in CLAUDE.md point to files that exist

Exit codes:
    0: All validations pass
    1: Validation failures found
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def get_project_root() -> Path:
    """Find project root by looking for CLAUDE.md."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "CLAUDE.md").exists():
            return current
        current = current.parent
    # Fallback to cwd
    return Path.cwd()


def extract_adr_links(claude_md: str) -> set[str]:
    """Extract ADR file paths from CLAUDE.md ADR index table."""
    # Match: | [0001](context/shared/adr/0001-*.md) |
    pattern = r"\[(\d{4})\]\((context/shared/adr/\d{4}-[^)]+\.md)\)"
    return {match[1] for match in re.findall(pattern, claude_md)}


def extract_known_issue_links(claude_md: str) -> set[str]:
    """Extract known issue file paths from CLAUDE.md."""
    # Match: | [title](context/shared/known-issues/*.md) |
    pattern = r"\[([^\]]+)\]\((context/shared/known-issues/[^)]+\.md)\)"
    return {match[1] for match in re.findall(pattern, claude_md)}


def get_actual_adrs(root: Path) -> set[str]:
    """Get all ADR files (excluding template)."""
    adr_dir = root / "context" / "shared" / "adr"
    if not adr_dir.exists():
        return set()
    return {
        f"context/shared/adr/{f.name}" for f in adr_dir.glob("*.md") if f.name != "0000-template.md"
    }


def get_actual_known_issues(root: Path) -> set[str]:
    """Get all known issue files (excluding example)."""
    issues_dir = root / "context" / "shared" / "known-issues"
    if not issues_dir.exists():
        return set()
    return {
        f"context/shared/known-issues/{f.name}"
        for f in issues_dir.glob("*.md")
        if f.name != "example.md"
    }


def main() -> int:
    """Run all validations and report results."""
    root = get_project_root()
    claude_md_path = root / "CLAUDE.md"

    if not claude_md_path.exists():
        print("ERROR: CLAUDE.md not found")
        return 1

    claude_md = claude_md_path.read_text()
    errors: list[str] = []

    # Check ADRs
    linked_adrs = extract_adr_links(claude_md)
    actual_adrs = get_actual_adrs(root)

    missing_from_index = actual_adrs - linked_adrs
    broken_links = linked_adrs - actual_adrs

    if missing_from_index:
        errors.append(
            "ADRs not in CLAUDE.md index:\n"
            + "\n".join(f"  - {adr}" for adr in sorted(missing_from_index))
        )

    if broken_links:
        errors.append(
            "ADR links in CLAUDE.md point to non-existent files:\n"
            + "\n".join(f"  - {adr}" for adr in sorted(broken_links))
        )

    # Check known issues
    linked_issues = extract_known_issue_links(claude_md)
    actual_issues = get_actual_known_issues(root)

    missing_issues = actual_issues - linked_issues
    broken_issue_links = linked_issues - actual_issues

    if missing_issues:
        errors.append(
            "Known issues not in CLAUDE.md:\n"
            + "\n".join(f"  - {issue}" for issue in sorted(missing_issues))
        )

    if broken_issue_links:
        errors.append(
            "Known issue links in CLAUDE.md point to non-existent files:\n"
            + "\n".join(f"  - {issue}" for issue in sorted(broken_issue_links))
        )

    # Report results
    if errors:
        print("CLAUDE.md validation failed:\n")
        for error in errors:
            print(error)
            print()
        print(
            "Fix: Update CLAUDE.md to include all ADRs and known issues, "
            "or remove stale references."
        )
        return 1

    print(
        f"CLAUDE.md validation passed: {len(linked_adrs)} ADRs, {len(linked_issues)} known issues"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
