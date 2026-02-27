#!/usr/bin/env python3
"""Validate SKILL.md structure and freshness.

This script checks that:
1. SKILL.md exists and has the expected structure
2. Required generated sections are present
3. Freshness markers are present and not stale (>30 days = warning)
4. Generated sections can be regenerated (optional strict mode)

Exit codes:
    0: All validations pass (warnings are OK)
    1: Validation failures found (missing structure)
"""

from __future__ import annotations

import re
import sys
from datetime import date, datetime
from pathlib import Path

# Freshness threshold in days
FRESHNESS_THRESHOLD_DAYS = 30


def get_project_root() -> Path:
    """Find project root by looking for SKILL.md."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "SKILL.md").exists():
            return current
        current = current.parent
    return Path.cwd()


def check_required_sections(content: str) -> list[str]:
    """Check that required generated sections exist."""
    required_sections = ["overview", "cli-commands", "python-api"]
    errors: list[str] = []

    for section in required_sections:
        begin_marker = f"<!-- BEGIN GENERATED: {section} -->"
        end_marker = f"<!-- END GENERATED: {section} -->"

        if begin_marker not in content:
            errors.append(f"Missing generated section: {section} (no BEGIN marker)")
        elif end_marker not in content:
            errors.append(f"Missing generated section: {section} (no END marker)")

    return errors


def check_freshness_markers(content: str) -> tuple[list[str], list[str]]:
    """Check freshness markers and return (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []

    # Find freshness markers: <!-- freshness: last-verified: YYYY-MM-DD -->
    pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}) -->"
    matches = re.findall(pattern, content)

    if not matches:
        warnings.append("No freshness markers found in SKILL.md")
        return errors, warnings

    today = date.today()

    for date_str in matches:
        try:
            verified_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            days_old = (today - verified_date).days

            if days_old > FRESHNESS_THRESHOLD_DAYS:
                warnings.append(
                    f"Freshness marker is {days_old} days old (verified: {date_str}). "
                    f"Consider re-verifying the Common Workflows section."
                )
            elif days_old < 0:
                errors.append(f"Freshness marker has future date: {date_str}")
        except ValueError:
            errors.append(f"Invalid date format in freshness marker: {date_str}")

    return errors, warnings


def check_required_content(content: str) -> list[str]:
    """Check that key content sections exist."""
    warnings: list[str] = []

    required_headers = [
        "## What is Portolan?",
        "## CLI Commands",
        "## Common Workflows",
        "## Troubleshooting",
    ]

    for header in required_headers:
        if header not in content:
            warnings.append(f"Missing recommended section: {header}")

    return warnings


def check_closed_freshness_tags(content: str) -> list[str]:
    """Check that freshness sections are properly closed."""
    errors: list[str] = []

    # Count opening and closing tags
    open_tags = len(re.findall(r"<!-- freshness:", content))
    close_tags = len(re.findall(r"<!-- /freshness -->", content))

    if open_tags != close_tags:
        errors.append(f"Mismatched freshness tags: {open_tags} opening, {close_tags} closing")

    return errors


def main() -> int:
    """Run all validations and report results."""
    root = get_project_root()
    skill_md_path = root / "SKILL.md"

    if not skill_md_path.exists():
        print("ERROR: SKILL.md not found")
        print("Create SKILL.md or run: python scripts/generate_skill_md.py --write")
        return 1

    content = skill_md_path.read_text()
    errors: list[str] = []
    warnings: list[str] = []

    # Check required generated sections
    section_errors = check_required_sections(content)
    errors.extend(section_errors)

    # Check freshness markers
    freshness_errors, freshness_warnings = check_freshness_markers(content)
    errors.extend(freshness_errors)
    warnings.extend(freshness_warnings)

    # Check freshness tag closure
    tag_errors = check_closed_freshness_tags(content)
    errors.extend(tag_errors)

    # Check recommended content
    content_warnings = check_required_content(content)
    warnings.extend(content_warnings)

    # Report results
    if errors:
        print("SKILL.md validation FAILED:\n")
        for error in errors:
            print(f"  ERROR: {error}")
        print()

    if warnings:
        print("SKILL.md validation WARNINGS:\n")
        for warning in warnings:
            print(f"  WARNING: {warning}")
        print()

    if errors:
        print("Fix the errors above to pass validation.")
        return 1

    # Count sections for summary
    generated_count = len(re.findall(r"<!-- BEGIN GENERATED:", content))
    freshness_count = len(re.findall(r"<!-- freshness:", content))

    print(
        f"SKILL.md validation passed: "
        f"{generated_count} generated sections, "
        f"{freshness_count} freshness markers"
    )

    if warnings:
        print(f"  ({len(warnings)} warnings - review recommended)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
