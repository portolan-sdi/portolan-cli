#!/usr/bin/env python3
"""Auto-update freshness markers in CLAUDE.md when related code changes.

This script maps code files to CLAUDE.md sections and updates freshness dates
when those files are modified. Run by pre-commit when mapped files change.

Usage:
    python scripts/update_freshness.py [changed_files...]

Exit codes:
    0: Success (markers updated or no updates needed)
    1: Error occurred
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

# Map code files to CLAUDE.md section headers
# When a mapped file changes, update the freshness marker for that section
FILE_TO_SECTION_MAP: dict[str, list[str]] = {
    "portolan_cli/output.py": ["Standardized Terminal Output"],
    "portolan_cli/errors.py": ["Standardized Terminal Output"],
    "portolan_cli/constants.py": ["Design Principles"],
    "portolan_cli/json_output.py": ["Standardized Terminal Output"],
    "portolan_cli/cli.py": ["Common Commands", "CLI Commands"],
}


def get_project_root() -> Path:
    """Find project root by looking for CLAUDE.md."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "CLAUDE.md").exists():
            return current
        current = current.parent
    return Path.cwd()


def update_freshness_marker(content: str, section_header: str, new_date: str) -> str:
    """Update freshness marker for a specific section.

    Looks for pattern:
        <!-- freshness: last-verified: YYYY-MM-DD -->
        ## Section Header
        ...
        <!-- /freshness -->

    And updates the date.
    """
    # Pattern: freshness marker followed by section header
    pattern = (
        r"(<!-- freshness: last-verified: )\d{4}-\d{2}-\d{2}( -->\n"
        rf"(?:#+\s*)?{re.escape(section_header)})"
    )

    replacement = rf"\g<1>{new_date}\g<2>"

    updated, count = re.subn(pattern, replacement, content, flags=re.IGNORECASE)

    if count > 0:
        return updated

    # Try alternate pattern: freshness marker directly before content
    alt_pattern = (
        r"(<!-- freshness: last-verified: )\d{4}-\d{2}-\d{2}( -->\n)"
        rf"(## {re.escape(section_header)})"
    )
    updated, count = re.subn(alt_pattern, rf"\g<1>{new_date}\g<2>\g<3>", content)

    return updated


def get_sections_for_files(changed_files: list[str]) -> set[str]:
    """Get all sections that need freshness updates based on changed files."""
    sections: set[str] = set()
    for file_path in changed_files:
        # Normalize path
        normalized = file_path.replace("\\", "/")
        # Check direct match
        if normalized in FILE_TO_SECTION_MAP:
            sections.update(FILE_TO_SECTION_MAP[normalized])
        # Check basename match
        for mapped_path, mapped_sections in FILE_TO_SECTION_MAP.items():
            if normalized.endswith(mapped_path):
                sections.update(mapped_sections)
    return sections


def main() -> int:
    """Update freshness markers for changed files."""
    root = get_project_root()
    claude_md_path = root / "CLAUDE.md"

    if not claude_md_path.exists():
        print("ERROR: CLAUDE.md not found")
        return 1

    # Get changed files from args (pre-commit passes them)
    changed_files = sys.argv[1:] if len(sys.argv) > 1 else []

    if not changed_files:
        # No files passed - nothing to update
        return 0

    # Get sections that need updating
    sections = get_sections_for_files(changed_files)

    if not sections:
        # No mapped sections for these files
        return 0

    # Update freshness markers
    today = datetime.now().strftime("%Y-%m-%d")
    content = claude_md_path.read_text()
    original = content

    for section in sections:
        content = update_freshness_marker(content, section, today)

    if content != original:
        claude_md_path.write_text(content)
        print(f"Updated freshness for: {', '.join(sorted(sections))}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
