#!/usr/bin/env python3
"""Sync installation instructions from the upstream portolan-skills repository.

This script fetches the README from portolan-sdi/portolan-skills and extracts
the installation section, writing it to portolan_cli/skills/INSTALL.md.

Usage:
    python scripts/sync_skills_readme.py           # Preview changes
    python scripts/sync_skills_readme.py --update  # Apply changes
    python scripts/sync_skills_readme.py --check   # CI mode (exit 1 if stale)
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

UPSTREAM_URL = "https://raw.githubusercontent.com/portolan-sdi/portolan-skills/main/README.md"
OUTPUT_PATH = Path(__file__).parent.parent / "portolan_cli" / "skills" / "INSTALL.md"


def fetch_readme() -> str:
    """Fetch README content from upstream repository."""
    with urllib.request.urlopen(UPSTREAM_URL, timeout=30) as response:
        return response.read().decode("utf-8")


def extract_installation_section(readme: str) -> str:
    """Extract installation section from README."""
    lines = readme.split("\n")
    in_section = False
    section_lines: list[str] = []

    for line in lines:
        # Start at ## Install
        if line.startswith("## Install"):
            in_section = True
            section_lines.append(line)
            continue

        # Stop at next ## heading
        if in_section and line.startswith("## ") and "Install" not in line:
            break

        if in_section:
            section_lines.append(line)

    if not section_lines:
        raise ValueError("Could not find ## Install section in README")

    return "\n".join(section_lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update", action="store_true", help="Write changes to file")
    parser.add_argument("--check", action="store_true", help="CI mode: exit 1 if stale")
    args = parser.parse_args()

    try:
        readme = fetch_readme()
        new_content = extract_installation_section(readme)
    except Exception as e:
        print(f"Error fetching upstream README: {e}", file=sys.stderr)
        return 1

    current_content = OUTPUT_PATH.read_text(encoding="utf-8") if OUTPUT_PATH.exists() else ""

    if new_content == current_content:
        print("INSTALL.md is up to date")
        return 0

    if args.check:
        print("INSTALL.md is stale. Run: python scripts/sync_skills_readme.py --update")
        return 1

    if args.update:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(new_content, encoding="utf-8")
        print(f"Updated {OUTPUT_PATH}")
        return 0

    # Preview mode
    print("Would write to", OUTPUT_PATH)
    print("-" * 40)
    print(new_content)
    print("-" * 40)
    print("Run with --update to apply")
    return 0


if __name__ == "__main__":
    sys.exit(main())
