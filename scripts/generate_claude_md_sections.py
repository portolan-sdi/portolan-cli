#!/usr/bin/env python3
"""Generate auto-updated sections for CLAUDE.md.

This script generates sections that should stay in sync with the codebase:
1. ADR Index - from context/shared/adr/
2. Known Issues - from context/shared/known-issues/
3. Test Markers - from pyproject.toml
4. CLI Commands - from portolan_cli/cli.py

Usage:
    python scripts/generate_claude_md_sections.py [--section SECTION] [--dry-run]

The script outputs the generated content to stdout. Use --dry-run to preview
without updating CLAUDE.md.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


def get_project_root() -> Path:
    """Find project root by looking for CLAUDE.md."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "CLAUDE.md").exists():
            return current
        current = current.parent
    return Path.cwd()


# =============================================================================
# ADR Index Generator
# =============================================================================


@dataclass
class ADRInfo:
    """Information about an ADR."""

    number: str
    title: str
    path: str


def extract_adr_title(adr_path: Path) -> str:
    """Extract title from ADR file (first # heading)."""
    try:
        content = adr_path.read_text()
        # Match first H1 heading
        match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if match:
            title = match.group(1).strip()
            # Remove ADR number prefix if present
            title = re.sub(r"^ADR[- ]\d+[:\s-]*", "", title, flags=re.IGNORECASE)
            return title
        # Fallback: derive from filename
        return adr_path.stem.split("-", 1)[-1].replace("-", " ").title()
    except (FileNotFoundError, UnicodeDecodeError):
        return adr_path.stem


def generate_adr_index(root: Path) -> str:
    """Generate ADR index table."""
    adr_dir = root / "context" / "shared" / "adr"
    if not adr_dir.exists():
        return "No ADRs found."

    adrs: list[ADRInfo] = []
    for adr_file in sorted(adr_dir.glob("*.md")):
        if adr_file.name == "0000-template.md":
            continue

        # Extract ADR number
        match = re.match(r"(\d{4})-(.+)\.md", adr_file.name)
        if match:
            number = match.group(1)
            title = extract_adr_title(adr_file)
            rel_path = f"context/shared/adr/{adr_file.name}"
            adrs.append(ADRInfo(number=number, title=title, path=rel_path))

    if not adrs:
        return "No ADRs found."

    lines = ["| ADR | Decision |", "|-----|----------|"]
    for adr in adrs:
        lines.append(f"| [{adr.number}]({adr.path}) | {adr.title} |")

    return "\n".join(lines)


# =============================================================================
# Known Issues Generator
# =============================================================================


@dataclass
class IssueInfo:
    """Information about a known issue."""

    name: str
    impact: str
    path: str


def extract_issue_impact(issue_path: Path) -> str:
    """Extract impact summary from known issue file."""
    try:
        content = issue_path.read_text()
        # Look for ## Impact or ## Summary section
        match = re.search(r"##\s+(?:Impact|Summary)\s*\n+([^\n#]+)", content, re.IGNORECASE)
        if match:
            return match.group(1).strip()[:80]  # Truncate to 80 chars
        # Fallback: first non-heading line
        for line in content.split("\n"):
            line = line.strip()
            if line and not line.startswith("#"):
                return line[:80]
        return "See file for details"
    except (FileNotFoundError, UnicodeDecodeError):
        return "See file for details"


def generate_known_issues(root: Path) -> str:
    """Generate known issues table."""
    issues_dir = root / "context" / "shared" / "known-issues"
    if not issues_dir.exists():
        return "No known issues documented."

    issues: list[IssueInfo] = []
    for issue_file in sorted(issues_dir.glob("*.md")):
        if issue_file.name == "example.md":
            continue

        name = issue_file.stem.replace("-", " ").title()
        impact = extract_issue_impact(issue_file)
        rel_path = f"context/shared/known-issues/{issue_file.name}"
        issues.append(IssueInfo(name=name, impact=impact, path=rel_path))

    if not issues:
        return "No known issues documented."

    lines = ["| Issue | Impact |", "|-------|--------|"]
    for issue in issues:
        lines.append(f"| [{issue.name}]({issue.path}) | {issue.impact} |")

    return "\n".join(lines)


# =============================================================================
# Test Markers Generator
# =============================================================================


def generate_test_markers(root: Path) -> str:
    """Generate test markers documentation from pyproject.toml."""
    pyproject_path = root / "pyproject.toml"
    if not pyproject_path.exists():
        return "No markers defined."

    try:
        content = pyproject_path.read_text()
        # Parse markers = [...] section
        marker_pattern = r"markers\s*=\s*\[(.*?)\]"
        match = re.search(marker_pattern, content, re.DOTALL)

        if not match:
            return "No markers defined."

        markers_section = match.group(1)
        markers: list[tuple[str, str]] = []

        for line in markers_section.split("\n"):
            line = line.strip().strip(",").strip('"').strip("'")
            if ":" in line:
                name, description = line.split(":", 1)
                markers.append((name.strip(), description.strip()))

        if not markers:
            return "No markers defined."

        lines = ["```python"]
        for name, desc in markers:
            lines.append(f"@pytest.mark.{name}  # {desc}")
        lines.append("```")

        return "\n".join(lines)

    except (FileNotFoundError, ValueError):
        return "Error reading pyproject.toml"


# =============================================================================
# CLI Commands Generator
# =============================================================================


@dataclass
class CLICommand:
    """Information about a CLI command."""

    name: str
    function: str
    line: int
    docstring: str | None


def extract_cli_commands(root: Path) -> list[CLICommand]:
    """Extract CLI commands from portolan_cli/cli.py."""
    cli_path = root / "portolan_cli" / "cli.py"
    if not cli_path.exists():
        return []

    commands: list[CLICommand] = []

    try:
        source = cli_path.read_text()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                command_name: str | None = None

                # Check decorators for @cli.command() or @cli.group()
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Call):
                        if isinstance(decorator.func, ast.Attribute):
                            if decorator.func.attr in ("command", "group"):
                                # Check for explicit name argument
                                if decorator.args:
                                    arg = decorator.args[0]
                                    if isinstance(arg, ast.Constant):
                                        command_name = str(arg.value)
                                else:
                                    # Use function name
                                    fn_name = node.name.replace("_cmd", "")
                                    command_name = fn_name.replace("_", "-")

                if command_name:
                    docstring = ast.get_docstring(node)
                    commands.append(
                        CLICommand(
                            name=command_name,
                            function=node.name,
                            line=node.lineno,
                            docstring=docstring,
                        )
                    )

    except (SyntaxError, FileNotFoundError):
        pass

    return sorted(commands, key=lambda c: c.name)


def generate_cli_commands(root: Path) -> str:
    """Generate CLI commands documentation."""
    commands = extract_cli_commands(root)

    if not commands:
        return "No CLI commands found."

    lines = ["| Command | Description |", "|---------|-------------|"]

    for cmd in commands:
        desc = "No description"
        if cmd.docstring:
            # First line of docstring
            first_line = cmd.docstring.split("\n")[0].strip()
            if first_line:
                desc = first_line[:60] + ("..." if len(first_line) > 60 else "")

        lines.append(f"| `portolan {cmd.name}` | {desc} |")

    return "\n".join(lines)


# =============================================================================
# Section Updater
# =============================================================================


def update_generated_section(claude_md: str, section_name: str, new_content: str) -> str:
    """Update an auto-generated section in CLAUDE.md.

    Sections are marked with:
    <!-- auto-generated: {section_name} -->
    ... content ...
    <!-- /auto-generated: {section_name} -->
    """
    pattern = (
        rf"(<!--\s*auto-generated:\s*{re.escape(section_name)}\s*-->\n)"
        rf".*?"
        rf"(\n<!--\s*/auto-generated:\s*{re.escape(section_name)}\s*-->)"
    )

    replacement = rf"\g<1>{new_content}\g<2>"

    updated, count = re.subn(pattern, replacement, claude_md, flags=re.DOTALL)

    if count == 0:
        print(f"Warning: Section '{section_name}' not found in CLAUDE.md", file=sys.stderr)
        return claude_md

    return updated


def add_freshness_marker(section_header: str, date: str | None = None) -> str:
    """Generate a freshness marker for a section."""
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    return f"<!-- freshness: last-verified: {date} -->\n{section_header}"


# =============================================================================
# Main
# =============================================================================


GENERATORS = {
    "adr-index": generate_adr_index,
    "known-issues": generate_known_issues,
    "test-markers": generate_test_markers,
    "cli-commands": generate_cli_commands,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate auto-updated sections for CLAUDE.md")
    parser.add_argument(
        "--section",
        choices=list(GENERATORS.keys()) + ["all"],
        default="all",
        help="Section to generate (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated content without updating CLAUDE.md",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update CLAUDE.md in place (requires auto-generated markers)",
    )

    args = parser.parse_args()
    root = get_project_root()

    sections_to_generate = list(GENERATORS.keys()) if args.section == "all" else [args.section]

    outputs: dict[str, str] = {}
    for section in sections_to_generate:
        generator = GENERATORS[section]
        outputs[section] = generator(root)

    if args.dry_run or not args.update:
        # Print generated content
        for section, content in outputs.items():
            print(f"\n{'=' * 60}")
            print(f"Section: {section}")
            print("=" * 60)
            print(content)
        return 0

    # Update CLAUDE.md
    claude_md_path = root / "CLAUDE.md"
    if not claude_md_path.exists():
        print("ERROR: CLAUDE.md not found", file=sys.stderr)
        return 1

    claude_md = claude_md_path.read_text()
    updated = claude_md

    for section, content in outputs.items():
        updated = update_generated_section(updated, section, content)

    if updated != claude_md:
        claude_md_path.write_text(updated)
        print(f"Updated CLAUDE.md with {len(outputs)} section(s)")
    else:
        print("No changes made (sections may not be marked for auto-generation)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
