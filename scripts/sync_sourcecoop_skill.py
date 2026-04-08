#!/usr/bin/env python3
"""Sync the Source Co-op skill CLI reference with actual command definitions.

This script auto-generates the CLI reference section in sourcecoop.md by
introspecting Click commands. It ensures the skill stays in sync with the
actual CLI interface.

Usage:
    python scripts/sync_sourcecoop_skill.py          # Preview changes
    python scripts/sync_sourcecoop_skill.py --update # Apply changes

Exit codes:
    0: Success (no changes needed, or --update applied)
    1: Changes needed (in check mode) or error occurred
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def get_project_root() -> Path:
    """Find project root by looking for pyproject.toml."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path.cwd()


def get_command_help(cmd_name: str, subcommand: str | None = None) -> tuple[str, list[str]]:
    """Get help text and examples for a command.

    Returns:
        Tuple of (short_help, examples).
    """
    try:
        from portolan_cli import cli

        # Navigate to the command
        if subcommand:
            group = cli.commands.get(cmd_name)
            if group and hasattr(group, "commands"):
                cmd = group.commands.get(subcommand)
            else:
                return "", []
        else:
            cmd = cli.commands.get(cmd_name)

        if cmd is None:
            return "", []

        help_text = cmd.help or ""
        short_help = help_text.split("\n\n")[0].strip() if help_text else ""

        # Extract examples from help text
        examples: list[str] = []
        in_examples = False
        for line in help_text.split("\n"):
            stripped = line.strip()
            if stripped.lower().startswith("example"):
                in_examples = True
                continue
            if in_examples and stripped.startswith("portolan "):
                examples.append(stripped)

        return short_help, examples

    except ImportError:
        return "", []


def generate_cli_reference() -> str:
    """Generate the CLI reference section for the Source Co-op skill."""
    # Commands used in the Source Co-op workflow
    workflow_commands = [
        ("init", None),
        ("config", "set"),
        ("config", "get"),
        ("config", "list"),
        ("add", None),
        ("metadata", "init"),
        ("metadata", "validate"),
        ("readme", None),
        ("push", None),
    ]

    lines = ["<!-- BEGIN GENERATED: cli-reference -->", "## CLI Command Reference", ""]

    for cmd_name, subcommand in workflow_commands:
        full_name = f"{cmd_name} {subcommand}" if subcommand else cmd_name
        short_help, examples = get_command_help(cmd_name, subcommand)

        lines.append(f"### `portolan {full_name}`")
        if short_help:
            lines.append(short_help)
        lines.append("")

        # Add curated examples for each command (not from help, for consistency)
        examples_map = {
            "init": [
                "portolan init                       # Initialize in current directory",
                "portolan init --auto                # Skip prompts, use defaults",
                'portolan init --title "My Catalog"  # Set title',
            ],
            "config set": [
                "portolan config set remote s3://bucket/path/   # Set remote destination",
                "portolan config set profile source-coop        # Set AWS profile",
            ],
            "config get": [
                "portolan config get remote                     # Get current remote",
            ],
            "config list": [
                "portolan config list                           # List all settings",
            ],
            "add": [
                "portolan add .                    # Add all files",
                "portolan add demographics/        # Add collection",
                "portolan add file1.parquet        # Add specific file",
            ],
            "metadata init": [
                "portolan metadata init                # Create template at catalog root",
                "portolan metadata init --recursive    # Create for catalog and all collections",
            ],
            "metadata validate": [
                "portolan metadata validate            # Validate metadata.yaml",
            ],
            "readme": [
                "portolan readme                    # Generate at catalog root",
                "portolan readme --recursive        # Generate for catalog and all collections",
                "portolan readme --check            # CI mode: exit 1 if stale",
            ],
            "push": [
                "portolan push                              # Push to configured remote",
                "portolan push --dry-run                    # Preview without uploading",
                "portolan push --workers 8                  # Parallel uploads (max 8 recommended)",
                "portolan push --verbose                    # Show per-file progress",
                "portolan push --profile source-coop        # Override AWS profile",
            ],
        }

        cmd_examples = examples_map.get(full_name, [])
        if cmd_examples:
            lines.append("```bash")
            lines.extend(cmd_examples)
            lines.append("```")
            lines.append("")

    lines.append("<!-- END GENERATED: cli-reference -->")
    return "\n".join(lines)


def update_skill_file(content: str, section_name: str, new_content: str) -> str:
    """Update a generated section in the skill file."""
    pattern = rf"<!-- BEGIN GENERATED: {section_name} -->.*?<!-- END GENERATED: {section_name} -->"
    return re.sub(pattern, new_content, content, flags=re.DOTALL)


def main() -> int:
    """Main entry point."""
    root = get_project_root()
    skill_path = root / "portolan_cli" / "skills" / "sourcecoop.md"

    # Add project root to path for imports
    sys.path.insert(0, str(root))

    if not skill_path.exists():
        print(f"ERROR: Skill file not found: {skill_path}")
        return 1

    update_mode = "--update" in sys.argv
    check_mode = "--check" in sys.argv

    # Read current content
    original_content = skill_path.read_text()

    # Generate new section
    cli_reference = generate_cli_reference()

    # Update content
    updated_content = update_skill_file(original_content, "cli-reference", cli_reference)

    # Check for changes
    if original_content == updated_content:
        print("sourcecoop.md is up to date")
        return 0

    if check_mode:
        print("sourcecoop.md CLI reference is out of date")
        print("Run: python scripts/sync_sourcecoop_skill.py --update")
        return 1

    if update_mode:
        skill_path.write_text(updated_content)
        print("Updated sourcecoop.md CLI reference")
        return 0

    # Preview mode
    print("=" * 60)
    print("CHANGES DETECTED - Preview of updated CLI reference:")
    print("=" * 60)
    print(cli_reference)
    print()
    print("Run with --update to apply changes")
    print("Run with --check to fail if out of date (for CI)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
