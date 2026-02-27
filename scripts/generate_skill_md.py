#!/usr/bin/env python3
"""Generate SKILL.md sections from code introspection.

This script generates the auto-generated sections of SKILL.md by:
1. Extracting the package docstring from portolan_cli/__init__.py
2. Introspecting Click commands for CLI documentation
3. Extracting public API exports

Usage:
    python scripts/generate_skill_md.py          # Preview generated sections
    python scripts/generate_skill_md.py --write  # Update SKILL.md in place

Exit codes:
    0: Success
    1: Error occurred
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


def get_project_root() -> Path:
    """Find project root by looking for SKILL.md or pyproject.toml."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "SKILL.md").exists() or (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path.cwd()


def get_package_docstring() -> str:
    """Extract the package docstring from portolan_cli/__init__.py."""
    root = get_project_root()
    init_file = root / "portolan_cli" / "__init__.py"
    if not init_file.exists():
        return "Portolan CLI - Publish and manage cloud-native geospatial data catalogs."

    content = init_file.read_text()
    # Extract docstring (first triple-quoted string)
    match = re.search(r'^"""(.+?)"""', content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return "Portolan CLI - Publish and manage cloud-native geospatial data catalogs."


def get_cli_commands() -> list[tuple[str, str, str, list[str]]]:
    """Introspect Click CLI and extract command info.

    Returns list of (name, help_text, docstring, examples).
    """
    try:
        from portolan_cli import cli
    except ImportError:
        print("Warning: Could not import portolan_cli", file=sys.stderr)
        return []

    commands: list[tuple[str, str, str, list[str]]] = []

    # Get main CLI help
    if hasattr(cli, "help"):
        pass  # Main CLI group, skip

    # Iterate through commands
    if hasattr(cli, "commands"):
        for name, cmd in sorted(cli.commands.items()):
            help_text = cmd.help or ""
            # Extract short description (first paragraph)
            short_help = help_text.split("\n\n")[0].strip() if help_text else ""

            # Extract examples from help text
            examples = _extract_examples(help_text)

            commands.append((name, short_help, help_text, examples))

            # Handle command groups (like config)
            if hasattr(cmd, "commands"):
                for sub_name, sub_cmd in sorted(cmd.commands.items()):
                    sub_help = sub_cmd.help or ""
                    sub_short = sub_help.split("\n\n")[0].strip() if sub_help else ""
                    sub_examples = _extract_examples(sub_help)
                    commands.append((f"{name} {sub_name}", sub_short, sub_help, sub_examples))

    return commands


def _extract_examples(help_text: str) -> list[str]:
    """Extract example commands from Click help text."""
    examples: list[str] = []
    in_examples = False

    for line in help_text.split("\n"):
        stripped = line.strip()
        if stripped.lower().startswith("example"):
            in_examples = True
            continue
        if in_examples:
            if stripped.startswith("portolan "):
                examples.append(stripped)
            elif stripped and not stripped.startswith("-"):
                # Non-example line, stop collecting
                if not stripped.startswith("portolan"):
                    pass  # Could be continuation
    return examples


def get_public_api() -> list[tuple[str, str]]:
    """Extract public API exports from __all__."""
    try:
        import portolan_cli

        exports = getattr(portolan_cli, "__all__", [])
        result: list[tuple[str, str]] = []

        for name in exports:
            obj = getattr(portolan_cli, name, None)
            if obj is None:
                continue
            doc = getattr(obj, "__doc__", "") or ""
            # First line of docstring
            short_doc = doc.split("\n")[0].strip() if doc else ""
            result.append((name, short_doc))

        return result
    except ImportError:
        return []


def generate_overview_section() -> str:
    """Generate the overview section."""
    docstring = get_package_docstring()

    return f"""<!-- BEGIN GENERATED: overview -->
## What is Portolan?

{docstring}

Portolan is a CLI for publishing and managing **cloud-native geospatial data catalogs**. It orchestrates format conversion (GeoParquet, COG), versioning, and sync to object storage (S3, GCS, Azure)—no running servers, just static files.

**Key concepts:**
- **STAC** (SpatioTemporal Asset Catalog) — The catalog metadata spec
- **GeoParquet** — Cloud-optimized vector data (columnar, spatial indexing)
- **COG** (Cloud-Optimized GeoTIFF) — Cloud-optimized raster data (HTTP range requests)
- **versions.json** — Single source of truth for version history, sync state, and checksums
<!-- END GENERATED: overview -->"""


def generate_cli_commands_section() -> str:
    """Generate the CLI commands section from Click introspection."""
    commands = get_cli_commands()

    if not commands:
        return """<!-- BEGIN GENERATED: cli-commands -->
## CLI Commands

Run `portolan --help` for available commands.
<!-- END GENERATED: cli-commands -->"""

    lines = ["<!-- BEGIN GENERATED: cli-commands -->", "## CLI Commands", ""]

    for name, short_help, _full_help, examples in commands:
        # Skip subcommands for now (handle main commands only)
        if " " in name and name.startswith("config "):
            continue  # config subcommands are shown under config
        if " " in name:
            continue

        lines.append(f"### `portolan {name}`")
        if short_help:
            lines.append(short_help)
            lines.append("")

        # Add examples if available
        if examples:
            lines.append("```bash")
            for ex in examples[:4]:  # Limit to 4 examples
                lines.append(ex)
            lines.append("```")
            lines.append("")

    lines.append("<!-- END GENERATED: cli-commands -->")
    return "\n".join(lines)


def generate_python_api_section() -> str:
    """Generate the Python API section."""
    exports = get_public_api()

    lines = [
        "<!-- BEGIN GENERATED: python-api -->",
        "## Python API",
        "",
        "Portolan exposes a Python API for programmatic access:",
        "",
        "```python",
        "from portolan_cli import Catalog, FormatType, detect_format",
        "",
        "# Initialize a catalog",
        'catalog = Catalog("/path/to/data")',
        "",
        "# Detect file format",
        'format_type = detect_format("data.parquet")  # Returns FormatType.GEOPARQUET',
        "```",
        "",
        "**Public exports:**",
    ]

    for name, doc in exports:
        if doc:
            lines.append(f"- `{name}` - {doc}")
        else:
            lines.append(f"- `{name}`")

    lines.append("<!-- END GENERATED: python-api -->")
    return "\n".join(lines)


def update_skill_md(content: str, section_name: str, new_content: str) -> str:
    """Update a generated section in SKILL.md content."""
    pattern = rf"<!-- BEGIN GENERATED: {section_name} -->.*?<!-- END GENERATED: {section_name} -->"
    return re.sub(pattern, new_content, content, flags=re.DOTALL)


def main() -> int:
    """Generate SKILL.md sections."""
    root = get_project_root()
    skill_md_path = root / "SKILL.md"

    # Add project root to path for imports
    sys.path.insert(0, str(root))

    write_mode = "--write" in sys.argv

    # Generate sections
    overview = generate_overview_section()
    cli_commands = generate_cli_commands_section()
    python_api = generate_python_api_section()

    if write_mode:
        if not skill_md_path.exists():
            print("ERROR: SKILL.md not found. Create it first.")
            return 1

        content = skill_md_path.read_text()

        # Update each section
        content = update_skill_md(content, "overview", overview)
        content = update_skill_md(content, "cli-commands", cli_commands)
        content = update_skill_md(content, "python-api", python_api)

        skill_md_path.write_text(content)
        print("Updated SKILL.md with generated sections")
        return 0
    else:
        # Preview mode
        print("=" * 60)
        print("OVERVIEW SECTION")
        print("=" * 60)
        print(overview)
        print()
        print("=" * 60)
        print("CLI COMMANDS SECTION")
        print("=" * 60)
        print(cli_commands)
        print()
        print("=" * 60)
        print("PYTHON API SECTION")
        print("=" * 60)
        print(python_api)
        print()
        print("Run with --write to update SKILL.md")
        return 0


if __name__ == "__main__":
    sys.exit(main())
