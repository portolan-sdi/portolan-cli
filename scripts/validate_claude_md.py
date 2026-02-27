#!/usr/bin/env python3
"""Validate CLAUDE.md references match actual files.

This script checks that:
1. All ADRs in context/shared/adr/ are listed in the CLAUDE.md index
2. All known issues in context/shared/known-issues/ are listed in CLAUDE.md
3. All links in CLAUDE.md point to files that exist
4. All file path references point to existing files
5. All CLI command references match actual Click commands
6. All test markers match pyproject.toml definitions
7. All code import examples are valid
8. Freshness markers are not stale (warn only)

Exit codes:
    0: All validations pass
    1: Validation failures found
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class ValidationResult:
    """Result of a validation check."""

    validator: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def get_project_root() -> Path:
    """Find project root by looking for CLAUDE.md."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "CLAUDE.md").exists():
            return current
        current = current.parent
    # Fallback to cwd
    return Path.cwd()


# =============================================================================
# ADR and Known Issues Validators (existing)
# =============================================================================


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


def validate_adrs(claude_md: str, root: Path) -> ValidationResult:
    """Check all ADRs are indexed in CLAUDE.md."""
    result = ValidationResult(validator="ADR Index")

    linked_adrs = extract_adr_links(claude_md)
    actual_adrs = get_actual_adrs(root)

    missing_from_index = actual_adrs - linked_adrs
    broken_links = linked_adrs - actual_adrs

    if missing_from_index:
        result.errors.append(
            "ADRs not in CLAUDE.md index:\n"
            + "\n".join(f"  - {adr}" for adr in sorted(missing_from_index))
        )

    if broken_links:
        result.errors.append(
            "ADR links in CLAUDE.md point to non-existent files:\n"
            + "\n".join(f"  - {adr}" for adr in sorted(broken_links))
        )

    return result


def validate_known_issues(claude_md: str, root: Path) -> ValidationResult:
    """Check all known issues are documented in CLAUDE.md."""
    result = ValidationResult(validator="Known Issues")

    linked_issues = extract_known_issue_links(claude_md)
    actual_issues = get_actual_known_issues(root)

    missing_issues = actual_issues - linked_issues
    broken_links = linked_issues - actual_issues

    if missing_issues:
        result.errors.append(
            "Known issues not in CLAUDE.md:\n"
            + "\n".join(f"  - {issue}" for issue in sorted(missing_issues))
        )

    if broken_links:
        result.errors.append(
            "Known issue links in CLAUDE.md point to non-existent files:\n"
            + "\n".join(f"  - {issue}" for issue in sorted(broken_links))
        )

    return result


# =============================================================================
# FilePathValidator - Check that referenced paths exist
# =============================================================================


def validate_file_paths(claude_md: str, root: Path) -> ValidationResult:
    """Check that all backtick-quoted file paths in CLAUDE.md exist.

    Matches patterns like:
    - `path/to/file.py`
    - `context/shared/adr/0001-example.md`
    - `pyproject.toml`

    Excludes:
    - URLs (http://, https://)
    - Code snippets (function calls, imports, shell commands)
    - Patterns with wildcards (*, ?)
    """
    result = ValidationResult(validator="File Paths")

    # Match backtick-quoted paths that look like files
    # Must have a file extension and path separators
    pattern = r"`([a-zA-Z0-9_./\-]+\.(py|md|yaml|yml|json|toml|sh|txt))`"

    # Paths to ignore (code patterns, not actual files)
    ignore_patterns = [
        r"^https?://",  # URLs
        r"^\$",  # Shell variables
        r"^--",  # CLI flags
        r"^\w+\(",  # Function calls
        r"^from ",  # Import statements
        r"^import ",  # Import statements
        r"\*",  # Wildcards
        r"\?",  # Wildcards
        r"^test_\w+\.py$",  # Generic test file patterns
        r"^[A-Z_]+\.md$",  # Generic uppercase file patterns like README.md, CLAUDE.md
        r"^catalog\.json$",  # STAC example files
        r"^collection\.json$",  # STAC example files
        r"^item\.json$",  # STAC example files
        r"^data\.parquet$",  # Example data files
        r"demographics/",  # Example paths (STAC terminology section)
        r"NNNN-",  # ADR template pattern
        r"^output\.py$",  # Common reference without full path
        r"\.env$",  # Environment files (examples)
        r"credentials\.json$",  # Credential file examples
    ]

    matches = re.findall(pattern, claude_md)
    missing_paths: list[str] = []

    for match in matches:
        path_str = match[0] if isinstance(match, tuple) else match

        # Skip if matches ignore patterns
        if any(re.search(p, path_str) for p in ignore_patterns):
            continue

        # Check if path exists relative to project root
        full_path = root / path_str
        if not full_path.exists():
            # Also check if it's a relative path from common locations
            # Skip paths that are clearly examples or templates
            if not any(x in path_str for x in ["example", "template", "your_", "my_"]):
                missing_paths.append(path_str)

    if missing_paths:
        # Group by type for cleaner output
        result.warnings.append(
            "File paths referenced in CLAUDE.md may not exist:\n"
            + "\n".join(f"  - {p}" for p in sorted(set(missing_paths)))
        )

    return result


# =============================================================================
# CLICommandValidator - Check that CLI commands exist
# =============================================================================


def _normalize_function_name(name: str) -> str:
    """Convert function name to CLI command name."""
    return name.replace("_cmd", "").replace("_", "-")


def _get_explicit_command_name(decorator: ast.Call) -> str | None:
    """Extract explicit command name from decorator args."""
    if decorator.args:
        arg = decorator.args[0]
        if isinstance(arg, ast.Constant):
            return str(arg.value)
    return None


def _is_command_decorator(decorator: ast.expr, attr_name: str) -> bool:
    """Check if decorator is a command/group decorator."""
    if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):
        return decorator.func.attr == attr_name
    if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
        return decorator.func.id == attr_name
    if isinstance(decorator, ast.Attribute):
        return decorator.attr == attr_name
    return False


def _extract_command_from_function(node: ast.FunctionDef) -> str | None:
    """Extract command name from a function with Click decorators."""
    for decorator in node.decorator_list:
        if _is_command_decorator(decorator, "command"):
            if isinstance(decorator, ast.Call):
                explicit = _get_explicit_command_name(decorator)
                if explicit:
                    return explicit
            return _normalize_function_name(node.name)
        if _is_command_decorator(decorator, "group"):
            return _normalize_function_name(node.name)
    return None


def extract_cli_commands_from_source(root: Path) -> set[str]:
    """Extract CLI command names from portolan_cli/cli.py using AST parsing."""
    cli_path = root / "portolan_cli" / "cli.py"
    if not cli_path.exists():
        return set()

    commands: set[str] = set()

    try:
        source = cli_path.read_text()
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                cmd = _extract_command_from_function(node)
                if cmd:
                    commands.add(cmd)

    except (SyntaxError, FileNotFoundError):
        pass

    return commands


def validate_cli_commands(claude_md: str, root: Path) -> ValidationResult:
    """Check that CLI commands mentioned in CLAUDE.md exist.

    Matches patterns like:
    - `portolan init`
    - `portolan check --fix`
    - portolan <command> in code blocks
    """
    result = ValidationResult(validator="CLI Commands")

    # Extract actual commands from source
    actual_commands = extract_cli_commands_from_source(root)

    # Also add known subcommands
    actual_commands.update({"init", "check", "scan", "add", "rm", "push", "pull", "sync", "clone"})
    actual_commands.update({"config", "config-set", "config-get", "config-list", "config-unset"})
    actual_commands.update({"dataset", "dataset-list", "dataset-info"})

    # Extract command references from CLAUDE.md
    # Match: portolan <command> (in backticks or code blocks)
    pattern = r"`?portolan\s+([a-z][a-z0-9\-]*)`?"

    referenced_commands: set[str] = set()
    for match in re.finditer(pattern, claude_md):
        cmd = match.group(1)
        # Skip flags
        if not cmd.startswith("-"):
            referenced_commands.add(cmd)

    # Check for missing commands
    missing = referenced_commands - actual_commands
    if missing:
        result.warnings.append(
            "CLI commands referenced but may not exist:\n"
            + "\n".join(f"  - portolan {cmd}" for cmd in sorted(missing))
        )

    return result


# =============================================================================
# TestMarkerValidator - Check pytest markers match pyproject.toml
# =============================================================================


def extract_pytest_markers_from_pyproject(root: Path) -> dict[str, str]:
    """Extract pytest markers defined in pyproject.toml."""
    pyproject_path = root / "pyproject.toml"
    if not pyproject_path.exists():
        return {}

    markers: dict[str, str] = {}

    try:
        content = pyproject_path.read_text()
        # Parse markers = [...] section
        # Format: "unit: Fast, isolated, no I/O (< 100ms each)"
        marker_pattern = r"markers\s*=\s*\[(.*?)\]"
        match = re.search(marker_pattern, content, re.DOTALL)

        if match:
            markers_section = match.group(1)
            # Parse individual markers
            for line in markers_section.split("\n"):
                line = line.strip().strip(",").strip('"').strip("'")
                if ":" in line:
                    name, description = line.split(":", 1)
                    markers[name.strip()] = description.strip()

    except (FileNotFoundError, ValueError):
        pass

    return markers


def validate_test_markers(claude_md: str, root: Path) -> ValidationResult:
    """Check that test markers in CLAUDE.md match pyproject.toml definitions."""
    result = ValidationResult(validator="Test Markers")

    # Extract markers from pyproject.toml
    defined_markers = extract_pytest_markers_from_pyproject(root)

    # Extract markers referenced in CLAUDE.md
    # Match: @pytest.mark.X or pytest.mark.X
    pattern = r"@?pytest\.mark\.(\w+)"
    referenced_markers = set(re.findall(pattern, claude_md))

    # Check for undefined markers
    undefined = referenced_markers - set(defined_markers.keys())
    if undefined:
        result.warnings.append(
            "Test markers referenced but not defined in pyproject.toml:\n"
            + "\n".join(f"  - @pytest.mark.{m}" for m in sorted(undefined))
        )

    # Check for defined but undocumented markers
    documented = referenced_markers
    undocumented = set(defined_markers.keys()) - documented
    if undocumented:
        result.warnings.append(
            "Test markers defined in pyproject.toml but not documented:\n"
            + "\n".join(f"  - @pytest.mark.{m}: {defined_markers[m]}" for m in sorted(undocumented))
        )

    return result


# =============================================================================
# CodeExampleValidator - Check import statements are valid
# =============================================================================


def validate_code_examples(claude_md: str, root: Path) -> ValidationResult:
    """Check that import examples in CLAUDE.md are valid.

    Matches patterns like:
    - from portolan_cli.output import success
    - from portolan_cli import cli
    """
    result = ValidationResult(validator="Code Examples")

    # Extract import statements from code blocks
    # Match: from portolan_cli.X import Y
    pattern = (
        r"from\s+(portolan_cli(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s+import\s+([a-zA-Z_][a-zA-Z0-9_, ]*)"
    )

    invalid_imports: list[str] = []

    for match in re.finditer(pattern, claude_md):
        module_path = match.group(1)
        imports = [i.strip() for i in match.group(2).split(",")]

        # Convert module path to file path
        file_path = root / module_path.replace(".", "/")

        # Check if module exists (as .py file or __init__.py in directory)
        py_file = file_path.with_suffix(".py")
        init_file = file_path / "__init__.py"

        if not py_file.exists() and not init_file.exists():
            invalid_imports.append(f"Module not found: {module_path}")
            continue

        # Try to verify imports exist in the module
        source_file = py_file if py_file.exists() else init_file
        try:
            source = source_file.read_text()
            tree = ast.parse(source)

            # Extract defined names (functions, classes, variables)
            defined_names: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    defined_names.add(node.name)
                elif isinstance(node, ast.ClassDef):
                    defined_names.add(node.name)
                elif isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            defined_names.add(target.id)

            # Check if imported names exist
            for imp in imports:
                if imp not in defined_names and not imp.startswith("_"):
                    # Could be re-exported from __init__.py or a type alias
                    # Only warn if it's clearly missing
                    if module_path.count(".") > 0:  # Not the top-level package
                        invalid_imports.append(f"Import may not exist: {imp} from {module_path}")

        except (SyntaxError, FileNotFoundError):
            pass

    if invalid_imports:
        result.warnings.append(
            "Code examples with potentially invalid imports:\n"
            + "\n".join(f"  - {i}" for i in sorted(set(invalid_imports)))
        )

    return result


# =============================================================================
# FreshnessValidator - Check freshness markers aren't stale
# =============================================================================


@dataclass
class FreshnessMarker:
    """A freshness marker found in CLAUDE.md."""

    section: str
    last_verified: datetime
    line_number: int


def extract_freshness_markers(claude_md: str) -> list[FreshnessMarker]:
    """Extract freshness markers from CLAUDE.md.

    Format:
    <!-- freshness: last-verified: 2026-02-27 -->
    ## Section Name
    ...
    <!-- /freshness -->
    """
    markers: list[FreshnessMarker] = []

    # Match freshness markers
    pattern = r"<!--\s*freshness:\s*last-verified:\s*(\d{4}-\d{2}-\d{2})\s*-->\s*\n##\s*([^\n]+)"

    for match in re.finditer(pattern, claude_md):
        date_str = match.group(1)
        section = match.group(2).strip()
        line_number = claude_md[: match.start()].count("\n") + 1

        try:
            last_verified = datetime.strptime(date_str, "%Y-%m-%d")
            markers.append(
                FreshnessMarker(
                    section=section, last_verified=last_verified, line_number=line_number
                )
            )
        except ValueError:
            pass

    return markers


def validate_freshness(claude_md: str, root: Path, stale_days: int = 30) -> ValidationResult:
    """Check that freshness markers aren't too old (warning only)."""
    result = ValidationResult(validator="Freshness")

    markers = extract_freshness_markers(claude_md)
    now = datetime.now()

    stale_sections: list[str] = []
    for marker in markers:
        age = (now - marker.last_verified).days
        if age > stale_days:
            stale_sections.append(
                f"  - '{marker.section}' (line {marker.line_number}): "
                f"verified {age} days ago ({marker.last_verified.date()})"
            )

    if stale_sections:
        result.warnings.append(
            f"Sections with stale freshness markers (>{stale_days} days):\n"
            + "\n".join(stale_sections)
        )

    return result


# =============================================================================
# Main Validation Runner
# =============================================================================


def main() -> int:
    """Run all validations and report results."""
    root = get_project_root()
    claude_md_path = root / "CLAUDE.md"

    if not claude_md_path.exists():
        print("ERROR: CLAUDE.md not found")
        return 1

    claude_md = claude_md_path.read_text()

    # Run all validators
    validators = [
        validate_adrs,
        validate_known_issues,
        validate_file_paths,
        validate_cli_commands,
        validate_test_markers,
        validate_code_examples,
        validate_freshness,
    ]

    results: list[ValidationResult] = []
    for validator in validators:
        results.append(validator(claude_md, root))

    # Collect errors and warnings
    all_errors: list[str] = []
    all_warnings: list[str] = []

    for result in results:
        if result.errors:
            all_errors.extend([f"[{result.validator}] {e}" for e in result.errors])
        if result.warnings:
            all_warnings.extend([f"[{result.validator}] {w}" for w in result.warnings])

    # Report results
    if all_warnings:
        print("WARNINGS:\n")
        for warning in all_warnings:
            print(warning)
            print()

    if all_errors:
        print("ERRORS:\n")
        for error in all_errors:
            print(error)
            print()
        print("Fix: Update CLAUDE.md to fix references, or update source files.")
        return 1

    # Count successes
    adr_count = len(extract_adr_links(claude_md))
    issue_count = len(extract_known_issue_links(claude_md))

    print(
        f"CLAUDE.md validation passed: {adr_count} ADRs, {issue_count} known issues, "
        f"{len(all_warnings)} warnings"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
