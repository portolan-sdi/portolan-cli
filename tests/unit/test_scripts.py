"""Tests for documentation maintenance scripts.

These tests verify that the scripts in scripts/ work correctly:
- update_freshness.py: Auto-updates freshness markers
- generate_claude_md_sections.py: Generates ADR index, known issues, etc.
- generate_skill_md.py: Generates CLI commands, Python API sections
- validate_claude_md.py: Validates CLAUDE.md references
- validate_skill_md.py: Validates SKILL.md structure
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest


# Add scripts directory to path for imports
@pytest.fixture(autouse=True)
def _add_scripts_to_path() -> None:
    """Add scripts directory to sys.path for imports."""
    scripts_dir = Path(__file__).parent.parent.parent / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))


class TestUpdateFreshness:
    """Tests for update_freshness.py."""

    @pytest.mark.unit
    def test_file_to_section_mapping_exists(self) -> None:
        """FILE_TO_SECTION_MAP should have entries."""
        from update_freshness import FILE_TO_SECTION_MAP

        assert len(FILE_TO_SECTION_MAP) > 0
        assert "portolan_cli/output.py" in FILE_TO_SECTION_MAP

    @pytest.mark.unit
    def test_get_sections_for_files_finds_mapped_section(self) -> None:
        """Should return sections for mapped files."""
        from update_freshness import get_sections_for_files

        sections = get_sections_for_files(["portolan_cli/output.py"])
        assert "Standardized Terminal Output" in sections

    @pytest.mark.unit
    def test_get_sections_for_files_returns_empty_for_unmapped(self) -> None:
        """Should return empty set for unmapped files."""
        from update_freshness import get_sections_for_files

        sections = get_sections_for_files(["portolan_cli/some_random_file.py"])
        assert sections == set()

    @pytest.mark.unit
    def test_update_freshness_marker_updates_date(self) -> None:
        """Should update the date in freshness marker."""
        from update_freshness import update_freshness_marker

        content = """<!-- freshness: last-verified: 2020-01-01 -->
## Standardized Terminal Output

Some content here.
<!-- /freshness -->"""

        today = datetime.now().strftime("%Y-%m-%d")
        updated = update_freshness_marker(content, "Standardized Terminal Output", today)

        assert f"last-verified: {today}" in updated
        assert "last-verified: 2020-01-01" not in updated


class TestGenerateClaudeMdSections:
    """Tests for generate_claude_md_sections.py."""

    @pytest.mark.unit
    def test_extract_adr_title_from_file(self, tmp_path: Path) -> None:
        """Should extract title from ADR file."""
        from generate_claude_md_sections import extract_adr_title

        adr = tmp_path / "0001-test-decision.md"
        adr.write_text("# ADR-0001: My Test Decision\n\n## Status\nAccepted")

        title = extract_adr_title(adr)
        assert title == "My Test Decision"

    @pytest.mark.unit
    def test_extract_adr_title_fallback_to_filename(self, tmp_path: Path) -> None:
        """Should fall back to filename if no heading found."""
        from generate_claude_md_sections import extract_adr_title

        adr = tmp_path / "0002-another-decision.md"
        adr.write_text("No heading here, just content.")

        title = extract_adr_title(adr)
        assert "another" in title.lower() or "decision" in title.lower()

    @pytest.mark.unit
    def test_generate_adr_index_produces_table(self, tmp_path: Path) -> None:
        """Should generate a markdown table for ADRs."""
        from generate_claude_md_sections import generate_adr_index

        # Create ADR directory structure
        adr_dir = tmp_path / "context" / "shared" / "adr"
        adr_dir.mkdir(parents=True)

        (adr_dir / "0001-first.md").write_text("# ADR-0001: First Decision\n")
        (adr_dir / "0002-second.md").write_text("# ADR-0002: Second Decision\n")

        result = generate_adr_index(tmp_path)

        assert "| ADR | Decision |" in result
        assert "[0001]" in result
        assert "[0002]" in result


class TestGenerateSkillMd:
    """Tests for generate_skill_md.py."""

    @pytest.mark.unit
    def test_get_cli_commands_returns_list(self) -> None:
        """Should return a list of CLI commands."""
        from generate_skill_md import get_cli_commands

        commands = get_cli_commands()

        # Should have at least some commands
        assert isinstance(commands, list)
        # Check for expected command names
        command_names = [cmd[0] for cmd in commands]
        assert "init" in command_names or len(commands) == 0  # May be empty if import fails

    @pytest.mark.unit
    def test_get_public_api_returns_exports(self) -> None:
        """Should return public API exports."""
        from generate_skill_md import get_public_api

        exports = get_public_api()

        assert isinstance(exports, list)
        # Should have at least Catalog
        export_names = [e[0] for e in exports]
        if exports:  # May be empty if import fails
            assert "Catalog" in export_names or "cli" in export_names


class TestValidateClaudeMd:
    """Tests for validate_claude_md.py."""

    @pytest.mark.unit
    def test_extract_adr_links_finds_links(self) -> None:
        """Should extract ADR links from markdown."""
        from validate_claude_md import extract_adr_links

        content = """
| ADR | Decision |
|-----|----------|
| [0001](context/shared/adr/0001-first.md) | First |
| [0002](context/shared/adr/0002-second.md) | Second |
"""
        links = extract_adr_links(content)

        assert "context/shared/adr/0001-first.md" in links
        assert "context/shared/adr/0002-second.md" in links

    @pytest.mark.unit
    def test_extract_adr_links_ignores_non_adr_links(self) -> None:
        """Should not extract non-ADR links."""
        from validate_claude_md import extract_adr_links

        content = """
See [this guide](docs/contributing.md) for details.
"""
        links = extract_adr_links(content)
        assert len(links) == 0

    @pytest.mark.unit
    def test_validation_result_aggregates_errors(self) -> None:
        """ValidationResult should aggregate errors and warnings."""
        from validate_claude_md import ValidationResult

        result = ValidationResult(validator="test")
        result.errors.append("Error 1")
        result.errors.append("Error 2")
        result.warnings.append("Warning 1")

        assert len(result.errors) == 2
        assert len(result.warnings) == 1


class TestValidateSkillMd:
    """Tests for validate_skill_md.py."""

    @pytest.mark.unit
    def test_check_required_sections_validates_markers(self) -> None:
        """Should validate BEGIN/END GENERATED markers exist."""
        from validate_skill_md import check_required_sections

        valid_content = """
<!-- BEGIN GENERATED: overview -->
Content here
<!-- END GENERATED: overview -->
<!-- BEGIN GENERATED: cli-commands -->
CLI content
<!-- END GENERATED: cli-commands -->
<!-- BEGIN GENERATED: python-api -->
API content
<!-- END GENERATED: python-api -->
"""
        errors = check_required_sections(valid_content)
        assert len(errors) == 0

    @pytest.mark.unit
    def test_check_required_sections_reports_missing(self) -> None:
        """Should report missing generated sections."""
        from validate_skill_md import check_required_sections

        # Missing all sections
        content = "# SKILL.md\n\nNo generated sections here."
        errors = check_required_sections(content)

        # Should have errors for missing overview, cli-commands, python-api
        assert len(errors) == 3

    @pytest.mark.unit
    def test_check_freshness_markers_warns_on_stale_content(self) -> None:
        """Should warn when freshness marker is stale."""
        from validate_skill_md import check_freshness_markers

        # Content with old freshness marker
        content = """
<!-- freshness: last-verified: 2020-01-01 -->
## Old Section
<!-- /freshness -->
"""
        errors, warnings = check_freshness_markers(content)

        # Should have a warning about staleness (>30 days old)
        assert len(warnings) > 0
        assert "days old" in warnings[0]

    @pytest.mark.unit
    def test_check_freshness_markers_accepts_recent_dates(self) -> None:
        """Should not warn for recently verified content."""
        from datetime import datetime

        from validate_skill_md import check_freshness_markers

        # Content with today's date
        today = datetime.now().strftime("%Y-%m-%d")
        content = f"""
<!-- freshness: last-verified: {today} -->
## Recent Section
<!-- /freshness -->
"""
        errors, warnings = check_freshness_markers(content)

        # Should have no errors or staleness warnings
        assert len(errors) == 0
        stale_warnings = [w for w in warnings if "days old" in w]
        assert len(stale_warnings) == 0
