"""Tests for documentation maintenance scripts.

These tests verify that the scripts in scripts/ work correctly:
- update_freshness.py: Auto-updates freshness markers
- generate_agents_md_sections.py: Generates the test-marker and command-surface sections.
- generate_skill_md.py: Generates CLI commands, Python API sections
- validate_agents_md.py: Validates AGENTS.md references
- validate_skill_md.py: Validates SKILL.md structure
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
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

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        updated = update_freshness_marker(content, "Standardized Terminal Output", today)

        assert f"last-verified: {today}" in updated
        assert "last-verified: 2020-01-01" not in updated


class TestValidateClaudeMd:
    """Tests for validate_agents_md.py."""

    @pytest.mark.unit
    def test_validation_result_aggregates_errors(self) -> None:
        """ValidationResult should aggregate errors and warnings."""
        from validate_agents_md import ValidationResult

        result = ValidationResult(validator="test")
        result.errors.append("Error 1")
        result.errors.append("Error 2")
        result.warnings.append("Warning 1")

        assert len(result.errors) == 2
        assert len(result.warnings) == 1
