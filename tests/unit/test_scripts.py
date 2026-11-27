"""Tests for documentation maintenance scripts.

These tests verify that the scripts in scripts/ work correctly:
- generate_agents_md_sections.py: Generates the test-marker and command-surface sections.
- generate_skill_md.py: Generates CLI commands, Python API sections
- validate_agents_md.py: Validates AGENTS.md references
- validate_skill_md.py: Validates SKILL.md structure
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


# Add scripts directory to path for imports
@pytest.fixture(autouse=True)
def _add_scripts_to_path() -> None:
    """Add scripts directory to sys.path for imports."""
    scripts_dir = Path(__file__).parent.parent.parent / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))


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
