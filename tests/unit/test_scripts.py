"""Tests for documentation maintenance scripts.

These tests verify that the scripts in scripts/ work correctly:
- generate_agents_md_sections.py: Generates the test-marker and command-surface sections.
- generate_skill_md.py: Generates CLI commands, Python API sections
- validate_agents_md.py: Validates AGENTS.md references
- validate_skill_md.py: Validates SKILL.md structure
"""

from __future__ import annotations

import pytest


class TestValidateClaudeMd:
    """Tests for validate_agents_md.py."""

    @pytest.mark.unit
    def test_validation_result_aggregates_errors(self) -> None:
        """ValidationResult should aggregate errors and warnings."""
        from scripts.validate_agents_md import ValidationResult

        result = ValidationResult(validator="test")
        result.errors.append("Error 1")
        result.errors.append("Error 2")
        result.warnings.append("Warning 1")

        assert len(result.errors) == 2
        assert len(result.warnings) == 1
