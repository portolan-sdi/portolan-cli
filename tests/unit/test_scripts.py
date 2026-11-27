"""Tests for documentation maintenance scripts."""

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
