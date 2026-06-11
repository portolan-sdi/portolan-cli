"""Tests for the skills module."""

from __future__ import annotations

import pytest

from portolan_cli.skills import SKILLS_REPO, get_install_instructions, get_skill, list_skills


class TestSkillsModule:
    """Tests for skills module functions."""

    @pytest.mark.unit
    def test_skills_repo_is_github_url(self) -> None:
        """SKILLS_REPO points to the external skills repository."""
        assert SKILLS_REPO == "https://github.com/portolan-sdi/portolan-skills"

    @pytest.mark.unit
    def test_list_skills_returns_empty(self) -> None:
        """list_skills returns empty list (skills are external)."""
        assert list_skills() == []

    @pytest.mark.unit
    def test_get_skill_returns_none(self) -> None:
        """get_skill returns None (skills are external)."""
        assert get_skill("sourcecoop") is None

    @pytest.mark.unit
    def test_get_install_instructions_returns_content(self) -> None:
        """get_install_instructions returns content from INSTALL.md."""
        instructions = get_install_instructions()
        assert isinstance(instructions, str)
        assert len(instructions) > 0
        # Should contain Claude Code instructions (synced from upstream)
        assert "claude plugin" in instructions.lower() or SKILLS_REPO in instructions
