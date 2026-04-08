"""Tests for the skills module."""

from __future__ import annotations

import pytest

from portolan_cli.skills import get_skill, iter_skills, list_skills


class TestSkillsModule:
    """Tests for skills module functions."""

    @pytest.mark.unit
    def test_list_skills_returns_sourcecoop(self) -> None:
        """list_skills should return at least the sourcecoop skill."""
        skills = list_skills()
        assert "sourcecoop" in skills

    @pytest.mark.unit
    def test_list_skills_returns_sorted(self) -> None:
        """list_skills should return skills in sorted order."""
        skills = list_skills()
        assert skills == sorted(skills)

    @pytest.mark.unit
    def test_get_skill_returns_content(self) -> None:
        """get_skill should return the skill content."""
        content = get_skill("sourcecoop")
        assert content is not None
        assert "Source Cooperative" in content
        assert "portolan" in content.lower()

    @pytest.mark.unit
    def test_get_skill_with_md_extension(self) -> None:
        """get_skill should work with .md extension."""
        content = get_skill("sourcecoop.md")
        assert content is not None
        assert "Source Cooperative" in content

    @pytest.mark.unit
    def test_get_skill_not_found(self) -> None:
        """get_skill should return None for non-existent skills."""
        content = get_skill("nonexistent-skill")
        assert content is None

    @pytest.mark.unit
    def test_iter_skills_yields_tuples(self) -> None:
        """iter_skills should yield (name, content) tuples."""
        skills_list = list(iter_skills())
        assert len(skills_list) > 0

        for name, content in skills_list:
            assert isinstance(name, str)
            assert isinstance(content, str)
            assert len(content) > 0

    @pytest.mark.unit
    def test_sourcecoop_skill_has_required_sections(self) -> None:
        """The sourcecoop skill should have all required workflow sections."""
        content = get_skill("sourcecoop")
        assert content is not None

        # Check for key sections
        assert "Prerequisites" in content
        assert "Workflow Overview" in content
        assert "Credential Setup" in content
        assert "Initialize" in content
        assert "Configure" in content
        assert "Add Files" in content
        assert "Metadata" in content
        assert "README" in content
        assert "Push" in content
        assert "Troubleshooting" in content

    @pytest.mark.unit
    def test_sourcecoop_skill_has_collection_subdirectory_note(self) -> None:
        """The sourcecoop skill should mention collection subdirectories."""
        content = get_skill("sourcecoop")
        assert content is not None
        assert "collection subdirector" in content.lower()
