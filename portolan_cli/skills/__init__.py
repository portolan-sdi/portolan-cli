"""Skills module for Portolan CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import resources
from typing import Any

SKILLS_REPO = "https://github.com/portolan-sdi/portolan-skills"


def get_install_instructions() -> str:
    """Return installation instructions (synced from upstream repo)."""
    try:
        files = resources.files("portolan_cli.skills")
        return files.joinpath("INSTALL.md").read_text(encoding="utf-8")
    except (TypeError, AttributeError, FileNotFoundError):
        return f"See: {SKILLS_REPO}"


def list_skills() -> list[str]:
    """Return empty list - skills are in external repo."""
    return []


def get_skill(_name: str) -> str | None:
    """Return None. The skills live in an external repo, so the name is ignored."""
    return None


@dataclass(frozen=True)
class SkillListing:
    """The result of ``portolan skills list``."""

    skills: list[str]
    instructions: str = field(repr=False)

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan skills list``."""
        return {"skills": self.skills, "url": SKILLS_REPO}


def list_skill_entries() -> SkillListing:
    """Return the skills and the install instructions."""
    return SkillListing(list_skills(), get_install_instructions())


@dataclass(frozen=True)
class SkillView:
    """The result of ``portolan skills show``."""

    name: str
    content: str | None
    instructions: str = field(repr=False)

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan skills show``."""
        return {"name": self.name, "content": self.content, "url": SKILLS_REPO}


def show_skill(name: str) -> SkillView:
    """Return the skill ``name`` and the install instructions."""
    return SkillView(name, get_skill(name), get_install_instructions())
