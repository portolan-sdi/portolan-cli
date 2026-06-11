"""Skills module for Portolan CLI.

Skills have moved to a separate repository. Installation instructions are
synced from https://github.com/portolan-sdi/portolan-skills via CI.
"""

from __future__ import annotations

from importlib import resources

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


def get_skill(name: str) -> str | None:
    """Return None - skills are in external repo."""
    return None
