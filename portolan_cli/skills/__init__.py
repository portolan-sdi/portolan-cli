"""Skills module for Portolan CLI.

Skills are markdown files that help AI agents assist users with specific workflows.
They provide structured guidance, scripts to run, and troubleshooting tips.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

# Skills are .md files in this package
SKILLS_PACKAGE = "portolan_cli.skills"


def list_skills() -> list[str]:
    """List all available skill names.

    Returns:
        List of skill names (without .md extension).
    """
    skill_names: list[str] = []

    try:
        # Python 3.9+ API
        files = resources.files(SKILLS_PACKAGE)
        for item in files.iterdir():
            if item.name.endswith(".md"):
                skill_names.append(item.name[:-3])  # Remove .md
    except (TypeError, AttributeError):
        # Fallback for older Python or edge cases
        skills_dir = Path(__file__).parent
        for path in skills_dir.glob("*.md"):
            skill_names.append(path.stem)

    return sorted(skill_names)


def get_skill(name: str) -> str | None:
    """Get the content of a skill by name.

    Args:
        name: Skill name (with or without .md extension).

    Returns:
        Skill content as string, or None if not found.
    """
    # Normalize name
    if name.endswith(".md"):
        name = name[:-3]

    filename = f"{name}.md"

    try:
        # Python 3.9+ API
        files = resources.files(SKILLS_PACKAGE)
        skill_file = files.joinpath(filename)
        return skill_file.read_text(encoding="utf-8")
    except (TypeError, AttributeError, FileNotFoundError):
        # Fallback
        skills_dir = Path(__file__).parent
        skill_path = skills_dir / filename
        if skill_path.exists():
            return skill_path.read_text(encoding="utf-8")
        return None


def iter_skills() -> Iterator[tuple[str, str]]:
    """Iterate over all skills, yielding (name, content) tuples.

    Yields:
        Tuples of (skill_name, skill_content).
    """
    for name in list_skills():
        content = get_skill(name)
        if content:
            yield name, content
