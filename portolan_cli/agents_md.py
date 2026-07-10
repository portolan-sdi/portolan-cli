"""Framework-free helpers for the ``AGENTS.md`` AI/agent metadata file.

Portolan requires every catalog and collection to carry an ``AGENTS.md`` file
(Markdown, minimal) referenced by a ``rel="agents"`` link in its STAC JSON
(ADR-0052, RULE-0080/0081). ``AGENTS.md`` is a **link**, not an asset, and its
content is human-authored — Portolan only scaffolds an empty template when the
file is absent and never overwrites an existing one.

This module is deliberately stdlib-only. It is imported by both the generation
paths (``catalog.py``, ``add.py``, ``metadata/fix.py``) and the validation layer
(``validation/rules.py``). Keeping it free of ``click``/``rich``/``config`` /
``output`` preserves the ``validation-no-framework-leakage`` import-linter
contract, the same reason ``pmtiles_links.py`` exists as a leaf.
"""

from __future__ import annotations

import json
from pathlib import Path, PurePath
from typing import Any

#: Canonical filename for the AI/agent metadata file (uppercase, matching the
#: cross-tool ``AGENTS.md`` convention — like ``README.md``).
AGENTS_MD_FILENAME = "AGENTS.md"

#: STAC link relation type that references the ``AGENTS.md`` file.
AGENTS_LINK_REL = "agents"

#: Media type the ``AGENTS.md`` link MUST declare.
AGENTS_MEDIA_TYPE = "text/markdown"

#: Default human-readable title for the ``AGENTS.md`` link.
AGENTS_LINK_TITLE = "Agent/LLM usage guide"

#: Relative href used when the ``AGENTS.md`` sits next to the STAC JSON.
AGENTS_LINK_HREF = f"./{AGENTS_MD_FILENAME}"


def find_agents_link(links: list[Any]) -> dict[str, Any] | None:
    """Return the first ``rel="agents"`` link in a STAC ``links`` array, if any."""
    for link in links:
        if isinstance(link, dict) and link.get("rel") == AGENTS_LINK_REL:
            return link
    return None


def build_agents_link(href: str = AGENTS_LINK_HREF) -> dict[str, str]:
    """Build a well-formed ``rel="agents"`` link dict pointing at ``AGENTS.md``."""
    return {
        "rel": AGENTS_LINK_REL,
        "href": href,
        "type": AGENTS_MEDIA_TYPE,
        "title": AGENTS_LINK_TITLE,
    }


def _href_targets_agents_md(href: str) -> bool:
    """True when ``href``'s final path segment is ``AGENTS.md``."""
    return PurePath(href).name == AGENTS_MD_FILENAME


def agents_link_is_wellformed(link: dict[str, Any]) -> bool:
    """True when an ``agents`` link points at ``AGENTS.md`` with the markdown type."""
    return (
        _href_targets_agents_md(str(link.get("href", ""))) and link.get("type") == AGENTS_MEDIA_TYPE
    )


def agents_md_gap(stac_json: Path) -> str | None:
    """Describe the ``AGENTS.md`` gap for one STAC object, or ``None`` when compliant.

    Compliant means: a single well-formed ``rel="agents"`` link is present
    (points at ``AGENTS.md``, ``type: text/markdown``) **and** the referenced
    file exists on disk. Every non-compliant case reported here is repairable by
    :func:`ensure_agents_md` (hence ``check --fix``).

    Returns ``None`` when the file is unreadable/malformed so that other rules
    (schema/JSON validity) own that failure instead of this one.
    """
    try:
        data = json.loads(stac_json.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None

    links = data.get("links", [])
    if not isinstance(links, list):
        return None

    link = find_agents_link(links)
    if link is None:
        return "missing rel='agents' AGENTS.md link"
    if not agents_link_is_wellformed(link):
        return "rel='agents' link must point at AGENTS.md with type 'text/markdown'"

    target = (stac_json.parent / str(link.get("href", ""))).resolve()
    if not target.exists():
        return f"rel='agents' link points at a missing file ({link.get('href')})"

    return None


def scaffold_content(title: str, *, is_catalog: bool) -> str:
    """Return minimal ``AGENTS.md`` template text for a catalog or collection.

    The template is intentionally sparse: it seeds the sections agents benefit
    from most (things not already in the README — access snippets, schema notes,
    data-quality caveats, example queries, related collections/join keys) as
    prompts to be filled in. Content is open-ended; publishers replace or delete
    prompts freely.
    """
    heading = f"# AGENTS.md — {title}\n"
    scope = "catalog" if is_catalog else "collection"
    intro = (
        f"\nGuidance for AI agents and LLMs working with this {scope}. This file "
        "supplements the README with practical, agent-oriented notes. Replace the "
        "prompts below with real content; delete anything that does not apply.\n"
    )

    if is_catalog:
        sections = [
            ("## Overview", "What this catalog publishes and how it is organized."),
            (
                "## Collections",
                "Brief description of each collection, with pointers to their AGENTS.md.",
            ),
            (
                "## Data access patterns",
                "Base URLs / object-store paths, CRS conventions, and code examples.",
            ),
            ("## License", "License information for the catalog's data."),
        ]
    else:
        sections = [
            ("## Overview", "What this collection contains and when to use it."),
            (
                "## Accessing the data",
                "Working code to load the data (e.g. DuckDB SQL, Python).",
            ),
            (
                "## Schema & field notes",
                "Field names, types, meanings, and any coded or sentinel values.",
            ),
            (
                "## Data quality & usage notes",
                "Privacy suppressions, known quirks, CRS/units, and other caveats.",
            ),
            ("## Example queries", "Practical, working analysis examples."),
            (
                "## Related collections",
                "Cross-references and join keys to complementary collections.",
            ),
        ]

    body = "".join(f"\n{header}\n\n<!-- {prompt} -->\n" for header, prompt in sections)
    return heading + intro + body


def _title_for(data: dict[str, Any]) -> str:
    """Derive a display title for the scaffold from a STAC object's own fields."""
    title = data.get("title")
    if isinstance(title, str) and title.strip():
        return title
    identifier = data.get("id")
    if isinstance(identifier, str) and identifier.strip():
        return identifier
    return "Portolan"


def ensure_agents_md(stac_json: Path) -> bool:
    """Ensure a STAC object has an ``AGENTS.md`` file and a well-formed link.

    Scaffolds ``AGENTS.md`` next to ``stac_json`` when it is absent (never
    overwriting an existing, human-authored file) and injects or normalizes the
    ``rel="agents"`` link in the STAC JSON. Idempotent — a compliant object is
    left untouched.

    Shared by the write paths (``init``/``add``) and ``check --fix`` so both
    produce identical output.

    Args:
        stac_json: Path to a ``catalog.json`` or ``collection.json``.

    Returns:
        True if the file and/or link were created or normalized; False if
        nothing changed (already compliant) or ``stac_json`` was unreadable.
    """
    try:
        data = json.loads(stac_json.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False

    changed = False

    # 1. Scaffold AGENTS.md next to the STAC object if it does not exist.
    agents_path = stac_json.parent / AGENTS_MD_FILENAME
    if not agents_path.exists():
        is_catalog = data.get("type") == "Catalog"
        agents_path.write_text(
            scaffold_content(_title_for(data), is_catalog=is_catalog),
            encoding="utf-8",
        )
        changed = True

    # 2. Ensure a well-formed rel="agents" link is present in the STAC JSON.
    links = data.setdefault("links", [])
    if not isinstance(links, list):
        return changed
    link = find_agents_link(links)
    if link is None:
        links.append(build_agents_link())
        changed = True
    elif not agents_link_is_wellformed(link):
        # Normalize a hand-edited / malformed link in place.
        link.update(build_agents_link())
        changed = True

    if changed:
        stac_json.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    return changed
