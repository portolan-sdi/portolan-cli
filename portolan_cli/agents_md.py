"""Framework-free helpers for the ``AGENTS.md`` AI/agent metadata file.

Portolan requires every catalog and collection to carry an ``AGENTS.md`` file
(Markdown, minimal) referenced by a ``rel="agents"`` link in its STAC JSON
(rashid PTL-FIL-001/-002). ``AGENTS.md`` is a **link**, not an asset, and its
content is human-authored — Portolan only scaffolds an empty template when the
file is absent and never overwrites an existing one.

This module is deliberately dependency-light: the stdlib, the shared atomic JSON
writer, and rashid's href helper, which is itself stdlib-only. It
is imported by both the generation paths (``catalog.py``, ``add.py``,
``metadata/fix.py``) and the ``check --fix`` adapter
(``validation/fixers.py``). Keeping it free of ``click``/``rich``/``config`` /
``output`` preserves the ``validation-is-an-adapter`` import-linter contract,
the same reason ``pmtiles_links.py`` exists as a leaf.
"""

from __future__ import annotations

import json
from pathlib import Path, PurePath
from typing import Any

from rashid.catalog import is_absolute_href

from portolan_cli.json_io import write_json_atomic

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


def visible_stac_files(catalog_root: Path) -> list[Path]:
    """Every ``catalog.json``/``collection.json`` in the *visible* catalog tree.

    Dot-directories (``.portolan/``, ``.git/``, editor scratch dirs) hold caches
    and backups, not published STAC objects; a sweep that descends into them
    rewrites files no publisher asked about. Shared by every catalog-wide sweep
    so they all walk exactly the same set.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        Sorted catalog paths first, then sorted collection paths.
    """
    found: list[Path] = []
    for pattern in ("catalog.json", "collection.json"):
        for path in sorted(catalog_root.rglob(pattern)):
            rel_parts = path.parent.relative_to(catalog_root).parts
            if any(part.startswith(".") for part in rel_parts):
                continue
            found.append(path)
    return found


def markdown_link_gap(stac_path: Path, data: dict[str, Any], *, rel: str, target: str) -> bool:
    """True when ``data`` fails rashid's sibling-markdown-link check for ``rel``.

    Replicates the four cases rashid's ``_check_markdown_link`` (PTL-FIL-002 for
    ``AGENTS.md``, PTL-FIL-003 for ``README.md``) flags: no link carrying ``rel``;
    a matching link whose ``type`` is not ``text/markdown``; an href that is
    missing, empty, or absolute; and an href that does not resolve to the sibling
    ``target`` (or resolves to a file that is absent). Like rashid, *every*
    matching link is graded, not just the first.

    Replicated rather than imported because rashid keeps ``_check_markdown_link``
    private. rashid#57 exported the COG predicate, the structural relations, and
    the multihash helpers, so those now come from ``rashid.api``; this one still
    has no public counterpart. A change to PTL-FIL-002/-003 must land here too.

    Args:
        stac_path: Path of the STAC JSON; its parent is the sibling directory.
        data: The parsed STAC object.
        rel: Link relation to grade (``"agents"`` / ``"describedby"``).
        target: Sibling filename the link must resolve to.

    Returns:
        True when the object needs repair.
    """
    links = data.get("links")
    if not isinstance(links, list):
        return True

    directory = stac_path.parent
    expected = (directory / target).resolve()
    matches = [link for link in links if isinstance(link, dict) and link.get("rel") == rel]
    if not matches:
        return True

    for link in matches:
        if link.get("type") != AGENTS_MEDIA_TYPE:
            return True
        href = link.get("href")
        if not isinstance(href, str) or not href or is_absolute_href(href):
            return True
        if (directory / href).resolve() != expected or not expected.is_file():
            return True
    return False


def agents_link_gap(stac_path: Path, data: dict[str, Any]) -> bool:
    """True when ``data``'s ``AGENTS.md`` link does not satisfy rashid PTL-FIL-002.

    The already-parsed counterpart of :func:`agents_md_gap`, for callers that
    hold the STAC dict (``check --fix`` fixers). Every case it reports is
    repaired by :func:`ensure_agents_md`.
    """
    return markdown_link_gap(stac_path, data, rel=AGENTS_LINK_REL, target=AGENTS_MD_FILENAME)


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
        write_json_atomic(stac_json, data)

    return changed


def ensure_agents_md_tree(catalog_root: Path) -> bool:
    """Scaffold ``AGENTS.md`` and its link across a whole catalog tree.

    The per-object :func:`ensure_agents_md` covers what a single write path
    touches; this covers everything ``add`` walks past, so a catalog created
    before (or by hand) gains its guide the next time it is written to
    rather than only through ``check --fix`` (issue #654).

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        True if any file was created or modified.
    """
    changed_any = False
    for stac_json in visible_stac_files(catalog_root):
        if ensure_agents_md(stac_json):
            changed_any = True
    return changed_any
