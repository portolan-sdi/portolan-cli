"""Framework-free helpers for the ``AGENTS.md`` AI/agent metadata file."""

from __future__ import annotations

import json
from pathlib import Path, PurePath, PurePosixPath
from typing import Any

from rashid_core.api import CatalogGraph, sidecar_link_gaps

from portolan_core.json_io import write_json_atomic
from portolan_core.stac_links import visible_stac_files

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


def markdown_link_gap(
    root: Path, stac_path: Path, data: dict[str, Any], *, rel: str, target: str
) -> bool:
    """True when ``data`` fails rashid's sibling-markdown-link check for ``rel``."""
    # The graph gives absolute node paths, so both sides resolve before the compare.
    root = root.resolve()
    document = (stac_path.parent.resolve() / stac_path.name).relative_to(root).as_posix()
    sibling = PurePosixPath(document).parent / target
    files = [str(sibling)] if (root / sibling).is_file() else []
    graph = CatalogGraph.from_documents(root, {document: data}, files)
    node = graph.nodes[PurePosixPath(document)]
    return bool(sidecar_link_gaps(graph, node, rel=rel, target=target))


def agents_link_gap(root: Path, stac_path: Path, data: dict[str, Any]) -> bool:
    """True when ``data``'s ``AGENTS.md`` link does not satisfy rashid PTL-FIL-002."""
    return markdown_link_gap(root, stac_path, data, rel=AGENTS_LINK_REL, target=AGENTS_MD_FILENAME)


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


def scaffold_content(title: str, *, is_catalog: bool) -> str:
    """Return minimal ``AGENTS.md`` template text for a catalog or collection."""
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
    """Ensure a STAC object has an ``AGENTS.md`` file and a well-formed link."""
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
    elif not agents_link_is_wellformed(link):
        # Normalize a hand-edited / malformed link in place.
        link.update(build_agents_link())
    else:
        # The link is right, so a new AGENTS.md file leaves the JSON as it is.
        return changed

    write_json_atomic(stac_json, data)
    return True


def ensure_agents_md_tree(catalog_root: Path) -> bool:
    """Scaffold ``AGENTS.md`` and its link across a whole catalog tree."""
    changed_any = False
    for stac_json in visible_stac_files(catalog_root):
        if ensure_agents_md(stac_json):
            changed_any = True
    return changed_any
