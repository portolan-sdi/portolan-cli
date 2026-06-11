"""Convert machine slugs into human-readable titles (Issue #502).

STAC Browser renders ``child``/``item`` link titles directly, so every
collection, item, and catalog portolan generates must carry a human-readable
``title``. When no human title is available we derive one from the slug:

    publico_areas_programaticas_desarrollo_social
    -> "Publico Areas Programaticas Desarrollo Social"

This is the inverse of the ``normalize_collection_id`` slugging in
:mod:`portolan_cli.collection_id`. The technical-name detection lives in
:func:`portolan_cli.stac.is_technical_name`; we reuse it (lazy import to avoid
a circular dependency, since ``stac`` imports this module).
"""

from __future__ import annotations

import re

# Separators that join words inside a slug. Forward slashes delimit nested
# collection paths (ADR-0032) and are handled separately (leaf only).
_SLUG_SEPARATORS: re.Pattern[str] = re.compile(r"[_\-]+")


def humanize_slug(slug: str) -> str:
    """Turn a machine slug into a human-readable, title-cased string.

    Only the leaf segment of a nested id is used (``climate/hittekaart`` ->
    ``Hittekaart``), since the title describes the leaf collection. Tokens that
    already contain an uppercase letter (acronyms like ``IGN``, CamelCase like
    ``DenHaag``) are preserved verbatim; all-lowercase tokens are capitalized.

    Args:
        slug: The slug to humanize (e.g. a collection/item id or directory name).

    Returns:
        A human-readable title, or "" for empty/None input.
    """
    if not slug:
        return ""

    # Use the leaf segment of a nested path (ADR-0032).
    leaf = slug.split("/")[-1]

    # Underscores/hyphens become word boundaries.
    spaced = _SLUG_SEPARATORS.sub(" ", leaf)

    tokens = spaced.split()
    humanized = [_capitalize_token(token) for token in tokens]
    return " ".join(humanized)


def _capitalize_token(token: str) -> str:
    """Capitalize a single token, preserving acronyms/CamelCase.

    A token that already contains an uppercase letter is assumed to be an
    acronym or intentional casing and is left untouched.
    """
    if any(char.isupper() for char in token):
        return token
    return token[:1].upper() + token[1:]


def derive_title(existing: str | None, fallback_id: str) -> str:
    """Return a human-readable title, preferring an existing human title.

    If ``existing`` is set and not a technical name, it is returned unchanged.
    Otherwise a title is derived from ``fallback_id`` via :func:`humanize_slug`.

    Args:
        existing: A title that may already be set (from source metadata, etc.).
        fallback_id: The slug/id to humanize when ``existing`` is unusable.

    Returns:
        A human-readable title string.
    """
    # Lazy import: stac imports this module, so a top-level import would cycle.
    from portolan_cli.stac import is_technical_name

    if existing and not is_technical_name(existing):
        return existing
    return humanize_slug(fallback_id)
