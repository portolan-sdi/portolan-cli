"""Convert machine slugs into human-readable titles."""

from __future__ import annotations

import re

# Separators that join words inside a slug. Forward slashes delimit nested
# collection paths and are handled separately (leaf only).
_SLUG_SEPARATORS: re.Pattern[str] = re.compile(r"[_\-]+")


def humanize_slug(slug: str) -> str:
    """Turn a machine slug into a human-readable, title-cased string."""
    if not slug:
        return ""

    # Use the leaf segment of a nested path.
    leaf = slug.rsplit("/", maxsplit=1)[-1]

    # Underscores/hyphens become word boundaries.
    spaced = _SLUG_SEPARATORS.sub(" ", leaf)

    tokens = spaced.split()
    humanized = [_capitalize_token(token) for token in tokens]
    return " ".join(humanized)


def _capitalize_token(token: str) -> str:
    """Capitalize a single token, preserving acronyms/CamelCase."""
    if any(char.isupper() for char in token):
        return token
    return token[:1].upper() + token[1:]


def derive_title(existing: str | None, fallback_id: str) -> str:
    """Return a human-readable title, preferring an existing human title."""
    # Lazy import: stac imports this module, so a top-level import would cycle.
    from portolan_cli.stac import is_technical_name

    if existing and not is_technical_name(existing):
        return existing
    return humanize_slug(fallback_id)
