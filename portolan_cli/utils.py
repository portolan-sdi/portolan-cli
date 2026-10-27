"""Shared utility functions.

Small helpers used across multiple modules.
"""

from __future__ import annotations

import posixpath
import re
from pathlib import Path, PurePath
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def format_size(size_bytes: int) -> str:
    """Format a file size in human-readable format.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Human-readable size string (e.g., "4.2MB", "100B").
    """
    if size_bytes < 1024:
        return f"{size_bytes}B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f}MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f}GB"


def href_root(path: Path) -> str:
    """The root string to hand pystac's ``normalize_hrefs``.

    ``normalize_hrefs`` tells a file from a directory by looking for a dot in
    the final path component, so a directory named ``tmp.XXXXXX`` reads as a
    file and the catalog lands in its parent (issue #401). A trailing slash is
    what settles it.

    The slash alone is not enough. pystac absolutizes a relative root against
    the working directory and drops the trailing slash on the way, which put the
    heuristic back in play for ``init``'s ``"."`` default (issue #731).
    Resolving first keeps the slash meaningful.

    This is the single writer for that string. Every ``normalize_hrefs`` call
    takes its root from here, and ``tests/unit/test_href_root.py`` fails if one
    builds it by hand instead.

    Args:
        path: Directory the catalog or collection is written to.

    Returns:
        The absolute directory path with exactly one trailing slash.
    """
    return f"{path.resolve()}/"


def relative_href(from_dir: PurePath, to_file: PurePath) -> str:
    r"""The POSIX href from a directory to a STAC file.

    A STAC href is a relative URL reference, so its separator is ``/`` on every
    platform. ``os.path.relpath`` returns the *native* one, and on Windows that
    shipped ``..\\catalog.json`` — not a Windows spelling of a parent link but a
    filename containing backslashes, which resolves nowhere (rashid
    ``PTL-LNK-006``). Normalizing both sides to POSIX first keeps the
    computation itself platform-independent.

    Both link ends flow through here so that ``root`` and ``parent`` are derived
    separately rather than sharing one href. Reusing the root href for ``parent``
    is what made a nested collection point past its own intermediate catalog
    (issue #711).

    Args:
        from_dir: Directory the link will live in.
        to_file: File the link points at.

    Returns:
        A relative POSIX href, e.g. ``../catalog.json``.
    """
    return posixpath.relpath(
        PurePath(to_file).as_posix(),
        PurePath(from_dir).as_posix(),
    )


def get_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Safely get a dict value, returning empty dict if not a dict.

    Args:
        data: Dictionary to read from.
        key: Key to look up.

    Returns:
        The value if it's a dict, otherwise an empty dict.
    """
    value = data.get(key, {})
    return value if isinstance(value, dict) else {}


def get_list(data: dict[str, Any], key: str) -> list[Any]:
    """Safely get a list value, returning empty list if not a list.

    Args:
        data: Dictionary to read from.
        key: Key to look up.

    Returns:
        The value if it's a list, otherwise an empty list.
    """
    value = data.get(key, [])
    return value if isinstance(value, list) else []


# =============================================================================
# URL credentials (#981). Extract and the log handlers share one sanitizer.
# =============================================================================

#: Query parameter names (lower case) that carry a credential.
CREDENTIAL_QUERY_PARAMS = frozenset(
    {
        "access_token",
        "access-token",
        "api-key",
        "api_key",
        "apikey",
        "auth",
        "authkey",
        "authorization",
        "client_secret",
        "key",
        "password",
        "rapidapi-key",
        "secret",
        "sig",
        "signature",
        "subscription-key",
        "token",
        "x-amz-credential",
        "x-amz-security-token",
        "x-amz-signature",
        "x-goog-signature",
    }
)

# A credential parameter anywhere in a text, also in a URL with no scheme,
# such as "phl.carto.com/api/v2/sql?api_key=K". Its value goes.
_CREDENTIAL_VALUE = re.compile(
    r"([?&](?:"
    + "|".join(re.escape(name) for name in sorted(CREDENTIAL_QUERY_PARAMS, key=len, reverse=True))
    + r")=)[^&\s<>\"'`]+",
    re.IGNORECASE,
)
# A URL inside a text, such as an error message that quotes a request. The
# second form is a path with a query and no host. A requests error quotes the
# request in that form: "Max retries exceeded with url: /wfs?api_key=...".
_URL_IN_TEXT = re.compile(
    r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"'`]+|(?<![^\s(:=])/[^\s<>\"'`?]*\?[^\s<>\"'`]+"
)


def redact_credentials(text: str) -> str:
    """Return ``text`` with no credential in any URL that it quotes.

    Each URL loses its userinfo and every credential query parameter. The
    host and the path stay, so the message still names the service. A local
    path stays too, because a message is not a published value. A credential
    parameter that no URL holds, as in a URL with no scheme, keeps its name
    and loses its value.

    Args:
        text: A message, a log line, or a URL.

    Returns:
        The text with each URL sanitized.
    """
    text = _URL_IN_TEXT.sub(lambda match: _sanitize_url(match.group()), text)
    return _CREDENTIAL_VALUE.sub(r"\1***", text)


def _sanitize_url(text: str) -> str:
    """Return the URL in ``text`` with no userinfo and no credential parameter.

    Punctuation at the end of ``text`` ends the sentence, not the URL.
    """
    url = text.rstrip(".,:;!?)")
    parsed = urlparse(url)
    changed = parsed.username is not None or parsed.password is not None
    if changed:
        parsed = parsed._replace(netloc=parsed.netloc.rsplit("@", 1)[1])
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    kept = [(name, item) for name, item in pairs if name.lower() not in CREDENTIAL_QUERY_PARAMS]
    if len(kept) != len(pairs):
        parsed = parsed._replace(query=urlencode(kept))
        changed = True
    return urlunparse(parsed) + text[len(url) :] if changed else text
