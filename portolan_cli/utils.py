"""Shared utility functions.

Small helpers used across multiple modules.
"""

from __future__ import annotations

import posixpath
import re
from pathlib import Path, PurePath
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_KIB = 1024


def format_size(size_bytes: int) -> str:
    """Format a file size in human-readable format."""
    if size_bytes < _KIB:
        return f"{size_bytes}B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f}MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f}GB"


def href_root(path: Path) -> str:
    """The root string to hand pystac's ``normalize_hrefs``."""
    return f"{path.resolve()}/"


def relative_href(from_dir: PurePath, to_file: PurePath) -> str:
    r"""The POSIX href from a directory to a STAC file."""
    return posixpath.relpath(
        PurePath(to_file).as_posix(),
        PurePath(from_dir).as_posix(),
    )


def get_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Safely get a dict value, returning empty dict if not a dict."""
    value = data.get(key, {})
    return value if isinstance(value, dict) else {}


def get_list(data: dict[str, Any], key: str) -> list[Any]:
    """Safely get a list value, returning empty list if not a list."""
    value = data.get(key, [])
    return value if isinstance(value, list) else []


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
_URL_IN_TEXT = re.compile(
    r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"'`]+|(?<![^\s(:=])/[^\s<>\"'`?]*\?[^\s<>\"'`]+"
)


# The userinfo of a URL: the text between "://" and the first "@" before the path.
_USERINFO = re.compile(r"(://)[^/@\s]*@")


def redact_credentials(text: str) -> str:
    """Return ``text`` with no credential in any URL that it quotes."""
    text = _URL_IN_TEXT.sub(lambda match: _sanitize_url(match.group()), text)
    return _CREDENTIAL_VALUE.sub(r"\1***", text)


def _sanitize_url(text: str) -> str:
    """Return the URL in ``text`` with no userinfo and no credential parameter.

    Punctuation at the end of ``text`` ends the sentence, not the URL.
    """
    url = text.rstrip(".,:;!?)")
    try:
        parsed = urlparse(url)
        changed = parsed.username is not None or parsed.password is not None
    except ValueError:
        return _USERINFO.sub(r"\1", text)
    if changed:
        parsed = parsed._replace(netloc=parsed.netloc.rsplit("@", 1)[1])
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    kept = [(name, item) for name, item in pairs if name.lower() not in CREDENTIAL_QUERY_PARAMS]
    if len(kept) != len(pairs):
        parsed = parsed._replace(query=urlencode(kept))
        changed = True
    return urlunparse(parsed) + text[len(url) :] if changed else text
