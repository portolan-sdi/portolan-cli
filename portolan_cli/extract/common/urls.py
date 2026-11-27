"""Query merging for request URLs, and the sanitizer for published strings."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from portolan_cli.utils import redact_credentials as redact_credentials

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

#: Query parameters (lower case) that only a WFS request uses. A WMS URL
#: built from a WFS URL drops them.
WFS_REQUEST_PARAMS = frozenset(
    {
        "bbox",
        "count",
        "cql_filter",
        "maxfeatures",
        "outputformat",
        "propertyname",
        "srsname",
        "startindex",
        "typename",
        "typenames",
    }
)

# A local path: POSIX absolute, Windows drive, or UNC share.
_LOCAL_PATH = re.compile(r"^(/|[A-Za-z]:[\\/]|\\\\)")
_LOCAL_SCHEMES = ("file:", "sqlite:")


def merge_query(
    url: str, overrides: Mapping[str, str], *, drop: Iterable[str] = frozenset()
) -> str:
    """Merge ``overrides`` into the query of ``url``."""
    parsed = urlparse(url)
    removed = {name.lower() for name in overrides} | {name.lower() for name in drop}
    kept = [(name, value) for name, value in parse_qsl(parsed.query) if name.lower() not in removed]
    kept.extend(overrides.items())
    return urlunparse(parsed._replace(query=urlencode(kept)))


def sanitize_published_string(value: str) -> str | None:
    """Return ``value`` safe to publish, or None when no safe form exists.

    Args:
        value: A string that goes into a published file.

    Returns:
        The safe string, or None. The caller omits the field for None.
    """
    if value.lower().startswith(_LOCAL_SCHEMES) or _LOCAL_PATH.match(value):
        return None
    return redact_credentials(value)
