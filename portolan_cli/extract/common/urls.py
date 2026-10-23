"""Query merging for request URLs, and the sanitizer for published strings.

One helper merges query parameters into every URL that extract builds:
the WFS GetFeature link and the WMS GetStyles and GetLegendGraphic URLs
(#981). Parameter names match without case, so ``SERVICE`` and ``service``
are one parameter. A request keeps the parameters it needs, such as an API
key.

``sanitize_published_string`` runs before a value goes into a published file:
``collection.json``, ``metadata.yaml``, or the extraction report. A catalog is
static files on public storage. A credential in a query string, or a local
path of the machine that ran the command, must not reach it (#981, #939).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

#: Query parameter names (lower case) that carry a credential.
CREDENTIAL_QUERY_PARAMS = frozenset(
    {
        "access_token",
        "api-key",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "client_secret",
        "key",
        "password",
        "secret",
        "sig",
        "signature",
        "token",
        "x-amz-credential",
        "x-amz-security-token",
        "x-amz-signature",
    }
)

#: Query parameters (lower case) that only a WFS request uses. A WMS URL
#: built from a WFS URL drops them (#981).
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
    """Merge ``overrides`` into the query of ``url``.

    Args:
        url: The URL, with or without a query.
        overrides: Parameters to set. Each one replaces an existing
            parameter of the same name, in any letter case.
        drop: Lower-case parameter names to remove.

    Returns:
        The URL with the merged, encoded query.
    """
    parsed = urlparse(url)
    removed = {name.lower() for name in overrides} | {name.lower() for name in drop}
    kept = [(name, value) for name, value in parse_qsl(parsed.query) if name.lower() not in removed]
    kept.extend(overrides.items())
    return urlunparse(parsed._replace(query=urlencode(kept)))


def sanitize_published_string(value: str) -> str | None:
    """Return ``value`` safe to publish, or None when no safe form exists.

    A URL loses its userinfo and every credential query parameter. A local
    location (``file:``, ``sqlite:``, or an absolute path) has no safe form,
    because it names the layout of the machine that ran the command. Other
    text is returned unchanged.

    Args:
        value: A string that goes into a published file.

    Returns:
        The safe string, or None. The caller omits the field for None.
    """
    if value.lower().startswith(_LOCAL_SCHEMES) or _LOCAL_PATH.match(value):
        return None
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return value
    if parsed.username is not None or parsed.password is not None:
        parsed = parsed._replace(netloc=parsed.netloc.rsplit("@", 1)[1])
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    kept = [(name, item) for name, item in pairs if name.lower() not in CREDENTIAL_QUERY_PARAMS]
    if len(kept) != len(pairs):
        parsed = parsed._replace(query=urlencode(kept))
    return urlunparse(parsed)
