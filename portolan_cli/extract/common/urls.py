"""Query merging for request URLs, and the sanitizer for published strings.

One helper merges query parameters into every URL that extract builds:
the WFS GetFeature link and the WMS GetStyles and GetLegendGraphic URLs
(#981). Parameter names match without case, so ``SERVICE`` and ``service``
are one parameter. A request keeps the parameters it needs, such as an API
key.

``redact_credentials`` is the one sanitizer. It removes the credential of
each URL in a text. Every message that extract raises, logs, or prints with a
URL in it goes through it, because a terminal log and a CI log keep the
message (#981).

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
        "authkey",
        "authorization",
        "client_secret",
        "key",
        "password",
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

# A credential parameter anywhere in a text, also in a URL with no scheme,
# such as "phl.carto.com/api/v2/sql?api_key=K". Its value goes.
_CREDENTIAL_VALUE = re.compile(
    r"([?&](?:"
    + "|".join(re.escape(name) for name in sorted(CREDENTIAL_QUERY_PARAMS, key=len, reverse=True))
    + r")=)[^&\s<>\"'`]+",
    re.IGNORECASE,
)
# A local path: POSIX absolute, Windows drive, or UNC share.
_LOCAL_PATH = re.compile(r"^(/|[A-Za-z]:[\\/]|\\\\)")
_LOCAL_SCHEMES = ("file:", "sqlite:")
# A URL inside a text, such as an error message that quotes a request. The
# second form is a path with a query and no host. A requests error quotes the
# request in that form: "Max retries exceeded with url: /wfs?api_key=...".
_URL_IN_TEXT = re.compile(
    r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"'`]+|(?<![^\s(:=])/[^\s<>\"'`?]*\?[^\s<>\"'`]+"
)


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

    Each URL in ``value`` loses its userinfo and every credential query
    parameter. This covers a URL that an error message quotes. A local
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
    return redact_credentials(value)


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
