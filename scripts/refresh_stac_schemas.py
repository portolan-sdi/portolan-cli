#!/usr/bin/env python3
"""Refresh the vendored upstream STAC JSON Schemas.

Portolan's shipped ``spec/schema/catalog.schema.json`` and
``collection.schema.json`` ``allOf``-reference the upstream STAC schemas by
absolute URL. To validate CLI output hermetically (no network in tests), the
transitive closure of those upstream schemas is vendored into the package at
``portolan_cli/validation/_vendored/stac/<version>/`` and served from a
``referencing.Registry`` (see ``portolan_cli.validation.schema_registry``).

This script re-fetches that closure. Run it only when bumping the pinned STAC
version (``STAC_VERSION`` below and the ``$ref``s in ``spec/schema/*.json``).
STAC v1.1.0 is an immutable published version, so the vendored copy does not
drift on its own; there is deliberately no automated network diff test.

Usage::

    uv run python scripts/refresh_stac_schemas.py
    uv run python scripts/refresh_stac_schemas.py --check   # CI: fail if stale

The closure is discovered by crawling ``$ref``s starting from the catalog and
collection roots. References are resolved relative to each document's retrieval
URL. Files are written under a path mirroring their URL so relative ``$ref``s
line up; the registry keys each resource by its retrieval URL (NOT the embedded
``$id``, which is malformed for some STAC v1.1.0 schemas, e.g. ``common.json``
declares ``$id`` ``.../commonjson``).
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urljoin, urlsplit

STAC_VERSION = "1.1.0"
_STAC_BASE = f"https://schemas.stacspec.org/v{STAC_VERSION}/"
_ROOTS = (
    f"{_STAC_BASE}catalog-spec/json-schema/catalog.json",
    f"{_STAC_BASE}collection-spec/json-schema/collection.json",
)

# portolan_cli/validation/_vendored/stac/<version>/
_VENDOR_DIR = (
    Path(__file__).resolve().parent.parent
    / "portolan_cli"
    / "validation"
    / "_vendored"
    / "stac"
    / STAC_VERSION
)


def _refs(node: object) -> list[str]:
    out: list[str] = []

    def walk(o: object) -> None:
        if isinstance(o, dict):
            for key, value in o.items():
                if key == "$ref" and isinstance(value, str):
                    out.append(value)
                else:
                    walk(value)
        elif isinstance(o, list):
            for item in o:
                walk(item)

    walk(node)
    return out


def crawl() -> dict[str, dict[str, object]]:
    """Fetch the transitive $ref closure of the STAC roots (STAC host only)."""
    fetched: dict[str, dict[str, object]] = {}
    queue: collections.deque[str] = collections.deque(_ROOTS)
    while queue:
        url = queue.popleft().split("#", 1)[0]
        if url in fetched:
            continue
        with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 (trusted host)
            doc = json.loads(resp.read().decode())
        fetched[url] = doc
        for ref in set(_refs(doc)):
            if ref.startswith("#") or ref.startswith("http://json-schema.org"):
                continue
            target = urljoin(url, ref).split("#", 1)[0]
            if target.startswith(_STAC_BASE) and target not in fetched:
                queue.append(target)
    return fetched


def _relpath(url: str) -> Path:
    """Map a schemas.stacspec.org URL to a vendored path under _VENDOR_DIR."""
    return Path(urlsplit(url).path.split(f"/v{STAC_VERSION}/", 1)[1])


def write(fetched: dict[str, dict[str, object]]) -> None:
    for url, doc in fetched.items():
        dest = _VENDOR_DIR / _relpath(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(doc, indent=2) + "\n")


def check(fetched: dict[str, dict[str, object]]) -> int:
    stale: list[str] = []
    for url, doc in fetched.items():
        dest = _VENDOR_DIR / _relpath(url)
        expected = json.dumps(doc, indent=2) + "\n"
        if not dest.exists() or dest.read_text() != expected:
            stale.append(str(_relpath(url)))
    if stale:
        print("Vendored STAC schemas are stale:", file=sys.stderr)
        for name in sorted(stale):
            print(f"  {name}", file=sys.stderr)
        print("Run: uv run python scripts/refresh_stac_schemas.py", file=sys.stderr)
        return 1
    print(f"Vendored STAC v{STAC_VERSION} closure is current ({len(fetched)} files).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail (exit 1) if the vendored copy differs from upstream; write nothing.",
    )
    args = parser.parse_args()

    fetched = crawl()
    if args.check:
        return check(fetched)
    write(fetched)
    print(f"Vendored STAC v{STAC_VERSION} closure: {len(fetched)} files -> {_VENDOR_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
