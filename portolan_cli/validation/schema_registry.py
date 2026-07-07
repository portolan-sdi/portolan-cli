"""Hermetic JSON Schema validation against the shipped Portolan spec schemas.

The shipped ``spec/schema/catalog.schema.json`` and ``collection.schema.json``
extend the upstream STAC v1.1.0 schemas via ``allOf`` + absolute-URL ``$ref``.
To validate documents without network access, the transitive closure of those
upstream schemas is vendored under ``_vendored/stac/<version>/`` (refresh with
``scripts/refresh_stac_schemas.py``) and served from a ``referencing.Registry``.

Two subtleties this module handles:

* **Mixed dialects.** The Portolan wrapper schemas are JSON Schema 2020-12; the
  vendored STAC schemas are draft-07. ``jsonschema`` resolves each referenced
  resource under its own declared ``$schema``, so a 2020-12 validator can follow
  a ``$ref`` into a draft-07 resource correctly.
* **Malformed upstream ``$id``.** Some STAC v1.1.0 schemas declare a broken
  ``$id`` (e.g. ``common.json`` → ``.../commonjson``). Relative ``$ref``s resolve
  against a resource's base URI, so each resource's ``$id`` is normalized to its
  canonical retrieval URL at load time. The vendored files on disk stay
  byte-identical to upstream so the refresh script's ``--check`` stays honest.

``format`` assertions are intentionally OFF. Portolan emits relative hrefs by
design and the href/IRI policy is unresolved (discussion #573), so enforcing
``format: iri`` here would reject valid Portolan output and pre-judge that
decision. Validation is structural only.

This module is part of the ``portolan_cli.validation`` extraction seam (the
future ``reis`` package, issue #563): it imports no CLI, output, or config
layer, and no application framework.
"""

from __future__ import annotations

import json
from functools import lru_cache
from importlib.resources import files
from typing import TYPE_CHECKING, Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

if TYPE_CHECKING:
    from importlib.abc import Traversable

# Pinned STAC version. Must match the ``$ref``s in spec/schema/*.json and the
# vendored directory name. Bump via scripts/refresh_stac_schemas.py.
STAC_VERSION = "1.1.0"

_STAC_BASE_URL = f"https://schemas.stacspec.org/v{STAC_VERSION}/"
_VENDOR_ANCHOR = f"_vendored/stac/{STAC_VERSION}"


def _iter_vendored(node: Traversable, prefix: str = "") -> list[tuple[str, str]]:
    """Yield (url-relative-path, text) for every vendored ``.json`` under ``node``."""
    out: list[tuple[str, str]] = []
    for child in node.iterdir():
        rel = f"{prefix}{child.name}"
        if child.is_dir():
            out.extend(_iter_vendored(child, prefix=f"{rel}/"))
        elif child.name.endswith(".json"):
            out.append((rel, child.read_text()))
    return out


@lru_cache(maxsize=1)
def build_stac_registry() -> Registry:
    """Build a ``referencing.Registry`` of the vendored STAC v1.1.0 closure.

    Each resource is keyed by its canonical retrieval URL
    (``https://schemas.stacspec.org/v{STAC_VERSION}/<path>``), which is what the
    shipped Portolan schemas' ``$ref``s point at. Cached: the vendored schemas
    never change at runtime.
    """
    root = files("portolan_cli.validation").joinpath(*_VENDOR_ANCHOR.split("/"))
    registry: Registry = Registry()
    for rel_path, text in _iter_vendored(root):
        url = f"{_STAC_BASE_URL}{rel_path}"
        contents: dict[str, Any] = json.loads(text)
        # Normalize the base URI so relative $refs resolve against the canonical
        # URL rather than a possibly-malformed upstream $id.
        contents["$id"] = url
        resource = Resource.from_contents(contents)
        registry = registry.with_resource(uri=url, resource=resource)
    return registry.crawl()


def validate_document(
    instance: dict[str, Any],
    schema: dict[str, Any],
    *,
    registry: Registry | None = None,
) -> list[str]:
    """Validate ``instance`` against ``schema``, returning error strings.

    ``schema`` is a Portolan spec schema (2020-12) that ``$ref``s the vendored
    STAC schemas, resolved from ``registry`` (defaults to the shared vendored
    registry). ``format`` assertions are off (see module docstring); validation
    is structural. Returns ``[]`` when valid, else ``["<json_path>: <message>"]``
    entries, sorted for stable output.
    """
    validator = Draft202012Validator(
        schema,
        registry=registry if registry is not None else build_stac_registry(),
    )
    errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.absolute_path))
    return [f"{e.json_path}: {e.message}" for e in errors]
