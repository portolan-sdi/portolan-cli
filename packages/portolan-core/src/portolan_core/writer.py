"""Write a Portolan catalog from STAC dictionaries held in memory (#1133).

An application gives :func:`plan_catalog` its STAC documents and the text of
each ``README.md`` and ``AGENTS.md``. It gets a :class:`WritePlan`: the objects
to put and to delete, in the order to apply them. The application applies the
plan with :func:`apply_plan` and a :class:`Sink`, or it loops over
``plan.operations`` and writes through its own storage layer.

Supported contract
------------------
The names in ``__all__`` are the supported interface. ``PutOperation``,
``DeleteOperation``, and ``WritePlan`` are frozen dataclasses of plain values
(``str``, ``bytes``, and tuples), so ``dataclasses.asdict`` gives plain data.
The ``Sink`` protocol has two methods, ``put(key, content, media_type)`` and
``delete(key)``. Any object with these two methods is a sink.

What the planner does
---------------------
- It adds the structural links ``root``, ``parent``, ``collection``,
  ``child``, and ``item`` that rashid's ``expected_links`` gives. Each
  ``child`` and ``item`` link gets the title of its target. An item without
  ``properties.title`` gives its ``id``. It removes every structural link the
  input had.
- It adds an ``agents`` link to ``./AGENTS.md`` and a ``describedby`` link to
  ``./README.md`` on each catalog and collection, and removes an old link of
  the same rel that points at a different sibling Markdown file. Other links
  pass through after these.
- It adds the newest Portolan profile URI that rashid bundles to a catalog or
  collection that declares none.
- It writes no ``self`` link. STAC requires a ``self`` link to be absolute,
  and the planner does not know the published URL.

What the planner never does
---------------------------
- It never reads storage and never reads asset bytes. It writes only catalog
  JSON and the documentation files. An asset passes through unchanged, so a
  directory asset such as a Zarr store keeps the ``file:size`` the caller gave
  and gets no ``file:checksum``.
- It deletes only keys from ``prior_managed_keys``. Each prior key that is not
  a new put key becomes a delete. A prior key must name catalog JSON or a
  documentation file, so a delete never removes asset data.

Keys are root-relative POSIX paths. ``key_prefix`` goes in front of every put
key and every delete key. ``prior_managed_keys`` are root-relative and carry no
prefix, the same form as the keys the caller passes in.

Order: the puts come first. A put comes after the puts of every object it
links down to, so a reader that starts at the root never meets a link to an
object that is not written yet. The deletes come last, after the new tree no
longer links to them.

Every check on the input runs before the planner makes any operation. A
failure raises a :class:`CatalogWriteError` subclass.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Protocol

from rashid_core.api import (
    STRUCTURAL_RELS,
    CatalogGraph,
    Node,
    expected_links,
    newest_profile_version,
    profile_uri,
    profile_version,
    resolve_local_href,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from obstore.store import ObjectStore

__all__ = [
    "CatalogWriteError",
    "DeleteOperation",
    "InvalidDocumentError",
    "InvalidKeyError",
    "LocalSink",
    "MissingOwnerError",
    "ObstoreSink",
    "PutOperation",
    "Sink",
    "WritePlan",
    "apply_plan",
    "plan_catalog",
]

# core.md, "AGENTS.md" and "README.md": the file name and its link rel.
_DOCUMENTATION = {"AGENTS.md": "agents", "README.md": "describedby"}
_MARKDOWN = "text/markdown"
_KINDS = {"catalog.json": "catalog", "collection.json": "collection"}


class CatalogWriteError(ValueError):
    """The input cannot become a conforming catalog. No operation was made."""


class InvalidKeyError(CatalogWriteError):
    """A key or the key prefix is not a relative POSIX path inside the tree."""


class InvalidDocumentError(CatalogWriteError):
    """A document lacks what the spec requires, or the tree is incomplete."""


class MissingOwnerError(CatalogWriteError):
    """A documentation file has no catalog or collection in its directory."""


@dataclass(frozen=True)
class PutOperation:
    """Write ``content`` at ``key`` with the given ``media_type``."""

    key: str
    content: bytes
    media_type: str


@dataclass(frozen=True)
class DeleteOperation:
    """Delete the object at ``key``. A sink treats a missing object as deleted."""

    key: str


@dataclass(frozen=True)
class WritePlan:
    """The operations that write a catalog, in the order to apply them."""

    operations: tuple[PutOperation | DeleteOperation, ...]


class Sink(Protocol):
    """Storage that :func:`apply_plan` writes through."""

    def put(self, key: str, content: bytes, media_type: str) -> None:
        """Write ``content`` at ``key``, replacing any object there."""

    def delete(self, key: str) -> None:
        """Delete the object at ``key``. A missing object is not an error."""


def plan_catalog(
    documents: Mapping[str, object],
    documentation: Mapping[str, str] | None = None,
    *,
    prior_managed_keys: Iterable[str] = (),
    key_prefix: str = "",
) -> WritePlan:
    """Plan the writes that publish a catalog.

    Args:
        documents: STAC catalogs, collections, and items, keyed by
            root-relative path (``catalog.json``,
            ``boundaries/collection.json``, ``boundaries/item-1/item-1.json``).
            The planner copies them and does not change them.
        documentation: Markdown text keyed by the path of a ``README.md`` or
            ``AGENTS.md``. Each file belongs to the catalog or collection in
            its directory. Every catalog and collection needs both.
        prior_managed_keys: Root-relative keys this writer put before. Each one
            that the new plan does not put becomes a delete.
        key_prefix: A prefix such as ``catalogs/demo`` for every put and
            delete key.

    Raises:
        InvalidKeyError: A key or the prefix is absolute, holds ``..``, a
            hidden directory, or an empty segment, or a prior key names a
            file that is not catalog metadata.
        InvalidDocumentError: A document is not the STAC object its file name
            says, lacks a title or description, lacks its README.md or
            AGENTS.md, or is not inside the tree.
        MissingOwnerError: A documentation file has no owner.
    """
    documentation = documentation or {}
    prefix = _check_key(key_prefix.rstrip("/")) if key_prefix.strip("/") else ""
    copies = {_check_key(key): _copy(key, doc) for key, doc in documents.items()}
    for key in documentation:
        _check_documentation(_check_key(key), copies)
    prior = {_check_prior(key) for key in prior_managed_keys}
    if "catalog.json" not in copies:
        raise InvalidDocumentError("the tree has no root catalog.json")

    graph = CatalogGraph.from_documents(Path(), {k: _strip(k, d) for k, d in copies.items()})
    links = {node.path.as_posix(): _links(graph, node, documentation) for node in graph.iter()}

    content: dict[str, tuple[bytes, str]] = {}
    for key, doc in copies.items():
        doc["links"] = [*links[key], *doc["links"]]
        text = json.dumps(doc, indent=2, ensure_ascii=False) + "\n"
        content[key] = (
            text.encode(),
            "application/geo+json" if _kind(key) == "item" else "application/json",
        )
    for key, text in documentation.items():
        content[key] = (text.encode(), _MARKDOWN)

    def full(key: str) -> str:
        return f"{prefix}/{key}" if prefix else key

    puts = [PutOperation(full(k), *content[k]) for k in sorted(content, key=_order)]
    deletes = [DeleteOperation(full(k)) for k in sorted(prior - content.keys(), key=_order)]
    return WritePlan(operations=(*puts, *deletes))


def apply_plan(plan: WritePlan, sink: Sink) -> None:
    """Apply each operation of ``plan`` to ``sink`` in plan order."""
    for operation in plan.operations:
        if isinstance(operation, PutOperation):
            sink.put(operation.key, operation.content, operation.media_type)
        else:
            sink.delete(operation.key)


class LocalSink:
    """A sink that writes into a local folder."""

    def __init__(self, root: Path) -> None:
        """Write under ``root``. The folder need not exist yet."""
        self.root = root.resolve()

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError(f"key leaves the sink root: {key!r}")
        return path

    def put(self, key: str, content: bytes, media_type: str) -> None:  # noqa: ARG002 - a file has no media type
        """Write the file atomically, so a reader never sees half of it."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
            Path(tmp).replace(path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def delete(self, key: str) -> None:
        """Remove the file. A missing file is not an error."""
        self._path(key).unlink(missing_ok=True)


class ObstoreSink:
    """A sink that writes to an obstore store: S3, GCS, Azure, local, or memory."""

    def __init__(self, store: ObjectStore) -> None:
        """Write to ``store``. A store prefix applies before the plan's keys."""
        self.store = store

    def put(self, key: str, content: bytes, media_type: str) -> None:
        """Put the object with its Content-Type."""
        import obstore  # Lazy, so a caller that only plans never loads obstore.
        from obstore.store import LocalStore

        # LocalStore refuses attributes, and a file has no Content-Type.
        local = isinstance(self.store, LocalStore)
        attributes = {} if local else {"Content-Type": media_type}
        # Catalog JSON and Markdown are small, so a single-part put is enough.
        obstore.put(self.store, key, content, attributes=attributes)

    def delete(self, key: str) -> None:
        """Delete the object. A missing object is not an error."""
        import obstore

        with contextlib.suppress(FileNotFoundError):
            obstore.delete(self.store, key)


def _check_key(key: str) -> str:
    path = PurePosixPath(key)
    if (
        not key
        or "\\" in key
        or path.as_posix() != key
        or path.is_absolute()
        or any(part in ("..", ".") or part.startswith(".") for part in path.parts)
    ):
        raise InvalidKeyError(f"key must be a relative POSIX path inside the tree: {key!r}")
    return key


def _check_prior(key: str) -> str:
    name = PurePosixPath(_check_key(key)).name
    if not name.endswith(".json") and name not in _DOCUMENTATION:
        raise InvalidKeyError(f"prior key is not catalog metadata, so it is never deleted: {key!r}")
    return key


def _kind(key: str) -> str:
    return _KINDS.get(PurePosixPath(key).name, "item")


def _copy(key: str, document: object) -> dict[str, Any]:
    if not key.endswith(".json"):
        raise InvalidKeyError(f"a STAC document key must end in .json: {key!r}")
    try:
        doc: Any = json.loads(json.dumps(document, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise InvalidDocumentError(f"{key} is not valid JSON: {exc}") from exc
    kind = _kind(key)
    expected = {"catalog": "Catalog", "collection": "Collection", "item": "Feature"}[kind]
    if not isinstance(doc, dict) or doc.get("type") != expected or "stac_version" not in doc:
        raise InvalidDocumentError(f"{key} must be a STAC {kind} with type {expected!r}")
    if kind != "item":
        # core.md, "Human-Readable Titles": a non-empty title and description.
        for field in ("title", "description"):
            if not isinstance(doc.get(field), str) or not doc[field].strip():
                raise InvalidDocumentError(f"{key} needs a non-empty {field!r}")
        extensions = doc.setdefault("stac_extensions", [])
        if not isinstance(extensions, list):
            raise InvalidDocumentError(f"{key} needs a list in 'stac_extensions'")
        if not any(isinstance(uri, str) and profile_version(uri) for uri in extensions):
            extensions.append(profile_uri(newest_profile_version()))
    links = doc.get("links", [])
    doc["links"] = (
        [link for link in links if isinstance(link, dict)] if isinstance(links, list) else []
    )
    return doc


def _check_documentation(key: str, documents: Mapping[str, dict[str, Any]]) -> None:
    path = PurePosixPath(key)
    if path.name not in _DOCUMENTATION:
        raise InvalidKeyError(f"documentation must be README.md or AGENTS.md: {key!r}")
    if not any((path.parent / name).as_posix() in documents for name in _KINDS):
        raise MissingOwnerError(f"{key} has no catalog.json or collection.json beside it")


def _strip(key: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Drop the links the planner makes. The planner adds them back later."""
    directory = PurePosixPath(key).parent
    owned = {rel: directory / name for name, rel in _DOCUMENTATION.items()}

    def made_here(link: dict[str, Any]) -> bool:
        rel, href = link.get("rel"), link.get("href")
        if rel in STRUCTURAL_RELS:
            return True
        if rel not in owned or _kind(key) == "item":
            return False
        target = resolve_local_href(PurePosixPath(key), href) if isinstance(href, str) else None
        # A link to a sibling Markdown file is an old documentation link. A
        # describedby link to a data dictionary or to a URL stays.
        return target is not None and target.parent == directory and target.suffix == ".md"

    doc["links"] = [link for link in doc["links"] if not made_here(link)]
    return doc


def _links(
    graph: CatalogGraph, node: Node, documentation: Mapping[str, str]
) -> list[dict[str, Any]]:
    key = node.path.as_posix()
    links: list[dict[str, Any]] = []
    for expected in expected_links(graph, node):
        link: dict[str, Any] = {"rel": expected.rel, "href": expected.href, "type": expected.type}
        if expected.rel in ("child", "item"):
            link["title"] = _title(expected.target)
        if expected.rel == "collection":
            # STAC 1.1.0 item spec: a collection link requires the collection
            # field. No graph query reads this field.
            node.data["collection"] = expected.target.id
        links.append(link)
    if node.kind == "item":
        if "collection" not in {link["rel"] for link in links}:
            raise InvalidDocumentError(f"{key} is an item outside every collection")
        return links
    for name, rel in _DOCUMENTATION.items():
        sidecar = (node.path.parent / name).as_posix()
        if sidecar not in documentation:
            raise InvalidDocumentError(f"{sidecar} is missing for {key}")
        links.append({"rel": rel, "href": f"./{name}", "type": _MARKDOWN})
    return links


def _title(node: Node) -> str:
    source = node.data.get("properties") if node.kind == "item" else node.data
    title = source.get("title") if isinstance(source, dict) else None
    return title if isinstance(title, str) and title.strip() else str(node.id)


def _order(key: str) -> tuple[int, bool, str]:
    """Deeper directories first. In one directory, the object file comes last."""
    path = PurePosixPath(key)
    return (-len(path.parent.parts), path.name in _KINDS, key)
