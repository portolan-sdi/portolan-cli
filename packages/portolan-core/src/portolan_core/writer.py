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
  ``properties.title`` gives its ``id``. It removes every relative structural
  link the input had. An absolute structural link, such as a ``child`` link to
  another host, passes through.
- It sets the ``collection`` field of each item, as STAC 1.1.0 requires with
  a ``collection`` link.
- It adds an ``agents`` link to ``./AGENTS.md`` and a ``describedby`` link to
  ``./README.md`` on each catalog and collection. An input link with the same
  rel to the same file gives its other fields, such as ``title`` and
  ``hreflang``. Every other link passes through after these.
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
  a new put key becomes a delete. A prior key must name a ``.json`` file, a
  ``README.md``, or an ``AGENTS.md``. It must not be a ``versions.json``, and no
  asset href of the new documents may resolve to it. The planner cannot know
  whether another ``.json`` key is an item or a data file that nothing links,
  so the caller must list only keys that a previous plan put.

Keys are root-relative POSIX paths. ``key_prefix`` goes in front of every put
key and every delete key. ``prior_managed_keys`` are root-relative and carry no
prefix, the same form as the keys the caller passes in.

Order: the puts come first. A put comes after the puts of every object it
links down to, so a reader that starts at the root never meets a link to an
object that is not written yet. The deletes come last, after the new tree no
longer links to them.

The same input gives the same operations, byte for byte. The JSON keeps the
key order of the input documents.

The planner refuses a plan in which two keys, or two directories of its keys,
differ only by case. On a case-insensitive store, such as Windows or the
default macOS file system, such keys name the same file or directory. The
planner applies this rule on every host, so a plan does not depend on where
it was made.

Every check on the input runs before the planner makes any operation. A
failure raises a :class:`CatalogWriteError` subclass.
"""

from __future__ import annotations

import contextlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Protocol

from rashid_core.api import (
    STRUCTURAL_RELS,
    CatalogGraph,
    Node,
    expected_links,
    is_absolute_href,
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

    A translated root counts as a language root only when the root catalog
    links it with a relative ``alternate`` link of type ``application/json``.
    Rashid does not follow an absolute ``alternate`` link, so such a tree gets
    the containment links of an ordinary subdirectory.

    Raises:
        InvalidKeyError: A key or the prefix is absolute, holds ``..``, a
            backslash, a hidden directory, or an empty segment. On Windows,
            a key has a drive such as ``C:``. Two keys or two directories of
            the plan differ only by case. A prior key names a file that the
            writer does not manage, a ``versions.json``, or an asset.
        InvalidDocumentError: A document is not the STAC object its file name
            says, has links or assets of the wrong shape, lacks a title or
            description, lacks its README.md or AGENTS.md, or is not inside
            the tree. A collection holds another collection: core.md, "Nested
            Catalogs, Flat Collections", says "A collection MUST NOT contain a
            child collection" (PORTO-CORE-004, PORTO-CORE-018).
        MissingOwnerError: A documentation file has no owner.
    """
    documentation = documentation or {}
    # One trailing slash is allowed. Any other empty segment is an error.
    prefix = _check_key(key_prefix.removesuffix("/")) if key_prefix else ""
    copies = {_check_key(key): _copy(key, doc) for key, doc in documents.items()}
    for key in documentation:
        _check_documentation(_check_key(key), copies)
    if "catalog.json" not in copies:
        raise InvalidDocumentError("the tree has no root catalog.json")
    assets = {path.casefold() for key, doc in copies.items() for path in _asset_paths(key, doc)}
    prior = {_check_prior(key, assets) for key in prior_managed_keys}
    deleted = prior - copies.keys() - documentation.keys()
    _check_case(copies.keys() | documentation.keys() | deleted)

    sidecars = {key: _strip(key, doc) for key, doc in copies.items()}
    try:
        graph = CatalogGraph.from_documents(Path(), copies)
    except ValueError as exc:
        # Rashid also refuses a key that the host cannot hold, such as C:/x on Windows.
        raise InvalidKeyError(str(exc)) from exc
    for node in graph.iter("collection"):
        if graph.enclosing_collection_of(node) is not None:
            raise InvalidDocumentError(
                f"{node.path} is inside another collection, and a collection MUST NOT"
                " contain a child collection (core.md, Nested Catalogs, Flat Collections)"
            )
    links = {
        node.path.as_posix(): _links(graph, node, documentation, sidecars[node.path.as_posix()])
        for node in graph.iter()
    }

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
    deletes = [DeleteOperation(full(k)) for k in sorted(deleted, key=_order)]
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
        path = self.root / key
        parent = path.parent.resolve()
        if path.name in ("", ".", "..") or not parent.is_relative_to(self.root):
            raise ValueError(f"key leaves the sink root: {key!r}")
        # Only the parent is resolved. Put and delete then act on a symlink at
        # the key itself, never on the file that the symlink points at.
        return parent / path.name

    def put(self, key: str, content: bytes, media_type: str) -> None:  # noqa: ARG002 - a file has no media type
        """Write the file atomically, so a reader never sees half of it."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        # A short fixed-length name, so a long key name still fits the limit.
        tmp = path.with_name(f".tmp-{os.urandom(6).hex()}")
        try:
            old = path.lstat()
        except FileNotFoundError:
            old = None
        keep = old is not None and stat.S_ISREG(old.st_mode)
        # The temporary file opens with the mode of the file it replaces, so
        # its content is never readable by more accounts than the final file.
        # A new file gets 0o666 minus the umask, as open() gives.
        mode = stat.S_IMODE(old.st_mode) if old is not None and keep else 0o666
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        fd = os.open(tmp, flags, mode)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
            if keep:
                # The umask can clear bits of the old mode. Put them back.
                tmp.chmod(mode)
            tmp.replace(path)
        except BaseException:
            tmp.unlink(missing_ok=True)
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


def _check_case(keys: set[str]) -> None:
    """Refuse two spellings of a key or a directory that differ only by case."""
    seen: dict[str, str] = {}
    for key in sorted(keys):
        path = PurePosixPath(key)
        for spelling in (path.as_posix(), *(p.as_posix() for p in path.parents[:-1])):
            first = seen.setdefault(spelling.casefold(), spelling)
            if first != spelling:
                raise InvalidKeyError(
                    f"{first!r} and {spelling!r} differ only by case,"
                    " which is unsafe on a case-insensitive store"
                )


def _check_prior(key: str, assets: set[str]) -> str:
    """Refuse a prior key that is not writer metadata. ``assets`` holds casefolded paths.

    The checks ignore case, because a case-insensitive store reads
    ``Style.json`` and ``style.json`` as one file.
    """
    name = PurePosixPath(_check_key(key)).name
    # versions.json is the Portolan version record. The writer never manages it.
    if name.casefold() == "versions.json" or (
        not name.endswith(".json") and name not in _DOCUMENTATION
    ):
        raise InvalidKeyError(f"prior key is not catalog metadata, so it is never deleted: {key!r}")
    folded = PurePosixPath(key.casefold())
    # A directory asset, such as a Zarr store, also protects every key inside it.
    if any(path.as_posix() in assets for path in (folded, *folded.parents[:-1])):
        raise InvalidKeyError(f"prior key is an asset of the new catalog, so it stays: {key!r}")
    return key


def _asset_paths(key: str, doc: dict[str, Any]) -> list[str]:
    """The root-relative paths that the local asset hrefs of ``doc`` name."""
    assets = doc.get("assets", {}).values()
    paths = (resolve_local_href(PurePosixPath(key), asset["href"]) for asset in assets)
    return [path.as_posix() for path in paths if path is not None]


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
    links = doc.setdefault("links", [])
    if not isinstance(links, list) or not all(
        isinstance(link, dict)
        and isinstance(link.get("rel"), str)
        and isinstance(link.get("href"), str)
        for link in links
    ):
        raise InvalidDocumentError(f"{key} needs a list of links, each with a string rel and href")
    assets = doc.get("assets", {})
    if not isinstance(assets, dict) or not all(
        isinstance(asset, dict) and isinstance(asset.get("href"), str) for asset in assets.values()
    ):
        raise InvalidDocumentError(f"{key} needs a mapping of assets, each with a string href")
    return doc


def _check_documentation(key: str, documents: Mapping[str, dict[str, Any]]) -> None:
    path = PurePosixPath(key)
    if path.name not in _DOCUMENTATION:
        raise InvalidKeyError(f"documentation must be README.md or AGENTS.md: {key!r}")
    if not any((path.parent / name).as_posix() in documents for name in _KINDS):
        raise MissingOwnerError(f"{key} has no catalog.json or collection.json beside it")


def _strip(key: str, doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Drop the links the planner makes, and return the dropped sidecar links by rel.

    The planner makes the local structural links and the links to the managed
    README.md and AGENTS.md. Every other link stays, in its input order.
    """
    path = PurePosixPath(key)
    owned = (
        {}
        if _kind(key) == "item"
        else {rel: path.parent / name for name, rel in _DOCUMENTATION.items()}
    )
    sidecars: dict[str, dict[str, Any]] = {}
    kept = []
    for link in doc["links"]:
        rel, href = link["rel"], link["href"]
        if rel in owned and resolve_local_href(path, href) == owned[rel]:
            sidecars.setdefault(rel, link)
        elif rel not in STRUCTURAL_RELS or is_absolute_href(href):
            kept.append(link)
    doc["links"] = kept
    return sidecars


def _links(
    graph: CatalogGraph,
    node: Node,
    documentation: Mapping[str, str],
    sidecars: Mapping[str, dict[str, Any]],
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
        made = {"rel": rel, "href": f"./{name}", "type": _MARKDOWN}
        # An input link to the same file keeps fields such as title and hreflang.
        links.append({**sidecars.get(rel, {}), **made})
    return links


def _title(node: Node) -> str:
    source = node.data.get("properties") if node.kind == "item" else node.data
    title = source.get("title") if isinstance(source, dict) else None
    return title if isinstance(title, str) and title.strip() else str(node.id)


def _order(key: str) -> tuple[int, bool, str]:
    """Deeper directories first. In one directory, the object file comes last."""
    path = PurePosixPath(key)
    return (-len(path.parent.parts), path.name in _KINDS, key)
