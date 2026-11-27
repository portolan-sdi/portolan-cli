"""Run-scoped state passed down as a parameter: one read of each shared file per run.

Memo keys are stat keys; writers call :meth:`RunState.invalidate`, and each key loads once across threads.
"""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, TypeVar

import yaml

from portolan_core import config as _config
from portolan_core.checksums import compute_checksum, compute_dir_checksum
from portolan_core.constants import MTIME_TOLERANCE_SECONDS
from portolan_core.versions import (
    SPEC_VERSION,
    Asset,
    SchemaInfo,
    VersionsFile,
    add_version,
    amend_version,
    next_version,
    parse_version,
    read_versions,
)

if TYPE_CHECKING:
    import os
    from collections.abc import Callable
    from pathlib import Path

T = TypeVar("T")

#: Tolerance for a recorded mtime with a fraction: only float rounding can move it.
FINE_MTIME_TOLERANCE_SECONDS = 1e-6

_StatKey = tuple[int, int, int, int]


def mtime_matches(current: float, recorded: float) -> bool:
    """Report whether two mtimes name the same write, within a strict tolerance.

    A fractional record gets 1 microsecond; a whole-second record (FAT, NFS, CIFS) gets ``MTIME_TOLERANCE_SECONDS``.
    """
    tolerance = (
        MTIME_TOLERANCE_SECONDS if float(recorded).is_integer() else FINE_MTIME_TOLERANCE_SECONDS
    )
    return abs(current - recorded) < tolerance


def stat_matches(st: os.stat_result, mtime: float | None, size: int | None) -> bool:
    """The one change predicate for every fast path: exact size and :func:`file_mtime_matches`.

    A record without an mtime or a size never matches.
    """
    if mtime is None or size is None or st.st_size != size:
        return False
    return file_mtime_matches(st.st_mtime, mtime)


def file_mtime_matches(current: float, recorded: float) -> bool:
    """Compare file mtimes, granting the coarse window only to two whole seconds.

    A fractional current mtime proves a fine clock, so a different timestamp is a real edit.
    """
    if float(recorded).is_integer() and not float(current).is_integer():
        return False
    return mtime_matches(current, recorded)


def _stat_key(st: os.stat_result) -> _StatKey:
    """The memo key of a file: inode, size, mtime, and status-change time.

    POSIX ctime moves on any write or ``os.utime``; Windows ctime is creation time.
    """
    return (st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def _empty_versions() -> VersionsFile:
    return VersionsFile(spec_version=SPEC_VERSION, current_version=None, versions=[])


def _next(versions_file: VersionsFile, minor: bool) -> str:
    """The next version string: a minor bump (the ``JsonFileBackend.publish`` rule) or a patch bump."""
    if minor and versions_file.current_version is not None:
        major, minor_part, _patch = parse_version(versions_file.current_version)
        return f"{major}.{minor_part + 1}.0"
    return next_version(versions_file)


def _copy(versions_file: VersionsFile) -> VersionsFile:
    """A copy whose version list a caller can edit without changing the memo."""
    return VersionsFile(
        spec_version=versions_file.spec_version,
        current_version=versions_file.current_version,
        versions=list(versions_file.versions),
    )


@dataclass
class StagedVersion:
    """The changes a run stages for one collection. Commit writes one version.

    ``amend`` holds only if every stage call asked for it; ``minor`` and ``breaking`` if any did.
    """

    assets: dict[str, Asset] = field(default_factory=dict)
    removed: set[str] = field(default_factory=set)
    messages: list[str] = field(default_factory=list)
    breaking: bool = False
    schema: SchemaInfo | None = None
    amend: bool = True
    minor: bool = False
    on_publish: Callable[[], None] | None = None

    @property
    def message(self) -> str | None:
        """The messages joined one per line, or None when no call gave one."""
        return "\n".join(self.messages) or None

    def apply(self, versions_file: VersionsFile) -> VersionsFile:
        """Return ``versions_file`` with this stage added as one new version.

        The input object returns unchanged when nothing changed, so ``result is versions_file`` skips a write.
        """
        if self.amend and versions_file.versions:
            return amend_version(versions_file, self.assets, self.removed)
        return add_version(
            versions_file,
            version=_next(versions_file, self.minor),
            assets=self.assets,
            breaking=self.breaking,
            schema=self.schema,
            message=self.message,
            removed=self.removed or None,
        )


class RunState:
    """The shared reads and the staged versions of one command run.

    Create one with :meth:`open` at the entry point of a command and pass it down.
    """

    def __init__(self, catalog_root: Path) -> None:
        """Hold the state of one run on the catalog at ``catalog_root``."""
        self.catalog_root = catalog_root
        self._resolved_root = catalog_root.resolve()
        self._lock = threading.Lock()
        self._output_claims: dict[Path, Path] = {}
        # One event per key that a worker loads now. See _once.
        self._flights: dict[tuple[str, Any, _StatKey, int], tuple[threading.Event, int]] = {}
        self._invalidations: dict[Path, int] = {}
        # A merge reads several files, so it has no single key for _once.
        self._config_lock = threading.RLock()
        self._versions: dict[Path, tuple[_StatKey, VersionsFile]] = {}
        self._projected: dict[str, tuple[_StatKey | None, int, VersionsFile]] = {}
        self._checksums: dict[Path, tuple[_StatKey, str]] = {}
        self._memo: dict[tuple[str, Path], tuple[_StatKey, Any]] = {}
        self._merged: dict[tuple[str, Path], tuple[tuple[Path, ...], tuple[Any, ...], Any]] = {}
        self._staged: dict[str, StagedVersion] = {}
        self._generation = 0

    @classmethod
    def open(cls, catalog_root: Path) -> RunState:
        """Start the state of a run on the catalog at ``catalog_root``."""
        return cls(catalog_root)

    # ── collections ─────────────────────────────────────────────────────────

    def reserve_output(self, source: Path, output: Path) -> bool:
        """Reserve an output for one source until this run ends; a repeat claim by that source passes.

        Different sources must pick different names even before either worker writes.
        """
        source, output = source.resolve(), output.resolve()
        with self._lock:
            owner = self._output_claims.setdefault(output, source)
            return owner == source

    def collection_dir(self, collection_id: str) -> Path:
        """The directory of a collection id such as ``climate/hittekaart``."""
        return self.catalog_root.joinpath(*collection_id.split("/"))

    def _collection_id(self, collection_dir: Path) -> str | None:
        try:
            return collection_dir.resolve().relative_to(self._resolved_root).as_posix()
        except ValueError:
            return None

    # ── versions.json ───────────────────────────────────────────────────────

    def _disk_versions(self, versions_path: Path) -> tuple[_StatKey | None, VersionsFile]:
        try:
            st = versions_path.stat()
        except FileNotFoundError:
            return None, _empty_versions()
        key = _stat_key(st)
        resolved = versions_path.resolve()
        parsed = self._once(
            "versions",
            self._versions,
            resolved,
            key,
            lambda: read_versions(versions_path),
            path=resolved,
        )
        return key, parsed

    def versions(self, collection_dir: Path, *, include_staged: bool = True) -> VersionsFile:
        """The ``versions.json`` of a collection, read once per run, with staged changes by default.

        Returns a new record; its ``Version`` objects are shared with the memo, so replace one, never edit it.
        """
        disk_key, disk = self._disk_versions(collection_dir / "versions.json")
        coll_id = self._collection_id(collection_dir) if include_staged and self._staged else None
        with self._lock:
            # Copy the stage with its generation, so another thread cannot change it.
            generation = self._generation
            staged = self._staged.get(coll_id) if coll_id is not None else None
            hit = self._projected.get(coll_id) if coll_id is not None else None
            if staged is not None:
                staged = replace(
                    staged,
                    assets=dict(staged.assets),
                    removed=set(staged.removed),
                    messages=list(staged.messages),
                )
        if staged is None or coll_id is None:
            return _copy(disk)
        if hit is not None and hit[0] == disk_key and hit[1] == generation:
            return _copy(hit[2])
        projected = staged.apply(disk)
        with self._lock:
            # A stage call since the read makes this projection old. Keep it out.
            if generation == self._generation:
                self._projected[coll_id] = (disk_key, generation, projected)
        return _copy(projected)

    def tracked_assets(self, collection_dir: Path) -> dict[str, Asset]:
        """The assets the collection tracks once this run commits, as a new map.

        It applies the staged assets and removals without building a version.
        """
        _key, disk = self._disk_versions(collection_dir / "versions.json")
        assets = dict(disk.versions[-1].assets) if disk.versions else {}
        coll_id = self._collection_id(collection_dir)
        with self._lock:
            staged = self._staged.get(coll_id) if coll_id is not None else None
            if staged is not None:
                assets.update(staged.assets)
                for key in staged.removed:
                    assets.pop(key, None)
        return assets

    # ── checksums and change checks ─────────────────────────────────────────

    def checksum(self, path: Path) -> str:
        """The SHA-256 of a file, hashed once per run while it stays unchanged.

        A directory asset is fingerprinted on every call: a file change inside it may not move its stat.
        """
        if path.is_dir():
            return compute_dir_checksum(path)
        resolved = path.resolve()
        key = _stat_key(resolved.stat())
        return self._once(
            "checksum",
            self._checksums,
            resolved,
            key,
            lambda: compute_checksum(resolved),
            path=resolved,
        )

    def unchanged(self, path: Path, asset: Asset) -> bool:
        """Report whether ``path`` still holds the bytes that ``asset`` records.

        Only a same-size file with another mtime is hashed; a missing file returns False.
        """
        try:
            st = path.stat()
        except FileNotFoundError:
            return False
        if path.is_dir():
            return self.checksum(path) == asset.sha256
        if st.st_size != asset.size_bytes:
            return False
        if stat_matches(st, asset.mtime, asset.size_bytes):
            return True
        return self.checksum(path) == asset.sha256

    # ── generic memo ────────────────────────────────────────────────────────

    def remember(self, kind: str, path: Path, value: Any) -> None:
        """Replace one memo entry with a writer's read of the current file; other kinds survive.

        A loader started before this call cannot restore its old value, even on a reused stat key.
        """
        resolved = path.resolve()
        key = _stat_key(resolved.stat())
        with self._lock:
            self._invalidations[resolved] = self._invalidations.get(resolved, 0) + 1
            self._memo[kind, resolved] = (key, value)

    def memo(self, kind: str, path: Path, loader: Callable[[Path], T]) -> T:
        """Load a value derived from one file once per run, keyed on ``kind`` and the file's stat.

        Use it for a Parquet footer, a bbox, or a freshness record. ``loader`` runs on a miss.
        """
        resolved = path.resolve()
        key = _stat_key(resolved.stat())
        value: T = self._once(
            f"memo:{kind}",
            self._memo,
            (kind, resolved),
            key,
            lambda: loader(path),
            path=resolved,
        )
        return value

    def _once(
        self,
        flight: str,
        store: dict[Any, tuple[_StatKey, T]],
        slot: Any,
        key: _StatKey,
        load: Callable[[], T],
        *,
        path: Path,
    ) -> T:
        """Return the value of ``slot`` at ``key``; concurrent misses on one key load it once.

        Waiters read the store again; when the load raises, each waiter loads again and gets its own error.
        """
        while True:
            hit = store.get(slot)
            if hit is not None and hit[0] == key:
                return hit[1]
            with self._lock:
                hit = store.get(slot)
                if hit is not None and hit[0] == key:
                    return hit[1]
                generation = self._invalidations.get(path, 0)
                name = (flight, slot, key, generation)
                active = self._flights.get(name)
                leader = active is None
                if active is None:
                    active = self._flights[name] = (threading.Event(), threading.get_ident())
                event, owner = active
            if not leader:
                # Re-entry from the flight owner: its outer call stores the result.
                if owner == threading.get_ident():
                    return load()
                event.wait()
                continue
            try:
                value = load()
                with self._lock:
                    # Race: a writer may invalidate while the load runs outside the lock.
                    if self._invalidations.get(path, 0) == generation:
                        store[slot] = (key, value)
                return value
            finally:
                with self._lock:
                    del self._flights[name]
                event.set()

    # ── config and metadata ─────────────────────────────────────────────────
    # Merged entries compare YAML parse objects by identity; methods return deep copies.

    def _yaml(self, path: Path) -> Any:
        """Parse a YAML file once per run while its stat stays the same."""
        return self.memo("config.yaml", path, _config.parse_yaml)

    def _merged_files(self, filename: str, directory: Path) -> dict[str, Any]:
        """The shared merge of ``filename`` for ``directory``. Do not mutate it."""
        # Deadlock: a YAML loader can read metadata, so do not wait while holding the merge lock.
        files = tuple(_config.find_portolan_files(directory, filename, self.catalog_root))
        root_file = _config.get_config_path(self._resolved_root)
        if filename == _config.CONFIG_FILENAME and root_file not in files and root_file.is_file():
            # The merge reads the catalog file for its legacy collections: section.
            files += (root_file,)
        try:
            sources: tuple[Any, ...] | None = tuple(self._yaml(path) for path in files)
        except yaml.YAMLError:
            sources = None  # The merge below raises the typed error.
        # Workers that miss one directory together merge it once.
        with self._config_lock:
            key = (filename, directory.resolve())
            hit = self._merged.get(key)
            if (
                hit is not None
                and sources is not None
                and hit[0] == files
                and all(old is new for old, new in zip(hit[1], sources, strict=True))
            ):
                merged: dict[str, Any] = hit[2]
                return merged
            merge = (
                _config.merge_config_files
                if filename == _config.CONFIG_FILENAME
                else _config.merge_metadata_files
            )
            merged = merge(directory, self.catalog_root, self._yaml)
            with self._lock:
                self._merged[key] = (files, sources or (), merged)
            return merged

    def config(self, directory: Path) -> dict[str, Any]:
        """The merged ``config.yaml`` hierarchy for ``directory``, as ``config.load_merged_config``.

        The environment applies on each call. The result is a copy that the caller may edit.
        """
        merged = self._merged_files(_config.CONFIG_FILENAME, directory)
        return copy.deepcopy(_config.with_env(merged))

    def metadata(self, directory: Path, *, inherit: bool = True) -> dict[str, Any]:
        """The merged ``metadata.yaml`` hierarchy for ``directory``, as a copy the caller may edit.

        With ``inherit=False``, only the file ``directory`` owns, as ``config.load_own_metadata`` reads it.
        """
        if not inherit:
            return copy.deepcopy(_config.own_metadata(directory, self._yaml))
        return copy.deepcopy(self._merged_files(_config.METADATA_FILENAME, directory))

    def setting(
        self,
        key: str,
        *,
        collection_dir: Path | None = None,
        cli_value: Any | None = None,
    ) -> Any | None:
        """Resolve a setting with the precedence of ``config.get_setting``.

        CLI value, environment, config of ``collection_dir`` or the catalog, then default; lists come back copied.
        """

        def layers() -> tuple[dict[str, Any], ...]:
            if collection_dir is not None:
                merged = self._merged_files(_config.CONFIG_FILENAME, collection_dir)
                return (_config.with_env(merged),)
            return (_config.catalog_file_config(self.catalog_root, self._yaml).data,)

        return _config.resolve_setting(key, cli_value, layers)

    def backend(self, collection_dir: Path) -> str:
        """The versioning backend from the collection config hierarchy, ``"file"`` when none is set."""
        name: str | None = self.setting("backend", collection_dir=collection_dir)
        return name or "file"

    # ── staging ─────────────────────────────────────────────────────────────

    def stage(
        self,
        collection_id: str,
        *,
        assets: dict[str, Asset] | None = None,
        removed: set[str] | None = None,
        message: str | None = None,
        breaking: bool = False,
        schema: SchemaInfo | None = None,
        amend: bool = False,
        minor: bool = False,
        on_publish: Callable[[], None] | None = None,
    ) -> None:
        """Stage asset changes for a collection; calls merge and the commit writes one version.

        Pass ``amend`` only when this command wrote the latest version; a later ``on_publish`` wins.
        """
        with self._lock:
            staged = self._staged.setdefault(collection_id, StagedVersion())
            for key in removed or ():
                staged.assets.pop(key, None)
                staged.removed.add(key)
            for key, asset in (assets or {}).items():
                staged.removed.discard(key)
                staged.assets[key] = asset
            if message and message not in staged.messages:
                staged.messages.append(message)
            staged.breaking = staged.breaking or breaking
            if schema is not None:
                staged.schema = schema
            staged.amend = staged.amend and amend
            staged.minor = staged.minor or minor
            if on_publish is not None:
                staged.on_publish = on_publish
            self._generation += 1

    def staged(self) -> dict[str, StagedVersion]:
        """The staged collections, by collection id. The result is a copy."""
        with self._lock:
            return dict(self._staged)

    def take_staged(self) -> dict[str, StagedVersion]:
        """Return the staged collections and clear them. The commit calls this."""
        with self._lock:
            staged, self._staged = self._staged, {}
            self._projected.clear()
            self._generation += 1
        return staged

    def restore_staged(self, pending: dict[str, StagedVersion]) -> None:
        """Restore work a failed commit did not write, keeping newer stages last."""
        if not pending:
            return
        with self._lock:
            for collection_id, older in pending.items():
                newer = self._staged.get(collection_id)
                if newer is not None:
                    for key in newer.removed:
                        older.assets.pop(key, None)
                    older.assets.update(newer.assets)
                    older.removed = (older.removed | newer.removed) - newer.assets.keys()
                    older.messages = list(dict.fromkeys([*older.messages, *newer.messages]))
                    older.breaking |= newer.breaking
                    older.schema = newer.schema if newer.schema is not None else older.schema
                    older.amend &= newer.amend
                    older.minor |= newer.minor
                    older.on_publish = newer.on_publish or older.on_publish
                self._staged[collection_id] = older
            self._projected.clear()
            self._generation += 1

    # ── invalidation ────────────────────────────────────────────────────────

    def invalidate(self, path: Path) -> None:
        """Drop every memo entry for ``path``; a writer calls it after each write.

        A coarse clock, a reused inode, or a writable mmap can leave the stat key equal.
        """
        resolved = path.resolve()
        with self._lock:
            self._invalidations[resolved] = self._invalidations.get(resolved, 0) + 1
            self._versions.pop(resolved, None)
            self._checksums.pop(resolved, None)
            for memo_key in [k for k in self._memo if k[1] == resolved]:
                del self._memo[memo_key]
            if resolved.name == "versions.json":
                coll_id = self._collection_id(resolved.parent)
                if coll_id is not None:
                    self._projected.pop(coll_id, None)
