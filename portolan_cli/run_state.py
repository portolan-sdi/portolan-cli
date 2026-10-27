"""Run-scoped state for one command: one read of each shared file per run.

Each command opens one :class:`RunState` and passes it down as a parameter.
The state reads each ``versions.json`` once, hashes each file once, and parses
each YAML file once. Every in-run reader asks the state, so a per-file loop
costs a ``stat`` and a dict lookup, not a read (issue #995).

Every memo entry is keyed on the file's inode, size, ``st_mtime_ns``, and
``st_ctime_ns``. A file that changes during the run gets a new key, and the
next call reads it again. A copy that restores the old mtime still moves the
status-change time. One limit stays: on a file system with a coarse clock, a
same-size rewrite in the same clock tick as the memo read can keep every field
of the key. ext4 can also give the inode of a replaced file to the next atomic
write. A writer in the run therefore calls :meth:`RunState.invalidate`.
A live writable memory map can also change bytes repeatedly without moving
any stat field. Its writer must invalidate the memo; stat alone cannot detect
those changes.

The state also holds the versions that a command stages. ``versions()`` returns
the disk record with the staged assets applied, so a reader in the same run
sees the files it already added. ``version_ops.commit_run`` writes each staged
collection once at the end of the run.

The state is a parameter and not a contextvar. mypy and import-linter can see a
parameter, and a worker that a plain ``executor.submit`` starts does not lose
it. Two workers that miss the same key at once load it once. The first one
loads, and the other one waits for its result. Workers on different keys load
at the same time, so the checksums of different files do not wait for each
other.

This module is in the import-linter ``layer-read`` layer. It imports only
``versions``, ``config``, ``sync.checksums``, ``constants``, and ``trace``. A
loader that needs a heavy library is a parameter of :meth:`RunState.memo`.
"""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, TypeVar

import yaml

from portolan_cli import config as _config
from portolan_cli.constants import MTIME_TOLERANCE_SECONDS
from portolan_cli.sync.checksums import compute_checksum, compute_dir_checksum
from portolan_cli.versions import (
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

#: The tolerance when the recorded mtime has a fraction of a second. The file
#: system kept sub-second time, so only float rounding can move it.
FINE_MTIME_TOLERANCE_SECONDS = 1e-6

_StatKey = tuple[int, int, int, int]


def mtime_matches(current: float, recorded: float) -> bool:
    """Report whether two mtimes name the same write of a file.

    A recorded mtime with a fraction comes from a file system that keeps
    sub-second time, so the tolerance is 1 microsecond. A whole-second mtime
    marks a coarse file system (FAT, some NFS and CIFS mounts), so the
    tolerance is ``MTIME_TOLERANCE_SECONDS``. The bound is strict.

    Args:
        current: The mtime of the file now, in seconds.
        recorded: The mtime that a record holds, in seconds.

    Returns:
        True when the difference is inside the tolerance.
    """
    tolerance = (
        MTIME_TOLERANCE_SECONDS if float(recorded).is_integer() else FINE_MTIME_TOLERANCE_SECONDS
    )
    return abs(current - recorded) < tolerance


def stat_matches(st: os.stat_result, mtime: float | None, size: int | None) -> bool:
    """Report whether a ``stat`` result matches a recorded mtime and size.

    This is the one change predicate for every fast path. The size must match
    exactly, and the mtime must pass :func:`mtime_matches`. The coarse window
    of a whole-second record applies only when the file mtime is also a whole
    second. A file mtime with a fraction shows a fine clock, so a different
    timestamp is a real edit and requires a checksum. A record without an
    mtime or a size never matches.

    Args:
        st: The ``stat`` result of the file now.
        mtime: The recorded mtime in seconds, or None.
        size: The recorded size in bytes, or None.

    Returns:
        True when the file is the one the record describes.
    """
    if mtime is None or size is None or st.st_size != size:
        return False
    return file_mtime_matches(st.st_mtime, mtime)


def file_mtime_matches(current: float, recorded: float) -> bool:
    """Compare file mtimes, granting the coarse window only to two whole seconds.

    A fractional current mtime proves the file has a fine clock, even when
    the recorded write happened to fall on a whole second.
    """
    if float(recorded).is_integer() and not float(current).is_integer():
        return False
    return mtime_matches(current, recorded)


def _stat_key(st: os.stat_result) -> _StatKey:
    """The memo key of a file: inode, size, mtime, and status-change time.

    The size and the mtime are the key on every platform. The status-change
    time is a POSIX safeguard: a write or an ``os.utime`` call moves it. On
    Windows ``st_ctime_ns`` is the creation time, which an in-place rewrite
    does not change.
    """
    return (st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)


def _empty_versions() -> VersionsFile:
    return VersionsFile(spec_version=SPEC_VERSION, current_version=None, versions=[])


def _next(versions_file: VersionsFile, minor: bool) -> str:
    """The version string of the next snapshot: a minor bump or a patch bump.

    The minor bump is the rule of ``JsonFileBackend.publish``, which the
    companion files of ``add`` went through before the run state staged them.
    """
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

    Attributes:
        assets: Asset key to the record to add or replace.
        removed: Asset keys that the version drops.
        messages: The distinct messages of the stage calls, in order.
        breaking: True when any stage call marked the change as breaking.
        schema: The schema of the last stage call that gave one.
        amend: True when every stage call asked to fold the changes into the
            latest version on disk. One call without it makes a new version.
        minor: True when any stage call asked for a minor bump. The version
            bumps the patch otherwise.
        on_publish: Optional callback to run after the plugin publishes.
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

        ``add_version`` and ``amend_version`` return the input object when
        nothing changed, so a caller tests ``result is versions_file`` to skip
        a write.
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

    Create one with :meth:`open` at the entry point of a command and pass it
    down. See the module docstring for the memo rules.
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
        """Reserve an output for one source until this run ends.

        A handler may repeat its caller's claim. Different sources must pick
        different names even before either worker has written its output.
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
        """The ``versions.json`` of a collection, read once per run.

        Args:
            collection_dir: The collection directory.
            include_staged: When True, the result holds the versions that this
                run staged for the collection as one more version. When False,
                it is the record on disk.

        Returns:
            A new record whose version list the caller may edit. The
            ``Version`` objects are shared with the memo, so a caller replaces
            a version and never edits its asset map. A collection with no
            ``versions.json`` gives an empty record.

        Raises:
            ValueError: If ``versions.json`` does not parse.
        """
        disk_key, disk = self._disk_versions(collection_dir / "versions.json")
        coll_id = self._collection_id(collection_dir) if include_staged and self._staged else None
        with self._lock:
            # Read the stage and its generation together, and project a copy, so
            # a stage call in another thread cannot change the projection.
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
        """The assets that the collection tracks once this run commits.

        The result is the latest version on disk, updated with the assets that
        the run staged, without the keys it staged for removal. It builds no
        version, so a reader that needs only the asset records does not
        project the staged change through ``add_version``.

        Args:
            collection_dir: The collection directory.

        Returns:
            Asset key to its record. A new map, which the caller may edit.

        Raises:
            ValueError: If ``versions.json`` does not parse.
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

        A directory asset such as a FileGDB gets the fingerprint of
        ``compute_dir_checksum`` on each call. A change to a file inside it does
        not always change the directory's own stat, so no memo key holds.

        Raises:
            FileNotFoundError: If ``path`` does not exist.
            ValueError: If ``path`` is not a regular file or a directory.
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

        The stat comes first. A size that differs means a change with no hash. A
        size and mtime that pass :func:`stat_matches` mean no change with no
        hash. Only a same-size file with another mtime is hashed.

        Args:
            path: The file or directory asset on disk.
            asset: The record from ``versions.json``.

        Returns:
            False when the file is missing or its content differs.
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
        """Replace one memo entry with a writer's read of the current file.

        Other kinds survive so a COG writer can seed both metadata and band
        statistics. A loader started before this call cannot restore its old
        value, even when the filesystem reused the stat key.
        """
        resolved = path.resolve()
        key = _stat_key(resolved.stat())
        with self._lock:
            self._invalidations[resolved] = self._invalidations.get(resolved, 0) + 1
            self._memo[kind, resolved] = (key, value)

    def memo(self, kind: str, path: Path, loader: Callable[[Path], T]) -> T:
        """Load a value derived from one file once per run.

        Use it for a Parquet footer, a bbox, or a freshness record. The key is
        ``kind`` plus the file's stat, so a change to the file loads it again.

        Args:
            kind: A name for the value, such as ``"parquet.footer"``.
            path: The file the value comes from.
            loader: Called with ``path`` on a miss.

        Returns:
            The value that ``loader`` returned for the current file.

        Raises:
            FileNotFoundError: If ``path`` does not exist.
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
        """Return the value of ``slot`` at ``key``, and load it once on a miss.

        The first worker that misses loads the value. Another worker that misses
        the same slot and key waits for it, then reads the store again. When the
        load raises, each waiting worker loads again and gets its own error.
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
                # A wrapper loader may delegate to another reader of this key.
                # Its outer call owns the flight and will store the result.
                if owner == threading.get_ident():
                    return load()
                event.wait()
                continue
            try:
                value = load()
                with self._lock:
                    # A writer can invalidate while this loader is outside
                    # the lock, even when the stat key itself stays unchanged.
                    if self._invalidations.get(path, 0) == generation:
                        store[slot] = (key, value)
                return value
            finally:
                with self._lock:
                    del self._flights[name]
                event.set()

    # ── config and metadata ─────────────────────────────────────────────────
    #
    # Each YAML file parses once per run through ``memo``, so ``invalidate``
    # drops it. A merged entry holds the parse objects it came from, and a
    # lookup compares them by identity. A file that changes, or that a writer
    # invalidates, parses to a new object, so the merge runs again.
    #
    # Each method returns a deep copy. Callers type the result as a plain
    # dict, and some of them edit it. A copy of a config mapping costs
    # microseconds. A read-only view would need every caller to change.

    def _yaml(self, path: Path) -> Any:
        """Parse a YAML file once per run while its stat stays the same."""
        return self.memo("config.yaml", path, _config.parse_yaml)

    def _merged_files(self, filename: str, directory: Path) -> dict[str, Any]:
        """The shared merge of ``filename`` for ``directory``. Do not mutate it."""
        # A YAML loader can read metadata through this state. Do not wait for
        # its flight while holding the merge lock that its metadata needs.
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
        """The merged ``config.yaml`` hierarchy that applies to ``directory``.

        It equals ``config.load_merged_config``. For the catalog root it equals
        ``config.load_catalog_config(root).data``. The environment applies on
        each call. The result is a copy that the caller may edit.
        """
        merged = self._merged_files(_config.CONFIG_FILENAME, directory)
        return copy.deepcopy(_config.with_env(merged))

    def metadata(self, directory: Path, *, inherit: bool = True) -> dict[str, Any]:
        """The merged ``metadata.yaml`` hierarchy that applies to ``directory``.

        Args:
            directory: A collection or catalog directory.
            inherit: When False, only the ``metadata.yaml`` that ``directory``
                owns, as ``config.load_own_metadata`` reads it.

        Returns:
            A copy that the caller may edit.
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

        The order is the CLI value, the environment, the config hierarchy of
        ``collection_dir`` (or the catalog config without one), then the
        built-in default. A list or mapping comes back as a copy.
        """

        def layers() -> tuple[dict[str, Any], ...]:
            if collection_dir is not None:
                merged = self._merged_files(_config.CONFIG_FILENAME, collection_dir)
                return (_config.with_env(merged),)
            return (_config.catalog_file_config(self.catalog_root, self._yaml).data,)

        return _config.resolve_setting(key, cli_value, layers)

    def backend(self, collection_dir: Path) -> str:
        """The versioning backend of a collection, ``"file"`` when none is set.

        It reads the config hierarchy of the collection, as the push check
        does, so a collection can set its own backend.
        """
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
        """Stage asset changes for a collection. The commit writes one version.

        Calls for one collection merge. A later asset record replaces an earlier
        one under the same key. A key that one call removes and a later call
        adds stays. A key that one call adds and a later call removes goes.

        Args:
            collection_id: The collection id, such as ``climate/hittekaart``.
            assets: Asset key to the record to add or replace. Each record
                carries its checksum, so the commit hashes nothing.
            removed: Asset keys to drop from the snapshot.
            message: A message for the version. Distinct messages join.
            breaking: Mark the version as breaking.
            schema: A schema fingerprint for the version.
            amend: Fold the changes into the latest version on disk. Pass it
                only when this command wrote that version, as a side step of
                ``add`` does. A collection with no version gets a new one.
            minor: Bump the minor of the version, not the patch. One call
                with it makes the whole version a minor bump.
            on_publish: Optional work to run once after the backend publishes.
                A later callback replaces an earlier one for this collection.
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
        """Drop every memo entry for ``path``.

        The stat key already catches a change. A writer calls this after it
        writes a file so that a reader never trusts a key that a coarse clock
        and an in-place write left equal.
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
