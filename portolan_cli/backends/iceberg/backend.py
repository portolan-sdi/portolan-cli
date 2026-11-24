"""``IcebergBackend``: a collection is one Iceberg table in a REST catalog.

DuckDB writes the rows (``session.py``) and the REST client writes the
version record (``rest.py``, ``versioning.py``). A publish is:

1. Read the table. A missing table is created at format version 3 with the
   column types DuckDB reports for the GeoParquet files, so a geometry
   column lands typed with its CRS (``ddl.py``).
2. Merge the asset set: the current version's assets, plus the new ones,
   minus the removed ones. Every Parquet data asset of the merged set is
   read, so the table always holds the whole collection and a re-add never
   doubles rows.
3. One DuckDB transaction: ``TRUNCATE`` (on an existing table) and
   ``INSERT BY NAME`` from the files. DuckDB commits a delete snapshot and
   an append snapshot, and ``main`` ends on the append.
4. One REST commit: the table property ``portolan.version.<v>`` and the tag
   ``v<v>`` on that snapshot, guarded by ``assert-ref-snapshot-id`` on
   ``main``. A writer that landed in between fails the guard with a 409.

Rollback moves ``main`` to the tagged snapshot over REST. Prune removes
the tags, the records, and the snapshots over REST, then deletes the data
files that no remaining snapshot reaches. DuckDB's writes are
merge-on-read (duckdb/duckdb-iceberg#1659): a truncate writes a deletion
vector and the kept snapshot still lists every older data file with it,
so on a table only this backend wrote, prune finds nothing unreachable
and reclaims no space. The catalog's own compaction does that. The
deletion runs anyway, because a compacted table does leave files behind
in the removed snapshots.

A table the pyiceberg backend wrote is read-only (``versioning.py``).
``hooks.iceberg_backend`` imports this module on first use.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from portolan_cli.api import (
    DriftReport,
    SchemaFingerprint,
    SchemaInfo,
    Version,
    span,
    write_json_atomic,
)
from portolan_cli.backends.iceberg import ddl
from portolan_cli.backends.iceberg.auth import token_provider
from portolan_cli.backends.iceberg.rest import (
    CommitConflictError,
    LoadTableResult,
    NoSuchTableError,
    RestCatalog,
)
from portolan_cli.backends.iceberg.session import Session
from portolan_cli.backends.iceberg.settings import load_settings
from portolan_cli.backends.iceberg.versioning import (
    MAIN_BRANCH,
    build_assets,
    compute_next_version,
    current_snapshot_id,
    current_version_from_metadata,
    is_legacy_table,
    legacy_current_version,
    legacy_versions_from_metadata,
    property_key,
    snapshot_id_of,
    tag_name,
    version_from_payload,
    version_payload,
    versions_from_metadata,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import httpx

    from portolan_cli.api import Asset, PostAddContext, PullResult
    from portolan_cli.backends.iceberg.session import Connection
    from portolan_cli.backends.iceberg.settings import IcebergSettings

logger = logging.getLogger(__name__)

#: The table property that marks a table this backend created, and the
#: version of its version record. A reader of a later format checks it.
RECORD_MARKER = ("portolan.record", "1")
#: The stac-geoparquet mirror of the items. A Parquet file, not table data.
ITEMS_MIRROR = "items.parquet"


class LegacyTableError(ValueError):
    """The table holds pyiceberg-era records. It is read-only here."""


@dataclass(frozen=True)
class RefreshResult:
    """The metadata location on ``collection.json`` before and after ``refresh``."""

    previous_location: str | None
    metadata_location: str | None

    @property
    def changed(self) -> bool:
        """Whether ``refresh`` wrote a different metadata location."""
        return self.previous_location != self.metadata_location


class SchemaChangeError(ValueError):
    """A column changed type. History must survive, so the table is not recreated."""


def _merge_into_collection_json(
    path: Path,
    fields: dict[str, Any],
    extensions: tuple[str, ...],
    assets: dict[str, dict[str, Any]],
) -> None:
    """Merge the Iceberg fields, extensions, and assets into ``collection.json``.

    The merge keeps every other field in the file, including the ones an
    enrichment hook wrote after Portolan saved the document (#947).
    """
    document: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    document.update(fields)
    declared = document.setdefault("stac_extensions", [])
    declared.extend(url for url in extensions if url not in declared)
    if assets:
        document.setdefault("assets", {}).update(assets)
    write_json_atomic(path, document)


def _is_fetchable(href: str) -> bool:
    """Can a reader retrieve this href over the web?

    rashid's PTL-AST-002 accepts a relative href or https, so an s3:// or
    gs:// location fails the same rule a published catalog has to meet.
    """
    from urllib.parse import urlparse

    return urlparse(href).scheme in ("", "https")


def _is_data_asset(key: str) -> bool:
    """Whether a versions asset key names Parquet rows of the collection."""
    name = PurePosixPath(key).name
    return name.endswith(".parquet") and name != ITEMS_MIRROR


class IcebergBackend:
    """One REST catalog, one namespace, one table per collection.

    Args:
        catalog_root: The Portolan catalog root. Asset hrefs resolve against it.
        settings: The backend settings. Read from the environment when None.
        http: The HTTP client for the catalog and the token endpoint, or None.
        connect: Opens the DuckDB connection, or None for ``duckdb.connect``.

    Construction reads the settings and nothing else. The first call that
    needs the catalog opens the connections.
    """

    def __init__(
        self,
        catalog_root: Path | None = None,
        *,
        settings: IcebergSettings | None = None,
        http: httpx.Client | None = None,
        connect: Callable[[], Connection] | None = None,
    ) -> None:
        self._catalog_root = catalog_root if catalog_root is not None else Path.cwd()
        self._settings = settings if settings is not None else load_settings()
        self._auth = token_provider(self._settings, http=http)
        self._catalog = RestCatalog(
            self._settings.uri, auth=self._auth, warehouse=self._settings.warehouse, http=http
        )
        self._session = (
            Session(self._settings, self._auth, connect=connect)
            if connect is not None
            else Session(self._settings, self._auth)
        )
        self._namespace_ready = False

    # ------------------------------------------------------------------ naming

    @staticmethod
    def _validate_collection(collection: str) -> str:
        if not collection or not collection.strip():
            raise ValueError("Collection name cannot be empty")
        if ".." in collection or "/" in collection or "\\" in collection:
            raise ValueError(f"Invalid collection name: {collection!r}")
        safe = Path(collection).name
        if safe in ("", ".", ".."):
            raise ValueError(f"Invalid collection name: {collection!r}")
        return safe

    def _table(self, collection: str) -> tuple[str, str]:
        return self._settings.namespace, self._validate_collection(collection)

    def _load(self, namespace: str, name: str) -> LoadTableResult | None:
        try:
            return self._catalog.load_table(namespace, name)
        except NoSuchTableError:
            return None

    def _ensure_namespace(self) -> None:
        if self._namespace_ready:
            return
        namespace = self._settings.namespace
        if not self._catalog.namespace_exists(namespace):
            self._catalog.create_namespace(namespace)
        self._namespace_ready = True

    def _commit(
        self,
        namespace: str,
        name: str,
        loaded: LoadTableResult,
        updates: list[dict[str, Any]],
        *,
        main_at: int | None = None,
    ) -> LoadTableResult:
        """Commit ``updates``, guarded on the table uuid and, when given, on ``main``."""
        requirements: list[dict[str, Any]] = [
            {"type": "assert-table-uuid", "uuid": loaded.metadata["table-uuid"]}
        ]
        if main_at is not None:
            requirements.append(
                {"type": "assert-ref-snapshot-id", "ref": MAIN_BRANCH, "snapshot-id": main_at}
            )
        return self._catalog.commit_table(
            namespace, name, requirements=requirements, updates=updates
        )

    @staticmethod
    def _refuse_legacy(loaded: LoadTableResult, collection: str) -> None:
        if is_legacy_table(loaded.metadata):
            raise LegacyTableError(
                f"Collection '{collection}' is a table the pyiceberg backend wrote. It is "
                "read-only here: publish a new collection with this backend to get a format "
                "version 3 table."
            )

    # ------------------------------------------------------------------ reads

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection, oldest first."""
        loaded = self._load(*self._table(collection))
        if loaded is None:
            return []
        if is_legacy_table(loaded.metadata):
            return legacy_versions_from_metadata(loaded.metadata)
        return versions_from_metadata(loaded.metadata)

    def _current_version(self, loaded: LoadTableResult, collection: str) -> Version:
        if is_legacy_table(loaded.metadata):
            legacy = legacy_current_version(loaded.metadata)
            if legacy is None:
                raise FileNotFoundError(f"No versions found for collection: {collection}")
            return legacy
        current = current_version_from_metadata(loaded.metadata)
        if current is not None:
            return current
        versions = versions_from_metadata(loaded.metadata)
        if not versions:
            raise FileNotFoundError(f"No versions found for collection: {collection}")
        logger.warning(
            "The current snapshot of '%s' carries no Portolan version tag; another writer "
            "committed after version %s. Reporting that version.",
            collection,
            versions[-1].version,
        )
        return versions[-1]

    def get_current_version(self, collection: str) -> Version:
        """Get the version ``main`` points at."""
        loaded = self._load(*self._table(collection))
        if loaded is None:
            raise FileNotFoundError(f"No versions found for collection: {collection}")
        return self._current_version(loaded, collection)

    def check_drift(self, collection: str) -> DriftReport:
        """Report the current version. The catalog is the one copy, so nothing drifts."""
        current: str | None = None
        loaded = self._load(*self._table(collection))
        if loaded is not None:
            try:
                current = self._current_version(loaded, collection).version
            except FileNotFoundError:
                current = None
        return DriftReport(
            has_drift=False,
            local_version=current,
            remote_version=current,
            message="The catalog holds the one copy of the version history",
        )

    # ------------------------------------------------------------------ publish

    def _partition_keys(self, collection: str) -> list[str]:
        """The ``partition:keys`` names of the collection, from its collection.json."""
        path = self._catalog_root / collection / "collection.json"
        if not path.exists():
            return []
        document = json.loads(path.read_text(encoding="utf-8"))
        return [str(key["name"]) for key in document.get("partition:keys", [])]

    def _data_paths(self, merged: dict[str, Asset], given: dict[str, str]) -> list[str]:
        """The local Parquet files of the merged asset set, in asset order."""
        paths: list[str] = []
        for key, asset in merged.items():
            if not _is_data_asset(key):
                continue
            path = (
                Path(given[key])
                if key in given
                else _local_asset_path(self._catalog_root, asset.href)
            )
            if not path.exists():
                raise ValueError(
                    f"Asset {asset.href} of the current version is missing locally at {path}. "
                    "Pull the collection first, or remove the asset."
                )
            paths.append(str(path))
        return paths

    def _describe(self, source: str) -> list[tuple[str, str]]:
        return [
            (str(row[0]), str(row[1])) for row in self._session.execute(ddl.describe_sql(source))
        ]

    def _create_table(
        self, namespace: str, name: str, columns: list[tuple[str, str]], partition_keys: list[str]
    ) -> LoadTableResult:
        self._ensure_namespace()
        self._session.execute(ddl.create_table_sql(self._session.table(name), columns))
        loaded = self._catalog.load_table(namespace, name)
        updates: list[dict[str, Any]] = []
        if partition_keys:
            fields = _current_fields(loaded.metadata)
            missing = [key for key in partition_keys if key not in fields]
            if missing:
                raise ValueError(
                    f"partition:keys name columns the files do not have: {', '.join(missing)}"
                )
            spec = {
                "spec-id": 1,
                "fields": [
                    {
                        "name": key,
                        "transform": "identity",
                        "source-id": fields[key],
                        "field-id": 1000 + index,
                    }
                    for index, key in enumerate(partition_keys)
                ],
            }
            updates.append({"action": "add-spec", "spec": spec})
            updates.append({"action": "set-default-spec", "spec-id": -1})
        updates.append({"action": "set-properties", "updates": dict([RECORD_MARKER])})
        return self._commit(namespace, name, loaded, updates)

    def _evolve_table(self, name: str, columns: list[tuple[str, str]]) -> None:
        """Add the new columns of the files. A changed type refuses the publish."""
        table = self._session.table(name)
        existing = dict(self._describe(table))
        # DuckDB reads an Iceberg geometry column back as GEOMETRY with no
        # CRS, so the comparison is on the type name before the parameter.
        changed = [
            f"{column} is {existing[column]} in the table and {kind} in the files"
            for column, kind in columns
            if column in existing and _base_type(existing[column]) != _base_type(kind)
        ]
        if changed:
            raise SchemaChangeError(
                "A column changed type, and the table keeps its history: " + "; ".join(changed)
            )
        new = [(column, kind) for column, kind in columns if column not in existing]
        for statement in ddl.add_columns_sql(table, new):
            self._session.execute(statement)

    def publish(
        self,
        collection: str,
        assets: dict[str, str],
        schema: SchemaFingerprint,
        breaking: bool,
        message: str,
        removed: set[str] | None = None,
        version: str | None = None,
    ) -> Version:
        """Publish a new version: replace the rows, then record the version."""
        namespace, name = self._table(collection)
        with span("iceberg.metadata", collection=collection):
            loaded = self._load(namespace, name)
            if loaded is not None:
                self._refuse_legacy(loaded, collection)
            versions = versions_from_metadata(loaded.metadata) if loaded else []
            newest = versions[-1].version if versions else None
            next_version = version or compute_next_version(newest, breaking)
            if any(v.version == next_version for v in versions):
                raise ValueError(f"Version {next_version} already exists in '{collection}'")
            current = current_version_from_metadata(loaded.metadata) if loaded else None
            if current is None and versions:
                current = versions[-1]
            new_assets, changes = build_assets(assets, collection=collection)
            merged: dict[str, Asset] = {**(current.assets if current else {}), **new_assets}
            for key in removed or ():
                merged.pop(key, None)
            paths = self._data_paths(merged, assets)
            if loaded is None and not paths:
                raise ValueError(
                    f"Collection '{collection}' has no Parquet data asset to publish as a table"
                )
            partition_keys = self._partition_keys(name)

        with span("iceberg.schema", assets=len(paths)):
            columns: list[tuple[str, str]] = []
            if paths:
                columns = self._describe(
                    ddl.read_parquet_sql(paths, hive_partitioning=bool(partition_keys))
                )
            if loaded is None:
                loaded = self._create_table(namespace, name, columns, partition_keys)
            elif columns:
                self._evolve_table(name, columns)

        before = current_snapshot_id(loaded.metadata)
        expected_snapshots = 0
        with span("iceberg.upload", assets=len(paths)), self._session.transaction():
            table = self._session.table(name)
            if before is not None:
                self._session.execute(f"TRUNCATE {table}")
                expected_snapshots += 1
            if paths:
                source = ddl.read_parquet_sql(paths, hive_partitioning=bool(partition_keys))
                self._session.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {source}")  # noqa: S608
                expected_snapshots += 1

        with span("iceberg.record", version=next_version):
            loaded = self._catalog.load_table(namespace, name)
            snapshot_id = current_snapshot_id(loaded.metadata)
            if snapshot_id is None:
                raise RuntimeError(f"No snapshot after the write of '{collection}'")
            _check_own_snapshots(
                loaded.metadata, before, snapshot_id, expected_snapshots, collection
            )
            schema_info = SchemaInfo(
                type=schema.get("hash", "unknown"),
                fingerprint={
                    "columns": schema.get("columns", []),
                    "types": schema.get("types", {}),
                },
            )
            payload = version_payload(next_version, breaking, message, merged, schema_info, changes)
            loaded = self._commit(
                namespace,
                name,
                loaded,
                [
                    {"action": "set-properties", "updates": {property_key(next_version): payload}},
                    {
                        "action": "set-snapshot-ref",
                        "ref-name": tag_name(next_version),
                        "type": "tag",
                        "snapshot-id": snapshot_id,
                    },
                ],
                main_at=snapshot_id,
            )
        created = _snapshot_time(loaded.metadata, snapshot_id)
        return version_from_payload(next_version, payload, created)

    # ------------------------------------------------------------------ rollback and prune

    def rollback(self, collection: str, target_version: str) -> Version:
        """Move ``main`` back to the snapshot of ``target_version``. No row is copied."""
        namespace, name = self._table(collection)
        loaded = self._load(namespace, name)
        if loaded is None:
            raise FileNotFoundError(f"No versions found for collection: {collection}")
        self._refuse_legacy(loaded, collection)
        target = snapshot_id_of(loaded.metadata, target_version)
        versions = {v.version: v for v in versions_from_metadata(loaded.metadata)}
        if target is None or target_version not in versions:
            raise ValueError(f"Version {target_version} not found in collection: {collection}")
        self._commit(
            namespace,
            name,
            loaded,
            [
                {
                    "action": "set-snapshot-ref",
                    "ref-name": MAIN_BRANCH,
                    "type": "branch",
                    "snapshot-id": target,
                }
            ],
            main_at=current_snapshot_id(loaded.metadata),
        )
        return versions[target_version]

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the ``keep`` newest and the current one."""
        namespace, name = self._table(collection)
        loaded = self._load(namespace, name)
        if loaded is None:
            return []
        self._refuse_legacy(loaded, collection)
        versions = versions_from_metadata(loaded.metadata)
        current = current_version_from_metadata(loaded.metadata)
        kept = {v.version for v in versions[-keep:]} if keep > 0 else set()
        if current is not None:
            kept.add(current.version)
        victims = [v for v in versions if v.version not in kept]
        if not victims or dry_run:
            return victims

        victim_ids = _snapshots_to_remove(loaded.metadata, kept, victims)
        location_before = loaded.metadata_location
        updates: list[dict[str, Any]] = [
            {"action": "remove-snapshot-ref", "ref-name": tag_name(v.version)} for v in victims
        ]
        updates.append(
            {"action": "remove-properties", "removals": [property_key(v.version) for v in victims]}
        )
        updates.append({"action": "remove-snapshots", "snapshot-ids": victim_ids})
        # Guarded on main: a writer that moved main since the read makes the
        # commit fail with a 409 instead of removing the snapshot it now needs.
        after = self._commit(
            namespace, name, loaded, updates, main_at=current_snapshot_id(loaded.metadata)
        )
        self._delete_orphans(namespace, name, location_before, victim_ids, after)
        return victims

    def _files_of(self, location: str, snapshot_id: int, *, live_only: bool) -> set[str]:
        # S608: the location is a quoted literal and the id an int, both from the catalog.
        rows = self._session.execute(
            "SELECT manifest_content, status, content, file_path FROM iceberg_metadata("  # noqa: S608
            f"{ddl.quote_literal(location)}, snapshot_from_id => {int(snapshot_id)})"
        )
        return {
            str(row[3]) for row in rows if not live_only or str(row[1]) in ("ADDED", "EXISTING")
        }

    def _delete_orphans(
        self,
        namespace: str,
        name: str,
        location_before: str,
        victim_ids: list[int],
        after: LoadTableResult,
    ) -> None:
        """Delete the data and delete files only the removed snapshots reached.

        The old metadata file still lists the removed snapshots, so their
        files are read from it. The files of every snapshot that remains,
        with any status, are kept. A failed delete is logged and counted;
        the versions are gone from the catalog either way.
        """
        storage = self._settings.storage
        if storage.kind != "s3" or storage.key_id is None:
            logger.info(
                "Prune of %s.%s left the data files of %d snapshot(s) in the warehouse: the "
                "backend has no storage credential to delete with.",
                namespace,
                name,
                len(victim_ids),
            )
            return
        victims: set[str] = set()
        for snapshot_id in victim_ids:
            victims |= self._files_of(location_before, snapshot_id, live_only=True)
        kept: set[str] = set()
        for snapshot in after.metadata.get("snapshots", []):
            kept |= self._files_of(
                after.metadata_location, int(snapshot["snapshot-id"]), live_only=False
            )
        orphans = _inside_table(sorted(victims - kept), after.metadata, namespace, name)
        failed = 0
        from portolan_cli import api

        for url in orphans:
            try:
                api.delete_file(
                    url,
                    profile=None,
                    s3_endpoint=storage.endpoint,
                    s3_region=storage.region,
                    s3_use_ssl=storage.use_ssl,
                )
            except Exception:
                failed += 1
                logger.warning("Could not delete the orphaned file %s", url, exc_info=True)
        logger.info(
            "Prune of %s.%s deleted %d orphaned file(s), %d failed",
            namespace,
            name,
            len(orphans) - failed,
            failed,
        )

    # ------------------------------------------------------------------ STAC

    def write_stac_fields(
        self, collection: str, collection_dir: Path, document: Any = None
    ) -> None:
        """Merge the table's ``table:*`` and ``iceberg:*`` fields into ``collection.json``.

        Args:
            collection: The collection id.
            collection_dir: The directory that holds ``collection.json``.
            document: The in-memory ``pystac.Collection`` of the add, when
                the caller has one, so it carries the same fields.
        """
        from portolan_cli.backends.iceberg.stac_generator import (
            STAC_ICEBERG_EXTENSION,
            STAC_TABLE_EXTENSION,
            generate_collection_metadata,
        )

        namespace, name = self._table(collection)
        loaded = self._catalog.load_table(namespace, name)
        row = self._session.fetch_one(f"SELECT count(*) FROM {self._session.table(name)}")  # noqa: S608
        row_count = int(row[0]) if row else 0
        fields = generate_collection_metadata(
            loaded,
            settings=self._settings,
            collection=name,
            prefix=self._catalog.prefix,
            row_count=row_count,
        )
        # The table's current metadata.json, under its own key (#883). The
        # role is "metadata" alone, because the GeoParquet file keeps the data
        # role. The extension says to add the asset only when a reader can
        # fetch the document, which PTL-AST-002 limits to a relative or https
        # href, so an s3:// warehouse carries the connection fields alone.
        assets: dict[str, dict[str, Any]] = {}
        location = fields.get("iceberg:metadata_location")
        if location and _is_fetchable(str(location)):
            assets["iceberg"] = {
                "href": str(location),
                "type": "application/vnd.apache.iceberg+json",
                "roles": ["metadata"],
                "description": "Apache Iceberg table metadata, read with DuckDB iceberg_scan()",
            }
        extensions = (STAC_TABLE_EXTENSION, STAC_ICEBERG_EXTENSION)
        if document is not None:
            import pystac

            for key, value in fields.items():
                document.extra_fields[key] = value
            for url in extensions:
                if url not in document.stac_extensions:
                    document.stac_extensions.append(url)
            for key, asset in assets.items():
                document.assets[key] = pystac.Asset.from_dict(asset)
        _merge_into_collection_json(
            Path(collection_dir) / "collection.json", fields, extensions, assets
        )

    def refresh(self, collection: str, *, remote: str | None = None) -> RefreshResult:
        """Merge the current Iceberg fields into ``collection.json`` again.

        The maintenance of a hosted catalog writes a new metadata file, so
        the ``iceberg:metadata_location`` that ``add`` wrote names an older
        file. The older file still reads the version ``add`` published. This
        method reads the table again and writes the current fields. With a
        remote, it also uploads ``collection.json``.

        Raises:
            FileNotFoundError: The collection has no ``collection.json``, or
                the catalog has no table for it.
        """
        namespace, name = self._table(collection)
        path = self._catalog_root / name / "collection.json"
        if not path.is_file():
            raise FileNotFoundError(f"No collection.json of collection '{name}' at {path}")
        if self._load(namespace, name) is None:
            raise FileNotFoundError(f"No Iceberg table {namespace}.{name} in the catalog")
        previous = json.loads(path.read_text(encoding="utf-8")).get("iceberg:metadata_location")
        self.write_stac_fields(name, path.parent)
        current = json.loads(path.read_text(encoding="utf-8")).get("iceberg:metadata_location")
        if remote is not None:
            from portolan_cli.api import upload_file

            upload_file(source=path, destination=f"{remote.rstrip('/')}/{name}/collection.json")
        return RefreshResult(previous_location=previous, metadata_location=current)

    def on_post_add(self, context: PostAddContext) -> None:
        """After ``add``: write the Iceberg fields, then upload the STAC files to the remote."""
        collection_id: str = context["collection_id"]
        try:
            self.write_stac_fields(collection_id, context["collection_dir"], context["collection"])
        except Exception:
            logger.warning(
                "Could not update STAC extensions for collection %s", collection_id, exc_info=True
            )
        remote: str | None = context.get("remote")
        if remote is None:
            return
        for item in context.get("items", [{"item_id": context["item_id"]}]):
            self._upload_stac_metadata(
                catalog_root=context["catalog_root"],
                collection_id=collection_id,
                item_id=item["item_id"],
                remote=remote,
            )

    def _upload_stac_metadata(
        self, catalog_root: Path, collection_id: str, item_id: str, remote: str
    ) -> None:
        """Upload the STAC JSON files. The rows live in the warehouse, not on the remote."""
        from portolan_cli.api import upload_file

        remote = remote.rstrip("/")
        item_json = catalog_root / collection_id / item_id / f"{item_id}.json"
        if item_json.exists():
            upload_file(
                source=item_json, destination=f"{remote}/{collection_id}/{item_id}/{item_id}.json"
            )
        collection_json = catalog_root / collection_id / "collection.json"
        if collection_json.exists():
            upload_file(
                source=collection_json, destination=f"{remote}/{collection_id}/collection.json"
            )
        catalog_json = catalog_root / "catalog.json"
        if catalog_json.exists():
            upload_file(source=catalog_json, destination=f"{remote}/catalog.json")

    # ------------------------------------------------------------------ pull and push

    def pull(
        self,
        remote_url: str,
        local_root: Path,
        collection: str,
        *,
        dry_run: bool = False,
    ) -> PullResult:
        """Download the assets of the current version from ``{remote_url}/{href}``."""
        from portolan_cli.api import PullResult, detail, download_file, error, info, success

        remote_url = remote_url.rstrip("/")
        try:
            with span("iceberg.load_table", collection=collection):
                version = self.get_current_version(collection)
        except FileNotFoundError:
            info(f"No versions found for collection '{collection}'")
            return PullResult(
                success=True,
                files_downloaded=0,
                files_skipped=0,
                local_version=None,
                remote_version=None,
                up_to_date=True,
            )
        if dry_run:
            info(f"[DRY RUN] Would pull {len(version.assets)} file(s) from {remote_url}")
            for asset_key in version.assets:
                detail(f"  {asset_key}")
            return PullResult(
                success=True,
                files_downloaded=len(version.assets),
                files_skipped=0,
                local_version=None,
                remote_version=version.version,
                dry_run=True,
            )
        downloaded = 0
        failed = 0
        with span("iceberg.download", assets=len(version.assets)) as stage:
            for asset in version.assets.values():
                dest = _local_asset_path(local_root, asset.href)
                dest.parent.mkdir(parents=True, exist_ok=True)
                with span("iceberg.download_file", href=asset.href):
                    result = download_file(source=f"{remote_url}/{asset.href}", destination=dest)
                if result.success:
                    downloaded += result.files_downloaded
                else:
                    failed += 1
            stage.set(downloaded=downloaded, failed=failed)
        if failed > 0:
            error(f"Failed to download {failed} file(s)")
            return PullResult(
                success=False,
                files_downloaded=downloaded,
                files_skipped=0,
                local_version=None,
                remote_version=version.version,
            )
        success(f"Pulled {downloaded} file(s) (version {version.version})")
        return PullResult(
            success=True,
            files_downloaded=downloaded,
            files_skipped=0,
            local_version=None,
            remote_version=version.version,
        )

    def supports_push(self) -> bool:
        """``add`` already wrote the rows to the catalog; there is nothing to push."""
        return False

    def push_blocked_message(self, remote: str | None) -> str:
        """The message ``push`` prints on this backend."""
        if remote:
            return (
                "Push is not needed with the 'iceberg' backend. "
                "The `add` command already uploads data to the configured remote."
            )
        return (
            "Push is not supported with the 'iceberg' backend. "
            "The iceberg backend manages versions through its catalog."
        )


def _snapshots_to_remove(
    metadata: dict[str, Any], kept: set[str], victims: list[Version]
) -> list[int]:
    """The snapshots prune removes, in the order the metadata lists them.

    The tagged snapshot of each removed version goes. So does every
    unreferenced snapshot older than the oldest kept version: the delete
    snapshot of a truncate, which DuckDB commits before the append, reaches
    the data files of the version before it and would keep them alive. A
    snapshot that any ref points at, a branch or a tag, stays unless it is
    the tag of a removed version. ``main`` always stays.
    """
    referenced = {int(ref["snapshot-id"]) for ref in metadata.get("refs", {}).values()}
    victim_tags = {snapshot_id_of(metadata, v.version) for v in victims}
    kept_ids = {snapshot_id_of(metadata, version) for version in kept}
    timestamps = {
        int(s["snapshot-id"]): int(s["timestamp-ms"]) for s in metadata.get("snapshots", [])
    }
    oldest_kept = min((timestamps[sid] for sid in kept_ids if sid in timestamps), default=None)
    main = current_snapshot_id(metadata)
    removed: list[int] = []
    for snapshot in metadata.get("snapshots", []):
        snapshot_id = int(snapshot["snapshot-id"])
        if snapshot_id == main:
            continue
        if snapshot_id in victim_tags or (
            snapshot_id not in referenced
            and oldest_kept is not None
            and int(snapshot["timestamp-ms"]) < oldest_kept
        ):
            removed.append(snapshot_id)
    return removed


def _check_own_snapshots(
    metadata: dict[str, Any], before: int | None, after: int, expected: int, collection: str
) -> None:
    """Raise when the snapshots since ``before`` are not the ones this publish made.

    DuckDB commits with optimistic concurrency, so a writer that landed
    before the transaction made it fail. A writer that lands between the
    transaction and ``loadTable`` leaves ``main`` on its own snapshot, and
    the tag would name the wrong rows. The chain from ``main`` back to the
    snapshot before the write then holds more snapshots than the
    transaction made.
    """
    parents = {
        int(s["snapshot-id"]): s.get("parent-snapshot-id") for s in metadata.get("snapshots", [])
    }
    steps = 0
    cursor: int | None = after
    while cursor is not None and cursor != before and cursor in parents:
        steps += 1
        parent = parents[cursor]
        cursor = int(parent) if parent is not None else None
    if cursor != before or steps != expected:
        raise CommitConflictError(
            f"Another writer committed to '{collection}' during the publish. Run it again.",
            status=409,
        )


def _split_url(url: str) -> tuple[str, str, list[str]] | None:
    """Return ``(scheme, bucket, key segments)`` of an object URL, or None when odd.

    A segment that is empty, ``.``, ``..``, or a percent-encoded form of a
    dot is odd: a store may collapse it, and the text would then name an
    object outside what the text says.
    """
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    segments = parts.path.strip("/").split("/")
    if (
        not parts.scheme
        or not parts.netloc
        or any(s in ("", ".", "..") or "%2e" in s.lower() for s in segments)
    ):
        return None
    return parts.scheme, parts.netloc, segments


def _inside_table(
    urls: list[str], metadata: dict[str, Any], namespace: str, name: str
) -> list[str]:
    """Keep only the URLs under the ``data/`` directory of the table.

    The file paths come from the manifests the catalog serves, and the
    table location comes from the same document. The backend deletes with
    the user's storage credential, so a damaged or hostile catalog must
    not widen the deletion. The location has to end with
    ``<namespace>/<name>``, the layout every catalog this backend targets
    uses, and each URL has to sit in that bucket under ``<location>/data/``
    with no dot segment. Anything else is skipped and logged.

    The settings name no bucket or prefix, so there is no anchor the
    catalog cannot change. What remains: a catalog that names another
    bucket the credential can reach, with a ``<namespace>/<name>/data/``
    directory in it, can make a prune delete files there. A configured
    warehouse prefix would close that, and is a follow-up with the
    ``config.yaml`` keys.
    """
    location = _split_url(str(metadata.get("location") or ""))
    if location is None or location[2][-2:] != [namespace, name]:
        logger.warning(
            "Prune of %s.%s deleted no file: the table location %r is not <warehouse>/%s/%s",
            namespace,
            name,
            metadata.get("location"),
            namespace,
            name,
        )
        return []
    scheme, bucket, table_key = location
    data_key = [*table_key, "data"]
    inside: list[str] = []
    for url in urls:
        parsed = _split_url(url)
        if (
            parsed is not None
            and parsed[0] == scheme
            and parsed[1] == bucket
            and parsed[2][: len(data_key)] == data_key
            and len(parsed[2]) > len(data_key)
        ):
            inside.append(url)
        else:
            logger.warning(
                "Prune of %s.%s skipped a file outside the table's data directory: %s",
                namespace,
                name,
                url,
            )
    return inside


def _local_asset_path(root: Path, href: str) -> Path:
    """Return ``root / href``, refusing an href that leaves ``root``.

    The href comes from the version record in the catalog. An absolute
    href, a ``..`` segment, or a symlink that leads out of the catalog
    would read or write a file the catalog must not reach.
    """
    candidate = (root / href).resolve()
    if PurePosixPath(href).is_absolute() or ".." in PurePosixPath(href).parts:
        raise ValueError(f"Asset href {href!r} does not stay inside the catalog")
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError(f"Asset href {href!r} does not stay inside the catalog")
    return candidate


def _base_type(duckdb_type: str) -> str:
    """The DuckDB type name without its parameter: ``GEOMETRY('EPSG:4326')`` -> ``GEOMETRY``."""
    return duckdb_type.split("(", 1)[0].strip().upper()


def _current_fields(metadata: dict[str, Any]) -> dict[str, int]:
    """Column name -> field id of the current schema."""
    current_id = metadata.get("current-schema-id")
    for schema in metadata.get("schemas", []):
        if schema.get("schema-id") == current_id:
            return {str(f["name"]): int(f["id"]) for f in schema.get("fields", [])}
    return {}


def _snapshot_time(metadata: dict[str, Any], snapshot_id: int) -> Any:
    from datetime import UTC, datetime

    for snapshot in metadata.get("snapshots", []):
        if int(snapshot["snapshot-id"]) == snapshot_id:
            return datetime.fromtimestamp(int(snapshot["timestamp-ms"]) / 1000, tz=UTC)
    return None
