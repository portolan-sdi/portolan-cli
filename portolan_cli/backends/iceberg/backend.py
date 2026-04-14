"""IcebergBackend: enterprise versioning backend using Apache Iceberg.

Implements the VersioningBackend protocol from portolan-cli, storing actual
data in Iceberg tables and version metadata in snapshot summary properties.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Transaction
from pyiceberg.table.update.snapshot import ExpireSnapshots

from portolan_cli.backends.iceberg.config import create_catalog
from portolan_cli.backends.iceberg.spatial import add_spatial_columns, detect_geohash_precision
from portolan_cli.backends.iceberg.versioning import (
    build_assets,
    compute_next_version,
    snapshot_to_version,
    version_to_snapshot_properties,
)
from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint
from portolan_cli.versions import Asset, SchemaInfo, Version

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

NAMESPACE = "portolake"


class IcebergBackend:
    """Enterprise versioning backend using Apache Iceberg.

    Implements the VersioningBackend protocol from portolan-cli.
    Discovered via get_backend("iceberg") when the [iceberg] extra is installed.

    Data is stored natively in Iceberg tables (copy-on-write). Version
    metadata is stored in snapshot summary properties.
    """

    def __init__(self, catalog: Catalog | None = None, catalog_root: Path | None = None) -> None:
        self._catalog: Catalog = catalog if catalog is not None else create_catalog(catalog_root)
        try:
            self._catalog.create_namespace(NAMESPACE)
        except Exception:
            pass  # Already exists

    def _validate_collection(self, collection: str) -> str:
        """Validate and sanitize collection name."""
        if not collection or not collection.strip():
            raise ValueError("Collection name cannot be empty")
        if ".." in collection or "/" in collection or "\\" in collection:
            raise ValueError(f"Invalid collection name: {collection!r}")
        safe = Path(collection).name
        if safe in ("", ".", ".."):
            raise ValueError(f"Invalid collection name: {collection!r}")
        return safe

    def _table_id(self, collection: str) -> str:
        return f"{NAMESPACE}.{self._validate_collection(collection)}"

    def _load_or_create_table(
        self, table_id: str, arrow_schema: pa.Schema, row_count: int = 0
    ) -> Table:
        """Load an existing table or create one with the given schema.

        For new tables with geometry, sets up Iceberg partition spec on the
        geohash column if the dataset is large enough (>=100K rows).
        """
        try:
            table = self._catalog.load_table(table_id)
            # Schema evolution: add new columns if needed
            with table.update_schema() as update:
                update.union_by_name(table.schema().as_arrow())
                update.union_by_name(arrow_schema)
            return self._catalog.load_table(table_id)
        except NoSuchTableError:
            table = self._catalog.create_table(table_id, schema=arrow_schema)
            self._apply_partition_spec(table, arrow_schema, row_count)
            return self._catalog.load_table(table_id)

    @staticmethod
    def _apply_partition_spec(table: Table, arrow_schema: pa.Schema, row_count: int) -> None:
        """Add Iceberg partition spec on geohash column if dataset is large enough."""
        precision = detect_geohash_precision(row_count)
        if precision is None:
            return

        geohash_col = f"geohash_{precision}"
        field_names = [f.name for f in arrow_schema]
        if geohash_col not in field_names:
            return

        with table.update_spec() as update:
            update.add_identity(geohash_col)

    def _get_current_version_str(self, table: Table) -> str | None:
        """Extract portolake.version from the current snapshot, if any."""
        snap = table.current_snapshot()
        if snap is None or snap.summary is None:
            return None
        return snap.summary.additional_properties.get("portolake.version")

    def get_current_version(self, collection: str) -> Version:
        """Get the current (latest) version of a collection."""
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError as exc:
            raise FileNotFoundError(f"No versions found for collection: {collection}") from exc
        snap = table.current_snapshot()
        if snap is None:
            raise FileNotFoundError(f"No versions found for collection: {collection}")
        return snapshot_to_version(snap)

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection, oldest first."""
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError:
            return []
        versions = []
        for snap in table.snapshots():
            if snap.summary and "portolake.version" in snap.summary.additional_properties:
                versions.append(snapshot_to_version(snap))
        return sorted(versions, key=lambda v: v.created)

    def publish(
        self,
        collection: str,
        assets: dict[str, str],
        schema: SchemaFingerprint,
        breaking: bool,
        message: str,
        removed: set[str] | None = None,
    ) -> Version:
        """Publish a new version of a collection.

        Reads actual Parquet data from asset files and writes it into the
        Iceberg table. Version metadata is stored in snapshot properties.
        """
        table_id = self._table_id(collection)

        current_version = self._get_current_version_str_safe(table_id)
        next_ver = compute_next_version(current_version, breaking)

        # Build asset metadata (sha256, size, href)
        new_asset_objects, changes = build_assets(assets, collection=collection)

        # Merge with previous snapshot's assets if we have history
        merged_assets = self._get_merged_assets(table_id, new_asset_objects, removed)

        schema_info = SchemaInfo(
            type=schema.get("hash", "unknown"),
            fingerprint={
                "columns": schema.get("columns", []),
                "types": schema.get("types", {}),
            },
        )

        props = version_to_snapshot_properties(
            next_ver, breaking, message, merged_assets, schema_info, changes
        )

        # Read actual Parquet data from asset files
        arrow_data = _read_parquet_assets(assets)

        if arrow_data is not None:
            table = self._load_or_create_table(
                table_id, arrow_data.schema, row_count=len(arrow_data)
            )
            table.append(arrow_data, snapshot_properties=props)
        else:
            # No parquet data to ingest (e.g., only removals)
            table = self._load_or_create_table_from_existing(table_id)
            table.append(_empty_table(table.schema().as_arrow()), snapshot_properties=props)

        # Reload to get the committed snapshot
        table = self._catalog.load_table(table_id)
        snap = table.current_snapshot()
        if snap is None:
            raise RuntimeError(f"No snapshot found after publish for: {table_id}")
        return snapshot_to_version(snap)

    def _get_current_version_str_safe(self, table_id: str) -> str | None:
        """Get current version string, returning None if table doesn't exist."""
        try:
            table = self._catalog.load_table(table_id)
            return self._get_current_version_str(table)
        except NoSuchTableError:
            return None

    def _get_merged_assets(
        self,
        table_id: str,
        new_assets: dict[str, Asset],
        removed: set[str] | None,
    ) -> dict[str, Asset]:
        """Merge new assets with previous version's assets."""
        merged: dict[str, Asset] = {}
        try:
            table = self._catalog.load_table(table_id)
            snap = table.current_snapshot()
            if snap is not None and snap.summary is not None:
                prev_version = snapshot_to_version(snap)
                merged.update(prev_version.assets)
        except NoSuchTableError:
            pass

        merged.update(new_assets)

        if removed:
            for key in removed:
                merged.pop(key, None)

        return merged

    def _load_or_create_table_from_existing(self, table_id: str) -> Table:
        """Load an existing table (for operations that don't have new data)."""
        return self._catalog.load_table(table_id)

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version.

        Uses Iceberg's native snapshot management to set the current snapshot
        pointer back to the target version. No data is copied — this is instant.
        """
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError as exc:
            raise FileNotFoundError(f"No versions found for collection: {collection}") from exc

        # Find the snapshot matching target_version
        target_snap = None
        for snap in table.snapshots():
            if (
                snap.summary
                and snap.summary.additional_properties.get("portolake.version") == target_version
            ):
                target_snap = snap
                break

        if target_snap is None:
            raise ValueError(f"Version {target_version} not found in collection: {collection}")

        table.manage_snapshots().set_current_snapshot(target_snap.snapshot_id).commit()

        table = self._catalog.load_table(table_id)
        current = table.current_snapshot()
        if current is None:
            raise RuntimeError(f"No snapshot found after rollback for: {table_id}")
        return snapshot_to_version(current)

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the N most recent."""
        table_id = self._table_id(collection)
        try:
            table = self._catalog.load_table(table_id)
        except NoSuchTableError:
            return []

        versioned_snapshots = []
        for snap in table.snapshots():
            if snap.summary and "portolake.version" in snap.summary.additional_properties:
                versioned_snapshots.append(snap)
        versioned_snapshots.sort(key=lambda s: s.timestamp_ms)

        if len(versioned_snapshots) <= keep:
            return []

        to_prune = versioned_snapshots[: len(versioned_snapshots) - keep]
        pruned_versions = [snapshot_to_version(s) for s in to_prune]

        if not dry_run:
            snapshot_ids = [s.snapshot_id for s in to_prune]
            expire = ExpireSnapshots(Transaction(table, autocommit=True))
            expire.by_ids(snapshot_ids).commit()

        return pruned_versions

    def get_stac_metadata(self, collection: str) -> dict[str, Any]:
        """Generate combined STAC metadata for a collection.

        Returns a dict with table:* (Layer 1) and iceberg:* (Layer 2) fields.
        NOT part of the VersioningBackend protocol — Iceberg-specific extension.
        """
        from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

        table_id = self._table_id(collection)
        table = self._catalog.load_table(table_id)
        return generate_collection_metadata(table)

    def check_drift(self, collection: str) -> DriftReport:
        """Check for drift between local and remote state."""
        table_id = self._table_id(collection)
        current = None
        try:
            table = self._catalog.load_table(table_id)
            current = self._get_current_version_str(table)
        except NoSuchTableError:
            pass

        return DriftReport(
            has_drift=False,
            local_version=current,
            remote_version=current,
            message="Drift detection pending sync implementation",
        )

    # ------------------------------------------------------------------
    # Optional lifecycle hooks (not part of VersioningBackend protocol)
    # ------------------------------------------------------------------

    def on_post_add(self, context: dict[str, Any]) -> None:
        """Post-add hook: update STAC extensions and upload metadata to remote.

        Called by portolan-cli's finalize_datasets() after versioning completes.
        Receives batch context with all items in the collection.
        """
        import logging

        import pystac

        logger = logging.getLogger(__name__)

        collection_id: str = context["collection_id"]
        collection_dir = context["collection_dir"]
        collection: pystac.Collection = context["collection"]

        # Iceberg state is the source of truth: reflects actual row count,
        # excludes derived columns (geohash_*, bbox_*), and uses Iceberg schema.
        # Overwrites table:*/iceberg:* fields set by portolan-cli's pipeline,
        # but merges stac_extensions and assets to preserve non-portolake entries.
        try:
            from portolan_cli.backends.iceberg.stac_generator import (
                STAC_ICEBERG_EXTENSION,
                STAC_TABLE_EXTENSION,
                generate_collection_metadata,
            )

            table = self._catalog.load_table(self._table_id(collection_id))
            stac_metadata = generate_collection_metadata(table)

            # table:* and iceberg:* fields go to extra_fields
            for key, value in stac_metadata.items():
                collection.extra_fields[key] = value

            # Merge extensions via pystac attribute — extra_fields["stac_extensions"]
            # is silently ignored by pystac's Collection.to_dict() serialization.
            for ext_url in (STAC_TABLE_EXTENSION, STAC_ICEBERG_EXTENSION):
                if ext_url not in collection.stac_extensions:
                    collection.stac_extensions.append(ext_url)

            # Set Iceberg data asset via pystac API (not extra_fields["assets"],
            # which is also ignored by pystac's serialization).
            collection.assets["data"] = pystac.Asset(
                href=table.location(),
                media_type="application/x-iceberg",
                roles=["data"],
                description=(
                    "Apache Iceberg table \u2014 use PyIceberg, DuckDB "
                    "iceberg_scan(), or Spark to query"
                ),
            )

            collection.normalize_hrefs(str(collection_dir))
            collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
        except Exception:
            logger.debug("Could not update STAC extensions for collection %s", collection_id)

        # Upload STAC metadata to remote (if configured)
        remote: str | None = context.get("remote")
        if remote is None:
            return

        # Support batch context (items list) and single-item backward compat
        items = context.get("items", [{"item_id": context["item_id"]}])
        for item_info in items:
            self._upload_stac_metadata(
                catalog_root=context["catalog_root"],
                collection_id=collection_id,
                item_id=item_info["item_id"],
                remote=remote,
            )

    def _upload_stac_metadata(
        self,
        catalog_root: Path,
        collection_id: str,
        item_id: str,
        remote: str,
    ) -> None:
        """Upload STAC metadata JSON files to remote storage.

        Only uploads STAC metadata (item JSON, collection JSON, catalog.json).
        Data files are NOT uploaded — they live in the Iceberg warehouse.
        """
        from portolan_cli.upload import upload_file

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

    def pull(
        self,
        remote_url: str,
        local_root: Path,
        collection: str,
        *,
        dry_run: bool = False,
    ) -> object:
        """Pull files from remote using Iceberg version info.

        Queries get_current_version() for asset info, then downloads each
        asset from {remote_url}/{href} to {local_root}/{href}.
        """
        from portolan_cli.download import download_file
        from portolan_cli.output import detail, error, info, success
        from portolan_cli.pull import PullResult

        remote_url = remote_url.rstrip("/")

        try:
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
        for _asset_key, asset in version.assets.items():
            source = f"{remote_url}/{asset.href}"
            dest = local_root / Path(asset.href)
            dest.parent.mkdir(parents=True, exist_ok=True)

            result = download_file(source=source, destination=dest)
            if result.success:
                downloaded += result.files_downloaded
            else:
                failed += 1

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
        """Iceberg backend does not support push — add already uploads."""
        return False

    def push_blocked_message(self, remote: str | None) -> str:
        """Return human-readable message explaining why push is blocked."""
        if remote:
            return (
                "Push is not needed with the 'iceberg' backend. "
                "The `add` command already uploads data to the configured remote."
            )
        return (
            "Push is not supported with the 'iceberg' backend. "
            "The iceberg backend manages versions through its catalog."
        )


def _read_parquet_assets(assets: dict[str, str]) -> pa.Table | None:
    """Read Parquet data from asset file paths, concatenate, and add spatial columns."""
    tables = []
    for path_str in assets.values():
        path = Path(path_str)
        if path.exists() and path.suffix == ".parquet":
            try:
                tables.append(pq.read_table(path))
            except Exception:
                pass  # Not a valid parquet file, skip data ingestion

    if not tables:
        return None

    if len(tables) == 1:
        result = tables[0]
    else:
        result = pa.concat_tables(tables, promote_options="default")

    # Add spatial columns (geohash + bbox) if geometry is present
    return add_spatial_columns(result)


def _empty_table(arrow_schema: pa.Schema) -> pa.Table:
    """Create an empty PyArrow table with the given schema."""
    return pa.table(
        {field.name: pa.array([], type=field.type) for field in arrow_schema}, schema=arrow_schema
    )
