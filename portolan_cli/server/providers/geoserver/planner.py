"""GeoServer provider plan and publish orchestration."""

from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path

from portolan_cli.server.model import (
    PlanAction,
    PlanEntry,
    PublishPlan,
    PublishResult,
    ResourceFormat,
    ServerCatalog,
    ServerResourceSpec,
)
from portolan_cli.server.planner import load_server_catalog
from portolan_cli.server.providers.geoserver.client import GeoServerClientProtocol


class GeoServerProvider:
    """GeoServer implementation of Portolan's server provider protocol."""

    def __init__(self, client: GeoServerClientProtocol, workspace: str | None = None) -> None:
        self._client = client
        self._workspace = workspace

    def plan(self, catalog_root: Path) -> PublishPlan:
        """Compute a read-only GeoServer publication plan."""
        catalog = load_server_catalog(catalog_root)
        return self.plan_catalog(catalog)

    def plan_catalog(self, catalog: ServerCatalog) -> PublishPlan:
        """Compute a read-only GeoServer publication plan for a loaded catalog."""
        workspace = self._workspace or catalog.catalog_id
        existing = self._client.list_portolan_resources(workspace)
        entries: list[PlanEntry] = []
        for candidate in catalog.candidates:
            if candidate.resource is None:
                entries.append(
                    PlanEntry(
                        collection=candidate.collection,
                        format=None,
                        action=PlanAction.SKIP,
                        reason=candidate.reason,
                    )
                )
                continue
            if candidate.resource.format not in {
                ResourceFormat.GEOPARQUET,
                ResourceFormat.COG,
            }:
                entries.append(
                    PlanEntry(
                        collection=candidate.collection,
                        format=candidate.resource.format,
                        action=PlanAction.SKIP,
                        reason="unsupported by GeoServer provider",
                    )
                )
                continue
            action = _action_for_existing(candidate.resource, existing.get(candidate.collection))
            entries.append(
                PlanEntry(
                    collection=candidate.collection,
                    format=candidate.resource.format,
                    action=action,
                )
            )
        return PublishPlan(workspace=workspace, entries=entries)

    def publish(self, catalog_root: Path) -> PublishResult:
        """Publish GeoParquet and COG resources to GeoServer."""
        catalog = load_server_catalog(catalog_root)
        return self.publish_catalog(catalog)

    def publish_catalog(self, catalog: ServerCatalog) -> PublishResult:
        """Publish a loaded catalog to GeoServer."""
        workspace = self._workspace or catalog.catalog_id
        self._client.ensure_workspace(workspace)
        existing = self._client.list_portolan_resources(workspace)
        published = 0
        skipped = 0
        errors: list[str] = []
        for candidate in catalog.candidates:
            if candidate.resource is None:
                skipped += 1
                continue
            if (
                _action_for_existing(candidate.resource, existing.get(candidate.collection))
                == PlanAction.EXISTS
            ):
                skipped += 1
                continue
            resource = _with_provenance(candidate.resource, catalog.catalog_href)
            server_name = geoserver_resource_name(resource.id)
            resource = _with_geoserver_name(resource, server_name)
            try:
                if resource.format == ResourceFormat.GEOPARQUET:
                    self._client.publish_geoparquet(
                        workspace,
                        server_name,
                        resource.href,
                        resource.metadata,
                        resource.primary_key,
                        resource.native_name,
                    )
                    published += 1
                elif resource.format == ResourceFormat.COG:
                    self._client.publish_cog(
                        workspace, server_name, resource.href, resource.metadata
                    )
                    published += 1
                else:
                    skipped += 1
            except Exception as exc:
                errors.append(f"{candidate.collection}: {exc}")
        return PublishResult(
            workspace=workspace,
            published=published,
            skipped=skipped,
            errors=errors,
        )

    def sync(self, catalog_root: Path) -> PublishResult:
        """Initial sync behavior matches idempotent publish without pruning."""
        return self.publish(catalog_root)


def geoserver_resource_name(collection_id: str) -> str:
    """Return a GeoServer-safe resource name for a STAC collection id."""
    name = collection_id.replace("/", "__")
    name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._-")
    return name or "collection"


def _with_geoserver_name(resource: ServerResourceSpec, server_name: str) -> ServerResourceSpec:
    metadata = dict(resource.metadata)
    metadata["portolan.geoserver_name"] = server_name
    return replace(resource, metadata=metadata)


def _with_provenance(resource: ServerResourceSpec, catalog_href: str) -> ServerResourceSpec:
    metadata = dict(resource.metadata)
    metadata["title"] = resource.title
    metadata["description"] = resource.description
    metadata["portolan.managed"] = True
    metadata["portolan.catalog"] = catalog_href
    metadata["portolan.collection"] = resource.source_collection
    metadata["portolan.asset"] = resource.href
    metadata["portolan.primary_key"] = resource.primary_key
    metadata["portolan.native_name"] = resource.native_name
    checksum = metadata.get("file:checksum") or metadata.get("checksum:multihash")
    if checksum is not None:
        metadata["portolan.checksum"] = checksum
    return replace(resource, metadata=metadata)


def _action_for_existing(
    resource: ServerResourceSpec, existing_metadata: dict[str, object] | None
) -> PlanAction:
    if existing_metadata is None:
        return PlanAction.CREATE
    if existing_metadata.get("portolan.asset") != resource.href:
        return PlanAction.UPDATE
    if (
        resource.primary_key is not None
        and existing_metadata.get("portolan.primary_key") != resource.primary_key
    ):
        return PlanAction.UPDATE
    checksum = resource.metadata.get("file:checksum") or resource.metadata.get("checksum:multihash")
    if checksum is not None and existing_metadata.get("portolan.checksum") != checksum:
        return PlanAction.UPDATE
    return PlanAction.EXISTS
