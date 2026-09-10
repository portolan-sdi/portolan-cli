"""Thin wrapper around python-geoservercloud."""

from __future__ import annotations

from typing import Any, Protocol


class GeoServerClientProtocol(Protocol):
    """Protocol used by the GeoServer provider."""

    def list_portolan_resources(self, workspace: str) -> dict[str, dict[str, Any]]:
        """Return Portolan-managed resources keyed by collection id."""

    def ensure_workspace(self, workspace: str) -> None:
        """Create the workspace if needed."""

    def publish_geoparquet(
        self,
        workspace: str,
        name: str,
        href: str,
        metadata: dict[str, object],
        primary_key: str | None = None,
        native_name: str | None = None,
    ) -> None:
        """Publish a GeoParquet resource."""

    def publish_cog(
        self, workspace: str, name: str, href: str, metadata: dict[str, object]
    ) -> None:
        """Publish a COG resource."""


class GeoServerClient:
    """Wrap geoservercloud behind Portolan's provider boundary."""

    def __init__(self, url: str, user: str, password: str, *, verifytls: bool = True) -> None:
        try:
            from geoservercloud import GeoServerCloud  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - exercised only without optional dep
            raise RuntimeError(
                "GeoServer support requires the 'geoservercloud' package. "
                "Install portolan-cli with GeoServer dependencies."
            ) from exc

        self._client: Any = GeoServerCloud(
            url=url,
            user=user,
            password=password,
            verifytls=verifytls,
        )

    def list_portolan_resources(self, workspace: str) -> dict[str, dict[str, Any]]:
        """Read Portolan provenance from GeoServer ResourceInfo metadata."""
        managed: dict[str, dict[str, Any]] = {}
        datastores, datastore_status = self._client.get_datastores(workspace)
        if datastore_status < 400 and isinstance(datastores, list):
            for datastore in datastores:
                name = datastore.get("name")
                if isinstance(name, str):
                    self._collect_feature_type(workspace, name, managed)

        coverage_stores, coverage_store_status = self._coverage_stores(workspace)
        if coverage_store_status < 400:
            for store in coverage_stores:
                self._collect_coverage(workspace, store, managed)
        return managed

    def ensure_workspace(self, workspace: str) -> None:
        """Create or update the workspace."""
        self._raise_on_error("create workspace", self._client.create_workspace(workspace))

    def publish_geoparquet(
        self,
        workspace: str,
        name: str,
        href: str,
        metadata: dict[str, object],
        primary_key: str | None = None,
        native_name: str | None = None,
    ) -> None:
        """Create a GeoParquet store and FeatureType with provenance metadata."""
        self._raise_on_error(
            "create GeoParquet datastore",
            self._client.create_datastore(
                workspace_name=workspace,
                datastore_name=name,
                datastore_type="GeoParquet",
                connection_parameters=_geoparquet_connection_parameters(
                    workspace, href, primary_key
                ),
                description=f"Portolan-managed GeoParquet resource {name}",
            ),
        )
        self._raise_on_error(
            "create FeatureType",
            self._client.create_feature_type(
                layer_name=name,
                workspace_name=workspace,
                datastore_name=name,
                title=str(metadata.get("title") or name),
                abstract=str(metadata.get("description") or ""),
                keywords=_string_list(metadata.get("keywords")),
                native_name=native_name or name,
            ),
        )
        self._put_feature_type_metadata(workspace, name, name, metadata)

    def publish_cog(
        self, workspace: str, name: str, href: str, metadata: dict[str, object]
    ) -> None:
        """Create a COG coverage store and Coverage with provenance metadata."""
        range_reader = "HTTP" if href.startswith(("http://", "https://")) else "FILE"
        self._raise_on_error(
            "create COG coverage store",
            self._client.create_coverage_store(
                workspace_name=workspace,
                coveragestore_name=name,
                url=f"cog://{href}",
                type="GeoTIFF",
                metadata={"cogSettings": {"rangeReaderSettings": range_reader}},
            ),
        )
        self._raise_on_error(
            "create Coverage",
            self._client.create_coverage(
                workspace_name=workspace,
                coveragestore_name=name,
                coverage_name=name,
                title=str(metadata.get("title") or name),
            ),
        )
        self._put_coverage_metadata(workspace, name, name, metadata)

    def _collect_feature_type(
        self, workspace: str, datastore: str, managed: dict[str, dict[str, Any]]
    ) -> None:
        rest_service = self._client.rest_service
        path = rest_service.rest_endpoints.featuretype(workspace, datastore, datastore)
        try:
            response = rest_service.rest_client.get(path)
        except Exception:
            return
        if response.status_code >= 400:
            return
        payload = response.json()
        feature_type = payload.get("featureType", {})
        if not isinstance(feature_type, dict):
            return
        metadata = _metadata_entries(feature_type.get("metadata"))
        collection = metadata.get("portolan.collection")
        if _is_managed(metadata.get("portolan.managed")) and isinstance(collection, str):
            managed[collection] = metadata

    def _collect_coverage(
        self, workspace: str, coverage_store: str, managed: dict[str, dict[str, Any]]
    ) -> None:
        try:
            coverage, status = self._client.get_coverage(workspace, coverage_store, coverage_store)
        except Exception:
            return
        if status >= 400 or not isinstance(coverage, dict):
            return
        metadata = _metadata_entries(coverage.get("metadata"))
        collection = metadata.get("portolan.collection")
        if _is_managed(metadata.get("portolan.managed")) and isinstance(collection, str):
            managed[collection] = metadata

    def _coverage_stores(self, workspace: str) -> tuple[list[str], int]:
        rest_service = self._client.rest_service
        endpoints = rest_service.rest_endpoints
        rest_client = rest_service.rest_client
        response = rest_client.get(endpoints.coveragestores(workspace))
        if response.status_code >= 400:
            return [], response.status_code
        payload = response.json()
        return coverage_store_names(payload), response.status_code

    def _put_feature_type_metadata(
        self, workspace: str, datastore: str, name: str, metadata: dict[str, object]
    ) -> None:
        payload = {"featureType": {"name": name, "metadata": _metadata_payload(metadata)}}
        rest_service = self._client.rest_service
        path = rest_service.rest_endpoints.featuretype(workspace, datastore, name)
        response = rest_service.rest_client.put(path, json=payload)
        self._raise_on_error("write FeatureType provenance", ("", response.status_code))

    def _put_coverage_metadata(
        self, workspace: str, coverage_store: str, name: str, metadata: dict[str, object]
    ) -> None:
        payload = {"coverage": {"name": name, "metadata": _metadata_payload(metadata)}}
        rest_service = self._client.rest_service
        path = rest_service.rest_endpoints.coverage(workspace, coverage_store, name)
        response = rest_service.rest_client.put(path, json=payload)
        self._raise_on_error("write Coverage provenance", ("", response.status_code))

    def _raise_on_error(self, operation: str, result: tuple[Any, int]) -> None:
        content, status = result
        if status >= 400:
            raise RuntimeError(f"{operation} failed with HTTP {status}: {content}")


def _metadata_payload(metadata: dict[str, object]) -> dict[str, object]:
    return {
        "entry": [
            {"@key": key, "$": _metadata_value(value)}
            for key, value in metadata.items()
            if value is not None
        ]
    }


def _geoparquet_connection_parameters(
    workspace: str, href: str, primary_key: str | None
) -> dict[str, object]:
    parameters: dict[str, object] = {
        "dbtype": "geoparquet",
        "uri": href,
        "namespace": f"http://{workspace}",
    }
    if primary_key is not None:
        parameters["primary_key_id"] = primary_key
    return parameters


def _metadata_value(value: object) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return value
    return str(value)


def coverage_store_names(payload: object) -> list[str]:
    """Return coverage store names from GeoServer REST payload variants."""
    if not isinstance(payload, dict):
        return []
    coverage_stores = payload.get("coverageStores", {})
    if not isinstance(coverage_stores, dict):
        return []
    stores = coverage_stores.get("coverageStore", [])
    if isinstance(stores, dict):
        stores = [stores]
    if not isinstance(stores, list):
        return []
    return [
        store["name"]
        for store in stores
        if isinstance(store, dict) and isinstance(store.get("name"), str)
    ]


def _metadata_entries(metadata: object) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    entries = metadata.get("entry", [])
    if isinstance(entries, dict):
        return dict(entries)
    result: dict[str, Any] = {}
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, dict) and isinstance(entry.get("@key"), str):
                result[entry["@key"]] = entry.get("$")
    return result


def _string_list(value: object) -> list[str] | None:
    if isinstance(value, list):
        return [str(item) for item in value]
    return None


def _is_managed(value: object) -> bool:
    return value is True or (isinstance(value, str) and value.lower() == "true")
