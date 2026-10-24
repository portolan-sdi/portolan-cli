"""ArcGIS extraction through the shared LayerSource driver.

``ArcGISLayerSource`` implements ``LayerSource`` for ArcGIS FeatureServer and
MapServer services, and ``extract_arcgis_catalog`` runs it with
``run_layer_extraction``. ImageServer rasters take their own path in
``imageserver/``.

- ``discover`` handles a single service, a layer URL, or a services root.
  A layer URL such as ``.../MapServer/0`` selects that one layer and marks
  it ``explicit`` (issue #652). For a root it walks the folders, applies the
  service filters, and lists each service's layers in service order (issue
  #1012). A single-layer service becomes one collection at the service
  directory. A multi-layer service nests one collection per layer. Tables
  join the layers only with ``include_tables`` (issue #812).
- ``fetch`` calls ``gpio.extract_arcgis``, applies the catalog's
  ``conversion.vector`` layout (issues #805, #834), writes, and restores the
  source CRS (issue #802). A failure names the layer (issue #866).
- ``styles`` converts the layer's ESRI drawingInfo (issue #490).

Typical usage:
    from portolan_cli.extract.arcgis.orchestrator import extract_arcgis_catalog

    result = extract_arcgis_catalog(
        url="https://services.arcgis.com/.../FeatureServer",
        output_dir=Path("./output"),
        layer_filter=["Census*"],
    )
    print(f"Extracted {result.summary.succeeded}/{result.summary.total_layers} layers")
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.conversion_config import VectorSettings, get_vector_settings
from portolan_cli.convert import apply_vector_settings
from portolan_cli.extract.arcgis.discovery import (
    ArcGISClient,
    ArcGISDiscoveryError,
    FolderTraversal,
    LayerInfo,
    ServiceDiscoveryResult,
    ServiceInfo,
    discover_layers,
    discover_services,
    discover_services_recursive,
)
from portolan_cli.extract.arcgis.metadata import extract_arcgis_metadata
from portolan_cli.extract.arcgis.url_parser import ArcGISURLType, parse_arcgis_url
from portolan_cli.extract.common.report import FolderCoverage, MetadataExtracted
from portolan_cli.extract.common.source import (
    Discovery,
    LayerMetadata,
    LayerRef,
    LayerSourceOptions,
    SourceLink,
    run_layer_extraction,
    slugify,
)
from portolan_cli.extract.common.styles import extract_esri_style
from portolan_cli.licensing import license_url_from_text

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from portolan_cli.extract.arcgis.metadata import ArcGISMetadata
    from portolan_cli.extract.common.progress import ExtractionProgress
    from portolan_cli.extract.common.report import ExtractionReport

logger = logging.getLogger(__name__)


def _coverage_from_traversal(traversal: FolderTraversal) -> FolderCoverage:
    """Map a discovery FolderTraversal to a serializable FolderCoverage."""
    return FolderCoverage(
        folders_visited=traversal.visited,
        folders_skipped=traversal.skipped,
        services_found=traversal.service_count,
    )


@dataclass
class ServicesRootDiscoveryResult:
    """Result of listing services from a services root URL.

    Used for --list-services mode and JSON output.

    Attributes:
        services: List of discovered services.
        folders: List of folder names in the services root.
        base_url: The services root URL that was queried.
        coverage: Optional folder traversal coverage when recursion was used.
    """

    services: list[ServiceInfo]
    folders: list[str]
    base_url: str
    coverage: FolderCoverage | None = None

    def to_dict(self) -> dict[str, object]:
        """Convert to JSON-serializable dict."""
        result: dict[str, object] = {
            "base_url": self.base_url,
            "services": [
                {
                    "name": s.name,
                    "type": s.service_type,
                    "url": s.get_url(self.base_url),
                }
                for s in self.services
            ],
            "folders": self.folders,
            "total_services": len(self.services),
        }
        if self.coverage is not None:
            result["folder_coverage"] = self.coverage.to_dict()
        return result


def list_services(
    url: str,
    *,
    service_types: Sequence[str] | None = None,
    service_filter: list[str] | None = None,
    token: str | None = None,
    recurse: bool = True,
    timeout: float = 60.0,
) -> ServicesRootDiscoveryResult:
    """List services from an ArcGIS services root or folder URL.

    Recurses into folders by default. Folders that error are skipped and
    recorded in the returned coverage.

    Args:
        url: ArcGIS services root or folder URL.
        service_types: Filter by service types (e.g., ["FeatureServer"]).
        service_filter: Glob patterns to filter service names.
        token: Optional ArcGIS token for authenticated endpoints.
        recurse: Whether to recurse into sub-folders (default True).
        timeout: Request timeout in seconds.

    Returns:
        ServicesRootDiscoveryResult with services, folders, and optional coverage.

    Raises:
        ValueError: If URL is not a services root or folder URL.
    """
    from portolan_cli.extract.common.filters import filter_services

    parsed = parse_arcgis_url(url)
    if parsed.url_type not in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
        msg = f"URL is not a services root or folder URL: {url}"
        raise ValueError(msg)

    client = ArcGISClient(timeout=timeout, token=token)
    types = list(service_types) if service_types else None
    if recurse:
        services, traversal = discover_services_recursive(url, client=client, service_types=types)
        coverage: FolderCoverage | None = _coverage_from_traversal(traversal)
        folders = traversal.visited
    else:
        services, folders = discover_services(
            url, client=client, service_types=types, return_folders=True
        )
        coverage = None

    if service_filter:
        service_names = [s.name for s in services]
        filtered_names = filter_services(
            service_names,
            include=service_filter,
            case_sensitive=False,
        )
        services = [s for s in services if s.name in filtered_names]

    return ServicesRootDiscoveryResult(
        services=services,
        folders=folders,
        base_url=parsed.base_url,
        coverage=coverage,
    )


def _service_output_dir(output_dir: Path, service_name: str) -> Path:
    """Map a (possibly folder-qualified) service name to a nested directory.

    "ecml/active_faults" -> output_dir/ecml/active_faults
    "Top" -> output_dir/top
    Each path segment is slugified independently so the folder hierarchy is
    preserved as nested subcatalogs.

    Slugification can collide when two service names differ only in characters
    it strips, ``turkiye`` and ``turkiye-alt`` for example. The second extraction
    writes into the first one's directory. This is a known limitation and is not
    mitigated; no uniquifying suffix is appended.
    """
    parts = [slugify(p) for p in service_name.split("/") if p]
    result = output_dir
    for part in parts:
        result = result / part
    return result


@dataclass
class ExtractionOptions:
    """Options for the extraction process.

    Attributes:
        workers: Number of parallel page requests per layer (gpio max_workers)
        retries: Attempts per layer, and per discovery request that fails
            with a transient error (issue #811)
        timeout: Per-request timeout in seconds, for layer discovery and for
            the feature-page requests gpio makes (issue #898)
        resume: Whether to resume from existing extraction report
        dry_run: If True, list layers without extracting
        vector: The GeoParquet layout to write. ``extract_arcgis_catalog``
            reads it from the catalog's ``conversion.vector`` block, except
            with ``raw``, which runs without a catalog (issue #834).
        raw: If True, skip auto-init (only create extraction files, no STAC catalog)
        no_styles: If True, skip style extraction from ESRI drawingInfo
        token: Optional ArcGIS token for authenticated endpoints
        recurse: Whether to recurse into sub-folders during discovery (default True)
        license: SPDX identifier for the harvested data, or "other" with
            license_url. Overrides any license URL found in the source's own
            license text (issue #686).
        license_url: URL of the license text.
        output_crs: CRS for the extracted GeoParquet. "native" keeps the
            service's source CRS (default). An EPSG code (e.g. "EPSG:4326")
            reprojects the coordinates to that CRS (issue #802).
        catalog_id: Catalog id for the created catalog. None derives it from
            the output directory name, which is the behavior before issue #821.
        include_tables: Extract the tables of a service too, as Parquet with
            no geometry (issue #812).
    """

    workers: int = 3
    retries: int = 3
    timeout: float = 60.0
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    vector: VectorSettings = field(default_factory=VectorSettings)
    no_styles: bool = False
    token: str | None = None
    recurse: bool = True
    license: str | None = None
    license_url: str | None = None
    output_crs: str = "native"
    catalog_id: str | None = None
    include_tables: bool = False

    def client(self) -> ArcGISClient:
        """Return the discovery client with this run's token, timeout, and retries."""
        return ArcGISClient(timeout=self.timeout, token=self.token, retries=self.retries)


def _is_wgs84_crs(crs: dict[str, object]) -> bool:
    """Return True when the PROJJSON CRS is WGS84 (EPSG:4326 or OGC:CRS84).

    A null CRS in GeoParquet already means CRS84, so a WGS84 output needs no
    explicit CRS tag (issue #802). gpio tags a native-4326 GeoJSON extraction as
    OGC:CRS84, whose ``to_epsg()`` is None, so this matches CRS84 as well.
    """
    from pyproj import CRS
    from pyproj.exceptions import CRSError

    try:
        parsed = CRS.from_json_dict(crs)
    except (CRSError, KeyError, TypeError, ValueError):
        # An unparsable CRS is not WGS84, so tag it rather than assume CRS84.
        return False
    return parsed.to_epsg() == 4326 or parsed == CRS("OGC:CRS84")


def _ensure_geoparquet_crs(output_path: Path, source_crs: dict[str, object] | None) -> None:
    """Write the source CRS into the GeoParquet metadata when the writer drops it.

    gpio.Table.write() emits ``crs: null`` even for a native extraction, so a file
    with projected coordinates would claim WGS84 (issue #802). This restamps the
    primary geometry column with ``source_crs`` and preserves the row groups.

    Args:
        output_path: The written GeoParquet file.
        source_crs: PROJJSON CRS the extractor detected, or None when unknown.
    """
    if source_crs is None or _is_wgs84_crs(source_crs):
        # No CRS to preserve, or WGS84 where the null tag is already correct.
        return

    import json

    import pyarrow.parquet as pq

    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with pq.ParquetFile(output_path) as parquet_file:
        schema = parquet_file.schema_arrow
        metadata = schema.metadata or {}
        geo_bytes = metadata.get(b"geo")
        if geo_bytes is None:
            return

        geo = json.loads(geo_bytes)
        primary = geo.get("primary_column")
        column = geo.get("columns", {}).get(primary)
        if column is None or column.get("crs") == source_crs:
            # Already tagged with the source CRS; nothing to rewrite.
            return

        column["crs"] = source_crs
        new_schema = schema.with_metadata({**metadata, b"geo": json.dumps(geo).encode("utf-8")})

        # Parquet keeps its CRS in the footer, so a rewrite is the only way to
        # change it. Copy the row groups verbatim to keep the covering
        # statistics intact.
        with pq.ParquetWriter(temp_path, new_schema, compression="zstd") as writer:
            for row_group in range(parquet_file.num_row_groups):
                writer.write_table(parquet_file.read_row_group(row_group))

    # Windows refuses to replace a file while a reader still holds it open. The
    # rename runs after the ParquetFile closes.
    temp_path.replace(output_path)


def _report_metadata_to_arcgis_metadata(
    report_metadata: MetadataExtracted,
) -> ArcGISMetadata:
    """Convert report MetadataExtracted to ArcGISMetadata.

    The extraction report stores a flattened version of the metadata.
    This function reconstructs the ArcGISMetadata object for conversion
    to ExtractedMetadata.

    Args:
        report_metadata: MetadataExtracted from the extraction report.

    Returns:
        ArcGISMetadata instance with the same data.
    """
    from portolan_cli.extract.arcgis.metadata import ArcGISMetadata

    return ArcGISMetadata(
        source_url=report_metadata.source_url,
        attribution=report_metadata.attribution,
        description=report_metadata.description,
        processing_notes=report_metadata.processing_notes,
        producer=report_metadata.producer,
        keywords=report_metadata.keywords,
        known_issues=report_metadata.known_issues,
        license_info_raw=report_metadata.license_info_raw,
    )


def _discover_and_filter_services(
    url: str,
    service_filter: list[str] | None,
    service_exclude: list[str] | None,
    *,
    client: ArcGISClient,
    folder: str | None = None,
    recurse: bool = True,
) -> tuple[list[ServiceInfo], FolderCoverage | None]:
    """Discover services (recursively by default), scope to a folder, and apply filters.

    Returns (services, coverage). When recurse is False the flat (non-recursive)
    discovery is used and coverage is None. When folder is set (SERVICES_FOLDER
    URL) only services under that folder prefix are kept.
    """
    from portolan_cli.extract.common.filters import filter_services

    coverage: FolderCoverage | None
    types = ["FeatureServer", "MapServer"]
    if recurse:
        services, traversal = discover_services_recursive(url, client=client, service_types=types)
        coverage = _coverage_from_traversal(traversal)
    else:
        services, _folders = discover_services(
            url, client=client, service_types=types, return_folders=True
        )
        coverage = None

    if folder:
        prefix = f"{folder.rstrip('/')}/"
        services = [s for s in services if s.name.startswith(prefix)]

    if service_filter or service_exclude:
        service_names = [s.name for s in services]
        filtered_names = filter_services(
            service_names,
            include=service_filter,
            exclude=service_exclude,
            case_sensitive=False,
        )
        services = [s for s in services if s.name in filtered_names]

    return services, coverage


def _service_metadata(url: str, found: ServiceDiscoveryResult) -> MetadataExtracted:
    """Read the service metadata that the extraction report records."""
    arcgis_metadata = extract_arcgis_metadata(
        {
            "copyrightText": found.copyright_text,
            "description": found.description,
            "serviceDescription": found.service_description,
            "documentInfo": {"Author": found.author, "Keywords": found.keywords},
            "accessInformation": found.access_information,
            "licenseInfo": found.license_info,
        },
        source_url=url,
    )
    return MetadataExtracted(
        source_url=url,
        description=arcgis_metadata.description,
        attribution=arcgis_metadata.attribution,
        keywords=arcgis_metadata.keywords,
        producer=arcgis_metadata.producer,
        processing_notes=arcgis_metadata.processing_notes,
        known_issues=arcgis_metadata.known_issues,
        license_info_raw=arcgis_metadata.license_info_raw,
    )


class ArcGISLayerError(Exception):
    """A layer fetch failed. The message names the layer and keeps the cause."""


@dataclass(frozen=True)
class ArcGISLayer:
    """What ``fetch`` and the metadata methods need for one ArcGIS layer."""

    info: LayerInfo
    service_url: str

    @property
    def url(self) -> str:
        """The layer's REST URL: service URL plus layer id."""
        return f"{self.service_url.rstrip('/')}/{self.info.id}"


class ArcGISLayerSource:
    """ArcGIS FeatureServer and MapServer layers as a ``LayerSource``."""

    name = "arcgis"
    summary = "ArcGIS FeatureServer, MapServer, and ImageServer services."

    def __init__(
        self,
        options: ExtractionOptions | None = None,
        *,
        service_filter: list[str] | None = None,
        service_exclude: list[str] | None = None,
    ) -> None:
        self.options = options or ExtractionOptions()
        self.service_filter = service_filter
        self.service_exclude = service_exclude

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:  # noqa: ARG002 - LayerSource signature
        """List the layers of a service, of one layer URL, or of every service under a root.

        A token in the URL query works as ``--token``. Each request URL is
        built from the URL without its query, so a pasted ``?f=json`` does
        not end up in front of a layer id.
        """
        from urllib.parse import parse_qsl, urlparse

        query = {name.lower(): value for name, value in parse_qsl(urlparse(source).query)}
        if query.get("token") and not self.options.token:
            self.options = replace(self.options, token=query["token"])
        parsed = parse_arcgis_url(source)
        if parsed.url_type in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
            return self._discover_root(source, parsed.base_url, parsed.folder)
        service_url = parsed.base_url
        found = discover_layers(service_url, self.options.client())
        layers = self._service_refs(found, service_url)
        empty_reason = None
        if parsed.layer_id is not None:
            # A layer URL names one layer of the service (issue #652).
            named = self._service_refs(found, service_url, tables=True)
            layers = [replace(ref, explicit=True) for ref in named if ref.id == parsed.layer_id]
            if not layers:
                listed = ", ".join(f"{ref.id} ({ref.name})" for ref in named) or "none"
                raise ArcGISDiscoveryError(
                    f"The service {service_url} has no layer {parsed.layer_id}. "
                    f"Its layers and tables: {listed}"
                )
        elif found.tables and not self.options.include_tables:
            count = len(found.tables)
            noun = "table" if count == 1 else "tables"
            if layers:
                logger.warning(
                    "The service lists %d %s that are not extracted. "
                    "Pass --include-tables to extract them.",
                    count,
                    noun,
                )
            else:
                empty_reason = (
                    f"The service lists {count} {noun} and no layers. "
                    "Pass --include-tables to extract tables."
                )
        discovery = self.discovery_from(
            service_url,
            _service_metadata(service_url, found),
            layers,
            license_info=found.license_info,
        )
        discovery.empty_reason = empty_reason
        return discovery

    def _service_refs(
        self,
        found: ServiceDiscoveryResult,
        service_url: str,
        *,
        tables: bool | None = None,
        service: ServiceInfo | None = None,
    ) -> list[LayerRef]:
        """Return a service's layers, then its tables, then its skipped layers.

        Args:
            found: The service's discovery result.
            service_url: The service URL that each layer's data keeps.
            tables: Include the tables. None follows ``include_tables``.
            service: The service of a services-root walk. Each layer then
                gets a path under the service directory.
        """
        extractable = [(info, "layer") for info in found.layers]
        if self.options.include_tables if tables is None else tables:
            extractable += [(info, "table") for info in found.tables]
        service_dir = _service_output_dir(Path(), service.name).as_posix() if service else None
        refs: list[LayerRef] = []
        for info, kind in extractable:
            path = None
            if service_dir is not None:
                # A single-layer service is one collection at the service directory.
                # A multi-layer service is a subcatalog with one collection per layer.
                single = len(extractable) == 1
                path = service_dir if single else f"{service_dir}/{slugify(info.name)}"
            refs.append(
                LayerRef(
                    id=info.id,
                    name=info.name,
                    path=path,
                    data=ArcGISLayer(info, service_url),
                    kind="table" if kind == "table" else "layer",
                )
            )
        reason = found.skip_reason
        if service is not None:
            reason = f"{service.name}: {reason}"
        refs += [
            LayerRef(
                id=info.id,
                name=info.name,
                path=f"{service_dir}/{slugify(info.name)}" if service_dir else None,
                data=ArcGISLayer(info, service_url),
                skip_reason=reason,
            )
            for info in found.skipped_layers
        ]
        return refs

    def _discover_root(self, source: str, base_url: str, folder: str | None) -> Discovery:
        """Walk a services root. Each layer keeps the URL of its own service.

        The layers keep the service order, skipped layers included, so the
        ``[i/N]`` progress index follows the services (issue #1012). A
        service that fails discovery is logged and left out.

        A services root spans services with different licenses and the
        catalog carries one metadata.yaml, so no harvested license applies
        (issue #686). The services' shared attribution, when they agree on
        one, names the producer (issue #1039).
        """
        client = self.options.client()
        services, coverage = _discover_and_filter_services(
            source,
            self.service_filter,
            self.service_exclude,
            client=client,
            folder=folder,
            recurse=self.options.recurse,
        )
        layers: list[LayerRef] = []
        errors: list[str] = []
        attributions: set[str] = set()
        for service in services:
            service_url = service.get_url(base_url)
            try:
                found = discover_layers(service_url, client)
            except Exception as err:  # noqa: BLE001 - one service must not stop the walk
                errors.append(f"{service.name}: {err}")
                logger.warning("Failed to discover layers from service '%s': %s", service.name, err)
                continue
            layers += self._service_refs(found, service_url, service=service)
            if found.copyright_text and found.copyright_text.strip():
                attributions.add(found.copyright_text.strip())
        extractable = [ref.data.info for ref in layers if ref.skip_reason is None]
        # The catalog names a producer only when every service names the same one.
        combined = ServiceDiscoveryResult(
            layers=extractable,
            copyright_text=attributions.pop() if len(attributions) == 1 else None,
        )
        discovery = self.discovery_from(source, _service_metadata(source, combined), layers)
        discovery.folder_coverage = coverage
        if not layers:
            failed = (
                f" Discovery failed for {len(errors)}. First error: {errors[0]}" if errors else ""
            )
            discovery.empty_reason = (
                f"The services root lists {len(services)} services with no layer.{failed}"
            )
        return discovery

    @staticmethod
    def discovery_from(
        source: str,
        metadata: MetadataExtracted | None,
        layers: list[LayerRef],
        *,
        license_info: str | None = None,
    ) -> Discovery:
        """Add the catalog metadata to the layers (issue #369).

        The service name titles the catalog and the service description
        describes it. A technical name is kept out of ``init_catalog`` and
        written to ``catalog.json`` only after a new catalog exists.
        """
        from portolan_cli.stac import is_technical_name

        title = None
        with contextlib.suppress(ValueError):
            title = parse_arcgis_url(source).service_name
        description = metadata.description if metadata else None
        seed = _report_metadata_to_arcgis_metadata(metadata).to_extracted() if metadata else None
        return Discovery(
            source_url=source,
            layers=layers,
            metadata=metadata,
            catalog_seed=seed,
            license_url=license_url_from_text(license_info),
            catalog_title=None if is_technical_name(title) else title,
            catalog_description=None if is_technical_name(description) else description,
            root_metadata=(title, description),
        )

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:  # noqa: ARG002 - LayerSource signature
        """Write the layer with ``gpio.extract_arcgis``, then restore its CRS."""
        arcgis_layer: ArcGISLayer = layer.data
        options = self.options
        import inspect

        import geoparquet_io as gpio  # type: ignore[import-untyped]

        # Check which optional parameters gpio.extract_arcgis supports (feature-detected
        # so older gpio versions continue to work without modification).
        sig = inspect.signature(gpio.extract_arcgis)
        kwargs: dict[str, object] = {}
        if "max_workers" in sig.parameters:
            kwargs["max_workers"] = options.workers
        if options.token and "token" in sig.parameters:
            kwargs["token"] = options.token
        # Apply the timeout to the feature-page requests too, not only to discovery.
        # Slow layers with complex geometry time out at gpio's 60s default otherwise,
        # and --timeout appears to do nothing (issue #898).
        if "timeout" in sig.parameters:
            kwargs["timeout"] = options.timeout
        # Keep the service's source CRS instead of the silent WGS84 reprojection that
        # the GeoJSON path forces (issue #802). Older gpio versions without output_crs
        # cannot preserve the CRS, so warn that the output stays WGS84.
        if "output_crs" in sig.parameters:
            kwargs["output_crs"] = options.output_crs
        else:
            logger.warning(
                "Installed geoparquet-io does not support output CRS selection. "
                "Output uses WGS84 (EPSG:4326), not '%s'.",
                options.output_crs,
            )
        try:
            table = gpio.extract_arcgis(arcgis_layer.url, **kwargs)
        except Exception as err:
            # gpio reads the features through DuckDB, whose error names only a
            # temp file. Keep the layer identity with it (issue #866).
            raise ArcGISLayerError(
                f"Layer {layer.name} (id {layer.id}) at {arcgis_layer.url}: "
                f"{type(err).__name__}: {err}"
            ) from err

        # Capture the CRS the extractor detected before the write drops it. gpio.Table
        # tags the native SR on the table but its writer emits crs: null, so a native
        # extraction would declare WGS84 over projected coordinates (issue #802).
        # gpio.Table.crs may return a dict, a string, or None. Only a PROJJSON dict
        # restamps a valid CRS, so ignore the other forms.
        captured_crs = getattr(table, "crs", None)
        source_crs = captured_crs if isinstance(captured_crs, dict) else None

        # The same layout step as `add` and `convert`. It skips a table with
        # no geometry column, such as an ArcGIS Table layer (issue #805).
        table = apply_vector_settings(table, options.vector)

        # Write to parquet
        table.write(str(dest))

        # Restore the source CRS that gpio's writer discarded (issue #802).
        _ensure_geoparquet_crs(dest, source_crs)

        # gpio.Table uses num_rows property instead of __len__
        return int(table.num_rows)

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:  # noqa: ARG002 - LayerSource signature
        """Read the layer's name and description from its REST details."""
        from portolan_cli.extract.arcgis.discovery import fetch_layer_details

        arcgis_layer: ArcGISLayer = layer.data
        name = layer.name
        description = None
        try:
            details = fetch_layer_details(arcgis_layer.service_url, layer.id, self.options.client())
            description = details.get("description")
            name = details.get("name", layer.name)
        except Exception as err:  # noqa: BLE001 - a missing description must not fail the catalog
            logger.debug(
                "Failed to fetch layer details for %s (id=%s) from %s: %s",
                layer.name,
                layer.id,
                arcgis_layer.service_url,
                err,
            )
        return LayerMetadata(
            title=name,
            description=description,
            layer_name=name,
            source_type="arcgis_featureserver",
            source_url=arcgis_layer.url,
        )

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:  # noqa: ARG002 - LayerSource signature
        """Convert the layer's ESRI drawingInfo to a style (issue #490)."""
        from portolan_cli.extract.arcgis.discovery import fetch_layer_details

        if self.options.no_styles or layer.kind == "table":
            return
        arcgis_layer: ArcGISLayer = layer.data
        try:
            # The client sends the token, so a secured layer has a style too.
            layer_json = fetch_layer_details(
                arcgis_layer.service_url, layer.id, self.options.client()
            )
        except ArcGISDiscoveryError as err:
            logger.warning("Could not fetch the ESRI layer JSON for %s: %s", layer.name, err)
            return
        style = extract_esri_style(layer_json, collection_dir, source_layer=slugify(layer.name))
        if style:
            logger.debug("Extracted style for %s: %s", layer.name, style.path)

    def source_link(self, source: str, layer: LayerRef) -> SourceLink:  # noqa: ARG002 - LayerSource signature
        """Link to the layer's REST endpoint (issue #353)."""
        arcgis_layer: ArcGISLayer = layer.data
        return SourceLink(href=arcgis_layer.url, title=f"Source ArcGIS layer: {layer.name}")


def extract_arcgis_catalog(
    url: str,
    output_dir: Path,
    *,
    layer_filter: list[str] | None = None,
    layer_exclude: list[str] | None = None,
    service_filter: list[str] | None = None,
    service_exclude: list[str] | None = None,
    options: ExtractionOptions | None = None,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract layers from an ArcGIS service to a Portolan catalog.

    Args:
        url: ArcGIS FeatureServer, MapServer, or services root URL
        output_dir: Directory to write extracted data
        layer_filter: Glob patterns to include layers (if None, include all)
        layer_exclude: Glob patterns to exclude layers
        service_filter: Glob patterns to include services (for services root URLs)
        service_exclude: Glob patterns to exclude services (for services root URLs)
        options: Extraction options (defaults to ExtractionOptions())
        on_progress: Callback for progress updates

    Returns:
        ExtractionReport with results for all layers

    Raises:
        ValueError: If URL is invalid
        ArcGISDiscoveryError: If service discovery fails
        MissingLicenseError: If no license can be resolved (issue #686).
    """
    options = options or ExtractionOptions()
    if not options.raw:
        options = replace(options, vector=get_vector_settings(output_dir))
    # ``workers`` is gpio's page concurrency inside one layer. Layers run one
    # at a time.
    driver = LayerSourceOptions(
        layer_filter=layer_filter,
        layer_exclude=layer_exclude,
        resume=options.resume,
        dry_run=options.dry_run,
        raw=options.raw,
        retries=options.retries,
        catalog_id=options.catalog_id,
        license=options.license,
        license_url=options.license_url,
    )
    layer_source = ArcGISLayerSource(
        options, service_filter=service_filter, service_exclude=service_exclude
    )
    return run_layer_extraction(layer_source, url, output_dir, driver, on_progress=on_progress)
