"""Builders for extraction tests (#947).

Reports come from the shared ``build_report``, so a test builds the same
report shape that ``run_layer_extraction`` writes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.extract.arcgis.discovery import LayerInfo as ArcGISLayerInfo
from portolan_cli.extract.arcgis.orchestrator import ArcGISLayer, ArcGISLayerSource
from portolan_cli.extract.common.source import (
    Discovery,
    LayerRef,
    build_catalog,
    build_report,
)
from portolan_cli.extract.wfs.discovery import LayerInfo, WFSDiscoveryResult

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.extract.common.report import (
        ExtractionReport,
        LayerResult,
        MetadataExtracted,
    )
    from portolan_cli.licensing import ResolvedLicense


def make_report(
    layers: list[LayerResult],
    source_url: str = "https://example.com/wfs",
    *,
    metadata: MetadataExtracted | None = None,
) -> ExtractionReport:
    """Build an extraction report for ``layers``.

    Args:
        layers: Layer results in report order.
        source_url: The report's source URL.
        metadata: Service metadata. None records the source URL only.

    Returns:
        The report with its summary.
    """
    return build_report(Discovery(source_url=source_url, layers=[], metadata=metadata), layers)


def make_layer_result(
    *,
    layer_id: int = 0,
    name: str = "test_layer",
    status: str = "success",
    output_path: str | None = "test_layer/test_layer.parquet",
    features: int | None = 100,
    size_bytes: int | None = 1000,
    error: str | None = None,
) -> LayerResult:
    """Build a layer result. Counts and duration apply only to a success."""
    from portolan_cli.extract.common.report import LayerResult

    return LayerResult(
        id=layer_id,
        name=name,
        status=status,
        output_path=output_path,
        features=features if status == "success" else None,
        size_bytes=size_bytes if status == "success" else None,
        duration_seconds=1.0 if status == "success" else None,
        warnings=[],
        error=error,
        attempts=1 if status != "failed" else 3,
    )


def make_wfs_layer(name: str, layer_id: int = 0) -> LayerInfo:
    """Build a WFS LayerInfo whose typename is its name."""
    return LayerInfo(
        name=name,
        typename=name,
        title=name.title(),
        abstract=f"Description of {name}",
        bbox=None,
        id=layer_id,
    )


def make_wfs_discovery(
    layers: list[LayerInfo],
    service_url: str = "https://example.com/wfs",
) -> WFSDiscoveryResult:
    """Build a WFS GetCapabilities result with test service metadata."""
    return WFSDiscoveryResult(
        service_url=service_url,
        layers=layers,
        service_title="Test WFS Service",
        service_abstract="A test WFS service",
        provider="Test Provider",
        keywords=["test", "wfs"],
        access_constraints=None,
        fees=None,
    )


def build_arcgis_catalog(
    output_dir: Path,
    report: ExtractionReport,
    resolved_license: ResolvedLicense | None = None,
    catalog_id: str | None = None,
) -> None:
    """Build the catalog for an ArcGIS ``report`` through the shared driver.

    Each layer comes from the report. Its collection is the parent directory
    of its output path, and its service URL is the report's source URL.
    """
    from pathlib import PurePath

    layers = [
        LayerRef(
            id=r.id,
            name=r.name,
            path=PurePath(r.output_path).parent.as_posix() if r.output_path else None,
            data=ArcGISLayer(
                ArcGISLayerInfo(id=r.id, name=r.name, layer_type="Feature Layer"),
                report.source_url,
            ),
        )
        for r in report.layers
    ]
    discovery = ArcGISLayerSource.discovery_from(
        report.source_url, report.metadata_extracted, layers
    )
    build_catalog(
        ArcGISLayerSource(),
        discovery,
        output_dir,
        report,
        resolved_license=resolved_license,
        catalog_id=catalog_id,
    )
