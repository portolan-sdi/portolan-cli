"""Builders for extraction tests (#947).

Reports come from the shared ``build_report``, so a test builds the same
report shape that ``run_layer_extraction`` writes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.extract.common.source import Discovery, build_report
from portolan_cli.extract.wfs.discovery import LayerInfo, WFSDiscoveryResult

if TYPE_CHECKING:
    from portolan_cli.extract.common.report import (
        ExtractionReport,
        LayerResult,
        MetadataExtracted,
    )


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
        contact_name="Test Contact",
        access_constraints=None,
        fees=None,
    )
