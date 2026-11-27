"""Extraction report models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_core.json_io import write_json_atomic

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass
class LayerResult:
    """Result of extracting a single layer."""

    id: int
    name: str
    status: str
    features: int | None
    size_bytes: int | None
    duration_seconds: float | None
    output_path: str | None
    warnings: list[str]
    error: str | None
    attempts: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "warnings": self.warnings,
            "attempts": self.attempts,
        }

        if self.features is not None:
            result["features"] = self.features
        if self.size_bytes is not None:
            result["size_bytes"] = self.size_bytes
        if self.duration_seconds is not None:
            result["duration_seconds"] = self.duration_seconds
        if self.output_path is not None:
            result["output_path"] = self.output_path
        if self.error is not None:
            result["error"] = self.error

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LayerResult:
        """Create LayerResult from dict."""
        return cls(
            id=data["id"],
            name=data["name"],
            status=data["status"],
            features=data.get("features"),
            size_bytes=data.get("size_bytes"),
            duration_seconds=data.get("duration_seconds"),
            output_path=data.get("output_path"),
            warnings=data.get("warnings", []),
            error=data.get("error"),
            attempts=data.get("attempts", 1),
        )


@dataclass
class ExtractionSummary:
    """Aggregate statistics for an extraction operation."""

    total_layers: int
    succeeded: int
    failed: int
    skipped: int
    empty: int
    total_features: int
    total_size_bytes: int
    total_duration_seconds: float
    wall_duration_seconds: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "total_layers": self.total_layers,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped": self.skipped,
            "empty": self.empty,
            "total_features": self.total_features,
            "total_size_bytes": self.total_size_bytes,
            "total_duration_seconds": self.total_duration_seconds,
        }
        if self.wall_duration_seconds is not None:
            result["wall_duration_seconds"] = self.wall_duration_seconds
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExtractionSummary:
        """Create ExtractionSummary from dict."""
        return cls(
            total_layers=data["total_layers"],
            succeeded=data["succeeded"],
            failed=data["failed"],
            skipped=data["skipped"],
            empty=data.get("empty", 0),  # Backward compatible default
            total_features=data["total_features"],
            total_size_bytes=data["total_size_bytes"],
            total_duration_seconds=data["total_duration_seconds"],
            wall_duration_seconds=data.get("wall_duration_seconds"),
        )


@dataclass
class MetadataExtracted:
    """Metadata harvested from source service."""

    source_url: str
    description: str | None
    attribution: str | None
    keywords: list[str] | None
    producer: str | None
    processing_notes: str | None
    known_issues: str | None
    license_info_raw: str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "source_url": self.source_url,
            "description": self.description,
            "attribution": self.attribution,
            "keywords": self.keywords,
            "producer": self.producer,
            "processing_notes": self.processing_notes,
            "known_issues": self.known_issues,
            "license_info_raw": self.license_info_raw,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MetadataExtracted:
        """Create MetadataExtracted from dict."""
        return cls(
            source_url=data["source_url"],
            description=data.get("description"),
            attribution=data.get("attribution"),
            keywords=data.get("keywords"),
            producer=data.get("producer"),
            processing_notes=data.get("processing_notes"),
            known_issues=data.get("known_issues"),
            license_info_raw=data.get("license_info_raw"),
        )


@dataclass
class FolderCoverage:
    """Coverage of a recursive services-root traversal."""

    folders_visited: list[str]
    folders_skipped: list[tuple[str, str]]
    services_found: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "folders_visited": self.folders_visited,
            "folders_skipped": [{"folder": f, "reason": r} for f, r in self.folders_skipped],
            "services_found": self.services_found,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FolderCoverage:
        """Create FolderCoverage from dict."""
        return cls(
            folders_visited=data.get("folders_visited", []),
            folders_skipped=[(s["folder"], s["reason"]) for s in data.get("folders_skipped", [])],
            services_found=data.get("services_found", 0),
        )


@dataclass
class ExtractionReport:
    """Complete extraction report for a service.

    Attributes:
        extraction_date: ISO 8601 timestamp of extraction.
        source_url: The service URL that was extracted.
        portolan_version: Version of Portolan CLI used.
        gpio_version: Version of geoparquet-io used.
        metadata_extracted: Harvested service metadata.
        layers: Results for each layer extraction attempt.
        summary: Aggregate statistics.
        folder_coverage: Optional coverage data from recursive folder traversal.
    """

    extraction_date: str
    source_url: str
    portolan_version: str
    gpio_version: str
    metadata_extracted: MetadataExtracted
    layers: list[LayerResult]
    summary: ExtractionSummary
    folder_coverage: FolderCoverage | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "extraction_date": self.extraction_date,
            "source_url": self.source_url,
            "portolan_version": self.portolan_version,
            "gpio_version": self.gpio_version,
            "metadata_extracted": self.metadata_extracted.to_dict(),
            "layers": [layer.to_dict() for layer in self.layers],
            "summary": self.summary.to_dict(),
        }
        if self.folder_coverage is not None:
            result["folder_coverage"] = self.folder_coverage.to_dict()
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExtractionReport:
        """Create ExtractionReport from dict."""
        return cls(
            extraction_date=data["extraction_date"],
            source_url=data["source_url"],
            portolan_version=data["portolan_version"],
            gpio_version=data["gpio_version"],
            metadata_extracted=MetadataExtracted.from_dict(data["metadata_extracted"]),
            layers=[LayerResult.from_dict(layer) for layer in data["layers"]],
            summary=ExtractionSummary.from_dict(data["summary"]),
            folder_coverage=(
                FolderCoverage.from_dict(data["folder_coverage"])
                if data.get("folder_coverage")
                else None
            ),
        )


@dataclass(frozen=True)
class ExtractRunResult:
    """One run of ``extract arcgis``, ``extract carto``, ``extract wfs``, or a plugin source."""

    report: ExtractionReport
    output_dir: Path

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of the command."""
        summary = self.report.summary
        data: dict[str, Any] = {
            "source_url": self.report.source_url,
            "output_dir": str(self.output_dir),
            "summary": {
                "total_layers": summary.total_layers,
                "succeeded": summary.succeeded,
                "failed": summary.failed,
                "skipped": summary.skipped,
                "empty": summary.empty,
                "total_features": summary.total_features,
                "total_size_bytes": summary.total_size_bytes,
            },
            "layers": [
                {
                    "id": layer.id,
                    "name": layer.name,
                    "status": layer.status,
                    "features": layer.features,
                    "output_path": layer.output_path,
                    "error": layer.error,
                }
                for layer in self.report.layers
            ],
        }
        if self.report.folder_coverage is not None:
            data["folder_coverage"] = self.report.folder_coverage.to_dict()
        return data


def run_extract_command(
    extract: Callable[..., ExtractionReport],
    url: str,
    output_dir: Path,
    options: Any,
    **arguments: Any,
) -> ExtractRunResult:
    """Run ``portolan extract carto``, ``extract wfs``, or the layer mode of ``extract arcgis``."""
    from portolan_cli.input_hardening import validate_catalog_id

    if options.catalog_id is not None:
        validate_catalog_id(options.catalog_id)
    report = extract(url=url, output_dir=output_dir, options=options, **arguments)
    return ExtractRunResult(report, output_dir)


def save_report(report: ExtractionReport, path: Path) -> None:
    """Save extraction report to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, report.to_dict())


def load_report(path: Path) -> ExtractionReport:
    """Load extraction report from JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Report not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    return ExtractionReport.from_dict(data)
