"""Extraction report models.

This module defines dataclasses for tracking extraction progress and results:
- LayerResult: Status of individual layer extraction
- ExtractionSummary: Aggregate statistics for the extraction
- MetadataExtracted: Harvested metadata from source service
- ExtractionReport: Complete extraction report

Report files are stored at `.portolan/extraction-report.json`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class LayerResult:
    """Result of extracting a single layer.

    Attributes:
        id: Layer ID (integer from service).
        name: Layer name from service metadata.
        status: Extraction status ("success", "failed", "skipped", "pending").
        features: Number of features extracted (None if failed).
        size_bytes: Output file size in bytes (None if failed).
        duration_seconds: Extraction duration (None if failed).
        output_path: Relative path to output parquet file (None if failed).
        warnings: List of non-fatal warnings during extraction.
        error: Error message if status is "failed" (None otherwise).
        attempts: Number of extraction attempts (including retries).
    """

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
    """Aggregate statistics for an extraction operation.

    Attributes:
        total_layers: Total number of layers in the service.
        succeeded: Number of successfully extracted layers.
        failed: Number of failed layer extractions.
        skipped: Number of skipped layers (e.g., from resume).
        total_features: Total features across all succeeded layers.
        total_size_bytes: Total output size in bytes.
        total_duration_seconds: Total extraction time.
    """

    total_layers: int
    succeeded: int
    failed: int
    skipped: int
    total_features: int
    total_size_bytes: int
    total_duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "total_layers": self.total_layers,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped": self.skipped,
            "total_features": self.total_features,
            "total_size_bytes": self.total_size_bytes,
            "total_duration_seconds": self.total_duration_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExtractionSummary:
        """Create ExtractionSummary from dict."""
        return cls(
            total_layers=data["total_layers"],
            succeeded=data["succeeded"],
            failed=data["failed"],
            skipped=data["skipped"],
            total_features=data["total_features"],
            total_size_bytes=data["total_size_bytes"],
            total_duration_seconds=data["total_duration_seconds"],
        )


@dataclass
class MetadataExtracted:
    """Metadata harvested from source service.

    Captures what was found in the service metadata. Null values indicate
    the field was checked but not present in the source.

    Attributes:
        source_url: The service URL that was extracted.
        description: Service description.
        attribution: Copyright/attribution text from service.
        keywords: Keywords from service metadata.
        contact_name: Contact name or author.
        processing_notes: Additional processing notes.
        known_issues: Access information or caveats.
        license_info_raw: Raw license text (not SPDX, for human review).
    """

    source_url: str
    description: str | None
    attribution: str | None
    keywords: list[str] | None
    contact_name: str | None
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
            "contact_name": self.contact_name,
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
            contact_name=data.get("contact_name"),
            processing_notes=data.get("processing_notes"),
            known_issues=data.get("known_issues"),
            license_info_raw=data.get("license_info_raw"),
        )


@dataclass
class ExtractionReport:
    """Complete extraction report for a service.

    This is the top-level report that gets written to
    `.portolan/extraction-report.json`.

    Attributes:
        extraction_date: ISO 8601 timestamp of extraction.
        source_url: The service URL that was extracted.
        portolan_version: Version of Portolan CLI used.
        gpio_version: Version of geoparquet-io used.
        metadata_extracted: Harvested service metadata.
        layers: Results for each layer extraction attempt.
        summary: Aggregate statistics.
    """

    extraction_date: str
    source_url: str
    portolan_version: str
    gpio_version: str
    metadata_extracted: MetadataExtracted
    layers: list[LayerResult]
    summary: ExtractionSummary

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "extraction_date": self.extraction_date,
            "source_url": self.source_url,
            "portolan_version": self.portolan_version,
            "gpio_version": self.gpio_version,
            "metadata_extracted": self.metadata_extracted.to_dict(),
            "layers": [layer.to_dict() for layer in self.layers],
            "summary": self.summary.to_dict(),
        }

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
        )


def save_report(report: ExtractionReport, path: Path) -> None:
    """Save extraction report to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2))


def load_report(path: Path) -> ExtractionReport:
    """Load extraction report from JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Report not found: {path}")

    data = json.loads(path.read_text())
    return ExtractionReport.from_dict(data)
