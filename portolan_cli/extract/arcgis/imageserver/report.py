"""Extraction report models for ImageServer raster extraction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from portolan_cli.utils import redact_credentials
from portolan_core.json_io import write_json_atomic

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerMetadata
    from portolan_cli.metadata_extraction import ExtractedMetadata


@dataclass
class TileResult:
    """Result of extracting a single tile from ImageServer."""

    tile_id: str
    status: str
    size_bytes: int | None
    duration_seconds: float | None
    output_path: str | None
    error: str | None
    attempts: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "tile_id": self.tile_id,
            "status": self.status,
            "attempts": self.attempts,
        }

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
    def from_dict(cls, data: dict[str, Any]) -> TileResult:
        """Create TileResult from dict."""
        return cls(
            tile_id=data["tile_id"],
            status=data["status"],
            size_bytes=data.get("size_bytes"),
            duration_seconds=data.get("duration_seconds"),
            output_path=data.get("output_path"),
            error=data.get("error"),
            attempts=data.get("attempts", 1),
        )


@dataclass
class ImageServerExtractionSummary:
    """Aggregate statistics for an ImageServer extraction."""

    total_tiles: int
    succeeded: int
    failed: int
    skipped: int
    total_size_bytes: int
    total_duration_seconds: float
    empty: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "total_tiles": self.total_tiles,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped": self.skipped,
            "empty": self.empty,
            "total_size_bytes": self.total_size_bytes,
            "total_duration_seconds": self.total_duration_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ImageServerExtractionSummary:
        """Create ImageServerExtractionSummary from dict."""
        return cls(
            total_tiles=data["total_tiles"],
            succeeded=data["succeeded"],
            failed=data["failed"],
            skipped=data["skipped"],
            empty=data.get("empty", 0),
            total_size_bytes=data["total_size_bytes"],
            total_duration_seconds=data["total_duration_seconds"],
        )


@dataclass
class ImageServerMetadataExtracted:
    """Metadata harvested from ImageServer service."""

    source_url: str
    service_name: str
    description: str | None
    copyright_text: str | None
    pixel_type: str
    band_count: int
    spatial_reference_wkid: int | None
    pixel_size_x: float
    pixel_size_y: float
    extent_bbox: list[float]
    # FeatureServer-parity fields
    service_description: str | None = None
    author: str | None = None
    keywords: list[str] | None = None
    license_info: str | None = None
    access_information: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "source_url": self.source_url,
            "service_name": self.service_name,
            "description": self.description,
            "copyright_text": self.copyright_text,
            "pixel_type": self.pixel_type,
            "band_count": self.band_count,
            "spatial_reference_wkid": self.spatial_reference_wkid,
            "pixel_size_x": self.pixel_size_x,
            "pixel_size_y": self.pixel_size_y,
            "extent_bbox": self.extent_bbox,
        }
        # Include FeatureServer-parity fields if present
        if self.service_description is not None:
            result["service_description"] = self.service_description
        if self.author is not None:
            result["author"] = self.author
        if self.keywords is not None:
            result["keywords"] = self.keywords
        if self.license_info is not None:
            result["license_info"] = self.license_info
        if self.access_information is not None:
            result["access_information"] = self.access_information
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ImageServerMetadataExtracted:
        """Create ImageServerMetadataExtracted from dict."""
        return cls(
            source_url=data["source_url"],
            service_name=data["service_name"],
            description=data.get("description"),
            copyright_text=data.get("copyright_text"),
            pixel_type=data["pixel_type"],
            band_count=data["band_count"],
            spatial_reference_wkid=data.get("spatial_reference_wkid"),
            pixel_size_x=data["pixel_size_x"],
            pixel_size_y=data["pixel_size_y"],
            extent_bbox=data["extent_bbox"],
            # FeatureServer-parity fields
            service_description=data.get("service_description"),
            author=data.get("author"),
            keywords=data.get("keywords"),
            license_info=data.get("license_info"),
            access_information=data.get("access_information"),
        )

    @classmethod
    def from_service_metadata(
        cls,
        metadata: ImageServerMetadata,
        source_url: str,
    ) -> ImageServerMetadataExtracted:
        """Create from ImageServerMetadata discovery result."""
        extent = metadata.full_extent
        extent_bbox = [
            extent["xmin"],
            extent["ymin"],
            extent["xmax"],
            extent["ymax"],
        ]

        # Extract spatial reference from full_extent
        sr = extent.get("spatialReference", {})
        wkid = sr.get("latestWkid") or sr.get("wkid")

        return cls(
            source_url=source_url,
            service_name=metadata.name,
            description=metadata.description,
            copyright_text=metadata.copyright_text,
            pixel_type=metadata.pixel_type,
            band_count=metadata.band_count,
            spatial_reference_wkid=wkid,
            pixel_size_x=metadata.pixel_size_x,
            pixel_size_y=metadata.pixel_size_y,
            extent_bbox=extent_bbox,
            # FeatureServer-parity fields
            service_description=metadata.service_description,
            author=metadata.author,
            keywords=metadata.keywords,
            license_info=metadata.license_info,
            access_information=metadata.access_information,
        )

    def to_extracted(self) -> ExtractedMetadata:
        """Convert to common ExtractedMetadata for seeding."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_core.licensing import license_url_from_text

        # Build processing_notes with service description and technical specs
        processing_notes_parts: list[str] = []

        if self.service_description:
            processing_notes_parts.append(self.service_description)

        # Add raster technical specifications
        tech_specs = [
            f"pixel_type: {self.pixel_type}",
            f"band_count: {self.band_count}",
            f"pixel_size: {self.pixel_size_x}x{self.pixel_size_y}",
        ]
        if self.spatial_reference_wkid:
            tech_specs.append(f"spatial_reference: EPSG:{self.spatial_reference_wkid}")

        processing_notes_parts.append("Technical specs: " + ", ".join(tech_specs))

        return ExtractedMetadata(
            source_url=self.source_url,
            source_type="arcgis_imageserver",
            description=self.description,
            attribution=self.copyright_text,
            keywords=self.keywords,
            producer=self.author,
            processing_notes="\n".join(processing_notes_parts),
            known_issues=self.access_information,
            license_raw=self.license_info,
            license_url=license_url_from_text(self.license_info),
        )


@dataclass
class ImageServerExtractionReport:
    """Complete extraction report for an ImageServer service."""

    extraction_type: str  # Always "imageserver"
    extraction_date: str
    source_url: str
    portolan_version: str
    riocogeo_version: str
    metadata_extracted: ImageServerMetadataExtracted
    tiles: list[TileResult]
    summary: ImageServerExtractionSummary

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "extraction_type": self.extraction_type,
            "extraction_date": self.extraction_date,
            "source_url": self.source_url,
            "portolan_version": self.portolan_version,
            "riocogeo_version": self.riocogeo_version,
            "metadata_extracted": self.metadata_extracted.to_dict(),
            "tiles": [tile.to_dict() for tile in self.tiles],
            "summary": self.summary.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ImageServerExtractionReport:
        """Create ImageServerExtractionReport from dict."""
        return cls(
            extraction_type=data.get("extraction_type", "imageserver"),
            extraction_date=data["extraction_date"],
            source_url=data["source_url"],
            portolan_version=data["portolan_version"],
            riocogeo_version=data["riocogeo_version"],
            metadata_extracted=ImageServerMetadataExtracted.from_dict(data["metadata_extracted"]),
            tiles=[TileResult.from_dict(t) for t in data["tiles"]],
            summary=ImageServerExtractionSummary.from_dict(data["summary"]),
        )


def build_imageserver_report(
    url: str,
    metadata: ImageServerMetadata,
    tile_results: list[TileResult],
    total_duration: float,
) -> ImageServerExtractionReport:
    """Build a complete ImageServerExtractionReport."""
    # The report goes to a file and into reprs, so it never holds a credential.
    url = redact_credentials(url)
    # Get versions
    try:
        from importlib.metadata import version

        portolan_version = version("portolan-cli")
    except Exception:
        portolan_version = "unknown"

    try:
        from importlib.metadata import version

        riocogeo_version = version("rio-cogeo")
    except Exception:
        riocogeo_version = "unknown"

    # Build summary
    succeeded = sum(1 for r in tile_results if r.status == "success")
    failed = sum(1 for r in tile_results if r.status == "failed")
    skipped = sum(1 for r in tile_results if r.status == "skipped")
    empty = sum(1 for r in tile_results if r.status == "empty")

    summary = ImageServerExtractionSummary(
        total_tiles=len(tile_results),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        empty=empty,
        total_size_bytes=sum(r.size_bytes or 0 for r in tile_results),
        total_duration_seconds=total_duration,
    )

    # Build metadata extracted
    metadata_extracted = ImageServerMetadataExtracted.from_service_metadata(metadata, url)

    return ImageServerExtractionReport(
        extraction_type="imageserver",
        extraction_date=datetime.now(UTC).isoformat(),
        source_url=url,
        portolan_version=portolan_version,
        riocogeo_version=riocogeo_version,
        metadata_extracted=metadata_extracted,
        tiles=tile_results,
        summary=summary,
    )


def save_imageserver_report(report: ImageServerExtractionReport, path: Path) -> None:
    """Save ImageServer extraction report to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, report.to_dict())
