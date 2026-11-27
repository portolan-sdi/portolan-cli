"""Carto metadata extraction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portolan_cli.metadata_extraction import ExtractedMetadata


@dataclass
class CartoMetadata:
    """Metadata extracted from a Carto account."""

    source_url: str
    account_name: str | None = None

    def to_extracted(self) -> ExtractedMetadata:
        """Convert to the common ExtractedMetadata format for metadata.yaml seeding."""
        from portolan_cli.metadata_extraction import ExtractedMetadata

        attribution = f"Carto account: {self.account_name}" if self.account_name else None
        return ExtractedMetadata(
            source_type="carto",
            source_url=self.source_url,
            description=None,
            attribution=attribution,
            keywords=None,
            processing_notes=None,
            known_issues=None,
            license_raw=None,
        )


def extract_carto_metadata(source_url: str, account_name: str | None = None) -> CartoMetadata:
    """Build CartoMetadata from a source URL and optional account name."""
    return CartoMetadata(source_url=source_url, account_name=account_name)
