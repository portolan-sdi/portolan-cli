"""Models for CSW/ISO 19139 metadata.

This module defines dataclasses for representing metadata extracted from
ISO 19139 XML records, typically fetched via CSW (Catalog Service for Web).
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portolan_cli.metadata_extraction import ExtractedMetadata


# Known license URL patterns mapped to SPDX identifiers (all lowercase for matching)
LICENSE_URL_TO_SPDX: dict[str, str] = {
    "creativecommons.org/licenses/by/4.0": "CC-BY-4.0",
    "creativecommons.org/licenses/by/3.0": "CC-BY-3.0",
    "creativecommons.org/licenses/by/2.0": "CC-BY-2.0",
    "creativecommons.org/licenses/by-sa/4.0": "CC-BY-SA-4.0",
    "creativecommons.org/licenses/by-sa/3.0": "CC-BY-SA-3.0",
    "creativecommons.org/licenses/by-nc/4.0": "CC-BY-NC-4.0",
    "creativecommons.org/licenses/by-nc/3.0": "CC-BY-NC-3.0",
    "creativecommons.org/licenses/by-nd/4.0": "CC-BY-ND-4.0",
    "creativecommons.org/licenses/by-nc-sa/4.0": "CC-BY-NC-SA-4.0",
    "creativecommons.org/licenses/by-nc-nd/4.0": "CC-BY-NC-ND-4.0",
    "creativecommons.org/publicdomain/zero/1.0": "CC0-1.0",
    "creativecommons.org/publicdomain/mark/1.0": "CC-PDDC",
    "opensource.org/licenses/mit": "MIT",
    "opensource.org/licenses/apache-2.0": "Apache-2.0",
    "opendatacommons.org/licenses/odbl": "ODbL-1.0",
    "opendatacommons.org/licenses/by": "ODC-By-1.0",
    "opendatacommons.org/licenses/pddl": "PDDL-1.0",
}


@dataclass
class ISOMetadata:
    """Metadata extracted from an ISO 19139 record.

    Represents the key fields from ISO 19115/19139 metadata that are useful
    for seeding Portolan metadata.yaml files.

    Attributes:
        file_identifier: Unique identifier for the metadata record.
        title: Dataset title.
        abstract: Dataset description/abstract.
        keywords: List of keywords from all thesauri.
        contact_organization: Primary contact organization name.
        contact_email: Primary contact email address.
        license_url: URL to license (e.g., Creative Commons).
        license_text: Human-readable license text.
        access_constraints: Access constraint description.
        lineage: Data lineage/provenance statement.
        thumbnail_url: URL to preview image.
        scale_denominator: Representative scale (e.g., 10000 for 1:10000).
        topic_category: ISO topic category code.
        maintenance_frequency: Update frequency code.
        date_created: Creation date (YYYY-MM-DD).
        date_revised: Last revision date.
        date_published: Publication date.
    """

    file_identifier: str
    title: str
    abstract: str | None = None
    keywords: list[str] | None = None
    contact_organization: str | None = None
    contact_email: str | None = None
    license_url: str | None = None
    license_text: str | None = None
    access_constraints: str | None = None
    lineage: str | None = None
    thumbnail_url: str | None = None
    scale_denominator: int | None = None
    topic_category: str | None = None
    maintenance_frequency: str | None = None
    date_created: str | None = None
    date_revised: str | None = None
    date_published: str | None = None

    def get_license_spdx(self) -> str | None:
        """Extract SPDX license identifier from license URL.

        Returns:
            SPDX identifier (e.g., "CC-BY-4.0") or None if unrecognized.
        """
        if not self.license_url:
            return None

        # Normalize URL for matching
        url_lower = self.license_url.lower()

        for pattern, spdx in LICENSE_URL_TO_SPDX.items():
            if pattern in url_lower:
                return spdx

        return None

    def has_useful_metadata(self) -> bool:
        """Check if this metadata has substantial content beyond identifier/title.

        Requires at least one "strong" field (abstract, license, contact, lineage)
        OR sufficient keywords (3+) to be considered useful.

        Returns:
            True if metadata contains useful fields beyond the basics.
        """
        strong_fields = [
            self.abstract,
            self.license_url,
            self.contact_organization,
            self.contact_email,
            self.lineage,
        ]

        has_strong = any(strong_fields)
        has_sufficient_keywords = bool(self.keywords and len(self.keywords) >= 3)

        return has_strong or has_sufficient_keywords

    def to_extracted_metadata(self, source_url: str) -> ExtractedMetadata:
        """Convert to ExtractedMetadata for use with metadata seeding.

        Args:
            source_url: The original source URL (WFS, etc.) for provenance.

        Returns:
            ExtractedMetadata compatible with seed_metadata_yaml.
        """
        from portolan_cli.metadata_extraction import ExtractedMetadata

        # Build license info string
        license_parts = []
        if self.license_text:
            license_parts.append(self.license_text)
        if self.license_url:
            spdx = self.get_license_spdx()
            if spdx:
                license_parts.append(f"SPDX: {spdx}")
            license_parts.append(self.license_url)
        if self.access_constraints:
            license_parts.append(f"Access: {self.access_constraints}")

        license_raw = "; ".join(license_parts) if license_parts else None

        # Build processing notes from lineage (truncate on word boundary)
        processing_notes = None
        if self.lineage:
            processing_notes = textwrap.shorten(self.lineage, width=500, placeholder="...")

        return ExtractedMetadata(
            source_type="csw_iso19139",
            source_url=source_url,
            description=self.abstract or self.title,
            attribution=self.contact_organization,
            keywords=self.keywords,
            contact_name=self.contact_organization,
            contact_email=self.contact_email,
            processing_notes=processing_notes,
            known_issues=None,
            license_raw=license_raw,
        )
